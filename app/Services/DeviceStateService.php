<?php

namespace App\Services;

use App\Models\User;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Redis;

class DeviceStateService
{
    private const PREFIX = 'user_devices:';
    private const TTL = 300;                     // device state ttl
    private const DB_THROTTLE = 10;             // update db throttle

    /**
     * 移除 Redis key 的前缀
     */
    private function removeRedisPrefix(string $key): string
    {
        $prefix = config('database.redis.options.prefix', '');
        return $prefix ? substr($key, strlen($prefix)) : $key;
    }

    /**
     * 批量设置设备
     * 用于 HTTP /alive 和 WebSocket report.devices
     */
    public function setDevices(int $userId, int $nodeId, array $ips): void
    {
        $key = self::PREFIX . $userId;
        $timestamp = time();

        $this->removeNodeDevices($nodeId, $userId);

        // Normalize: strip port suffix and deduplicate
        $ips = array_values(array_unique(array_map([self::class, 'normalizeIP'], $ips)));

        if (!empty($ips)) {
            $fields = [];
            foreach ($ips as $ip) {
                $fields["{$nodeId}:{$ip}"] = $timestamp;
            }
            Redis::hMset($key, $fields);
            Redis::expire($key, self::TTL);
        }

        $this->notifyUpdate($userId);
    }

    /**
     * 获取某节点的所有设备数据
     * 返回: {userId: [ip1, ip2, ...], ...}
     *
     * 关键：必须用 SCAN 迭代代替 KEYS('*')，后者在 Redis 里是 O(N) 单线程阻塞命令。
     * 16k+ 用户 + 多节点高频上报时会把整个 Redis 卡住 (影响队列/缓存/会话)。
     */
    public function getNodeDevices(int $nodeId): array
    {
        $prefix = "{$nodeId}:";
        $result = [];

        foreach ($this->scanUserDeviceKeys() as $actualKey) {
            $uid = (int) substr($actualKey, strlen(self::PREFIX));
            $data = Redis::hgetall($actualKey);
            foreach ($data as $field => $timestamp) {
                if (str_starts_with($field, $prefix)) {
                    $ip = substr($field, strlen($prefix));
                    $result[$uid][] = $ip;
                }
            }
        }

        return $result;
    }

    /**
     * 用 SCAN 迭代 user_devices:* 的所有 key，yield 去掉 Redis 前缀后的原始 key。
     *
     * phpredis 和 predis 的 SCAN 行为略不同：phpredis 的 Redis::scan 需要传
     * &$cursor 按引用，predis 的 client->scan 返回 [cursor, keys]。
     * 这里用 Redis::command('SCAN', ...) 兜底，兼容两套客户端。
     */
    private function scanUserDeviceKeys(): \Generator
    {
        $configuredPrefix = (string) config('database.redis.options.prefix', '');
        $pattern = $configuredPrefix . self::PREFIX . '*';
        $cursor = '0';

        do {
            // Laravel 封装：phpredis/predis 都接受 ['match'=>..., 'count'=>...]
            // 返回 [cursor, keys]；phpredis 遇到空批时 keys 可能是 false
            try {
                $resp = Redis::scan($cursor, ['match' => $pattern, 'count' => 500]);
            } catch (\Throwable $e) {
                break;
            }
            if (!is_array($resp) || count($resp) < 2) {
                break;
            }
            [$cursor, $keys] = $resp;

            if (is_array($keys) && !empty($keys)) {
                foreach ($keys as $key) {
                    yield $this->removeRedisPrefix($key);
                }
            }
        } while ((string) $cursor !== '0');
    }

    /**
     * 删除某节点某用户的设备
     */
    public function removeNodeDevices(int $nodeId, int $userId): void
    {
        $key = self::PREFIX . $userId;
        $prefix = "{$nodeId}:";

        foreach (Redis::hkeys($key) as $field) {
            if (str_starts_with($field, $prefix)) {
                Redis::hdel($key, $field);
            }
        }
    }

    /**
     * 清除节点所有设备数据（用于节点断开连接）
     */
    public function clearAllNodeDevices(int $nodeId): array
    {
        $oldDevices = $this->getNodeDevices($nodeId);
        $prefix = "{$nodeId}:";

        foreach ($oldDevices as $userId => $ips) {
            $key = self::PREFIX . $userId;
            foreach (Redis::hkeys($key) as $field) {
                if (str_starts_with($field, $prefix)) {
                    Redis::hdel($key, $field);
                }
            }
            $this->notifyUpdate($userId);
        }

        return array_keys($oldDevices);
    }

    /**
     * get user device count (deduplicated by IP, filter expired data)
     */
    public function getDeviceCount(int $userId): int
    {
        $data = Redis::hgetall(self::PREFIX . $userId);
        $now = time();
        $ips = [];

        foreach ($data as $field => $timestamp) {
            if ($now - $timestamp <= self::TTL) {
                $ips[] = substr($field, strpos($field, ':') + 1);
            }
        }

        return count(array_unique($ips));
    }

    /**
     * get user device count (for alivelist interface)
     */
    public function getAliveList(Collection $users): array
    {
        if ($users->isEmpty()) {
            return [];
        }

        $result = [];
        foreach ($users as $user) {
            $count = $this->getDeviceCount($user->id);
            if ($count > 0) {
                $result[$user->id] = $count;
            }
        }

        return $result;
    }

    /**
     * get devices of multiple users (for sync.devices, filter expired data)
     */
    public function getUsersDevices(array $userIds): array
    {
        $result = [];
        $now = time();
        foreach ($userIds as $userId) {
            $data = Redis::hgetall(self::PREFIX . $userId);
            if (!empty($data)) {
                $ips = [];
                foreach ($data as $field => $timestamp) {
                    if ($now - $timestamp <= self::TTL) {
                        $ips[] = substr($field, strpos($field, ':') + 1);
                    }
                }
                if (!empty($ips)) {
                    $result[$userId] = array_unique($ips);
                }
            }
        }

        return $result;
    }

    /**
     * Strip port from IP address: "1.2.3.4:12345" → "1.2.3.4", "[::1]:443" → "::1"
     */
    private static function normalizeIP(string $ip): string
    {
        // [IPv6]:port
        if (preg_match('/^\[(.+)\]:\d+$/', $ip, $m)) {
            return $m[1];
        }
        // IPv4:port
        if (preg_match('/^(\d+\.\d+\.\d+\.\d+):\d+$/', $ip, $m)) {
            return $m[1];
        }
        return $ip;
    }

    /**
     * notify update (throttle control)
     *
     * 关键：必须保留 throttle。每个 WS report.devices 都调用这里，
     * 没有节流的话等于每秒 N 个节点 * M 个用户 = 数千次 UPDATE v2_user，
     * 直接把 MySQL 主库打到 IO 瓶颈（老版本线上队列积压的另一个源头）。
     *
     * 原代码用的 setnx+expire 有"setnx 成功但 expire 前进程崩"的竞争窗口，
     * 这里换成 SET NX EX 原子命令。
     */
    public function notifyUpdate(int $userId): void
    {
        $dbThrottleKey = "device:db_throttle:{$userId}";

        $acquired = Redis::set($dbThrottleKey, 1, 'EX', self::DB_THROTTLE, 'NX');
        if (!$acquired) {
            return;
        }

        User::query()
            ->whereKey($userId)
            ->update([
                'online_count' => $this->getDeviceCount($userId),
                'last_online_at' => now(),
            ]);
    }
}
