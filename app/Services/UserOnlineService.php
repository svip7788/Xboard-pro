<?php

namespace App\Services;

use App\Models\Server;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Redis;

/**
 * 兼容层 shim。
 *
 * 老 UserOnlineService 在设备限制重构（commit 420521d）中被 DeviceStateService 取代，
 * 但仍有插件（如 OnlineDevices）硬编码引用这个类。保留它以免插件 500，
 * 实现直接基于 DeviceStateService 使用的 Redis hash: user_devices:{userId}。
 *
 * 行为尽量贴合老接口：返回 ['devices' => [ ['ip','last_seen','node_type'], ... ]]，
 * node_type 用 "{serverType}{nodeId}" 兼容原解析逻辑。
 */
class UserOnlineService
{
    private const PREFIX = 'user_devices:';
    private const TTL = 300;

    public static function getUserDevices(int $userId): array
    {
        $data = Redis::hgetall(self::PREFIX . $userId);
        $now = time();
        $devices = [];

        foreach ($data as $field => $timestamp) {
            $timestamp = (int) $timestamp;
            if (($now - $timestamp) > self::TTL) {
                continue;
            }

            $pos = strpos($field, ':');
            if ($pos === false) {
                continue;
            }

            $nodeId = (int) substr($field, 0, $pos);
            $ip = substr($field, $pos + 1);
            if ($nodeId <= 0 || $ip === '') {
                continue;
            }

            $devices[] = [
                'ip' => $ip,
                'last_seen' => $timestamp,
                'node_type' => self::resolveNodeType($nodeId),
            ];
        }

        return ['devices' => $devices];
    }

    /**
     * 兼容老调用方的设备计数方法。
     */
    public static function calculateDeviceCount(array $ips): int
    {
        return count(array_unique(array_filter($ips)));
    }

    private static function resolveNodeType(int $nodeId): string
    {
        return Cache::remember(
            "user_online_service:node_type:{$nodeId}",
            300,
            function () use ($nodeId) {
                $server = Server::find($nodeId);
                return $server ? ($server->type . $nodeId) : "unknown{$nodeId}";
            }
        );
    }
}
