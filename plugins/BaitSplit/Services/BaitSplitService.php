<?php

namespace Plugin\BaitSplit\Services;

use App\Models\Plugin as PluginModel;
use App\Models\User;
use App\Support\Setting;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Facades\Redis;
use InvalidArgumentException;

class BaitSplitService
{
    private const STATE_KEY = 'bait_split_state';

    public function __construct(private readonly array $config)
    {
    }

    public static function fromDatabase(): self
    {
        $rawConfig = PluginModel::query()
            ->where('code', 'bait_split')
            ->value('config');

        $config = is_array($rawConfig)
            ? $rawConfig
            : (is_string($rawConfig) ? (json_decode($rawConfig, true) ?: []) : []);

        return new self($config);
    }

    public function filterServers(array $servers, User $user): array
    {
        $state = $this->state();
        if (!$state['enabled'] || !$this->isTargetUser($user)) {
            return $servers;
        }

        $prefix = $state['active_prefix'];
        if (!$this->matchesPrefix((int) $user->id, $prefix, $state['secret'])) {
            return $servers;
        }

        $group = $this->groupForUser((int) $user->id, $prefix, $state['secret']);
        $host = $group === 'A' ? $state['host_a'] : $state['host_b'];
        $targetIds = $this->targetServerIds();

        foreach ($servers as &$server) {
            if (in_array((int) ($server['id'] ?? 0), $targetIds, true)) {
                $server['host'] = $host;
            }
        }
        unset($server);

        try {
            $key = "bait_split:exposure:{$state['round']}:{$group}";
            Redis::sadd($key, (string) $user->id);
            Redis::expire($key, 86400 * 30);
        } catch (\Throwable) {
            // 统计失败不能影响用户获取订阅。
        }

        return $servers;
    }

    public function start(string $hostA, string $hostB, ?string $prefix = null): array
    {
        $hostA = $this->validateHost($hostA);
        $hostB = $this->validateHost($hostB);

        if ($this->targetGroupId() <= 0) {
            throw new InvalidArgumentException('请先在插件配置中填写白银组 ID');
        }
        if ($this->targetServerIds() === []) {
            throw new InvalidArgumentException('请先在插件配置中填写需要替换的节点 ID');
        }

        $state = $this->state();
        if ($prefix !== null) {
            $prefix = trim($prefix);
            if (!preg_match('/^[01]*$/', $prefix)) {
                throw new InvalidArgumentException('分支前缀只能包含 0 和 1');
            }
            $state['active_prefix'] = $prefix;
        }

        if ($state['secret'] === '') {
            $state['secret'] = bin2hex(random_bytes(32));
        }

        $candidateCount = count($this->candidateIds($state['active_prefix'], $state['secret']));
        if ($candidateCount === 0) {
            throw new InvalidArgumentException('当前分支没有符合条件的白银组用户');
        }

        $state['round']++;
        $state['enabled'] = true;
        $state['host_a'] = $hostA;
        $state['host_b'] = $hostB;
        $this->saveState($state);

        return $this->status();
    }

    public function recordResult(string $result): array
    {
        $result = strtolower(trim($result));
        if (!in_array($result, ['a', 'b', 'both', 'none'], true)) {
            throw new InvalidArgumentException('结果只能是 A、B、both 或 none');
        }

        $state = $this->state();
        if (!$state['enabled']) {
            throw new InvalidArgumentException('当前没有正在运行的排查轮次');
        }

        $prefix = $state['active_prefix'];
        $candidateIds = $this->candidateIds($prefix, $state['secret']);
        $state['enabled'] = false;
        $state['history'][] = [
            'round' => $state['round'],
            'prefix' => $prefix,
            'result' => $result,
            'candidate_count' => count($candidateIds),
            'recorded_at' => time(),
        ];
        $state['history'] = array_slice($state['history'], -100);

        if (count($candidateIds) === 1) {
            $userId = $candidateIds[0];
            $expected = strtolower($this->groupForUser($userId, $prefix, $state['secret']));
            $positive = $result === 'both' || $result === $expected;
            if ($positive) {
                $finding = $state['findings'][(string) $userId] ?? [
                    'user_id' => $userId,
                    'confirmations' => 0,
                ];
                $finding['confirmations']++;
                $state['findings'][(string) $userId] = $finding;
            }
            $state['host_a'] = '';
            $state['host_b'] = '';
            $this->saveState($state);
            return $this->status();
        }

        $childA = $prefix . '0';
        $childB = $prefix . '1';
        $positive = [];
        $deferred = [];

        if ($result === 'a' || $result === 'both') {
            $positive[] = $childA;
        } else {
            $deferred[] = $childA;
        }
        if ($result === 'b' || $result === 'both') {
            $positive[] = $childB;
        } else {
            $deferred[] = $childB;
        }

        if ($result === 'none') {
            $state['active_prefix'] = $prefix;
        } else {
            $state['positive_queue'] = $this->uniquePrefixes(array_merge(
                $state['positive_queue'],
                $positive
            ));
            $state['deferred_queue'] = $this->uniquePrefixes(array_merge(
                $state['deferred_queue'],
                $deferred
            ));
            $state['active_prefix'] = $this->shiftNextPrefix($state);
        }

        $state['host_a'] = '';
        $state['host_b'] = '';
        $this->saveState($state);

        return $this->status();
    }

    public function disable(): array
    {
        $state = $this->state();
        $state['enabled'] = false;
        $state['host_a'] = '';
        $state['host_b'] = '';
        $this->saveState($state);

        return $this->status();
    }

    public function status(): array
    {
        $state = $this->state();
        $eligibleCount = $this->eligibleUsersQuery()->count();
        $candidateIds = $state['secret'] === ''
            ? []
            : $this->candidateIds($state['active_prefix'], $state['secret']);
        $counts = ['A' => 0, 'B' => 0];

        foreach ($candidateIds as $userId) {
            $counts[$this->groupForUser($userId, $state['active_prefix'], $state['secret'])]++;
        }

        $exposed = ['A' => 0, 'B' => 0];
        if ($state['round'] > 0) {
            try {
                $exposed['A'] = (int) Redis::scard("bait_split:exposure:{$state['round']}:A");
                $exposed['B'] = (int) Redis::scard("bait_split:exposure:{$state['round']}:B");
            } catch (\Throwable) {
                // Redis 不可用时仅缺少拉取统计。
            }
        }

        return [
            'enabled' => $state['enabled'],
            'round' => $state['round'],
            'active_prefix' => $state['active_prefix'],
            'eligible_count' => $eligibleCount,
            'candidate_count' => $state['secret'] === '' ? $eligibleCount : count($candidateIds),
            'group_counts' => $counts,
            'exposed_counts' => $exposed,
            'positive_queue' => $state['positive_queue'],
            'deferred_queue' => $state['deferred_queue'],
            'findings' => array_values($state['findings']),
            'host_a' => $state['host_a'],
            'host_b' => $state['host_b'],
            'target_group_id' => $this->targetGroupId(),
            'target_server_ids' => $this->targetServerIds(),
        ];
    }

    private function state(): array
    {
        $state = admin_setting(self::STATE_KEY, []);
        if (!is_array($state)) {
            $state = [];
        }

        return array_merge([
            'enabled' => false,
            'secret' => '',
            'round' => 0,
            'active_prefix' => '',
            'positive_queue' => [],
            'deferred_queue' => [],
            'findings' => [],
            'history' => [],
            'host_a' => '',
            'host_b' => '',
        ], $state);
    }

    private function saveState(array $state): void
    {
        app(Setting::class)->set(self::STATE_KEY, $state);
    }

    private function isTargetUser(User $user): bool
    {
        return (int) $user->group_id === $this->targetGroupId();
    }

    private function targetGroupId(): int
    {
        return (int) ($this->config['target_group_id'] ?? 0);
    }

    private function targetServerIds(): array
    {
        $ids = $this->config['target_server_ids'] ?? [];
        if (is_string($ids)) {
            $ids = json_decode($ids, true) ?: [];
        }

        return array_values(array_unique(array_filter(
            array_map('intval', is_array($ids) ? $ids : []),
            fn(int $id): bool => $id > 0
        )));
    }

    private function candidateIds(string $prefix, string $secret): array
    {
        if ($secret === '' || $this->targetGroupId() <= 0) {
            return [];
        }

        return $this->eligibleUsersQuery()
            ->pluck('id')
            ->map(fn($id): int => (int) $id)
            ->filter(fn(int $id): bool => $this->matchesPrefix($id, $prefix, $secret))
            ->values()
            ->all();
    }

    private function eligibleUsersQuery(): Builder
    {
        return User::query()
            ->where('group_id', $this->targetGroupId())
            ->where('is_admin', 0)
            ->where('banned', 0)
            ->where('transfer_enable', '>', 0)
            ->where(function ($query) {
                $query->whereNull('expired_at')
                    ->orWhere('expired_at', '>', time());
            });
    }

    private function matchesPrefix(int $userId, string $prefix, string $secret): bool
    {
        foreach (str_split($prefix) as $index => $expected) {
            if ($this->bitAt($userId, $index, $secret) !== (int) $expected) {
                return false;
            }
        }
        return true;
    }

    private function groupForUser(int $userId, string $prefix, string $secret): string
    {
        return $this->bitAt($userId, strlen($prefix), $secret) === 0 ? 'A' : 'B';
    }

    private function bitAt(int $userId, int $index, string $secret): int
    {
        $block = intdiv($index, 256);
        $bitIndex = $index % 256;
        $digest = hash_hmac('sha256', "{$block}:{$userId}", $secret, true);
        $byte = ord($digest[intdiv($bitIndex, 8)]);

        return ($byte >> (7 - ($bitIndex % 8))) & 1;
    }

    private function validateHost(string $host): string
    {
        $host = strtolower(trim($host));
        $valid = filter_var($host, FILTER_VALIDATE_IP)
            || filter_var($host, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME);

        if (!$valid) {
            throw new InvalidArgumentException("无效的节点域名或 IP：{$host}");
        }
        return $host;
    }

    private function uniquePrefixes(array $prefixes): array
    {
        return array_values(array_unique(array_filter(
            $prefixes,
            fn($prefix): bool => is_string($prefix) && preg_match('/^[01]*$/', $prefix)
        )));
    }

    private function shiftNextPrefix(array &$state): string
    {
        while ($state['positive_queue'] !== []) {
            $prefix = array_shift($state['positive_queue']);
            if ($this->candidateIds($prefix, $state['secret']) !== []) {
                return $prefix;
            }
        }

        while ($state['deferred_queue'] !== []) {
            $prefix = array_shift($state['deferred_queue']);
            if ($this->candidateIds($prefix, $state['secret']) !== []) {
                return $prefix;
            }
        }

        return '';
    }
}
