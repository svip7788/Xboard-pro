<?php

namespace Plugin\BaitSplit\Services;

use App\Models\Plugin as PluginModel;
use App\Models\User;
use App\Support\Setting;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;
use InvalidArgumentException;

class BaitSplitService
{
    private const STATE_KEY = 'bait_split_state';
    private const MIN_BUCKETS = 2;
    private const MAX_BUCKETS = 10;

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
        foreach ($this->state()['campaigns'] as $campaign) {
            if (
                !$campaign['enabled']
                || (int) $campaign['target_group_id'] !== (int) $user->group_id
                || !$this->matchesPath((int) $user->id, $campaign)
            ) {
                continue;
            }

            $bucket = $this->bucketForCampaign(
                (int) $user->id,
                count($campaign['active_path']),
                $campaign
            );
            $host = $campaign['domains'][$bucket] ?? null;
            if (!$host) {
                continue;
            }

            foreach ($servers as &$server) {
                if (in_array((int) ($server['id'] ?? 0), $campaign['target_server_ids'], true)) {
                    $server['host'] = $host;
                }
            }
            unset($server);

            $this->recordExposure($campaign, $bucket, (int) $user->id);
            break;
        }

        return $servers;
    }

    public function campaigns(): array
    {
        return array_values(array_map(
            fn(array $campaign): array => $this->campaignStatus($campaign),
            $this->state()['campaigns']
        ));
    }

    public function persistMigration(): void
    {
        $raw = admin_setting(self::STATE_KEY, []);
        if (!is_array($raw) || !isset($raw['campaigns'])) {
            $this->saveState($this->migrateLegacyState(is_array($raw) ? $raw : []));
        }
    }

    public function saveCampaign(
        ?string $campaignId,
        string $name,
        int $targetGroupId,
        array $targetServerIds
    ): array {
        $targetServerIds = $this->normalizeIds($targetServerIds);
        if ($targetGroupId <= 0 || $targetServerIds === []) {
            throw new InvalidArgumentException('必须选择用户组和至少一个节点');
        }

        $state = $this->state();
        $campaignId = $campaignId ?: (string) Str::uuid();
        $existing = $state['campaigns'][$campaignId] ?? null;
        if (
            $existing
            && $existing['enabled']
            && (int) $existing['target_group_id'] !== $targetGroupId
        ) {
            throw new InvalidArgumentException('运行中的任务不能修改用户组，请先停止并重置');
        }

        $campaign = $existing ?: $this->newCampaign($campaignId);
        if ($existing && (int) $existing['target_group_id'] !== $targetGroupId) {
            if (
                (int) $existing['target_group_id'] > 0
                && ($existing['secret'] !== '' || $existing['round'] > 0)
            ) {
                throw new InvalidArgumentException('修改用户组前必须先重置任务');
            }
            $campaign = array_merge(
                $this->newCampaign($campaignId),
                ['name' => $campaign['name']]
            );
        }

        $campaign['name'] = trim($name) ?: "用户组 {$targetGroupId}";
        $campaign['target_group_id'] = $targetGroupId;
        $campaign['target_server_ids'] = $targetServerIds;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function deleteCampaign(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if ($campaign['enabled']) {
            throw new InvalidArgumentException('运行中的任务不能删除，请先停止');
        }

        unset($state['campaigns'][$campaignId]);
        $this->saveState($state);

        return $this->campaigns();
    }

    public function startCampaign(string $campaignId, array $domains): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        $domains = $this->normalizeDomains($domains);
        $bucketCount = count($domains);

        if ($campaign['target_server_ids'] === []) {
            throw new InvalidArgumentException('请先选择需要替换域名的节点');
        }
        if ($campaign['bucket_count'] > 0 && $campaign['bucket_count'] !== $bucketCount) {
            throw new InvalidArgumentException(
                "该任务固定为 {$campaign['bucket_count']} 组；如需修改组数，请先重置任务"
            );
        }

        foreach ($state['campaigns'] as $other) {
            if (
                $other['id'] !== $campaignId
                && $other['enabled']
                && (int) $other['target_group_id'] === (int) $campaign['target_group_id']
            ) {
                throw new InvalidArgumentException('同一用户组已有运行中的排查任务');
            }
        }

        if ($campaign['secret'] === '') {
            $campaign['secret'] = bin2hex(random_bytes(32));
        }
        $campaign['bucket_count'] = $bucketCount;
        if ($this->candidateIds($campaign) === []) {
            throw new InvalidArgumentException('当前分支没有符合条件的有效用户');
        }

        $campaign['round']++;
        $campaign['domains'] = $domains;
        $campaign['enabled'] = true;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function recordResult(string $campaignId, array $positiveBuckets): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if (!$campaign['enabled']) {
            throw new InvalidArgumentException('该任务当前没有运行中的轮次');
        }

        $positiveBuckets = array_values(array_unique(array_map('intval', $positiveBuckets)));
        foreach ($positiveBuckets as $bucket) {
            if ($bucket < 0 || $bucket >= $campaign['bucket_count']) {
                throw new InvalidArgumentException('封锁结果包含无效分组');
            }
        }

        $candidateIds = $this->candidateIds($campaign);
        $campaign['enabled'] = false;
        $campaign['history'][] = [
            'round' => $campaign['round'],
            'path' => $campaign['active_path'],
            'positive_buckets' => $positiveBuckets,
            'candidate_count' => count($candidateIds),
            'recorded_at' => time(),
        ];
        $campaign['history'] = array_slice($campaign['history'], -100);

        if (count($candidateIds) === 1) {
            $userId = $candidateIds[0];
            $expectedBucket = $this->bucketForCampaign(
                $userId,
                count($campaign['active_path']),
                $campaign
            );
            if (in_array($expectedBucket, $positiveBuckets, true)) {
                $finding = $campaign['findings'][(string) $userId] ?? [
                    'user_id' => $userId,
                    'confirmations' => 0,
                ];
                $finding['confirmations']++;
                $campaign['findings'][(string) $userId] = $finding;
            }
        } elseif ($positiveBuckets === []) {
            // 无分组被封锁时保留当前候选池，下一轮重新验证。
        } else {
            $positivePaths = [];
            $deferredPaths = [];
            for ($bucket = 0; $bucket < $campaign['bucket_count']; $bucket++) {
                $path = [...$campaign['active_path'], $bucket];
                if (in_array($bucket, $positiveBuckets, true)) {
                    $positivePaths[] = $path;
                } else {
                    $deferredPaths[] = $path;
                }
            }

            $campaign['positive_queue'] = $this->uniquePaths(array_merge(
                $campaign['positive_queue'],
                $positivePaths
            ));
            $campaign['deferred_queue'] = $this->uniquePaths(array_merge(
                $campaign['deferred_queue'],
                $deferredPaths
            ));
            $campaign['active_path'] = $this->shiftNextPath($campaign);
        }

        $campaign['domains'] = [];
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function disableCampaign(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        $campaign['enabled'] = false;
        $campaign['domains'] = [];
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function resetCampaign(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if ($campaign['enabled']) {
            throw new InvalidArgumentException('请先停止运行中的任务');
        }

        $campaign = array_merge($this->newCampaign($campaignId), [
            'name' => $campaign['name'],
            'target_group_id' => $campaign['target_group_id'],
            'target_server_ids' => $campaign['target_server_ids'],
        ]);
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    private function campaignStatus(array $campaign): array
    {
        $eligibleCount = $this->eligibleUsersQuery($campaign['target_group_id'])->count();
        $hasGrouping = $campaign['secret'] !== '' && $campaign['bucket_count'] > 0;
        $candidateIds = $hasGrouping ? $this->candidateIds($campaign) : [];
        $bucketCounts = array_fill(0, $campaign['bucket_count'], 0);
        foreach ($candidateIds as $userId) {
            $bucket = $this->bucketForCampaign(
                $userId,
                count($campaign['active_path']),
                $campaign
            );
            $bucketCounts[$bucket]++;
        }

        $exposedCounts = array_fill(0, $campaign['bucket_count'], 0);
        if ($campaign['round'] > 0) {
            foreach ($exposedCounts as $bucket => $_) {
                try {
                    $key = $campaign['hash_algo'] === 'bit'
                        ? "bait_split:exposure:{$campaign['round']}:{$this->bucketLabel($bucket)}"
                        : $this->exposureKey($campaign['id'], $campaign['round'], $bucket);
                    $exposedCounts[$bucket] = (int) Redis::scard($key);
                } catch (\Throwable) {
                    break;
                }
            }
        }

        return [
            'id' => $campaign['id'],
            'name' => $campaign['name'],
            'enabled' => $campaign['enabled'],
            'target_group_id' => $campaign['target_group_id'],
            'target_server_ids' => $campaign['target_server_ids'],
            'round' => $campaign['round'],
            'bucket_count' => $campaign['bucket_count'],
            'domains' => $campaign['domains'],
            'active_path' => $campaign['active_path'],
            'active_path_label' => $this->pathLabel($campaign['active_path']),
            'eligible_count' => $eligibleCount,
            'candidate_count' => $hasGrouping ? count($candidateIds) : $eligibleCount,
            'bucket_counts' => $bucketCounts,
            'exposed_counts' => $exposedCounts,
            'bucket_labels' => array_map(
                fn(int $index): string => $this->bucketLabel($index),
                array_keys($bucketCounts)
            ),
            'positive_queue' => array_map([$this, 'pathLabel'], $campaign['positive_queue']),
            'deferred_queue' => array_map([$this, 'pathLabel'], $campaign['deferred_queue']),
            'findings' => array_values($campaign['findings']),
        ];
    }

    private function state(): array
    {
        $raw = admin_setting(self::STATE_KEY, []);
        $raw = is_array($raw) ? $raw : [];
        if (isset($raw['campaigns']) && is_array($raw['campaigns'])) {
            $campaigns = [];
            foreach ($raw['campaigns'] as $id => $campaign) {
                if (is_array($campaign)) {
                    $campaigns[$id] = $this->normalizeCampaign($campaign, (string) $id);
                }
            }
            return ['version' => 2, 'campaigns' => $campaigns];
        }

        return $this->migrateLegacyState($raw);
    }

    private function migrateLegacyState(array $legacy): array
    {
        $groupId = (int) ($this->config['target_group_id'] ?? 0);
        $serverIds = $this->normalizeIds($this->config['target_server_ids'] ?? []);
        if ($groupId <= 0 && $legacy === []) {
            return ['version' => 2, 'campaigns' => []];
        }

        $campaign = $this->newCampaign('legacy');
        $campaign['name'] = $groupId > 0 ? '原排查任务' : '待修复原任务';
        $campaign['target_group_id'] = $groupId;
        $campaign['target_server_ids'] = $serverIds;
        $campaign['enabled'] = $groupId > 0 && (bool) ($legacy['enabled'] ?? false);
        $campaign['secret'] = (string) ($legacy['secret'] ?? '');
        $campaign['hash_algo'] = 'bit';
        $campaign['round'] = (int) ($legacy['round'] ?? 0);
        $campaign['bucket_count'] = 2;
        $campaign['active_path'] = array_map(
            'intval',
            str_split((string) ($legacy['active_prefix'] ?? ''))
        );
        $campaign['positive_queue'] = $this->legacyPrefixes($legacy['positive_queue'] ?? []);
        $campaign['deferred_queue'] = $this->legacyPrefixes($legacy['deferred_queue'] ?? []);
        $campaign['findings'] = is_array($legacy['findings'] ?? null) ? $legacy['findings'] : [];
        $campaign['history'] = is_array($legacy['history'] ?? null) ? $legacy['history'] : [];
        $campaign['domains'] = array_values(array_filter([
            $legacy['host_a'] ?? '',
            $legacy['host_b'] ?? '',
        ]));

        return ['version' => 2, 'campaigns' => ['legacy' => $campaign]];
    }

    private function saveState(array $state): void
    {
        app(Setting::class)->set(self::STATE_KEY, $state);
    }

    private function newCampaign(string $id): array
    {
        return [
            'id' => $id,
            'name' => '',
            'target_group_id' => 0,
            'target_server_ids' => [],
            'enabled' => false,
            'secret' => '',
            'hash_algo' => 'mod',
            'round' => 0,
            'bucket_count' => 0,
            'domains' => [],
            'active_path' => [],
            'positive_queue' => [],
            'deferred_queue' => [],
            'findings' => [],
            'history' => [],
        ];
    }

    private function normalizeCampaign(array $campaign, string $id): array
    {
        $hasHashAlgorithm = array_key_exists('hash_algo', $campaign);
        $campaign = array_merge($this->newCampaign($id), $campaign);
        $campaign['id'] = $id;
        $campaign['target_group_id'] = (int) $campaign['target_group_id'];
        $campaign['target_server_ids'] = $this->normalizeIds($campaign['target_server_ids']);
        $campaign['bucket_count'] = (int) $campaign['bucket_count'];
        $campaign['hash_algo'] = $id === 'legacy' && !$hasHashAlgorithm
            ? 'bit'
            : ($campaign['hash_algo'] === 'bit' ? 'bit' : 'mod');
        $campaign['active_path'] = $this->normalizePath($campaign['active_path']);
        $campaign['positive_queue'] = $this->normalizePaths($campaign['positive_queue']);
        $campaign['deferred_queue'] = $this->normalizePaths($campaign['deferred_queue']);
        $campaign['domains'] = is_array($campaign['domains']) ? array_values($campaign['domains']) : [];
        $campaign['findings'] = is_array($campaign['findings']) ? $campaign['findings'] : [];
        $campaign['history'] = is_array($campaign['history']) ? $campaign['history'] : [];
        return $campaign;
    }

    private function requireCampaign(array $state, string $campaignId): array
    {
        $campaign = $state['campaigns'][$campaignId] ?? null;
        if (!$campaign) {
            throw new InvalidArgumentException('排查任务不存在');
        }
        return $campaign;
    }

    private function candidateIds(array $campaign, ?array $path = null): array
    {
        if ($campaign['secret'] === '' || $campaign['target_group_id'] <= 0) {
            return [];
        }

        $path ??= $campaign['active_path'];
        return $this->eligibleUsersQuery($campaign['target_group_id'])
            ->pluck('id')
            ->map(fn($id): int => (int) $id)
            ->filter(fn(int $id): bool => $this->matchesPath($id, $campaign, $path))
            ->values()
            ->all();
    }

    private function eligibleUsersQuery(int $groupId): Builder
    {
        return User::query()
            ->where('group_id', $groupId)
            ->where('is_admin', 0)
            ->where('banned', 0)
            ->where('transfer_enable', '>', 0)
            ->where(function ($query) {
                $query->whereNull('expired_at')
                    ->orWhere('expired_at', '>', time());
            });
    }

    private function matchesPath(int $userId, array $campaign, ?array $path = null): bool
    {
        $path ??= $campaign['active_path'];
        foreach ($path as $depth => $expectedBucket) {
            if (
                $this->bucketForCampaign(
                    $userId,
                    $depth,
                    $campaign
                ) !== $expectedBucket
            ) {
                return false;
            }
        }
        return true;
    }

    private function bucketForUser(
        int $userId,
        int $depth,
        string $secret,
        int $bucketCount
    ): int {
        if ($bucketCount <= 0) {
            return 0;
        }
        $digest = hash_hmac('sha256', "{$depth}:{$userId}", $secret, true);
        $number = unpack('N', substr($digest, 0, 4))[1];
        return $number % $bucketCount;
    }

    private function bucketForCampaign(int $userId, int $depth, array $campaign): int
    {
        if ($campaign['hash_algo'] === 'bit') {
            return $this->legacyBitAt($userId, $depth, $campaign['secret']);
        }

        return $this->bucketForUser(
            $userId,
            $depth,
            $campaign['secret'],
            $campaign['bucket_count']
        );
    }

    private function legacyBitAt(int $userId, int $index, string $secret): int
    {
        $block = intdiv($index, 256);
        $bitIndex = $index % 256;
        $digest = hash_hmac('sha256', "{$block}:{$userId}", $secret, true);
        $byte = ord($digest[intdiv($bitIndex, 8)]);

        return ($byte >> (7 - ($bitIndex % 8))) & 1;
    }

    private function shiftNextPath(array &$campaign): array
    {
        while ($campaign['positive_queue'] !== []) {
            $path = array_shift($campaign['positive_queue']);
            if ($this->candidateIds($campaign, $path) !== []) {
                return $path;
            }
        }
        while ($campaign['deferred_queue'] !== []) {
            $path = array_shift($campaign['deferred_queue']);
            if ($this->candidateIds($campaign, $path) !== []) {
                return $path;
            }
        }
        return [];
    }

    private function normalizeDomains(array $domains): array
    {
        $domains = array_values(array_unique(array_map(
            fn($domain): string => strtolower(trim((string) $domain)),
            $domains
        )));
        if (count($domains) < self::MIN_BUCKETS || count($domains) > self::MAX_BUCKETS) {
            throw new InvalidArgumentException(
                '每个任务必须设置 2 至 10 个不同域名或 IP'
            );
        }
        foreach ($domains as $domain) {
            $valid = filter_var($domain, FILTER_VALIDATE_IP)
                || filter_var($domain, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME);
            if (!$valid) {
                throw new InvalidArgumentException("无效的节点域名或 IP：{$domain}");
            }
        }
        return $domains;
    }

    private function normalizeIds(array|string $ids): array
    {
        if (is_string($ids)) {
            $ids = json_decode($ids, true) ?: [];
        }
        return array_values(array_unique(array_filter(
            array_map('intval', $ids),
            fn(int $id): bool => $id > 0
        )));
    }

    private function normalizePath(mixed $path): array
    {
        return is_array($path) ? array_values(array_map('intval', $path)) : [];
    }

    private function normalizePaths(mixed $paths): array
    {
        return is_array($paths)
            ? array_values(array_map([$this, 'normalizePath'], $paths))
            : [];
    }

    private function uniquePaths(array $paths): array
    {
        $unique = [];
        foreach ($paths as $path) {
            $path = $this->normalizePath($path);
            $unique[json_encode($path)] = $path;
        }
        return array_values($unique);
    }

    private function legacyPrefixes(array $prefixes): array
    {
        return array_values(array_map(
            fn($prefix): array => array_map('intval', str_split((string) $prefix)),
            array_filter(
                $prefixes,
                fn($prefix): bool => is_string($prefix) && preg_match('/^[01]*$/', $prefix)
            )
        ));
    }

    private function pathLabel(array $path): string
    {
        if ($path === []) {
            return '根分支';
        }
        return implode(' → ', array_map(
            fn(int $bucket): string => $this->bucketLabel($bucket),
            $path
        ));
    }

    private function bucketLabel(int $index): string
    {
        return chr(65 + $index);
    }

    private function recordExposure(array $campaign, int $bucket, int $userId): void
    {
        try {
            $key = $campaign['hash_algo'] === 'bit'
                ? "bait_split:exposure:{$campaign['round']}:{$this->bucketLabel($bucket)}"
                : $this->exposureKey($campaign['id'], $campaign['round'], $bucket);
            Redis::sadd($key, (string) $userId);
            Redis::expire($key, 86400 * 30);
        } catch (\Throwable) {
            // 统计失败不能影响用户获取订阅。
        }
    }

    private function exposureKey(string $campaignId, int $round, int $bucket): string
    {
        return "bait_split:exposure:{$campaignId}:{$round}:{$bucket}";
    }
}
