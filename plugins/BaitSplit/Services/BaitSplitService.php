<?php

namespace Plugin\BaitSplit\Services;

use App\Models\Plugin as PluginModel;
use App\Models\Server;
use App\Models\User;
use App\Support\Setting;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;
use InvalidArgumentException;
use RuntimeException;

class BaitSplitService
{
    private const STATE_KEY = 'bait_split_state';
    private const STATE_LOCK = 'bait_split:admin_state';
    private const IP_PENDING_KEY = 'bait_split:ip_rotate_pending';
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
                ($campaign['router']['enabled'] ?? false)
                && $this->campaignMatchesGroup($campaign, (int) $user->group_id)
            ) {
                $campaign = $this->assignPoolOnFirstPull($campaign, $user);
                $router = $campaign['router'];
                $userId = (int) $user->id;
                $override = $router['overrides'][(string) $userId] ?? null;
                $override = $override && $this->overrideIsActive($override)
                    ? $override
                    : null;
                $assignedPoolId = $router['assignments'][
                    (string) $userId
                ] ?? null;
                if (!$override && $assignedPoolId === null) {
                    return [];
                }
                $assignedPool = $assignedPoolId !== null
                    ? ($router['pools'][$assignedPoolId] ?? null)
                    : null;
                $treeNodeId = (string) ($assignedPool['tree_node_id'] ?? '');
                if (
                    !$override
                    && $treeNodeId !== ''
                    && ($router['investigation_nodes'][$treeNodeId]['status'] ?? null)
                        === 'blocked'
                ) {
                    return [];
                }
                return $this->filterRoutedServers($servers, $user, $campaign);
            }
            if (
                !$campaign['serving']
                || !$this->campaignMatchesGroup($campaign, (int) $user->group_id)
            ) {
                continue;
            }

            $assignment = $this->assignmentForUser($campaign, (int) $user->id);
            if (!$assignment) {
                continue;
            }

            foreach ($servers as &$server) {
                if (in_array((int) ($server['id'] ?? 0), $campaign['target_server_ids'], true)) {
                    $server['host'] = $assignment['host'];
                }
            }
            unset($server);

            $this->recordExposure($campaign, $assignment, (int) $user->id);
            break;
        }

        return $servers;
    }

    public function failClosedServers(array $servers, User $user): array
    {
        foreach ($this->state()['campaigns'] as $campaign) {
            if (
                ($campaign['router']['enabled'] ?? false)
                && $this->campaignMatchesGroup($campaign, (int) $user->group_id)
            ) {
                $managedServerIds = array_flip($campaign['target_server_ids']);
                return array_values(array_filter(
                    $servers,
                    fn(array $server): bool => !isset(
                        $managedServerIds[(int) ($server['id'] ?? 0)]
                    )
                ));
            }
        }
        return $servers;
    }

    public function campaigns(): array
    {
        $state = $this->state();
        $changed = false;
        foreach ($state['campaigns'] as &$campaign) {
            if ($this->pruneMergedSourceTrees($campaign) > 0) {
                $changed = true;
            }
        }
        unset($campaign);
        if ($changed) {
            $this->saveState($state);
        }
        return array_values(array_map(
            fn(array $campaign): array => $this->campaignStatus($campaign),
            $state['campaigns']
        ));
    }

    public function createPingTask(string $host): array
    {
        $host = $this->normalizePingHost($host);
        $key = $this->boceApiKey();
        $nodeIds = Cache::remember(
            'bait_split:boce_ping_node_ids',
            86400,
            fn(): array => $this->fetchBocePingNodeIds($key)
        );
        if ($nodeIds === []) {
            throw new InvalidArgumentException('未获取到可用的国内检测节点');
        }
        $response = Http::timeout(20)->retry(2, 500)->get(
            'https://api.boce.com/v3/task/create/ping',
            [
                'key' => $key,
                'node_ids' => implode(',', $nodeIds),
                'host' => $host,
                'node_type' => '1',
            ]
        );
        $payload = $response->json();
        $taskId = (string) ($payload['data']['id'] ?? '');
        if (
            !$response->successful()
            || (int) ($payload['error_code'] ?? -1) !== 0
            || $taskId === ''
        ) {
            throw new InvalidArgumentException(
                '创建 Ping 任务失败：' . (
                    (string) ($payload['error'] ?? '') ?: "HTTP {$response->status()}"
                )
            );
        }
        return [
            'id' => $taskId,
            'host' => $host,
            'node_count' => count($nodeIds),
        ];
    }

    public function pingTaskResult(string $taskId): array
    {
        if (!preg_match('/^[A-Za-z0-9_-]{8,128}$/', $taskId)) {
            throw new InvalidArgumentException('Ping 任务 ID 无效');
        }
        $response = Http::timeout(20)->retry(2, 500)->get(
            "https://api.boce.com/v3/task/ping/{$taskId}",
            ['key' => $this->boceApiKey()]
        );
        $payload = $response->json();
        if (!$response->successful() || !is_array($payload)) {
            throw new InvalidArgumentException("获取 Ping 结果失败：HTTP {$response->status()}");
        }
        if (!(bool) ($payload['done'] ?? false)) {
            return ['done' => false];
        }

        $items = [];
        $successCount = 0;
        foreach ((array) ($payload['list'] ?? []) as $row) {
            if (!is_array($row)) {
                continue;
            }
            $loss = (float) ($row['packet_loss'] ?? 100);
            $ok = (int) ($row['error_code'] ?? -1) === 0 && $loss < 100;
            if ($ok) {
                $successCount++;
            }
            $items[] = [
                'node_name' => (string) ($row['node_name'] ?? ''),
                'isp' => (string) ($row['ip_isp'] ?? ''),
                'ip' => (string) ($row['ip'] ?? ''),
                'packet_loss' => $loss,
                'latency' => (float) (
                    $row['round_trip_avg']
                    ?? $row['round_trip_time_avg']
                    ?? 0
                ),
                'ok' => $ok,
                'error' => (string) ($row['error'] ?? ''),
            ];
        }
        $total = count($items);
        $status = $successCount === 0
            ? 'unreachable'
            : ($successCount < $total ? 'partial' : 'reachable');
        return [
            'done' => true,
            'status' => $status,
            'success_count' => $successCount,
            'total_count' => $total,
            'items' => $items,
        ];
    }

    public function exposureUsers(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        $assignmentUserIds = [];

        foreach ($campaign['branch_hosts'] as $index => $assignment) {
            $assignmentUserIds[$index] = $this->branchExposureUserIds(
                $campaign,
                $assignment
            );
        }

        $users = User::query()
            ->whereIn('id', array_values(array_unique(array_merge([], ...$assignmentUserIds))))
            ->get(['id', 'email'])
            ->keyBy(fn(User $user): int => (int) $user->id);

        return array_map(
            fn(array $assignment, int $index): array => [
                'path' => $assignment['path'],
                'label' => $this->pathLabel($assignment['path']),
                'host' => $assignment['host'],
                'users' => array_values(array_filter(array_map(
                    fn(int $userId): ?array => isset($users[$userId])
                        ? ['id' => $userId, 'email' => $users[$userId]->email]
                        : null,
                    $assignmentUserIds[$index]
                ))),
            ],
            $campaign['branch_hosts'],
            array_keys($campaign['branch_hosts'])
        );
    }

    public function initializeRouter(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if ($campaign['router']) {
            return $this->campaignStatus($campaign);
        }

        $userIds = $this->eligibleUsersQuery($campaign['target_group_ids'])
            ->pluck('id')->map('intval')->values()->all();
        $campaign['router'] = $this->newRouter($userIds);

        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function savePool(string $campaignId, array $data): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $poolId = trim((string) ($data['id'] ?? '')) ?: 'pool-' . Str::uuid();
        $existing = $router['pools'][$poolId] ?? [];
        $reservedTypes = [
            'default' => 'default',
            'danger' => 'danger',
            'probe' => 'probe',
            'emergency' => 'emergency',
            'blacklist' => 'blacklist',
        ];
        if (isset($reservedTypes[$poolId])) {
            $data['type'] = $reservedTypes[$poolId];
        }
        $this->snapshotRouterConfig($router);
        $router['pools'][$poolId] = $this->normalizePool(
            array_merge($existing, $data, ['id' => $poolId]),
            $poolId
        );
        $this->assertWebhookIdAvailable(
            $router,
            $router['pools'][$poolId]['webhook_id'],
            $poolId
        );
        $this->validateAllPoolOverflowChains($router['pools']);
        $this->assertPoolTypeUniqueness($router['pools']);
        if ($campaign['router']['enabled']) {
            $this->validateRouterCoverage($campaign);
        }
        $router['config_version']++;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function deletePool(string $campaignId, string $poolId): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $pool = $router['pools'][$poolId] ?? null;
        if (!$pool) {
            throw new InvalidArgumentException('用户池不存在');
        }
        if ($pool['tree_node_id'] !== '') {
            throw new InvalidArgumentException('树形排查用户池不能单独删除');
        }
        if (in_array($poolId, ['default', 'danger', 'probe', 'emergency', 'blacklist'], true)) {
            throw new InvalidArgumentException('系统基础池不能删除');
        }
        $hasAssignments = in_array($poolId, $router['assignments'], true);
        if ($hasAssignments) {
            throw new InvalidArgumentException('该用户池仍有用户，不能删除');
        }
        foreach ($router['overrides'] as $override) {
            if (
                $this->overrideIsActive($override)
                && $override['pool_id'] === $poolId
            ) {
                throw new InvalidArgumentException('该用户池仍被单用户规则引用');
            }
        }
        foreach ($router['pools'] as $otherPool) {
            if (($otherPool['overflow_pool_id'] ?? '') === $poolId) {
                throw new InvalidArgumentException('该用户池仍被其他用户池设为满员转入目标');
            }
        }
        $this->snapshotRouterConfig($router);
        unset($router['pools'][$poolId]);
        $router['config_version']++;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function setRouterEnabled(string $campaignId, bool $enabled): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        if ($enabled) {
            foreach ($state['campaigns'] as $other) {
                if (
                    $other['id'] !== $campaignId
                    && $this->campaignsShareGroup($other, $campaign)
                    && (($other['router']['enabled'] ?? false) || $other['serving'])
                ) {
                    throw new InvalidArgumentException('同一用户组已有运行中的域名任务');
                }
            }
            $campaign['target_server_ids'] = array_values(array_diff(
                $this->managedServerIds($campaign['target_group_ids']),
                $campaign['excluded_server_ids']
            ));
            if ($campaign['target_server_ids'] === []) {
                throw new InvalidArgumentException('目标权限组暂无可用节点');
            }
            $this->validateRouterCoverage($campaign);
            $campaign['enabled'] = false;
            $campaign['serving'] = false;
        }
        $campaign['router']['enabled'] = $enabled;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function syncRouterUsers(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $this->syncRouterPopulation($campaign);
        if ($campaign['router']['enabled']) {
            $campaign['target_server_ids'] = array_values(array_diff(
                $this->managedServerIds($campaign['target_group_ids']),
                $campaign['excluded_server_ids']
            ));
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function saveUserOverride(string $campaignId, int $userId, array $data): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $user = User::query()
            ->where('id', $userId)
            ->whereIn('group_id', $campaign['target_group_ids'])
            ->first(['id']);
        if (!$user) {
            throw new InvalidArgumentException('用户不存在或不属于目标权限组');
        }
        $poolId = trim((string) ($data['pool_id'] ?? ''));
        if ($poolId !== '' && !isset($campaign['router']['pools'][$poolId])) {
            throw new InvalidArgumentException('指定用户池不存在');
        }
        $override = $this->normalizeOverride([
            'pool_id' => $poolId,
            'host' => $data['host'] ?? '',
            'node_hosts' => $data['node_hosts'] ?? [],
            'server_name' => $data['server_name'] ?? '',
            'transport_host' => $data['transport_host'] ?? '',
            'locked' => $data['locked'] ?? true,
            'note' => $data['note'] ?? '',
            'expires_at' => $data['expires_at'] ?? 0,
            'updated_at' => time(),
        ]);
        if (
            $override['pool_id'] === ''
            && $override['host'] === ''
            && $override['node_hosts'] === []
        ) {
            throw new InvalidArgumentException('必须指定用户池或单用户域名');
        }
        $campaign['router']['overrides'][(string) $userId] = $override;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function deleteUserOverride(string $campaignId, int $userId): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        unset($campaign['router']['overrides'][(string) $userId]);
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function usersForPool(
        string $campaignId,
        string $poolId,
        string $keyword = '',
        int $page = 1,
        int $perPage = 50
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        if (!isset($campaign['router']['pools'][$poolId])) {
            throw new InvalidArgumentException('用户池不存在');
        }

        $keyword = trim($keyword);
        $userIds = $this->poolMemberIds($campaign, $poolId);
        if ($keyword !== '' && $userIds !== []) {
            $matched = [];
            foreach (array_chunk($userIds, 5000) as $chunk) {
                $matched = array_merge($matched, User::query()
                    ->whereIn('id', $chunk)
                    ->where(function (Builder $builder) use ($keyword): void {
                        if (ctype_digit($keyword)) {
                            $builder->orWhere('id', (int) $keyword);
                        }
                        $builder->orWhere('email', 'like', '%' . $keyword . '%');
                    })
                    ->orderBy('id')->pluck('id')->map('intval')->all());
            }
            sort($matched);
            $userIds = $matched;
        }
        $perPage = max(10, min(100, $perPage));
        $total = count($userIds);
        $lastPage = max(1, (int) ceil($total / $perPage));
        $page = max(1, min($lastPage, $page));

        $items = $this->userRows(array_slice(
                $userIds,
                ($page - 1) * $perPage,
                $perPage
            ));
        $stats = $this->poolExposureStats(
            $campaign,
            $poolId,
            array_column($items, 'id')
        );
        $exposedMap = array_flip($this->poolExposureIds($campaign, $poolId));
        $items = array_map(function (array $user) use ($stats, $exposedMap): array {
            $stat = $stats[$user['id']] ?? ['count' => 0, 'last_at' => 0];
            return $user + [
                'pulled' => isset($exposedMap[$user['id']]),
                'pull_count' => $stat['count'],
                'last_pulled_at' => $stat['last_at'],
            ];
        }, $items);

        return [
            'items' => $items,
            'pagination' => [
                'page' => $page,
                'per_page' => $perPage,
                'total' => $total,
                'last_page' => $lastPage,
            ],
        ];
    }

    public function movePulledPoolUsers(
        string $campaignId,
        string $sourcePoolId,
        string $targetPoolId
    ): array {
        return $this->movePoolUsersByExposure(
            $campaignId,
            $sourcePoolId,
            $targetPoolId,
            true
        );
    }

    public function moveUnpulledPoolUsers(
        string $campaignId,
        string $sourcePoolId,
        string $targetPoolId
    ): array {
        return $this->movePoolUsersByExposure(
            $campaignId,
            $sourcePoolId,
            $targetPoolId,
            false
        );
    }

    public function searchUsers(string $campaignId, string $keyword): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $keyword = trim($keyword);
        $query = User::query()
            ->whereIn('group_id', $campaign['target_group_ids']);
        if ($keyword !== '') {
            $query->where(function (Builder $builder) use ($keyword): void {
                if (ctype_digit($keyword)) {
                    $builder->orWhere('id', (int) $keyword);
                }
                $builder->orWhere('email', 'like', '%' . $keyword . '%');
            });
        }
        return $query->limit(50)->get(['id', 'email', 'group_id'])
            ->map(function (User $user) use ($campaign): array {
                $override = $campaign['router']['overrides'][(string) $user->id] ?? null;
                $override = $override && $this->overrideIsActive($override)
                    ? $override
                    : null;
                $poolId = $this->effectivePoolId(
                    $campaign,
                    (int) $user->id
                );
                $pool = $campaign['router']['pools'][$poolId] ?? null;
                $overrideHosts = $override ? array_values(array_unique(
                    array_filter(array_merge(
                        [(string) ($override['host'] ?? '')],
                        array_values((array) ($override['node_hosts'] ?? []))
                    ))
                )) : [];
                $poolHosts = $pool ? array_values(array_unique(array_filter(
                    array_merge(
                        [(string) ($pool['host'] ?? '')],
                        array_values((array) ($pool['node_hosts'] ?? []))
                    )
                ))) : [];
                $hosts = $overrideHosts ?: $poolHosts;
                return [
                    'id' => (int) $user->id,
                    'email' => $user->email,
                    'group_id' => (int) $user->group_id,
                    'pool_id' => $poolId,
                    'pool_name' => (string) ($pool['name'] ?? '未分组'),
                    'pool_type' => (string) ($pool['type'] ?? ''),
                    'pool_status' => (string) ($pool['status'] ?? ''),
                    'pool_hosts' => $hosts,
                    'override' => $override,
                ];
            })->values()->all();
    }

    public function overrideUsers(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $rows = $this->userRows(array_map(
            'intval',
            array_keys($campaign['router']['overrides'])
        ));
        $rows = array_values(array_filter(
            $rows,
            fn(array $user): bool => in_array(
                $user['group_id'],
                $campaign['target_group_ids'],
                true
            )
        ));
        return array_map(function (array $user) use ($campaign): array {
            $override = $campaign['router']['overrides'][(string) $user['id']];
            return $user + [
                'pool_id' => $this->effectivePoolId($campaign, $user['id']),
                'override' => $override,
                'active' => $this->overrideIsActive($override),
            ];
        }, $rows);
    }

    public function rollbackRouterConfig(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $snapshot = array_pop($router['config_history']);
        if (!$snapshot) {
            throw new InvalidArgumentException('没有可回滚的配置版本');
        }
        // 新快照只含 pools；旧快照若带 overrides 仍兼容恢复
        $router['pools'] = $snapshot['pools'] ?? $router['pools'];
        if (isset($snapshot['overrides']) && is_array($snapshot['overrides'])) {
            $router['overrides'] = $snapshot['overrides'];
        }
        $router['config_version']++;
        if ($router['enabled']) {
            $this->validateRouterCoverage($campaign);
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function createInvestigationRoot(
        string $campaignId,
        string $poolId,
        string $name = ''
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $sourcePool = $router['pools'][$poolId] ?? null;
        if (!$sourcePool || $sourcePool['tree_node_id'] !== '') {
            throw new InvalidArgumentException('来源用户池不存在或已属于排查树');
        }
        foreach ($router['investigation_nodes'] as $node) {
            if (
                $node['pool_id'] === $poolId
                && $node['status'] !== 'archived'
            ) {
                throw new InvalidArgumentException('该用户池已有进行中的排查树');
            }
        }

        $userIds = array_values(array_filter(
            $this->poolMemberIds($campaign, $poolId),
            fn(int $userId): bool => !$this->overrideBlocksAutomation(
                $router['overrides'][(string) $userId] ?? null
            )
        ));
        if ($userIds === []) {
            throw new InvalidArgumentException('该用户池没有可进入树形排查的固定用户');
        }

        $nodeId = 'tree-' . Str::uuid();
        $rootPoolId = 'tree-pool-' . Str::uuid();
        $sourceHost = (string) ($sourcePool['host'] ?? '');
        $nodeName = trim($name) ?: $sourcePool['name']
            . ($sourceHost !== '' ? " · {$sourceHost}" : '')
            . ' 排查树';
        $router['pools'][$rootPoolId] = $this->normalizePool(array_merge(
            $sourcePool,
            [
                'id' => $rootPoolId,
                'name' => $nodeName . ' / 根组',
                'type' => 'probe',
                'webhook_id' => '',
                'status' => 'blocked',
                'capacity' => count($userIds),
                'overflow_pool_id' => '',
                'tree_node_id' => $nodeId,
                'note' => "来源：{$sourcePool['name']}",
            ]
        ), $rootPoolId);
        foreach ($userIds as $userId) {
            unset($router['overrides'][(string) $userId]);
            $router['assignments'][(string) $userId] = $rootPoolId;
        }
        $router['investigation_nodes'][$nodeId] = $this->normalizeInvestigationNode([
            'id' => $nodeId,
            'root_id' => $nodeId,
            'depth' => 0,
            'name' => $nodeName,
            'status' => 'blocked',
            'pool_id' => $rootPoolId,
            'source_pool_id' => $poolId,
            'user_ids' => $userIds,
            'children' => [],
        ], $nodeId);
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function deleteInvestigationTree(
        string $campaignId,
        string $nodeId
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $root = $router['investigation_nodes'][$nodeId] ?? null;
        if (!$root || (string) ($root['parent_id'] ?? '') !== '') {
            throw new InvalidArgumentException('只能从根节点删除整棵排查树');
        }

        $nodeIds = [];
        $poolIds = [];
        foreach ($router['investigation_nodes'] as $id => $node) {
            if ($id === $nodeId || $node['root_id'] === $nodeId) {
                $nodeIds[] = $id;
                $poolIds[] = $node['pool_id'];
            }
        }
        $poolMap = array_flip(array_values(array_unique($poolIds)));
        $releasedUserIds = [];
        foreach ($router['assignments'] as $userId => $poolId) {
            if (isset($poolMap[$poolId])) {
                unset($router['assignments'][$userId]);
                $releasedUserIds[] = (int) $userId;
            }
        }
        $router['untested_ids'] = $this->normalizeIds(array_merge(
            $router['untested_ids'],
            $releasedUserIds
        ));
        foreach ($router['pools'] as &$pool) {
            if (isset($poolMap[$pool['overflow_pool_id']])) {
                $pool['overflow_pool_id'] = '';
            }
        }
        unset($pool);
        foreach (array_keys($poolMap) as $poolId) {
            unset($router['pools'][$poolId]);
            try {
                Redis::del($this->routerPoolExposureKey($campaign, $poolId));
                Redis::del($this->routerPoolExposureCountKey($campaign, $poolId));
                Redis::del($this->routerPoolExposureLastKey($campaign, $poolId));
            } catch (\Throwable) {
                // 清理统计失败不能阻止删除排查树。
            }
        }
        foreach ($nodeIds as $id) {
            unset($router['investigation_nodes'][$id]);
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return [
            'campaign' => $this->campaignStatus($campaign),
            'released_count' => count(array_unique($releasedUserIds)),
        ];
    }

    public function splitInvestigationNode(
        string $campaignId,
        string $nodeId,
        array $branches
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $node = $router['investigation_nodes'][$nodeId] ?? null;
        if (!$node || $node['children'] !== []) {
            throw new InvalidArgumentException('排查节点不存在或已经拆分');
        }
        if (!in_array($node['status'], ['active', 'blocked'], true)) {
            throw new InvalidArgumentException('只有活动或被墙节点可以继续拆分');
        }
        $branches = $this->normalizeInvestigationBranches($router, $branches);
        $branchCount = count($branches);
        $userIds = array_values(array_filter(
            $node['user_ids'],
            fn(int $userId): bool => !$this->overrideBlocksAutomation(
                $router['overrides'][(string) $userId] ?? null
            )
        ));
        if (count($userIds) < $branchCount) {
            throw new InvalidArgumentException('用户人数少于拆分组数');
        }
        $children = $this->createInvestigationChildren(
            $campaign,
            $node,
            $userIds,
            $branches,
            $nodeId
        );
        $router['investigation_nodes'][$nodeId]['children'] = $children;
        $router['investigation_nodes'][$nodeId]['status'] = 'split';
        $router['investigation_nodes'][$nodeId]['updated_at'] = time();
        if (
            isset($router['pools'][$node['pool_id']])
            && $router['pools'][$node['pool_id']]['tree_node_id'] === $nodeId
        ) {
            $router['pools'][$node['pool_id']]['enabled'] = false;
            $router['pools'][$node['pool_id']]['status'] = 'blocked';
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function setInvestigationNodeStatus(
        string $campaignId,
        string $nodeId,
        string $status
    ): array {
        if (!in_array($status, ['safe', 'blocked'], true)) {
            throw new InvalidArgumentException('节点状态无效');
        }
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $node = $router['investigation_nodes'][$nodeId] ?? null;
        if (!$node || $node['children'] !== []) {
            throw new InvalidArgumentException('只能修改未拆分的叶子节点');
        }
        $releasedUserIds = [];
        if ($status === 'blocked') {
            $releasedUserIds = $this->releaseUnexposedNodeUsers(
                $campaign,
                $nodeId
            );
        }
        $router['investigation_nodes'][$nodeId]['status'] = $status;
        $router['investigation_nodes'][$nodeId]['updated_at'] = time();
        if (
            isset($router['pools'][$node['pool_id']])
            && $router['pools'][$node['pool_id']]['tree_node_id'] === $nodeId
        ) {
            $router['pools'][$node['pool_id']]['status'] =
                $status === 'safe' ? 'active' : 'blocked';
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return [
            'campaign' => $this->campaignStatus($campaign),
            'released_count' => count($releasedUserIds),
            'suspect_count' => count(
                $router['investigation_nodes'][$nodeId]['user_ids']
            ),
        ];
    }

    public function updateInvestigationNodeHost(
        string $campaignId,
        string $nodeId,
        string $host,
        string $webhookId = ''
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $node = $router['investigation_nodes'][$nodeId] ?? null;
        if (!$node || $node['children'] !== [] || $node['status'] === 'archived') {
            throw new InvalidArgumentException('只能修改当前末级分支的域名');
        }
        $pool = $router['pools'][$node['pool_id']] ?? null;
        if (!$pool || $pool['tree_node_id'] !== $nodeId) {
            throw new InvalidArgumentException('分支对应用户池不存在');
        }
        $host = $this->normalizeOptionalHost($host);
        if ($host === '') {
            throw new InvalidArgumentException('域名或IP不能为空');
        }
        $webhookId = trim($webhookId);
        $this->assertWebhookIdAvailable(
            $router,
            $webhookId,
            $node['pool_id']
        );
        $this->assertInvestigationHostAvailable(
            $router,
            $host,
            $node['pool_id']
        );
        $this->snapshotRouterConfig($router);
        $router['pools'][$node['pool_id']]['host'] = $host;
        $router['pools'][$node['pool_id']]['webhook_id'] = $webhookId;
        $router['config_version']++;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return $this->campaignStatus($campaign);
    }

    public function assertIpWebhookSignature(
        string $timestamp,
        string $rawBody,
        string $signature
    ): void {
        $secret = trim((string) ($this->config['ip_webhook_secret'] ?? ''));
        if (strlen($secret) < 32) {
            throw new RuntimeException('IP 轮换接口密钥未配置');
        }
        if (
            !ctype_digit($timestamp)
            || abs(time() - (int) $timestamp) > 300
        ) {
            throw new InvalidArgumentException('请求时间戳无效或已过期');
        }
        $signature = strtolower(trim($signature));
        $expected = hash_hmac(
            'sha256',
            $timestamp . "\n" . $rawBody,
            $secret
        );
        if (
            !preg_match('/^[a-f0-9]{64}$/', $signature)
            || !hash_equals($expected, $signature)
        ) {
            throw new InvalidArgumentException('接口签名无效');
        }
    }

    public function rotateCampaignIp(
        string $campaignId,
        string $oldIp,
        string $newIp,
        string $targetId = '',
        string $reason = 'blocked'
    ): array {
        $oldIp = trim($oldIp);
        $newIp = $this->normalizePublicIp($newIp);
        $targetId = trim($targetId);
        $reason = $this->normalizeRotationReason($reason);
        if ($oldIp !== '') {
            $oldIp = $this->normalizePublicIp($oldIp);
        }
        if ($targetId === '') {
            throw new InvalidArgumentException('必须提供目标唯一标识 target_id');
        }
        if ($oldIp !== '' && $oldIp === $newIp) {
            throw new InvalidArgumentException('新旧 IP 不能相同');
        }

        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        // 换 IP 高频且可逆（再来一次 webhook），不做全量配置快照
        if (isset($router['pools'][$targetId])) {
            $targetPoolIds = [$targetId];
        } elseif (isset($router['investigation_nodes'][$targetId])) {
            $targetPoolIds = [
                $router['investigation_nodes'][$targetId]['pool_id'],
            ];
        } else {
            $matchedPoolId = '';
            foreach ($router['pools'] as $poolId => $pool) {
                if (($pool['webhook_id'] ?? '') === $targetId) {
                    $matchedPoolId = $poolId;
                    break;
                }
            }
            if ($matchedPoolId === '') {
                throw new InvalidArgumentException('目标唯一标识不存在');
            }
            $targetPoolIds = [$matchedPoolId];
        }

        // 诱捕窗口内来源池：首墙只换 IP；二墙拉过武装 IP 的人进观察3。
        if (
            (bool) ($this->config['decoy_enabled'] ?? false)
            && $this->decoyInWindow()
            && count($targetPoolIds) === 1
            && in_array(
                $targetPoolIds[0],
                $this->resolveDecoySourcePoolIds($router),
                true
            )
        ) {
            $poolId = $targetPoolIds[0];
            $nowTs = time();
            if ($reason === 'blocked') {
                $wall = $this->handleDecoyWall(
                    $campaign,
                    $router,
                    $poolId,
                    $oldIp,
                    $newIp,
                    $nowTs
                );
            } else {
                $wall = $this->handleDecoyMachineIp(
                    $router,
                    $poolId,
                    $newIp,
                    $nowTs
                );
            }
            $router['config_version']++;
            $state['campaigns'][$campaignId] = $campaign;
            $this->saveState($state);
            return [
                'campaign_id' => $campaignId,
                'target_id' => $targetId,
                'reason' => $reason,
                'old_ip' => $oldIp,
                'new_ip' => $newIp,
                'wall' => $wall,
                'decoy' => true,
                'host_frozen' => false,
                'updated_pool_ids' => [$poolId],
                'updated_node_host_count' => 0,
                'updated_override_count' => 0,
                'config_version' => $router['config_version'],
            ];
        }

        $updatedPoolIds = [];
        $updatedNodeHostCount = 0;
        foreach ($targetPoolIds as $poolId) {
            $pool = &$router['pools'][$poolId];
            $changed = false;
            if ($oldIp === '') {
                if (($pool['host'] ?? '') !== $newIp) {
                    $pool['host'] = $newIp;
                    $changed = true;
                }
            } elseif (($pool['host'] ?? '') === $oldIp) {
                $pool['host'] = $newIp;
                $changed = true;
            }
            foreach ($pool['node_hosts'] as &$host) {
                if ($oldIp === '') {
                    if ($host !== $newIp) {
                        $host = $newIp;
                        $updatedNodeHostCount++;
                        $changed = true;
                    }
                } elseif ($host === $oldIp) {
                    $host = $newIp;
                    $updatedNodeHostCount++;
                    $changed = true;
                }
            }
            unset($host);
            if ($changed) {
                $updatedPoolIds[] = $poolId;
            }
            unset($pool);
        }

        $updatedOverrideCount = 0;
        foreach ($router['overrides'] as &$override) {
            if (!in_array($override['pool_id'] ?? '', $targetPoolIds, true)) {
                continue;
            }
            $changed = false;
            if ($oldIp === '') {
                if (
                    ($override['host'] ?? '') !== ''
                    && ($override['host'] ?? '') !== $newIp
                ) {
                    $override['host'] = $newIp;
                    $changed = true;
                }
            } elseif (($override['host'] ?? '') === $oldIp) {
                $override['host'] = $newIp;
                $changed = true;
            }
            foreach ($override['node_hosts'] as &$host) {
                if ($oldIp === '') {
                    if ($host !== $newIp) {
                        $host = $newIp;
                        $changed = true;
                    }
                } elseif ($host === $oldIp) {
                    $host = $newIp;
                    $changed = true;
                }
            }
            unset($host);
            if ($changed) {
                $override['updated_at'] = time();
                $updatedOverrideCount++;
            }
        }
        unset($override);

        if ($updatedPoolIds === [] && $updatedOverrideCount === 0) {
            // 主机已是新 IP（人工补录 / 重复投递）→ 幂等成功，不再记分
            $alreadyNew = true;
            foreach ($targetPoolIds as $poolId) {
                if (($router['pools'][$poolId]['host'] ?? '') !== $newIp) {
                    $alreadyNew = false;
                    break;
                }
            }
            if ($alreadyNew) {
                return [
                    'campaign_id' => $campaignId,
                    'target_id' => $targetId,
                    'reason' => $reason,
                    'old_ip' => $oldIp,
                    'new_ip' => $newIp,
                    'wall' => null,
                    'already' => true,
                    'updated_pool_ids' => [],
                    'updated_node_host_count' => 0,
                    'updated_override_count' => 0,
                    'config_version' => $router['config_version'] ?? 0,
                ];
            }
            // old_ip 对不上（诱捕冻结 / 人工改过 / 乱序）：仍强制对齐到 webhook 的新 IP，禁止丢事件
            if ($oldIp !== '') {
                foreach ($targetPoolIds as $poolId) {
                    $pool = &$router['pools'][$poolId];
                    if (($pool['host'] ?? '') === $newIp) {
                        unset($pool);
                        continue;
                    }
                    Log::warning('BaitSplit 换 IP old_ip 不匹配，强制写入 new_ip', [
                        'pool_id' => $poolId,
                        'host' => $pool['host'] ?? '',
                        'old_ip' => $oldIp,
                        'new_ip' => $newIp,
                    ]);
                    $pool['host'] = $newIp;
                    foreach ($pool['node_hosts'] as &$host) {
                        if ($host === $oldIp || $host === '' || $host !== $newIp) {
                            $host = $newIp;
                            $updatedNodeHostCount++;
                        }
                    }
                    unset($host);
                    $updatedPoolIds[] = $poolId;
                    unset($pool);
                }
            }
            if ($updatedPoolIds === [] && $updatedOverrideCount === 0) {
                throw new InvalidArgumentException(
                    $oldIp === ''
                        ? '目标分组已经是该新 IP，无需更新'
                        : '目标分组中没有找到正在使用该旧 IP 的配置'
                );
            }
        }
        $wall = $this->processWallEvent(
            $campaign,
            $router,
            $targetPoolIds,
            $oldIp,
            $newIp,
            $reason
        );
        $router['config_version']++;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return [
            'campaign_id' => $campaignId,
            'target_id' => $targetId,
            'reason' => $reason,
            'old_ip' => $oldIp,
            'new_ip' => $newIp,
            'wall' => $wall,
            'already' => false,
            'updated_pool_ids' => array_values($updatedPoolIds),
            'updated_node_host_count' => $updatedNodeHostCount,
            'updated_override_count' => $updatedOverrideCount,
            'config_version' => $router['config_version'],
        ];
    }

    public function mergeInvestigationNodes(
        string $campaignId,
        array $nodeIds,
        string $name,
        array $branches
    ): array {
        $nodeIds = array_values(array_unique(array_filter(array_map(
            'strval',
            $nodeIds
        ))));
        if ($nodeIds === []) {
            throw new InvalidArgumentException('请至少选择一棵旧排查树');
        }

        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $branches = $this->normalizeInvestigationBranches($router, $branches);
        $sourceRoots = [];
        $sourceNodes = [];
        $userMap = [];
        $releasedUserMap = [];
        foreach ($nodeIds as $nodeId) {
            $root = $router['investigation_nodes'][$nodeId] ?? null;
            if (
                !$root
                || (string) ($root['parent_id'] ?? '') !== ''
                || $root['status'] === 'archived'
            ) {
                throw new InvalidArgumentException('只能合并未归档的根树');
            }
            $sourceRoots[$nodeId] = $root;
        }
        foreach ($sourceRoots as $rootId => $root) {
            foreach ($router['investigation_nodes'] as $sourceNodeId => $node) {
                if ($node['root_id'] !== $rootId) {
                    continue;
                }
                $sourceNodes[$sourceNodeId] = $node;
                if ($node['children'] !== []) {
                    continue;
                }
                $pool = $router['pools'][$node['pool_id']] ?? null;
                if (!$pool || $pool['tree_node_id'] !== $sourceNodeId) {
                    throw new InvalidArgumentException('旧树末级用户池不存在');
                }
                $exposedMap = array_flip(
                    $this->investigationNodeExposureIds($campaign, $node)
                );
                $mergeAllUsers = $node['status'] === 'blocked';
                foreach ($node['user_ids'] as $userId) {
                    if (
                        ($router['assignments'][(string) $userId] ?? '')
                        !== $node['pool_id']
                    ) {
                        continue;
                    }
                    $override = $router['overrides'][(string) $userId] ?? null;
                    if ($this->overrideBlocksAutomation($override)) {
                        $this->detachLockedOverrideFromPool(
                            $router,
                            $userId,
                            $pool
                        );
                        continue;
                    }
                    if ($mergeAllUsers || isset($exposedMap[$userId])) {
                        $userMap[$userId] = true;
                        continue;
                    }
                    unset($router['assignments'][(string) $userId]);
                    $releasedUserMap[$userId] = true;
                }
            }
        }
        $userIds = array_map('intval', array_keys($userMap));
        if (count($userIds) < count($branches)) {
            throw new InvalidArgumentException('可合并用户人数少于新分组数');
        }
        $releasedUserIds = array_map('intval', array_keys($releasedUserMap));
        $router['untested_ids'] = $this->normalizeIds(array_merge(
            $router['untested_ids'],
            $releasedUserIds
        ));

        $rootId = 'tree-' . Str::uuid();
        $rootPoolId = 'tree-pool-' . Str::uuid();
        $rootName = trim($name) ?: '合并排查树 · ' . date('m-d H:i');
        $router['pools'][$rootPoolId] = $this->normalizePool([
            'id' => $rootPoolId,
            'name' => $rootName . ' / 合并根组',
            'type' => 'probe',
            'enabled' => false,
            'status' => 'blocked',
            'capacity' => count($userIds),
            'overflow_pool_id' => '',
            'tree_node_id' => $rootId,
            'note' => '合并来源：' . implode('、', array_map(
                fn(array $node): string => $node['name'],
                $sourceRoots
            )),
        ], $rootPoolId);
        $root = $this->normalizeInvestigationNode([
            'id' => $rootId,
            'root_id' => $rootId,
            'depth' => 0,
            'name' => $rootName,
            'status' => 'split',
            'pool_id' => $rootPoolId,
            'user_ids' => $userIds,
            'source_node_ids' => $nodeIds,
            'children' => [],
        ], $rootId);
        $router['investigation_nodes'][$rootId] = $root;
        $children = $this->createInvestigationChildren(
            $campaign,
            $root,
            $userIds,
            $branches,
            $rootId
        );
        $router['investigation_nodes'][$rootId]['children'] = $children;

        foreach ($sourceNodes as $sourceNodeId => $sourceNode) {
            $sourcePoolId = $sourceNode['pool_id'];
            $this->deletePoolExposureStats($campaign, $sourcePoolId);
            unset($router['pools'][$sourcePoolId]);
            unset($router['investigation_nodes'][$sourceNodeId]);
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return [
            'campaign' => $this->campaignStatus($campaign),
            'merged_count' => count($userIds),
            'released_count' => count($releasedUserIds),
            'source_count' => count($sourceRoots),
        ];
    }

    public function moveInvestigationNodeUsers(
        string $campaignId,
        string $nodeId,
        string $targetPoolId
    ): array {
        return $this->moveInvestigationNodeUsersByExposure(
            $campaignId,
            $nodeId,
            $targetPoolId,
            true
        );
    }

    public function moveUnpulledInvestigationNodeUsers(
        string $campaignId,
        string $nodeId,
        string $targetPoolId
    ): array {
        return $this->moveInvestigationNodeUsersByExposure(
            $campaignId,
            $nodeId,
            $targetPoolId,
            false
        );
    }

    private function moveInvestigationNodeUsersByExposure(
        string $campaignId,
        string $nodeId,
        string $targetPoolId,
        bool $moveExposed
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $node = $router['investigation_nodes'][$nodeId] ?? null;
        if (
            !$node
            || !in_array($node['status'], ['active', 'safe'], true)
            || $node['children'] !== []
        ) {
            throw new InvalidArgumentException(
                '只能转移观察中或已确认安全的叶子节点'
            );
        }
        $targetPool = $router['pools'][$targetPoolId] ?? null;
        if (
            !$targetPool
            || $targetPoolId === $node['pool_id']
            || !$this->poolIsUsable($targetPool)
            || !in_array(
                $targetPool['type'],
                [
                    'default',
                    'probe',
                    'observation',
                    'safe',
                    'custom',
                    'emergency',
                ],
                true
            )
        ) {
            throw new InvalidArgumentException('请选择可用的普通用户池');
        }
        $exposedMap = array_flip(
            $this->investigationNodeExposureIds($campaign, $node)
        );
        $userIds = array_values(array_filter(
            $node['user_ids'],
            function (int $userId) use (
                $router,
                $node,
                $exposedMap,
                $moveExposed
            ): bool {
                return isset($exposedMap[$userId]) === $moveExposed
                    && ($router['assignments'][(string) $userId] ?? '')
                        === $node['pool_id'];
            }
        ));
        if ($userIds === []) {
            throw new InvalidArgumentException(
                $moveExposed
                    ? '该节点没有可转移的已拉取用户'
                    : '该节点没有可转移的未拉取用户'
            );
        }
        $allocations = $this->allocateUsersToPoolChain(
            $campaign,
            $userIds,
            $targetPoolId,
            [
                'default',
                'probe',
                'observation',
                'safe',
                'custom',
                'emergency',
            ]
        );
        foreach ($allocations as $userId => $allocatedPoolId) {
            $router['overrides'][(string) $userId] =
                $this->normalizeOverride([
                    'pool_id' => $allocatedPoolId,
                    'locked' => true,
                    'note' => $moveExposed
                        ? '排查树已拉取用户手动迁移锁定'
                        : '排查树未拉取用户手动迁移锁定',
                    'updated_at' => time(),
                ]);
            $router['assignments'][(string) $userId] = $allocatedPoolId;
        }
        $router['untested_ids'] = array_values(array_diff(
            $router['untested_ids'],
            $userIds
        ));
        $remainingUserIds = array_values(array_diff(
            $node['user_ids'],
            array_map('intval', array_keys($allocations))
        ));
        $router['investigation_nodes'][$nodeId]['user_ids'] =
            $remainingUserIds;
        $router['investigation_nodes'][$nodeId]['updated_at'] = time();
        if (
            isset($router['pools'][$node['pool_id']])
            && $router['pools'][$node['pool_id']]['tree_node_id'] === $nodeId
        ) {
            $router['pools'][$node['pool_id']]['capacity'] =
                count($remainingUserIds);
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return [
            'campaign' => $this->campaignStatus($campaign),
            'moved_count' => count($allocations),
            'remaining_count' => count($remainingUserIds),
        ];
    }

    public function investigationNodeUsers(
        string $campaignId,
        string $nodeId,
        string $keyword = '',
        int $page = 1,
        int $perPage = 50
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $node = $campaign['router']['investigation_nodes'][$nodeId] ?? null;
        if (!$node) {
            throw new InvalidArgumentException('排查节点不存在');
        }
        $query = User::query()->whereIn('id', $node['user_ids']);
        $keyword = trim($keyword);
        if ($keyword !== '') {
            $query->where(function (Builder $builder) use ($keyword): void {
                if (ctype_digit($keyword)) {
                    $builder->orWhere('id', (int) $keyword);
                }
                $builder->orWhere('email', 'like', '%' . $keyword . '%');
            });
        }
        $matchedIds = $query->orderBy('id')->pluck('id')->map('intval')
            ->values()->all();
        $perPage = max(10, min(100, $perPage));
        $total = count($matchedIds);
        $lastPage = max(1, (int) ceil($total / $perPage));
        $page = max(1, min($lastPage, $page));
        $pageIds = array_slice($matchedIds, ($page - 1) * $perPage, $perPage);
        $exposed = array_flip(
            $this->investigationNodeExposureIds($campaign, $node)
        );
        return [
            'items' => array_map(
                fn(array $user): array => $user + [
                    'exposed' => isset($exposed[$user['id']]),
                ],
                $this->userRows($pageIds)
            ),
            'pagination' => [
                'page' => $page,
                'per_page' => $perPage,
                'total' => $total,
                'last_page' => $lastPage,
            ],
        ];
    }

    public function persistMigration(): void
    {
        $raw = admin_setting(self::STATE_KEY, []);
        if (!is_array($raw) || !isset($raw['campaigns'])) {
            $this->saveState($this->migrateLegacyState(is_array($raw) ? $raw : []));
            return;
        }
        $needsMigration = (int) ($raw['version'] ?? 0) < 3;
        foreach ($raw['campaigns'] as $campaign) {
            if (
                !is_array($campaign)
                || !isset($campaign['target_group_ids'])
                || (int) ($campaign['router']['version'] ?? 0) < 5
                || !array_key_exists(
                    'investigation_nodes',
                    (array) ($campaign['router'] ?? [])
                )
            ) {
                $needsMigration = true;
                break;
            }
        }
        if ($needsMigration) {
            $this->saveState($this->state());
        }
    }

    public function saveCampaign(
        ?string $campaignId,
        string $name,
        array $targetGroupIds,
        array $excludedServerIds = []
    ): array {
        $targetGroupIds = $this->normalizeIds($targetGroupIds);
        if ($targetGroupIds === []) {
            throw new InvalidArgumentException('必须选择至少一个用户组');
        }
        $candidateServerIds = $this->managedServerIds($targetGroupIds);
        if ($candidateServerIds === []) {
            throw new InvalidArgumentException('所选用户组暂无可用节点');
        }
        $excludedServerIds = array_values(array_intersect(
            $this->normalizeIds($excludedServerIds),
            $candidateServerIds
        ));
        $targetServerIds = array_values(
            array_diff($candidateServerIds, $excludedServerIds)
        );
        if ($targetServerIds === []) {
            throw new InvalidArgumentException('至少保留一个需要替换域名的节点');
        }

        $state = $this->state();
        $campaignId = $campaignId ?: (string) Str::uuid();
        foreach ($state['campaigns'] as $otherId => $otherCampaign) {
            if (
                $otherId !== $campaignId
                && array_intersect(
                    $targetGroupIds,
                    $otherCampaign['target_group_ids']
                ) !== []
            ) {
                throw new InvalidArgumentException(
                    '所选用户组已属于其他任务，请先从原任务移除'
                );
            }
        }
        $existing = $state['campaigns'][$campaignId] ?? null;
        $groupsChanged = $existing
            && $existing['target_group_ids'] !== $targetGroupIds;
        if (
            $groupsChanged
            && ($existing['serving'] || ($existing['router']['enabled'] ?? false))
        ) {
            throw new InvalidArgumentException('运行中的任务不能修改用户组');
        }
        $campaign = $existing ?: $this->newCampaign($campaignId);
        if (!$existing) {
            $campaign['generation'] = bin2hex(random_bytes(8));
        }
        if ($groupsChanged && !$existing['router']) {
            if ($existing['secret'] !== '' || $existing['round'] > 0) {
                throw new InvalidArgumentException('修改用户组前必须先重置任务');
            }
            $campaign = array_merge(
                $this->newCampaign($campaignId),
                ['name' => $campaign['name']]
            );
        }

        $campaign['name'] = trim($name) ?: '多用户组任务';
        $campaign['target_group_ids'] = $targetGroupIds;
        $campaign['target_group_id'] = $targetGroupIds[0];
        $campaign['target_server_ids'] = $targetServerIds;
        $campaign['excluded_server_ids'] = $excludedServerIds;
        if ($campaign['router'] && $groupsChanged) {
            $this->syncRouterPopulation($campaign);
        }
        if ($campaign['router']['enabled'] ?? false) {
            $this->validateRouterCoverage($campaign);
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function deleteCampaign(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if ($campaign['serving'] || ($campaign['router']['enabled'] ?? false)) {
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

        if ($campaign['router']['enabled'] ?? false) {
            throw new InvalidArgumentException('全量接管运行中，不能启动旧分组任务');
        }
        if ($campaign['enabled']) {
            throw new InvalidArgumentException('当前轮次已经启动');
        }
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
                && ($other['serving'] || ($other['router']['enabled'] ?? false))
                && $this->campaignsShareGroup($other, $campaign)
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
        $campaign['serving'] = true;
        $campaign['branch_hosts'] = $this->replaceActiveBranchHosts($campaign, $domains);
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function replaceDomains(string $campaignId, array $domains): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if (!$campaign['enabled']) {
            throw new InvalidArgumentException('当前没有运行中的轮次');
        }

        $domains = $this->normalizeDomains($domains);
        if (count($domains) !== $campaign['bucket_count']) {
            throw new InvalidArgumentException(
                "该任务固定为 {$campaign['bucket_count']} 组；实时更换时域名数量不能改变"
            );
        }
        if ($domains === $campaign['domains']) {
            throw new InvalidArgumentException('新域名与当前域名相同');
        }

        $campaign['domains'] = $domains;
        $campaign['branch_hosts'] = $this->replaceActiveBranchHosts($campaign, $domains);
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
        $campaign['serving'] = false;
        if ($campaign['router']) {
            $campaign['router']['enabled'] = false;
        }
        $campaign['domains'] = [];
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    public function resetCampaign(string $campaignId): array
    {
        $state = $this->state();
        $campaign = $this->requireCampaign($state, $campaignId);
        if ($campaign['serving'] || ($campaign['router']['enabled'] ?? false)) {
            throw new InvalidArgumentException('请先停止运行中的任务');
        }

        $campaign = array_merge($this->newCampaign($campaignId), [
            'name' => $campaign['name'],
            'target_group_id' => $campaign['target_group_id'],
            'target_group_ids' => $campaign['target_group_ids'],
            'target_server_ids' => $campaign['target_server_ids'],
            'excluded_server_ids' => $campaign['excluded_server_ids'],
            'generation' => bin2hex(random_bytes(8)),
        ]);
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return $this->campaignStatus($campaign);
    }

    private function campaignStatus(array $campaign): array
    {
        $eligibleCount = $this->eligibleUsersQuery($campaign['target_group_ids'])->count();
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
        if ($campaign['enabled'] && $campaign['round'] > 0) {
            foreach ($exposedCounts as $bucket => $_) {
                $path = [...$campaign['active_path'], $bucket];
                foreach ($campaign['branch_hosts'] as $assignment) {
                    if ($assignment['path'] === $path) {
                        $exposedCounts[$bucket] = count(
                            $this->branchExposureUserIds($campaign, $assignment)
                        );
                        break;
                    }
                }
            }
        }

        return [
            'id' => $campaign['id'],
            'name' => $campaign['name'],
            'enabled' => $campaign['enabled'],
            'serving' => $campaign['serving'],
            'target_group_id' => $campaign['target_group_id'],
            'target_group_ids' => $campaign['target_group_ids'],
            'target_server_ids' => $campaign['target_server_ids'],
            'excluded_server_ids' => $campaign['excluded_server_ids'],
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
            'branch_hosts' => array_map(
                fn(array $assignment): array => [
                    'path' => $assignment['path'],
                    'label' => $this->pathLabel($assignment['path']),
                    'host' => $assignment['host'],
                    'round' => $assignment['round'],
                    'bucket' => $assignment['bucket'],
                ],
                $campaign['branch_hosts']
            ),
            'router' => $campaign['router']
                ? $this->routerStatus($campaign)
                : null,
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
            return ['version' => 3, 'campaigns' => $campaigns];
        }

        return $this->migrateLegacyState($raw);
    }

    private function migrateLegacyState(array $legacy): array
    {
        $groupId = (int) ($this->config['target_group_id'] ?? 0);
        $serverIds = $this->normalizeIds($this->config['target_server_ids'] ?? []);
        if ($groupId <= 0 && $legacy === []) {
            return ['version' => 3, 'campaigns' => []];
        }

        $campaign = $this->newCampaign('legacy');
        $campaign['name'] = $groupId > 0 ? '原排查任务' : '待修复原任务';
        $campaign['target_group_id'] = $groupId;
        $campaign['target_group_ids'] = $groupId > 0 ? [$groupId] : [];
        $campaign['target_server_ids'] = $serverIds;
        $campaign['enabled'] = $groupId > 0 && (bool) ($legacy['enabled'] ?? false);
        $campaign['serving'] = $campaign['enabled'];
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
        if ($campaign['serving'] && $campaign['domains'] !== []) {
            $campaign['branch_hosts'] = $this->replaceActiveBranchHosts(
                $campaign,
                $campaign['domains']
            );
        }

        return ['version' => 3, 'campaigns' => ['legacy' => $campaign]];
    }

    private function saveState(array $state): void
    {
        $campaigns = [];
        foreach ((array) ($state['campaigns'] ?? []) as $id => $campaign) {
            $router = $campaign['router'] ?? null;
            if (is_array($router)) {
                // 调用方普遍持有 $router = &$campaign['router']，引用会跟着数组拷贝一起传进来。
                // 不先 unset 断开，压缩就会写回调用方内存，存盘后的读取路径会撞上被剔掉的字段。
                unset($campaign['router']);
                $campaign['router'] = $this->compactRouterState($router);
            }
            $campaigns[$id] = $campaign;
        }
        $state['campaigns'] = $campaigns;
        app(Setting::class)->set(self::STATE_KEY, $state);
    }

    /**
     * 压缩路由状态：历史快照只保留池配置（不含 overrides），最多 5 份。
     * 旧版把每次改池/换 IP 的全量 overrides 打进 history，轻松撑爆 settings.value。
     */
    private function compactRouterState(array $router): array
    {
        $slim = [];
        foreach (
            array_slice(array_values((array) ($router['config_history'] ?? [])), -5) as $snap
        ) {
            if (!is_array($snap)) {
                continue;
            }
            $slim[] = [
                'version' => (int) ($snap['version'] ?? 0),
                'pools' => is_array($snap['pools'] ?? null) ? $snap['pools'] : [],
                'created_at' => (int) ($snap['created_at'] ?? 0),
            ];
        }
        $router['config_history'] = $slim;

        // overrides 一条 267 字节里有一半是空默认值，读回时 normalizeOverride 会补齐，
        // 存盘时剔掉能让整行状态小一半，每次读写都跟着快
        $overrides = [];
        foreach ((array) ($router['overrides'] ?? []) as $userId => $override) {
            if (!is_array($override)) {
                continue;
            }
            $slimOverride = [];
            foreach ($override as $field => $value) {
                if (
                    $value === '' || $value === []
                    || ($field === 'expires_at' && (int) $value === 0)
                    || ($field === 'locked' && $value === true)
                ) {
                    continue;
                }
                $slimOverride[$field] = $value;
            }
            $overrides[$userId] = $slimOverride;
        }
        $router['overrides'] = $overrides;
        return $router;
    }

    private function newCampaign(string $id): array
    {
        return [
            'id' => $id,
            'name' => '',
            'target_group_id' => 0,
            'target_group_ids' => [],
            'target_server_ids' => [],
            'excluded_server_ids' => [],
            'enabled' => false,
            'serving' => false,
            'generation' => '',
            'secret' => '',
            'hash_algo' => 'mod',
            'round' => 0,
            'bucket_count' => 0,
            'domains' => [],
            'active_path' => [],
            'positive_queue' => [],
            'deferred_queue' => [],
            'branch_hosts' => [],
            'router' => null,
            'findings' => [],
            'history' => [],
        ];
    }

    private function normalizeCampaign(array $campaign, string $id): array
    {
        $hasHashAlgorithm = array_key_exists('hash_algo', $campaign);
        $hasServing = array_key_exists('serving', $campaign);
        $hasBranchHosts = array_key_exists('branch_hosts', $campaign);
        $campaign = array_merge($this->newCampaign($id), $campaign);
        $campaign['id'] = $id;
        $campaign['target_group_id'] = (int) $campaign['target_group_id'];
        $campaign['target_group_ids'] = $this->normalizeIds(
            $campaign['target_group_ids'] ?: [$campaign['target_group_id']]
        );
        $campaign['target_group_id'] = $campaign['target_group_ids'][0] ?? 0;
        $campaign['target_server_ids'] = $this->normalizeIds($campaign['target_server_ids']);
        $campaign['excluded_server_ids'] = $this->normalizeIds($campaign['excluded_server_ids']);
        $campaign['bucket_count'] = (int) $campaign['bucket_count'];
        $campaign['generation'] = (string) $campaign['generation'];
        $campaign['hash_algo'] = $id === 'legacy' && !$hasHashAlgorithm
            ? 'bit'
            : ($campaign['hash_algo'] === 'bit' ? 'bit' : 'mod');
        $campaign['active_path'] = $this->normalizePath($campaign['active_path']);
        $campaign['positive_queue'] = $this->normalizePaths($campaign['positive_queue']);
        $campaign['deferred_queue'] = $this->normalizePaths($campaign['deferred_queue']);
        $campaign['domains'] = is_array($campaign['domains']) ? array_values($campaign['domains']) : [];
        $campaign['serving'] = $hasServing
            ? (bool) $campaign['serving']
            : (bool) $campaign['enabled'];
        $campaign['branch_hosts'] = $this->normalizeBranchHosts($campaign['branch_hosts']);
        $campaign['router'] = is_array($campaign['router'])
            ? $this->normalizeRouter($campaign['router'])
            : null;
        if (!$hasBranchHosts && $campaign['serving'] && $campaign['domains'] !== []) {
            $campaign['branch_hosts'] = $this->replaceActiveBranchHosts(
                $campaign,
                $campaign['domains']
            );
        }
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
        if ($campaign['secret'] === '' || $campaign['target_group_ids'] === []) {
            return [];
        }

        $path ??= $campaign['active_path'];
        return $this->eligibleUsersQuery($campaign['target_group_ids'])
            ->pluck('id')
            ->map(fn($id): int => (int) $id)
            ->filter(fn(int $id): bool => $this->matchesPath($id, $campaign, $path))
            ->values()
            ->all();
    }

    /**
     * 可用用户 ID 集合（缓存 60 秒）。
     * 面板每次翻页/转组都要用，全量 pluck 三万多行要 600ms+，缓存后基本为零。
     *
     * @return array<int,true>
     */
    private function eligibleUserIdSet(array $groupIds): array
    {
        $ids = $this->normalizeIds($groupIds);
        sort($ids);
        return Cache::remember(
            'bait_split:eligible_ids:' . md5(implode(',', $ids)),
            60,
            fn(): array => array_fill_keys(
                $this->eligibleUsersQuery($ids)->pluck('id')->map('intval')->all(),
                true
            )
        );
    }

    /**
     * 池成员 ID：直接从 overrides / assignments 反查，不再全表扫描。
     *
     * @return int[] 已按 ID 升序
     */
    private function poolMemberIds(array $campaign, string $poolId): array
    {
        $router = $campaign['router'];
        $eligible = $this->eligibleUserIdSet($campaign['target_group_ids']);
        $claimed = [];
        $mine = [];
        foreach ($router['overrides'] as $key => $override) {
            $uid = (int) $key;
            if ($uid <= 0 || ($override['pool_id'] ?? '') === '') {
                continue;
            }
            if (!$this->overrideIsActive($override)) {
                continue;
            }
            $claimed[$uid] = true;
            if ($override['pool_id'] === $poolId && isset($eligible[$uid])) {
                $mine[] = $uid;
            }
        }
        foreach ($router['assignments'] as $key => $assigned) {
            $uid = (int) $key;
            if ($uid <= 0 || isset($claimed[$uid])) {
                continue;
            }
            $claimed[$uid] = true;
            if ((string) $assigned === $poolId && isset($eligible[$uid])) {
                $mine[] = $uid;
            }
        }
        // 没有任何归属的用户实际吃的是默认组的 IP，转组时必须算进来
        if (($router['pools'][$poolId]['type'] ?? '') === 'default') {
            foreach (array_keys($eligible) as $uid) {
                if (!isset($claimed[$uid])) {
                    $mine[] = $uid;
                }
            }
        }
        sort($mine);
        return $mine;
    }

    private function eligibleUsersQuery(int|array $groupIds): Builder
    {
        $groupIds = $this->normalizeIds(is_array($groupIds) ? $groupIds : [$groupIds]);
        return User::query()
            ->whereIn('group_id', $groupIds)
            ->where('banned', 0)
            ->where('transfer_enable', '>', 0)
            ->where(function ($query) {
                $query->whereNull('expired_at')
                    ->orWhere('expired_at', '>', time());
            });
    }

    private function campaignMatchesGroup(array $campaign, int $groupId): bool
    {
        return in_array($groupId, $campaign['target_group_ids'], true);
    }

    private function campaignsShareGroup(array $left, array $right): bool
    {
        return array_intersect(
            $left['target_group_ids'],
            $right['target_group_ids']
        ) !== [];
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

    private function releaseUnexposedNodeUsers(
        array &$campaign,
        string $nodeId
    ): array {
        $router = &$campaign['router'];
        $node = $router['investigation_nodes'][$nodeId];
        $exposedMap = array_flip(
            $this->investigationNodeExposureIds($campaign, $node)
        );
        $retainedUserIds = [];
        $releasedUserIds = [];
        foreach ($node['user_ids'] as $userId) {
            $override = $router['overrides'][(string) $userId] ?? null;
            if ($this->overrideBlocksAutomation($override)) {
                $retainedUserIds[] = $userId;
                continue;
            }
            if (
                ($router['assignments'][(string) $userId] ?? '')
                !== $node['pool_id']
            ) {
                continue;
            }
            if (isset($exposedMap[$userId])) {
                $retainedUserIds[] = $userId;
                continue;
            }
            unset($router['assignments'][(string) $userId]);
            $releasedUserIds[] = $userId;
        }
        $retainedUserIds = $this->normalizeIds($retainedUserIds);
        $router['investigation_nodes'][$nodeId]['user_ids'] = $retainedUserIds;
        $router['investigation_nodes'][$nodeId]['released_count'] =
            (int) ($node['released_count'] ?? 0)
            + count($releasedUserIds);
        if (isset($router['pools'][$node['pool_id']])) {
            $router['pools'][$node['pool_id']]['capacity'] =
                count($retainedUserIds);
        }
        $router['untested_ids'] = $this->normalizeIds(array_merge(
            $router['untested_ids'],
            $releasedUserIds
        ));
        return $releasedUserIds;
    }

    private function pruneMergedSourceTrees(array &$campaign): int
    {
        if (!is_array($campaign['router'] ?? null)) {
            return 0;
        }
        $router = &$campaign['router'];
        $sourceRootIds = [];
        foreach ($router['investigation_nodes'] as $node) {
            foreach ($node['source_node_ids'] as $sourceNodeId) {
                $sourceNode = $router['investigation_nodes'][$sourceNodeId]
                    ?? null;
                if ($sourceNode && $sourceNode['root_id'] !== $node['root_id']) {
                    $sourceRootIds[$sourceNode['root_id']] = true;
                }
            }
        }
        if ($sourceRootIds === []) {
            return 0;
        }

        $nodeIds = [];
        $poolIds = [];
        foreach ($router['investigation_nodes'] as $nodeId => $node) {
            if (isset($sourceRootIds[$node['root_id']])) {
                $nodeIds[] = $nodeId;
                if ($node['pool_id'] !== '') {
                    $poolIds[$node['pool_id']] = true;
                }
            }
        }
        foreach ($router['assignments'] as $userId => $poolId) {
            if (!isset($poolIds[$poolId])) {
                continue;
            }
            $userId = (int) $userId;
            $override = $router['overrides'][(string) $userId] ?? null;
            if ($this->overrideBlocksAutomation($override)) {
                $pool = $router['pools'][$poolId] ?? null;
                if ($pool) {
                    $this->detachLockedOverrideFromPool(
                        $router,
                        $userId,
                        $pool
                    );
                } else {
                    unset($router['assignments'][(string) $userId]);
                }
                continue;
            }
            unset($router['assignments'][(string) $userId]);
            $router['untested_ids'][] = $userId;
        }
        $router['untested_ids'] = $this->normalizeIds(
            $router['untested_ids']
        );
        foreach (array_keys($poolIds) as $poolId) {
            $this->deletePoolExposureStats($campaign, $poolId);
            unset($router['pools'][$poolId]);
        }
        foreach ($nodeIds as $nodeId) {
            unset($router['investigation_nodes'][$nodeId]);
        }
        return count($sourceRootIds);
    }

    private function deletePoolExposureStats(
        array $campaign,
        string $poolId
    ): void {
        try {
            Redis::del($this->routerPoolExposureKey($campaign, $poolId));
            Redis::del($this->routerPoolExposureCountKey($campaign, $poolId));
            Redis::del($this->routerPoolExposureLastKey($campaign, $poolId));
        } catch (\Throwable) {
            // 统计清理失败不能阻止主状态更新。
        }
    }

    private function normalizeInvestigationBranches(
        array $router,
        array $branches
    ): array {
        if (count($branches) < 2 || count($branches) > 10) {
            throw new InvalidArgumentException('每次必须拆分为 2 至 10 组');
        }
        $hosts = [];
        foreach ($branches as $index => &$branch) {
            $host = $this->normalizeOptionalHost($branch['host'] ?? '');
            if ($host === '') {
                throw new InvalidArgumentException('每个分支都必须填写域名或IP');
            }
            if (isset($hosts[$host])) {
                throw new InvalidArgumentException('同一次拆分不能使用重复域名');
            }
            $hosts[$host] = true;
            $branch['host'] = $host;
            $branch['name'] = trim((string) ($branch['name'] ?? ''))
                ?: '分支 ' . $this->branchLabel($index);
        }
        unset($branch);
        foreach (array_keys($hosts) as $host) {
            $this->assertInvestigationHostAvailable($router, $host);
        }
        return $branches;
    }

    private function assertInvestigationHostAvailable(
        array $router,
        string $host,
        string $ignoredPoolId = ''
    ): void {
        foreach ($router['pools'] as $poolId => $pool) {
            if ($poolId === $ignoredPoolId) {
                continue;
            }
            $poolHosts = array_merge(
                [(string) ($pool['host'] ?? '')],
                array_values((array) ($pool['node_hosts'] ?? []))
            );
            if (in_array($host, $poolHosts, true)) {
                throw new InvalidArgumentException(
                    "域名 {$host} 已被其他用户池使用"
                );
            }
        }
    }

    private function assertWebhookIdAvailable(
        array $router,
        string $webhookId,
        string $ignoredPoolId = ''
    ): void {
        if ($webhookId === '') {
            return;
        }
        if (!preg_match('/^[A-Za-z0-9._:-]{1,100}$/', $webhookId)) {
            throw new InvalidArgumentException(
                '接口标识只能包含字母、数字、点、横线、下划线和冒号'
            );
        }
        foreach ($router['pools'] as $poolId => $pool) {
            if ($poolId === $ignoredPoolId) {
                continue;
            }
            if (
                $poolId === $webhookId
                || ($pool['webhook_id'] ?? '') === $webhookId
            ) {
                throw new InvalidArgumentException('接口标识已被其他用户池使用');
            }
        }
        foreach ($router['investigation_nodes'] as $node) {
            if (
                $node['pool_id'] !== $ignoredPoolId
                && ($node['id'] ?? '') === $webhookId
            ) {
                throw new InvalidArgumentException('接口标识已被其他排查分支使用');
            }
        }
    }

    private function createInvestigationChildren(
        array &$campaign,
        array $parent,
        array $userIds,
        array $branches,
        string $shuffleKey
    ): array {
        $router = &$campaign['router'];
        usort($userIds, fn(int $left, int $right): int => strcmp(
            hash_hmac(
                'sha256',
                "{$shuffleKey}:{$left}",
                $router['secret']
            ),
            hash_hmac(
                'sha256',
                "{$shuffleKey}:{$right}",
                $router['secret']
            )
        ));
        $branchCount = count($branches);
        $baseSize = intdiv(count($userIds), $branchCount);
        $remainder = count($userIds) % $branchCount;
        $offset = 0;
        $children = [];
        foreach ($branches as $index => $branch) {
            $size = $baseSize + ($index < $remainder ? 1 : 0);
            $branchUsers = array_slice($userIds, $offset, $size);
            $offset += $size;
            $childId = 'tree-' . Str::uuid();
            $poolId = 'tree-pool-' . Str::uuid();
            $childName = $parent['name'] . ' / ' . $branch['name'];
            $router['pools'][$poolId] = $this->normalizePool([
                'id' => $poolId,
                'name' => $childName,
                'type' => 'probe',
                'host' => $branch['host'],
                'enabled' => true,
                'status' => 'active',
                'capacity' => count($branchUsers),
                'overflow_pool_id' => '',
                'tree_node_id' => $childId,
                'note' => "父节点：{$parent['name']}",
            ], $poolId);
            foreach ($branchUsers as $userId) {
                unset($router['overrides'][(string) $userId]);
                $router['assignments'][(string) $userId] = $poolId;
            }
            $router['investigation_nodes'][$childId] =
                $this->normalizeInvestigationNode([
                    'id' => $childId,
                    'parent_id' => $parent['id'],
                    'root_id' => $parent['root_id'],
                    'depth' => $parent['depth'] + 1,
                    'name' => $childName,
                    'status' => 'active',
                    'pool_id' => $poolId,
                    'user_ids' => $branchUsers,
                    'children' => [],
                ], $childId);
            $children[] = $childId;
        }
        return $children;
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

    private function normalizePingHost(string $host): string
    {
        $host = strtolower(trim($host));
        if (str_contains($host, '://')) {
            $host = (string) parse_url($host, PHP_URL_HOST);
        }
        $host = rtrim($host, '.');
        if (
            $host === ''
            || strlen($host) > 253
            || (
                !filter_var($host, FILTER_VALIDATE_IP)
                && !filter_var($host, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME)
            )
        ) {
            throw new InvalidArgumentException('请输入有效的域名或 IP');
        }
        return $host;
    }

    private function boceApiKey(): string
    {
        $key = trim((string) admin_setting('bait_split_boce_api_key', ''));
        if (!preg_match('/^[A-Za-z0-9]{16,128}$/', $key)) {
            throw new InvalidArgumentException('未配置有效的拨测 API Key');
        }
        return $key;
    }

    private function fetchBocePingNodeIds(string $key): array
    {
        $response = Http::timeout(20)->retry(2, 500)->get(
            'https://api.boce.com/v3/node/list',
            ['key' => $key]
        );
        $payload = $response->json();
        $nodes = (array) ($payload['data']['list'] ?? []);
        if (
            !$response->successful()
            || (int) ($payload['error_code'] ?? -1) !== 0
            || $nodes === []
        ) {
            return [];
        }

        $selected = [];
        foreach (['电信', '联通', '移动'] as $isp) {
            $regions = [];
            foreach ($nodes as $node) {
                if (
                    !is_array($node)
                    || !str_contains((string) ($node['isp_name'] ?? ''), $isp)
                    || isset($regions[(string) ($node['node_name'] ?? '')])
                ) {
                    continue;
                }
                $nodeId = (int) ($node['id'] ?? 0);
                if ($nodeId <= 0) {
                    continue;
                }
                $selected[] = $nodeId;
                $regions[(string) ($node['node_name'] ?? '')] = true;
                if (count($regions) >= 2) {
                    break;
                }
            }
        }
        if ($selected === []) {
            $selected = array_map(
                fn(array $node): int => (int) ($node['id'] ?? 0),
                array_slice(array_filter($nodes, 'is_array'), 0, 6)
            );
        }
        return $this->normalizeIds($selected);
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

    private function normalizeBranchHosts(mixed $assignments): array
    {
        if (!is_array($assignments)) {
            return [];
        }

        return array_values(array_filter(array_map(
            function ($assignment): ?array {
                if (!is_array($assignment) || trim((string) ($assignment['host'] ?? '')) === '') {
                    return null;
                }
                return [
                    'path' => $this->normalizePath($assignment['path'] ?? []),
                    'host' => trim((string) $assignment['host']),
                    'round' => max(0, (int) ($assignment['round'] ?? 0)),
                    'bucket' => max(0, (int) ($assignment['bucket'] ?? 0)),
                ];
            },
            $assignments
        )));
    }

    private function replaceActiveBranchHosts(array $campaign, array $domains): array
    {
        $assignments = array_values(array_filter(
            $campaign['branch_hosts'],
            fn(array $assignment): bool => !$this->pathStartsWith(
                $assignment['path'],
                $campaign['active_path']
            )
        ));

        foreach ($domains as $bucket => $host) {
            $assignments[] = [
                'path' => [...$campaign['active_path'], $bucket],
                'host' => $host,
                'round' => $campaign['round'],
                'bucket' => $bucket,
            ];
        }

        usort(
            $assignments,
            fn(array $left, array $right): int => $left['path'] <=> $right['path']
        );
        return $assignments;
    }

    private function pathStartsWith(array $path, array $prefix): bool
    {
        return array_slice($path, 0, count($prefix)) === $prefix;
    }

    private function assignmentForUser(array $campaign, int $userId): ?array
    {
        $assignments = $campaign['branch_hosts'];
        usort(
            $assignments,
            fn(array $left, array $right): int => count($right['path']) <=> count($left['path'])
        );
        foreach ($assignments as $assignment) {
            if ($this->matchesPath($userId, $campaign, $assignment['path'])) {
                return $assignment;
            }
        }
        return null;
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

    private function filterRoutedServers(array $servers, User $user, array $campaign): array
    {
        $router = $campaign['router'];
        $userId = (int) $user->id;
        $override = $router['overrides'][(string) $userId] ?? null;
        $override = $override && $this->overrideIsActive($override) ? $override : null;
        $poolIds = $this->routingPoolIds($campaign, $userId, $override);
        $result = [];
        $deliveredPoolIds = [];
        $managedServerIds = array_flip($campaign['target_server_ids']);

        foreach ($servers as $server) {
            $serverId = (int) ($server['id'] ?? 0);
            if (!isset($managedServerIds[$serverId])) {
                $result[] = $server;
                continue;
            }
            $host = $override
                ? $this->hostFromRule($override, $serverId)
                : '';
            $selectedPool = null;
            foreach ($poolIds as $poolId) {
                $candidate = $router['pools'][$poolId] ?? null;
                if (!$candidate || !$this->poolIsUsable($candidate)) {
                    continue;
                }
                $candidateHost = $this->hostFromRule($candidate, $serverId);
                if ($candidateHost === '') {
                    continue;
                }
                $selectedPool = $candidate;
                if ($host === '') {
                    $host = $candidateHost;
                }
                break;
            }
            if ($host === '') {
                continue;
            }
            $server['host'] = $host;
            $this->applyConnectionOverrides(
                $server,
                $override,
                $selectedPool,
                null
            );
            // 只要成功下发托管节点就记拉取（含个人 host 覆盖），供二墙按武装窗口筛人
            if ($selectedPool) {
                $deliveredPoolIds[$selectedPool['id']] = true;
            }
            $assignPool = (string) (
                ($override['pool_id'] ?? '') !== ''
                    ? $override['pool_id']
                    : ($router['assignments'][(string) $userId] ?? '')
            );
            if ($assignPool !== '' && isset($router['pools'][$assignPool])) {
                $deliveredPoolIds[$assignPool] = true;
            }
            $result[] = $server;
        }

        $deliveredPoolIds = array_keys($deliveredPoolIds);
        $this->recordRouterExposure(
            $campaign,
            $userId,
            $deliveredPoolIds
        );
        return $result;
    }

    private function routingPoolIds(
        array $campaign,
        int $userId,
        ?array $override
    ): array {
        $router = $campaign['router'];
        $poolIds = [];
        if ($override && $override['pool_id'] !== '') {
            $poolIds[] = $override['pool_id'];
        } elseif (!$override) {
            if (isset($router['assignments'][(string) $userId])) {
                $poolIds[] = $router['assignments'][(string) $userId];
            }
        }
        $poolIds[] = $this->poolIdByType($router, 'default');
        $poolIds[] = $this->poolIdByType($router, 'emergency');
        return array_values(array_unique(array_filter($poolIds)));
    }

    private function managedServerIds(array $groupIds): array
    {
        return Server::query()
            ->where('enabled', 1)
            ->where('show', 1)
            ->where(function (Builder $query) use ($groupIds): void {
                foreach ($groupIds as $groupId) {
                    $query->orWhereJsonContains('group_ids', (string) $groupId);
                }
            })
            ->pluck('id')
            ->map('intval')
            ->values()
            ->all();
    }

    private function normalizeDecoyDomain(array $domain): array
    {
        $current = null;
        if (is_array($domain['current'] ?? null)) {
            $ids = $this->normalizeIds($domain['current']['ids'] ?? []);
            if ($ids !== []) {
                $current = [
                    'ids' => $ids,
                    'batch' => max(1, (int) ($domain['current']['batch'] ?? 1)),
                    'hold' => (string) ($domain['current']['hold'] ?? ''),
                    'started_at' => (int) ($domain['current']['started_at'] ?? 0),
                ];
            }
        }
        $queue = [];
        foreach ((array) ($domain['queue'] ?? []) as $item) {
            if (!is_array($item)) {
                continue;
            }
            $ids = $this->normalizeIds($item['ids'] ?? []);
            if ($ids === []) {
                continue;
            }
            $queue[] = [
                'ids' => $ids,
                'batch' => max(1, (int) ($item['batch'] ?? 1)),
                'hold' => (string) ($item['hold'] ?? ''),
            ];
        }
        $phase = (string) ($domain['phase'] ?? '');
        if (!in_array($phase, ['idle', 'armed', 'testing'], true)) {
            // 兼容旧数据：有队列/在测批视为 testing，否则 idle
            $phase = ($current !== null || $queue !== []) ? 'testing' : 'idle';
        }
        return [
            'phase' => $phase,
            'armed' => (bool) ($domain['armed'] ?? ($phase !== 'idle')),
            'armed_ip' => (string) ($domain['armed_ip'] ?? ''),
            'armed_at' => (int) ($domain['armed_at'] ?? 0),
            'cur_ip' => (string) ($domain['cur_ip'] ?? ''),
            'dead_ip' => (string) ($domain['dead_ip'] ?? ''),
            'current' => $current,
            'queue' => $queue,
            'dispatched_at' => (int) ($domain['dispatched_at'] ?? 0),
            'group_size' => max(0, (int) ($domain['group_size'] ?? 0)),
            'proven' => (bool) ($domain['proven'] ?? false),
        ];
    }

    private function newRouter(array $userIds): array
    {
        $generation = bin2hex(random_bytes(8));
        $pools = [];
        foreach ([
            ['default', '默认组', 'default'],
            ['danger', '危险组', 'danger'],
            ['probe', '当前测试组', 'probe'],
            ['emergency', '应急组', 'emergency'],
            ['blacklist', '封禁组', 'blacklist'],
        ] as [$id, $name, $type]) {
            $pools[$id] = $this->normalizePool([
                'id' => $id,
                'name' => $name,
                'type' => $type,
                'enabled' => $id !== 'blacklist',
            ], $id);
        }
        return [
            'version' => 5,
            'enabled' => false,
            'generation' => $generation,
            'secret' => bin2hex(random_bytes(32)),
            'config_version' => 1,
            'config_history' => [],
            'pools' => $pools,
            'assignments' => [],
            'overrides' => [],
            'snapshot_user_ids' => array_values($userIds),
            'untested_ids' => array_values($userIds),
            'investigation_nodes' => [],
            'wall_hits' => [],
            'wall_score' => [],
            'wall_last' => [],
            'wall_log' => [],
            'decoy' => [
                'active_date' => '',
                'domains' => [],
            ],
            'decoy_nights' => [],
            'search_origin' => [],
            'suspect_queue' => [],
        ];
    }

    private function normalizeRouter(array $router): array
    {
        $defaults = $this->newRouter([]);
        $router = array_merge($defaults, $router);
        unset(
            $router['batch_size'],
            $router['active_batch'],
            $router['danger_batches'],
            $router['history']
        );
        $router['version'] = 5;
        $router['enabled'] = (bool) $router['enabled'];
        $router['generation'] = (string) $router['generation'];
        $router['secret'] = (string) $router['secret'];
        $router['config_version'] = max(1, (int) $router['config_version']);
        $router['config_history'] = is_array($router['config_history'])
            ? array_slice($router['config_history'], -5)
            : [];
        // 丢掉历史里的全量 overrides（旧数据瘦身）
        foreach ($router['config_history'] as &$snap) {
            if (is_array($snap)) {
                unset($snap['overrides']);
            }
        }
        unset($snap);
        $pools = [];
        foreach ((array) $router['pools'] as $id => $pool) {
            if (is_array($pool)) {
                $poolId = (string) ($pool['id'] ?? $id);
                $pools[$poolId] = $this->normalizePool($pool, $poolId);
            }
        }
        if (!isset($pools['blacklist'])) {
            $pools['blacklist'] = $defaults['pools']['blacklist'];
        }
        $router['pools'] = $pools ?: $defaults['pools'];
        $router['assignments'] = array_filter(
            array_map('strval', (array) $router['assignments']),
            fn(string $poolId): bool => isset($router['pools'][$poolId])
        );
        $overrides = [];
        foreach ((array) $router['overrides'] as $userId => $override) {
            if (is_array($override) && (int) $userId > 0) {
                $normalized = $this->normalizeOverride($override);
                if (
                    $normalized['pool_id'] !== ''
                    || $normalized['host'] !== ''
                    || $normalized['node_hosts'] !== []
                ) {
                    $overrides[(string) (int) $userId] = $normalized;
                }
            }
        }
        $router['overrides'] = $overrides;
        $router['snapshot_user_ids'] = $this->normalizeIds($router['snapshot_user_ids']);
        $router['untested_ids'] = $this->normalizeIds($router['untested_ids']);
        $wallLast = [];
        foreach ((array) ($router['wall_last'] ?? []) as $userId => $ts) {
            if ((int) $userId > 0) {
                $wallLast[(string) (int) $userId] = (int) $ts;
            }
        }
        $wallRetain = time() - 14 * 86400;
        $wallHits = [];
        foreach ((array) ($router['wall_hits'] ?? []) as $userId => $count) {
            $key = (string) (int) $userId;
            if ((int) $userId <= 0 || (int) $count <= 0) {
                continue;
            }
            if (($wallLast[$key] ?? 0) < $wallRetain) {
                continue;
            }
            $wallHits[$key] = (int) $count;
        }
        $router['wall_hits'] = $wallHits;
        $wallScore = [];
        foreach ((array) ($router['wall_score'] ?? []) as $userId => $score) {
            $key = (string) (int) $userId;
            if (!isset($wallHits[$key]) || (float) $score <= 0) {
                continue;
            }
            $wallScore[$key] = round((float) $score, 4);
        }
        $router['wall_score'] = $wallScore;
        $router['wall_last'] = array_intersect_key($wallLast, $wallHits);
        $searchOrigin = [];
        foreach ((array) ($router['search_origin'] ?? []) as $userId => $poolId) {
            $key = (string) (int) $userId;
            if ((int) $userId <= 0 || (string) $poolId === '') {
                continue;
            }
            $searchOrigin[$key] = (string) $poolId;
        }
        $router['search_origin'] = $searchOrigin;
        $suspectQueue = [];
        foreach ((array) ($router['suspect_queue'] ?? []) as $item) {
            if (!is_array($item)) {
                continue;
            }
            $ids = $this->normalizeIds($item['ids'] ?? []);
            if ($ids === []) {
                continue;
            }
            $suspectQueue[] = [
                'ids' => $ids,
                'origin' => (string) ($item['origin'] ?? ''),
                'at' => (int) ($item['at'] ?? 0),
            ];
        }
        $router['suspect_queue'] = array_slice($suspectQueue, -50);
        $router['wall_log'] = is_array($router['wall_log'] ?? null)
            ? array_slice(array_values($router['wall_log']), -200)
            : [];
        $decoy = is_array($router['decoy'] ?? null) ? $router['decoy'] : [];
        $domains = [];
        foreach ((array) ($decoy['domains'] ?? []) as $poolId => $domain) {
            $poolId = (string) $poolId;
            if ($poolId === '' || !is_array($domain)) {
                continue;
            }
            $domains[$poolId] = $this->normalizeDecoyDomain($domain);
        }
        $router['decoy'] = [
            'active_date' => (string) ($decoy['active_date'] ?? ''),
            'domains' => $domains,
        ];
        $decoyNights = [];
        foreach ((array) ($router['decoy_nights'] ?? []) as $userId => $count) {
            if ((int) $userId > 0 && (int) $count > 0) {
                $decoyNights[(string) (int) $userId] = (int) $count;
            }
        }
        $router['decoy_nights'] = $decoyNights;
        $investigationNodes = [];
        foreach ((array) ($router['investigation_nodes'] ?? []) as $id => $node) {
            if (
                is_array($node)
                && !(bool) ($node['legacy'] ?? false)
                && !str_starts_with((string) ($node['id'] ?? $id), 'legacy-')
            ) {
                $nodeId = (string) ($node['id'] ?? $id);
                $normalizedNode = $this->normalizeInvestigationNode(
                    $node,
                    $nodeId
                );
                if (
                    $normalizedNode['source_pool_id'] === ''
                    && $normalizedNode['depth'] === 0
                ) {
                    $treePool = $router['pools'][$normalizedNode['pool_id']]
                        ?? null;
                    $note = (string) ($treePool['note'] ?? '');
                    if (str_starts_with($note, '来源：')) {
                        $sourceName = mb_substr($note, 3);
                        foreach ($router['pools'] as $sourcePoolId => $sourcePool) {
                            if (
                                $sourcePool['tree_node_id'] === ''
                                && $sourcePool['name'] === $sourceName
                            ) {
                                $normalizedNode['source_pool_id'] =
                                    $sourcePoolId;
                                break;
                            }
                        }
                    }
                }
                $investigationNodes[$nodeId] = $normalizedNode;
            }
        }
        $router['investigation_nodes'] = $investigationNodes;
        foreach ($router['pools'] as $poolId => $pool) {
            $treeNodeId = (string) ($pool['tree_node_id'] ?? '');
            if (
                $treeNodeId !== ''
                && !isset($router['investigation_nodes'][$treeNodeId])
            ) {
                unset($router['pools'][$poolId]);
            }
        }
        $router['assignments'] = array_filter(
            $router['assignments'],
            fn(string $poolId): bool => isset($router['pools'][$poolId])
        );
        foreach ($router['overrides'] as $userId => &$override) {
            if (
                $override['pool_id'] !== ''
                && !isset($router['pools'][$override['pool_id']])
            ) {
                $override['pool_id'] = '';
                if (
                    $override['host'] === ''
                    && $override['node_hosts'] === []
                ) {
                    unset($router['overrides'][$userId]);
                }
            }
        }
        unset($override);
        return $router;
    }

    private function normalizePool(array $pool, string $poolId): array
    {
        $type = (string) ($pool['type'] ?? 'custom');
        $status = (string) ($pool['status'] ?? 'available');
        $strategy = (string) ($pool['strategy'] ?? 'manual');
        if (!in_array($type, ['default', 'danger', 'blacklist', 'probe', 'observation', 'emergency', 'safe', 'custom'], true)) {
            $type = 'custom';
        }
        $host = $this->normalizeOptionalHost($pool['host'] ?? '');
        $nodeHosts = [];
        foreach ((array) ($pool['node_hosts'] ?? []) as $serverId => $nodeHost) {
            $serverId = (int) $serverId;
            $nodeHost = $this->normalizeOptionalHost($nodeHost);
            if ($serverId > 0 && $nodeHost !== '') {
                $nodeHosts[(string) $serverId] = $nodeHost;
            }
        }
        return [
            'id' => $poolId,
            'webhook_id' => trim((string) ($pool['webhook_id'] ?? '')),
            'name' => trim((string) ($pool['name'] ?? $poolId)) ?: $poolId,
            'type' => $type,
            'host' => $host,
            'node_hosts' => $nodeHosts,
            'server_name' => $this->normalizeOptionalHost($pool['server_name'] ?? ''),
            'transport_host' => $this->normalizeOptionalHost($pool['transport_host'] ?? ''),
            'enabled' => (bool) ($pool['enabled'] ?? true),
            'status' => in_array($status, ['available', 'active', 'suspected', 'blocked', 'standby'], true)
                ? $status
                : 'available',
            'capacity' => max(0, (int) ($pool['capacity'] ?? 0)),
            'overflow_pool_id' => trim((string) ($pool['overflow_pool_id'] ?? '')),
            'tree_node_id' => trim((string) ($pool['tree_node_id'] ?? '')),
            'strategy' => in_array($strategy, ['manual', 'least', 'round_robin'], true)
                ? $strategy
                : 'manual',
            'note' => trim((string) ($pool['note'] ?? '')),
            'last_rotation_at' => max(0, (int) ($pool['last_rotation_at'] ?? 0)),
        ];
    }

    private function normalizeOverride(array $override): array
    {
        $nodeHosts = [];
        foreach ((array) ($override['node_hosts'] ?? []) as $serverId => $host) {
            $serverId = (int) $serverId;
            $host = $this->normalizeOptionalHost($host);
            if ($serverId > 0 && $host !== '') {
                $nodeHosts[(string) $serverId] = $host;
            }
        }
        return [
            'pool_id' => trim((string) ($override['pool_id'] ?? '')),
            'host' => $this->normalizeOptionalHost($override['host'] ?? ''),
            'node_hosts' => $nodeHosts,
            'server_name' => $this->normalizeOptionalHost($override['server_name'] ?? ''),
            'transport_host' => $this->normalizeOptionalHost($override['transport_host'] ?? ''),
            'locked' => (bool) ($override['locked'] ?? true),
            'note' => mb_substr(trim((string) ($override['note'] ?? '')), 0, 200),
            'expires_at' => max(0, (int) ($override['expires_at'] ?? 0)),
            'updated_at' => (int) ($override['updated_at'] ?? time()),
        ];
    }

    private function normalizeInvestigationNode(array $node, string $nodeId): array
    {
        $status = (string) ($node['status'] ?? 'active');
        return [
            'id' => $nodeId,
            'parent_id' => ($node['parent_id'] ?? null) !== null
                ? (string) $node['parent_id']
                : null,
            'root_id' => (string) ($node['root_id'] ?? $nodeId),
            'depth' => max(0, (int) ($node['depth'] ?? 0)),
            'name' => trim((string) ($node['name'] ?? '排查节点')) ?: '排查节点',
            'status' => in_array(
                $status,
                ['active', 'safe', 'blocked', 'split', 'archived'],
                true
            ) ? $status : 'active',
            'pool_id' => (string) ($node['pool_id'] ?? ''),
            'source_pool_id' => (string) ($node['source_pool_id'] ?? ''),
            'user_ids' => $this->normalizeIds($node['user_ids'] ?? []),
            'released_count' => max(0, (int) ($node['released_count'] ?? 0)),
            'source_node_ids' => array_values(array_unique(array_filter(
                array_map('strval', (array) ($node['source_node_ids'] ?? []))
            ))),
            'children' => array_values(array_unique(array_map(
                'strval',
                (array) ($node['children'] ?? [])
            ))),
            'created_at' => (int) ($node['created_at'] ?? time()),
            'updated_at' => (int) ($node['updated_at'] ?? time()),
        ];
    }

    private function routerStatus(array $campaign): array
    {
        $router = $campaign['router'];
        $eligibleMap = $this->eligibleUserIdSet($campaign['target_group_ids']);
        $eligibleIds = array_keys($eligibleMap);
        $poolCounts = array_fill_keys(array_keys($router['pools']), 0);
        $poolMemberIds = array_fill_keys(array_keys($router['pools']), []);
        $classifiedPoolIds = [];
        foreach ($eligibleIds as $userId) {
            $poolId = $this->classifiedPoolId($campaign, $userId);
            $classifiedPoolIds[$userId] = $poolId;
            if ($poolId !== null && isset($poolCounts[$poolId])) {
                $poolCounts[$poolId]++;
                $poolMemberIds[$poolId][] = $userId;
            }
        }
        $pools = [];
        $pulledUserMap = [];
        foreach ($router['pools'] as $pool) {
            $exposureIds = $this->poolExposureIds($campaign, $pool['id']);
            $pool['member_count'] = $poolCounts[$pool['id']] ?? 0;
            $pool['pulled_count'] = count(array_intersect(
                $poolMemberIds[$pool['id']] ?? [],
                $exposureIds
            ));
            foreach ($exposureIds as $userId) {
                if (isset($eligibleMap[$userId])) {
                    $pulledUserMap[$userId] = true;
                }
            }
            $pools[] = $pool;
        }
        $groupedCount = 0;
        $unpulledUngroupedCount = 0;
        foreach ($eligibleIds as $userId) {
            $override = $router['overrides'][(string) $userId] ?? null;
            $grouped = $classifiedPoolIds[$userId] !== null
                || ($override && $this->overrideIsActive($override));
            if ($grouped) {
                $groupedCount++;
            } elseif (!isset($pulledUserMap[$userId])) {
                $unpulledUngroupedCount++;
            }
        }
        $investigationNodes = [];
        foreach ($router['investigation_nodes'] as $node) {
            $pool = $router['pools'][$node['pool_id']] ?? null;
            $exposedIds = $pool
                ? $this->investigationNodeExposureIds($campaign, $node)
                : [];
            $exposedMap = array_flip($exposedIds);
            $mergeableCount = count(array_filter(
                $node['user_ids'],
                function (int $userId) use (
                    $router,
                    $node,
                    $exposedMap
                ): bool {
                    $override = $router['overrides'][(string) $userId] ?? null;
                    return !$this->overrideBlocksAutomation($override)
                        && (
                            $node['status'] === 'blocked'
                            || isset($exposedMap[$userId])
                        )
                        && ($router['assignments'][(string) $userId] ?? '')
                            === $node['pool_id'];
                }
            ));
            $investigationNodes[] = [
                'id' => $node['id'],
                'parent_id' => $node['parent_id'],
                'root_id' => $node['root_id'],
                'depth' => $node['depth'],
                'name' => $node['name'],
                'status' => $node['status'],
                'pool_id' => $node['pool_id'],
                'webhook_id' => (string) ($pool['webhook_id'] ?? ''),
                'host' => (string) ($pool['host'] ?? ''),
                'user_count' => count($node['user_ids']),
                'mergeable_count' => $mergeableCount,
                'released_count' => $node['released_count'],
                'source_node_ids' => $node['source_node_ids'],
                'pulled_count' => count(array_intersect(
                    $node['user_ids'],
                    $exposedIds
                )),
                'children' => $node['children'],
                'created_at' => $node['created_at'],
                'updated_at' => $node['updated_at'],
            ];
        }
        return [
            'enabled' => $router['enabled'],
            'config_version' => $router['config_version'],
            'pools' => $pools,
            'snapshot_count' => count($router['snapshot_user_ids']),
            'untested_count' => count($router['untested_ids']),
            'pulled_user_count' => count($pulledUserMap),
            'grouped_user_count' => $groupedCount,
            'unpulled_ungrouped_count' => $unpulledUngroupedCount,
            'override_count' => count(array_filter(
                $router['overrides'],
                fn(array $override, string|int $userId): bool =>
                    isset($eligibleMap[(int) $userId])
                    && $this->overrideIsActive($override),
                ARRAY_FILTER_USE_BOTH
            )),
            'investigation_nodes' => $investigationNodes,
        ];
    }

    private function effectivePoolId(array $campaign, int $userId): string
    {
        return $this->classifiedPoolId($campaign, $userId)
            ?? $this->poolIdByType($campaign['router'], 'default');
    }

    private function classifiedPoolId(array $campaign, int $userId): ?string
    {
        $router = $campaign['router'];
        $override = $router['overrides'][(string) $userId] ?? null;
        if ($override && $this->overrideIsActive($override) && $override['pool_id'] !== '') {
            return $override['pool_id'];
        }
        return $router['assignments'][(string) $userId] ?? null;
    }

    private function overrideIsActive(array $override): bool
    {
        $expiresAt = (int) ($override['expires_at'] ?? 0);
        return $expiresAt <= 0 || $expiresAt > time();
    }

    private function overrideBlocksAutomation(?array $override): bool
    {
        return $override !== null
            && $this->overrideIsActive($override)
            && (bool) $override['locked'];
    }

    private function detachLockedOverrideFromPool(
        array &$router,
        int $userId,
        array $pool
    ): void {
        $override = $router['overrides'][(string) $userId] ?? null;
        if ($override && $override['pool_id'] === $pool['id']) {
            $override['pool_id'] = '';
            foreach ([
                'host',
                'node_hosts',
                'server_name',
                'transport_host',
            ] as $field) {
                if (empty($override[$field])) {
                    $override[$field] = $pool[$field] ?? (
                        $field === 'node_hosts' ? [] : ''
                    );
                }
            }
            $router['overrides'][(string) $userId] =
                $this->normalizeOverride($override);
        }
        unset($router['assignments'][(string) $userId]);
    }

    private function poolIdByType(array $router, string $type): string
    {
        foreach ($router['pools'] as $pool) {
            if ($pool['type'] === $type) {
                return $pool['id'];
            }
        }
        return '';
    }

    private function hostFromRule(array $rule, int $serverId): string
    {
        return (string) (($rule['node_hosts'][(string) $serverId] ?? '') ?: ($rule['host'] ?? ''));
    }

    private function applyConnectionOverrides(
        array &$server,
        ?array $override,
        ?array $pool,
        ?array $emergency
    ): void {
        $rules = array_values(array_filter([$override, $pool, $emergency]));
        $serverName = '';
        $transportHost = '';
        foreach ($rules as $rule) {
            $serverName = $serverName ?: (string) ($rule['server_name'] ?? '');
            $transportHost = $transportHost ?: (string) ($rule['transport_host'] ?? '');
        }
        if ($serverName !== '') {
            $type = (string) ($server['type'] ?? '');
            $tlsMode = (int) data_get($server, 'protocol_settings.tls', 0);
            if (in_array($type, ['hysteria', 'tuic', 'anytls'], true)) {
                data_set($server, 'protocol_settings.tls.server_name', $serverName);
            } elseif ($tlsMode === 2) {
                data_set($server, 'protocol_settings.reality_settings.server_name', $serverName);
            } else {
                data_set($server, 'protocol_settings.tls_settings.server_name', $serverName);
            }
        }
        if ($transportHost !== '') {
            $network = (string) data_get($server, 'protocol_settings.network', '');
            if ($network === 'tcp') {
                data_set(
                    $server,
                    'protocol_settings.network_settings.header.request.headers.Host',
                    [$transportHost]
                );
            } elseif ($network === 'ws') {
                data_set(
                    $server,
                    'protocol_settings.network_settings.headers.Host',
                    $transportHost
                );
            } elseif ($network === 'h2') {
                data_set(
                    $server,
                    'protocol_settings.network_settings.host',
                    [$transportHost]
                );
            } else {
                data_set(
                    $server,
                    'protocol_settings.network_settings.host',
                    $transportHost
                );
                data_set(
                    $server,
                    'protocol_settings.network_settings.headers.Host',
                    $transportHost
                );
            }
        }
    }

    private function normalizeOptionalHost(mixed $host): string
    {
        $host = strtolower(trim((string) $host));
        if ($host === '') {
            return '';
        }
        $valid = filter_var($host, FILTER_VALIDATE_IP)
            || filter_var($host, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME);
        if (!$valid) {
            throw new InvalidArgumentException("无效的域名或 IP：{$host}");
        }
        return $host;
    }

    private function normalizePublicIp(mixed $ip): string
    {
        $ip = trim((string) $ip);
        if (!filter_var(
            $ip,
            FILTER_VALIDATE_IP,
            FILTER_FLAG_IPV4
                | FILTER_FLAG_NO_PRIV_RANGE
                | FILTER_FLAG_NO_RES_RANGE
        )) {
            throw new InvalidArgumentException("无效的公网 IPv4：{$ip}");
        }
        return $ip;
    }

    private function validateRouterCoverage(array $campaign): void
    {
        $router = $campaign['router'];
        foreach (['default', 'danger', 'probe', 'emergency'] as $type) {
            $poolId = $this->poolIdByType($router, $type);
            if (
                $poolId === ''
                || !$this->poolIsUsable($router['pools'][$poolId] ?? [])
            ) {
                throw new InvalidArgumentException("缺少已启用的{$type}用户池");
            }
            $this->validatePoolCoverage($campaign, $router['pools'][$poolId]);
        }
        foreach ($router['pools'] as $pool) {
            if ($this->poolIsUsable($pool)) {
                $this->validatePoolCoverage($campaign, $pool);
            }
        }
    }

    private function poolIsUsable(array $pool): bool
    {
        return (bool) ($pool['enabled'] ?? false)
            && ($pool['status'] ?? 'blocked') !== 'blocked';
    }

    private function movePoolUsersByExposure(
        string $campaignId,
        string $sourcePoolId,
        string $targetPoolId,
        bool $moveExposed
    ): array {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        if ($sourcePoolId === $targetPoolId) {
            throw new InvalidArgumentException('来源组和目标组不能相同');
        }
        if (!isset($router['pools'][$sourcePoolId])) {
            throw new InvalidArgumentException('来源用户池不存在');
        }
        $targetPool = $router['pools'][$targetPoolId] ?? null;
        if (
            !$targetPool
            || !$this->poolIsUsable($targetPool)
            || in_array($targetPool['type'], ['danger', 'blacklist'], true)
        ) {
            throw new InvalidArgumentException('目标用户池不存在、不可用或为危险/封禁组');
        }

        $exposedMap = array_flip($this->poolExposureIds($campaign, $sourcePoolId));
        $userIds = array_values(array_filter(
            $this->poolMemberIds($campaign, $sourcePoolId),
            fn(int $userId): bool => isset($exposedMap[$userId]) === $moveExposed
        ));
        if ($userIds === []) {
            throw new InvalidArgumentException(
                $moveExposed
                    ? '该用户池没有可转移的已拉取用户'
                    : '该用户池没有可转移的未拉取用户'
            );
        }

        $allowedTypes = ['default', 'probe', 'observation', 'emergency', 'safe', 'custom'];
        $allocations = $this->allocateUsersToPoolChain(
            $campaign,
            $userIds,
            $targetPoolId,
            $allowedTypes
        );
        $now = time();
        foreach ($allocations as $userId => $allocatedPoolId) {
            $router['overrides'][(string) $userId] = $this->normalizeOverride([
                'pool_id' => $allocatedPoolId,
                'locked' => true,
                'note' => $moveExposed
                    ? '用户池已拉取用户手动迁移锁定'
                    : '用户池未拉取用户手动迁移锁定',
                'updated_at' => $now,
            ]);
            $router['assignments'][(string) $userId] = $allocatedPoolId;
        }
        $router['untested_ids'] = array_values(array_diff(
            $router['untested_ids'],
            $userIds
        ));
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return [
            'campaign' => $this->campaignStatus($campaign),
            'moved_count' => count($allocations),
        ];
    }

    private function assignPoolOnFirstPull(array $campaign, User $user): array
    {
        $userId = (int) $user->id;
        if (
            (bool) $user->banned
            || (int) $user->transfer_enable <= 0
            || ((int) $user->expired_at > 0 && (int) $user->expired_at <= time())
            || isset($campaign['router']['assignments'][(string) $userId])
        ) {
            return $campaign;
        }
        $override = $campaign['router']['overrides'][(string) $userId] ?? null;
        if ($override && $this->overrideIsActive($override)) {
            return $campaign;
        }

        try {
            Cache::lock(self::STATE_LOCK, 15)
                ->get(function () use (&$campaign, $userId): void {
                    $state = $this->state();
                    $latest = $this->requireRouterCampaign(
                        $state,
                        $campaign['id']
                    );
                    if (
                        !$latest['router']['enabled']
                        || isset(
                            $latest['router']['assignments'][(string) $userId]
                        )
                    ) {
                        $campaign = $latest;
                        return;
                    }
                    $override = $latest['router']['overrides'][
                        (string) $userId
                    ] ?? null;
                    if ($override && $this->overrideIsActive($override)) {
                        $campaign = $latest;
                        return;
                    }

                    $memberCounts = [];
                    foreach ($latest['router']['snapshot_user_ids'] as $memberId) {
                        $memberPoolId = $this->classifiedPoolId(
                            $latest,
                            $memberId
                        );
                        if ($memberPoolId !== null) {
                            $memberCounts[$memberPoolId] = (
                                $memberCounts[$memberPoolId] ?? 0
                            ) + 1;
                        }
                    }
                    $poolId = $this->poolIdByType(
                        $latest['router'],
                        'default'
                    );
                    $visited = [];
                    while ($poolId !== '') {
                        if (isset($visited[$poolId])) {
                            break;
                        }
                        $visited[$poolId] = true;
                        $pool = $latest['router']['pools'][$poolId] ?? null;
                        if (!$pool || !$this->poolIsUsable($pool)) {
                            break;
                        }
                        if (
                            $pool['capacity'] === 0
                            || ($memberCounts[$poolId] ?? 0) < $pool['capacity']
                        ) {
                            $latest['router']['assignments'][
                                (string) $userId
                            ] = $poolId;
                            $state['campaigns'][$latest['id']] = $latest;
                            $this->saveState($state);
                            $campaign = $latest;
                            return;
                        }
                        $poolId = (string) $pool['overflow_pool_id'];
                    }
                    $emergencyPoolId = $this->poolIdByType(
                        $latest['router'],
                        'emergency'
                    );
                    $emergencyPool = $latest['router']['pools'][
                        $emergencyPoolId
                    ] ?? null;
                    if (
                        $emergencyPoolId !== ''
                        && $this->poolIsUsable($emergencyPool ?? [])
                        && (
                            $emergencyPool['capacity'] === 0
                            || ($memberCounts[$emergencyPoolId] ?? 0)
                                < $emergencyPool['capacity']
                        )
                    ) {
                        $latest['router']['assignments'][
                            (string) $userId
                        ] = $emergencyPoolId;
                        $state['campaigns'][$latest['id']] = $latest;
                        $this->saveState($state);
                    }
                    $campaign = $latest;
                });
        } catch (\Throwable) {
            // 并发抢锁或配置变化时暂用默认池，下次拉取继续尝试。
        }
        return $campaign;
    }

    private function allocateUsersToPoolChain(
        array $campaign,
        array $userIds,
        string $targetPoolId,
        array $allowedTypes
    ): array {
        $router = $campaign['router'];
        $chain = [];
        $visited = [];
        $poolId = $targetPoolId;
        while ($poolId !== '') {
            if (isset($visited[$poolId])) {
                throw new InvalidArgumentException('用户池满员转入配置存在循环');
            }
            $visited[$poolId] = true;
            $pool = $router['pools'][$poolId] ?? null;
            if (!$pool || !$this->poolIsUsable($pool)) {
                throw new InvalidArgumentException('满员转入链包含不可用用户池');
            }
            if (!in_array($pool['type'], $allowedTypes, true)) {
                throw new InvalidArgumentException('满员转入用户池类型不匹配');
            }
            $chain[$poolId] = $pool;
            $poolId = (string) ($pool['overflow_pool_id'] ?? '');
        }

        $memberCounts = array_fill_keys(array_keys($chain), 0);
        foreach ($router['snapshot_user_ids'] as $memberId) {
            $memberPoolId = $this->classifiedPoolId($campaign, $memberId);
            if ($memberPoolId !== null && isset($memberCounts[$memberPoolId])) {
                $memberCounts[$memberPoolId]++;
            }
        }

        $allocations = [];
        foreach ($userIds as $userId) {
            $currentPoolId = $this->effectivePoolId($campaign, $userId);
            $allocated = false;
            foreach ($chain as $candidateId => $candidate) {
                if ($currentPoolId === $candidateId) {
                    $allocations[$userId] = $candidateId;
                    $allocated = true;
                    break;
                }
                if (
                    $candidate['capacity'] === 0
                    || $memberCounts[$candidateId] < $candidate['capacity']
                ) {
                    $allocations[$userId] = $candidateId;
                    $memberCounts[$candidateId]++;
                    $allocated = true;
                    break;
                }
            }
            if (!$allocated) {
                throw new InvalidArgumentException('目标用户池及后续用户池容量均已满');
            }
        }
        return $allocations;
    }

    private function validatePoolCoverage(array $campaign, array $pool): void
    {
        foreach ($campaign['target_server_ids'] as $serverId) {
            if ($this->hostFromRule($pool, $serverId) === '') {
                throw new InvalidArgumentException("用户池“{$pool['name']}”未覆盖节点 {$serverId}");
            }
        }
    }

    private function validatePoolOverflowChain(array $pools, string $startPoolId): void
    {
        $startPool = $pools[$startPoolId];
        $allowedTypes = match ($startPool['type']) {
            'danger' => ['danger', 'custom'],
            'blacklist' => [],
            default => [
                'default',
                'probe',
                'observation',
                'emergency',
                'safe',
                'custom',
            ],
        };
        $visited = [];
        $poolId = $startPoolId;
        while ($poolId !== '') {
            if (isset($visited[$poolId])) {
                throw new InvalidArgumentException('用户池满员转入配置存在循环');
            }
            $visited[$poolId] = true;
            $pool = $pools[$poolId] ?? null;
            if (!$pool) {
                throw new InvalidArgumentException('满员后转入的用户池不存在');
            }
            if (
                $poolId !== $startPoolId
                && !in_array($pool['type'], $allowedTypes, true)
            ) {
                throw new InvalidArgumentException('满员后转入的用户池类型不匹配');
            }
            $poolId = (string) ($pool['overflow_pool_id'] ?? '');
        }
    }

    private function validateAllPoolOverflowChains(array $pools): void
    {
        foreach (array_keys($pools) as $poolId) {
            $this->validatePoolOverflowChain($pools, (string) $poolId);
        }
    }

    private function assertPoolTypeUniqueness(array $pools): void
    {
        $counts = [];
        foreach ($pools as $pool) {
            if (in_array($pool['type'], ['default', 'danger', 'blacklist', 'emergency'], true)) {
                $counts[$pool['type']] = ($counts[$pool['type']] ?? 0) + 1;
                if ($counts[$pool['type']] > 1) {
                    throw new InvalidArgumentException("{$pool['type']} 类型只能有一个用户池");
                }
            }
        }
    }

    private function snapshotRouterConfig(array &$router): void
    {
        // 只快照池配置，供回滚域名/分组结构；不拷贝 overrides（上万条会把 state 撑爆）
        $router['config_history'][] = [
            'version' => $router['config_version'],
            'pools' => $router['pools'],
            'created_at' => time(),
        ];
        $router['config_history'] = array_slice($router['config_history'], -5);
    }

    private function requireRouterCampaign(array $state, string $campaignId): array
    {
        $campaign = $this->requireCampaign($state, $campaignId);
        if (!$campaign['router']) {
            throw new InvalidArgumentException('请先初始化域名调度系统');
        }
        return $campaign;
    }

    private function syncRouterPopulation(array &$campaign): void
    {
        $router = &$campaign['router'];
        $eligibleMap = $this->eligibleUserIdSet($campaign['target_group_ids']);
        $eligible = array_keys($eligibleMap);
        $known = array_flip($router['snapshot_user_ids']);
        foreach ($eligible as $userId) {
            if (!isset($known[$userId])) {
                $router['snapshot_user_ids'][] = $userId;
                $router['untested_ids'][] = $userId;
            }
        }
        $filterIds = fn(array $ids): array => array_values(array_filter(
            $ids,
            fn(int $id): bool => isset($eligibleMap[$id])
        ));
        $router['snapshot_user_ids'] = $filterIds($router['snapshot_user_ids']);
        $router['untested_ids'] = $filterIds($router['untested_ids']);
        $router['assignments'] = array_filter(
            $router['assignments'],
            fn(string $poolId, string|int $userId): bool =>
                isset($eligibleMap[(int) $userId]),
            ARRAY_FILTER_USE_BOTH
        );
        $router['overrides'] = array_filter(
            $router['overrides'],
            fn(array $override, string|int $userId): bool =>
                isset($eligibleMap[(int) $userId]),
            ARRAY_FILTER_USE_BOTH
        );
        foreach ($router['investigation_nodes'] as $nodeId => &$node) {
            $node['user_ids'] = $filterIds($node['user_ids']);
            $node['updated_at'] = time();
            $poolId = $node['pool_id'];
            if (isset($router['pools'][$poolId])) {
                $router['pools'][$poolId]['capacity'] = count(
                    $node['user_ids']
                );
                if ($node['user_ids'] === []) {
                    $router['pools'][$poolId]['enabled'] = false;
                    $node['status'] = 'archived';
                }
            }
        }
        unset($node);
    }

    private function branchLabel(int $index): string
    {
        return chr(65 + max(0, min(25, $index)));
    }

    private function userRows(array $userIds): array
    {
        if ($userIds === []) {
            return [];
        }
        $order = array_flip($userIds);
        return User::query()
            ->whereIn('id', $userIds)
            ->get(['id', 'email', 'group_id'])
            ->sortBy(fn(User $user): int => $order[(int) $user->id] ?? PHP_INT_MAX)
            ->map(fn(User $user): array => [
                'id' => (int) $user->id,
                'email' => $user->email,
                'group_id' => (int) $user->group_id,
            ])->values()->all();
    }

    private function recordRouterExposure(
        array $campaign,
        int $userId,
        array $poolIds
    ): void {
        try {
            foreach ($poolIds as $poolId) {
                $poolKey = $this->routerPoolExposureKey($campaign, $poolId);
                Redis::sadd($poolKey, (string) $userId);
                Redis::expire($poolKey, 86400 * 90);
                $countKey = $this->routerPoolExposureCountKey($campaign, $poolId);
                $lastKey = $this->routerPoolExposureLastKey($campaign, $poolId);
                Redis::hincrby($countKey, (string) $userId, 1);
                Redis::hset($lastKey, (string) $userId, time());
                Redis::expire($countKey, 86400 * 90);
                Redis::expire($lastKey, 86400 * 90);
            }
        } catch (\Throwable) {
            // 统计失败不能影响订阅。
        }
    }

    private function poolExposureIds(array $campaign, string $poolId): array
    {
        try {
            return $this->normalizeIds(Redis::smembers(
                $this->routerPoolExposureKey($campaign, $poolId)
            ));
        } catch (\Throwable) {
            return [];
        }
    }

    private function investigationNodeExposureIds(
        array $campaign,
        array $node
    ): array {
        $exposedIds = $this->poolExposureIds(
            $campaign,
            (string) $node['pool_id']
        );
        $sourcePoolId = (string) ($node['source_pool_id'] ?? '');
        if ($sourcePoolId !== '') {
            $exposedIds = array_merge(
                $exposedIds,
                $this->poolExposureIds($campaign, $sourcePoolId)
            );
        }
        return $this->normalizeIds(array_intersect(
            $node['user_ids'],
            $exposedIds
        ));
    }

    private function poolExposureStats(
        array $campaign,
        string $poolId,
        array $userIds
    ): array {
        $stats = [];
        try {
            $countKey = $this->routerPoolExposureCountKey($campaign, $poolId);
            $lastKey = $this->routerPoolExposureLastKey($campaign, $poolId);
            foreach ($userIds as $userId) {
                $userId = (int) $userId;
                $stats[$userId] = [
                    'count' => (int) Redis::hget($countKey, (string) $userId),
                    'last_at' => (int) Redis::hget($lastKey, (string) $userId),
                ];
            }
        } catch (\Throwable) {
            // 统计不可用时返回空值，不能影响用户列表。
        }
        return $stats;
    }

    public function wallReport(string $campaignId, int $limit = 100): array
    {
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = $campaign['router'];
        $limit = max(1, min(200, $limit));
        $events = array_reverse(
            array_slice($router['wall_log'] ?? [], -$limit)
        );
        $hits = $router['wall_hits'] ?? [];
        // 优先按加权分排（小批命中权重高），没有分的退回按次数
        $scores = $router['wall_score'] ?? [];
        $rank = [];
        foreach ($hits as $userId => $count) {
            $rank[$userId] = (float) ($scores[$userId] ?? 0);
        }
        arsort($rank);
        arsort($hits);
        $topIds = array_slice(
            array_keys(array_sum($rank) > 0 ? $rank : $hits),
            0,
            50,
            true
        );
        $emails = User::query()
            ->whereIn('id', array_map('intval', array_values($topIds)))
            ->pluck('email', 'id');
        $topSuspects = [];
        foreach ($topIds as $userId) {
            $topSuspects[] = [
                'user_id' => (int) $userId,
                'email' => (string) ($emails[(int) $userId] ?? ''),
                'hits' => (int) ($hits[$userId] ?? 0),
                'score' => round((float) ($scores[$userId] ?? 0), 3),
                'nights' => (int) ($router['decoy_nights'][(string) $userId] ?? 0),
                'last_at' => (int) ($router['wall_last'][$userId] ?? 0),
            ];
        }
        $poolName = fn(string $poolId): string =>
            (string) ($router['pools'][$poolId]['name'] ?? $poolId);
        $decoy = $router['decoy'];
        $decoyDomains = [];
        $testingCount = 0;
        foreach ((array) ($decoy['domains'] ?? []) as $poolId => $domain) {
            if (!is_array($domain)) {
                continue;
            }
            $cur = is_array($domain['current'] ?? null) ? $domain['current'] : null;
            $curCount = $cur ? count($this->normalizeIds($cur['ids'] ?? [])) : 0;
            $testingCount += $curCount;
            $queueUsers = 0;
            foreach ((array) ($domain['queue'] ?? []) as $item) {
                $queueUsers += count($this->normalizeIds($item['ids'] ?? []));
            }
            $phase = (string) ($domain['phase'] ?? 'idle');
            $decoyDomains[] = [
                'pool_id' => (string) $poolId,
                'pool_name' => $poolName((string) $poolId),
                'phase' => $phase,
                'armed_ip' => (string) ($domain['armed_ip'] ?? ''),
                'cur_ip' => (string) ($domain['cur_ip'] ?? ''),
                'current_batch' => $curCount,
                'current_level' => $cur ? (int) ($cur['batch'] ?? 0) : 0,
                'started_at' => $cur ? (int) ($cur['started_at'] ?? 0) : 0,
                'queue_items' => count((array) ($domain['queue'] ?? [])),
                'queue_users' => $queueUsers,
                'group_size' => (int) ($domain['group_size'] ?? 0),
                'dispatched_at' => (int) ($domain['dispatched_at'] ?? 0),
                'proven' => (bool) ($domain['proven'] ?? false),
            ];
        }
        $decoySourceIds = $this->resolveDecoySourcePoolIds($router);
        return [
            'events' => $events,
            'top_suspects' => $topSuspects,
            'total_tracked' => count($router['wall_hits'] ?? []),
            'settings' => [
                'auto_isolate' => $this->configBool('wall_auto_isolate', true),
                'hit_threshold' => max(1, (int) ($this->config['wall_hit_threshold'] ?? 2)),
                'lookback_seconds' => max(60, (int) ($this->config['wall_lookback_seconds'] ?? 3600)),
                'fresh_max_seconds' => max(300, (int) ($this->config['wall_fresh_max_seconds'] ?? 7200)),
                'observe_pool_id' => $this->resolveWallObservePoolId($router),
                'observe_pool_raw' => (string) ($this->config['wall_observe_pool_id'] ?? ''),
            ],
            'decoy' => [
                'enabled' => (bool) ($this->config['decoy_enabled'] ?? false),
                'in_window' => $this->decoyInWindow(),
                'active_date' => (string) ($decoy['active_date'] ?? ''),
                'start' => (string) ($this->config['decoy_start'] ?? '01:00'),
                'end' => (string) ($this->config['decoy_end'] ?? '08:00'),
                'batch_size' => $this->decoyBatchSize(),
                'min_batch' => $this->decoyMinBatch(),
                'observe_minutes' => max(5, (int) ($this->config['decoy_observe_minutes'] ?? 40)),
                'source_pool_ids' => $decoySourceIds,
                'source_pool_names' => array_map($poolName, $decoySourceIds),
                'isolate_pool_id' => $this->resolveDecoyIsolatePoolId($router),
                'isolate_pool_name' => $poolName($this->resolveDecoyIsolatePoolId($router)),
                'isolate_chain_names' => array_map(
                    $poolName,
                    $this->decoyIsolateChain($router)
                ),
                'search_pool_ids' => $this->decoySearchPoolIds($router),
                'search_pool_names' => array_map(
                    $poolName,
                    $this->decoySearchPoolIds($router)
                ),
                'convict_at' => $this->decoyConvictAt(),
                'suspect_queue_users' => array_sum(array_map(
                    fn(array $item): int => count($this->normalizeIds($item['ids'] ?? [])),
                    (array) ($router['suspect_queue'] ?? [])
                )),
                'confirm_pool_id' => $this->resolveDecoyConfirmPoolId($router),
                'confirm_pool_name' => $poolName($this->resolveDecoyConfirmPoolId($router)),
                'domains' => $decoyDomains,
                'active_domains' => count($decoyDomains),
                'testing_count' => $testingCount,
                'raw' => [
                    'confirm' => (string) ($this->config['decoy_confirm_pool_id'] ?? ''),
                    'isolate' => (string) ($this->config['decoy_isolate_pool_id'] ?? ''),
                ],
            ],
            'pending_ip_rotates' => $this->pendingIpRotateCount(),
        ];
    }

    /** 换 IP 事件入队（拿不到写锁时不丢事件）。 */
    public function enqueueIpRotate(array $data): int
    {
        try {
            return (int) Redis::rpush(self::IP_PENDING_KEY, json_encode([
                'event_id' => (string) ($data['event_id'] ?? ''),
                'campaign_id' => (string) ($data['campaign_id'] ?? ''),
                'instance_id' => (string) ($data['instance_id'] ?? ''),
                'target_id' => (string) ($data['target_id'] ?? ''),
                'old_ip' => (string) ($data['old_ip'] ?? ''),
                'new_ip' => (string) ($data['new_ip'] ?? ''),
                'reason' => (string) ($data['reason'] ?? 'blocked'),
                'queued_at' => time(),
            ], JSON_UNESCAPED_UNICODE));
        } catch (\Throwable $e) {
            Log::error('BaitSplit 换 IP 入队失败', [
                'error' => $e->getMessage(),
                'event_id' => $data['event_id'] ?? '',
            ]);
            throw $e;
        }
    }

    public function pendingIpRotateCount(): int
    {
        try {
            return (int) Redis::llen(self::IP_PENDING_KEY);
        } catch (\Throwable) {
            return 0;
        }
    }

    /**
     * 消费待处理换 IP。每条独立抢锁，失败则退回队列头部并停止（避免乱序）。
     *
     * @return array{processed:int,failed:int,remaining:int,results:array}
     */
    public function drainPendingIpRotates(int $max = 80): array
    {
        $processed = 0;
        $failed = 0;
        $results = [];
        for ($i = 0; $i < $max; $i++) {
            try {
                $raw = Redis::lpop(self::IP_PENDING_KEY);
            } catch (\Throwable) {
                break;
            }
            if (!$raw) {
                break;
            }
            $data = is_string($raw) ? json_decode($raw, true) : null;
            if (!is_array($data) || ($data['campaign_id'] ?? '') === '' || ($data['new_ip'] ?? '') === '') {
                $failed++;
                continue;
            }
            $eventKey = 'bait_split:ip_rotate_event:'
                . hash('sha256', $data['campaign_id'] . ':' . ($data['event_id'] ?? uniqid('p', true)));
            try {
                $result = Cache::lock(self::STATE_LOCK, 45)->block(
                    25,
                    function () use ($data, $eventKey): array {
                        $cached = Cache::get($eventKey);
                        if (is_array($cached)) {
                            $cached['duplicate'] = true;
                            return $cached;
                        }
                        $result = $this->rotateCampaignIp(
                            (string) $data['campaign_id'],
                            (string) ($data['old_ip'] ?? ''),
                            (string) $data['new_ip'],
                            (string) ($data['target_id'] ?? ''),
                            (string) ($data['reason'] ?? 'blocked')
                        );
                        $result['event_id'] = (string) ($data['event_id'] ?? '');
                        $result['instance_id'] = (string) ($data['instance_id'] ?? '');
                        $result['duplicate'] = false;
                        $result['from_pending'] = true;
                        Cache::put($eventKey, $result, now()->addDay());
                        return $result;
                    }
                );
                $processed++;
                $results[] = [
                    'event_id' => $data['event_id'] ?? '',
                    'target_id' => $data['target_id'] ?? '',
                    'new_ip' => $data['new_ip'] ?? '',
                    'already' => !empty($result['already']),
                ];
            } catch (LockTimeoutException) {
                try {
                    Redis::lpush(self::IP_PENDING_KEY, is_string($raw) ? $raw : json_encode($data));
                } catch (\Throwable) {
                }
                $failed++;
                Log::warning('BaitSplit pending 换 IP 等待锁超时，已退回队列', [
                    'event_id' => $data['event_id'] ?? '',
                ]);
                break;
            } catch (InvalidArgumentException $e) {
                // 配置对不上：记失败但不堵队列
                $failed++;
                Log::warning('BaitSplit pending 换 IP 校验失败，丢弃', [
                    'event_id' => $data['event_id'] ?? '',
                    'error' => $e->getMessage(),
                ]);
            } catch (\Throwable $e) {
                try {
                    Redis::lpush(self::IP_PENDING_KEY, is_string($raw) ? $raw : json_encode($data));
                } catch (\Throwable) {
                }
                $failed++;
                Log::error('BaitSplit pending 换 IP 执行失败，已退回队列', [
                    'event_id' => $data['event_id'] ?? '',
                    'error' => $e->getMessage(),
                ]);
                break;
            }
        }
        return [
            'processed' => $processed,
            'failed' => $failed,
            'remaining' => $this->pendingIpRotateCount(),
            'results' => $results,
        ];
    }

    private function normalizeRotationReason(string $reason): string
    {
        $reason = strtolower(trim($reason));
        $machineAliases = [
            'machine', 'down', 'crash', 'offline', 'reboot', 'maintenance',
            '挂壁', '机器', '机器挂壁', '宕机', '故障', '维护', '重启',
        ];
        return in_array($reason, $machineAliases, true) ? 'machine' : 'blocked';
    }

    private function poolExposureLastMap(array $campaign, string $poolId): array
    {
        try {
            $raw = Redis::hgetall(
                $this->routerPoolExposureLastKey($campaign, $poolId)
            );
            $map = [];
            foreach ((array) $raw as $userId => $ts) {
                if ((int) $userId > 0) {
                    $map[(int) $userId] = (int) $ts;
                }
            }
            return $map;
        } catch (\Throwable) {
            return [];
        }
    }

    private function resolveWallObservePoolId(array $router): string
    {
        $configured = trim((string) ($this->config['wall_observe_pool_id'] ?? ''));
        if ($configured !== '') {
            if (isset($router['pools'][$configured])) {
                return $configured;
            }
            foreach ($router['pools'] as $poolId => $pool) {
                if (($pool['webhook_id'] ?? '') === $configured) {
                    return $poolId;
                }
            }
        }
        // 优先名称含「观察3」，避免误落到观察1（信任池）
        foreach ($router['pools'] as $poolId => $pool) {
            if (($pool['type'] ?? '') !== 'observation') {
                continue;
            }
            $name = (string) ($pool['name'] ?? '');
            if (preg_match('/观察\s*3|observe\s*3|obs\s*3/iu', $name)) {
                return (string) $poolId;
            }
        }
        foreach ($router['pools'] as $poolId => $pool) {
            if (($pool['type'] ?? '') === 'observation') {
                return (string) $poolId;
            }
        }
        return '';
    }

    private function resolvePoolIdByRef(array $router, string $ref): string
    {
        $ref = trim($ref);
        if ($ref === '') {
            return '';
        }
        if (isset($router['pools'][$ref])) {
            return $ref;
        }
        foreach ($router['pools'] as $poolId => $pool) {
            if (($pool['webhook_id'] ?? '') === $ref) {
                return $poolId;
            }
        }
        foreach ($router['pools'] as $poolId => $pool) {
            if (trim((string) ($pool['name'] ?? '')) === $ref) {
                return (string) $poolId;
            }
        }
        return '';
    }

    private function resolveDecoySourcePoolIds(array $router): array
    {
        $ids = [];
        foreach (explode(',', (string) ($this->config['decoy_source_pool_ids'] ?? '')) as $ref) {
            $poolId = $this->resolvePoolIdByRef($router, $ref);
            if ($poolId !== '' && !in_array($poolId, $ids, true)) {
                $ids[] = $poolId;
            }
        }
        if ($ids === []) {
            $fallback = $this->resolveWallObservePoolId($router);
            if ($fallback !== '') {
                $ids[] = $fallback;
            }
        }
        return $ids;
    }

    /**
     * 隔离链：来源池 → 观察3 → 观察4 → … → 安全组。
     * 隔离池自己被墙时要往下一级甩，否则嫌疑原地打转永远收敛不了。
     */
    private function decoyIsolateChain(array $router): array
    {
        $chain = [];
        $push = function (string $poolId) use (&$chain, $router): void {
            if ($poolId === '' || !isset($router['pools'][$poolId])) {
                return;
            }
            // 安全组/危险组/封禁组只认人工操作，不接受自动下沉
            if (in_array(
                (string) ($router['pools'][$poolId]['type'] ?? ''),
                ['safe', 'danger', 'blacklist'],
                true
            )) {
                return;
            }
            if (!in_array($poolId, $chain, true)) {
                $chain[] = $poolId;
            }
        };
        $raw = trim((string) ($this->config['decoy_isolate_chain'] ?? ''));
        if ($raw !== '') {
            foreach (explode(',', $raw) as $ref) {
                $push($this->resolvePoolIdByRef($router, $ref));
            }
            return $chain;
        }
        $push($this->resolvePoolIdByRef(
            $router,
            (string) ($this->config['decoy_isolate_pool_id'] ?? '')
        ));
        $push($this->resolveWallObservePoolId($router));
        $base = $chain[0] ?? '';
        $numbered = [];
        foreach ($router['pools'] as $poolId => $pool) {
            if (($pool['type'] ?? '') !== 'observation') {
                continue;
            }
            if (preg_match('/观察\s*(\d+)/u', (string) ($pool['name'] ?? ''), $m)) {
                $numbered[(int) $m[1]] = (string) $poolId;
            }
        }
        ksort($numbered);
        $baseNum = 0;
        foreach ($numbered as $num => $poolId) {
            if ($poolId === $base) {
                $baseNum = $num;
            }
        }
        // 只顺延观察N；安全组/危险组等靠人工管理，绝不自动当下游
        foreach ($numbered as $num => $poolId) {
            if ($num > $baseNum) {
                $push($poolId);
            }
        }
        return $chain;
    }

    /** 末级返回空串：没有下游可甩，保持武装等人工处理。 */
    private function resolveDecoyIsolatePoolId(
        array $router,
        string $sourcePoolId = ''
    ): string {
        $chain = $this->decoyIsolateChain($router);
        if ($chain === []) {
            return '';
        }
        if ($sourcePoolId !== '') {
            $idx = array_search($sourcePoolId, $chain, true);
            if ($idx !== false) {
                return (string) ($chain[$idx + 1] ?? '');
            }
        }
        return (string) $chain[0];
    }

    private function decoyBatchSize(): int
    {
        return max(1, (int) ($this->config['decoy_batch_size'] ?? 60));
    }

    private function decoyMinBatch(): int
    {
        return max(1, (int) ($this->config['decoy_min_batch'] ?? 8));
    }

    private function decoyObserveSeconds(): int
    {
        return max(300, (int) ($this->config['decoy_observe_minutes'] ?? 40) * 60);
    }

    private function resolveDecoyConfirmPoolId(array $router): string
    {
        $ref = $this->resolvePoolIdByRef(
            $router,
            (string) ($this->config['decoy_confirm_pool_id'] ?? '')
        );
        return $ref !== '' ? $ref : $this->poolIdByType($router, 'danger');
    }

    private function decoyInWindow(): bool
    {
        $parse = static function (string $value, int $fallback): int {
            if (preg_match('/^(\d{1,2}):(\d{2})$/', trim($value), $m)) {
                return ((int) $m[1]) * 60 + (int) $m[2];
            }
            return $fallback;
        };
        $start = $parse((string) ($this->config['decoy_start'] ?? ''), 60);
        $end = $parse((string) ($this->config['decoy_end'] ?? ''), 480);
        $now = now();
        $cur = ((int) $now->format('G')) * 60 + (int) $now->format('i');
        if ($start === $end) {
            return false;
        }
        return $start < $end
            ? ($cur >= $start && $cur < $end)
            : ($cur >= $start || $cur < $end);
    }

    /**
     * 来源池简易分流（暂不做金丝雀诱捕）：
     * - 当晚第一次墙：不管，只换新 IP 并武装。
     * - 之后每次墙：武装窗口内拉过该 IP 的人全部进观察3，换新 IP 后立刻重新武装。
     * - 等默认组/测试组/观察2 不墙后，再对观察3 慢慢诱捕（后续）。
     */
    public function runDecoyOrchestration(): array
    {
        // 优先消化积压换 IP，避免锁冲突导致永久丢事件
        $drain = $this->drainPendingIpRotates(80);
        $decoyEnabled = (bool) ($this->config['decoy_enabled'] ?? false);
        if (!$decoyEnabled) {
            return ['skipped' => 'disabled', 'drain' => $drain];
        }
        // 窗口外（白天收回）多等一会儿，保证 8 点必达
        $wait = $this->decoyInWindow() ? 12 : 25;
        $lease = $this->decoyInWindow() ? 45 : 90;
        try {
            $result = Cache::lock(self::STATE_LOCK, $lease)->block(
                $wait,
                fn(): array => $this->runDecoyLocked()
            );
            $result['drain'] = $drain;
            return $result;
        } catch (LockTimeoutException) {
            Log::warning('BaitSplit bait:decoy 抢锁失败', [
                'in_window' => $this->decoyInWindow(),
                'pending' => $drain['remaining'] ?? 0,
            ]);
            return ['skipped' => 'locked', 'drain' => $drain];
        }
    }

    private function emptyDecoyDomain(): array
    {
        return [
            'phase' => 'idle',
            'armed' => false,
            'armed_ip' => '',
            'armed_at' => 0,
            'cur_ip' => '',
            'dead_ip' => '',
            'current' => null,
            'queue' => [],
            'dispatched_at' => 0,
            'group_size' => 0,
            'proven' => false,
        ];
    }

    private function applyPoolHost(array &$router, string $poolId, string $ip): void
    {
        if ($ip === '' || !isset($router['pools'][$poolId])) {
            return;
        }
        $router['pools'][$poolId]['host'] = $ip;
        $router['pools'][$poolId]['last_rotation_at'] = time();
        if (!isset($router['pools'][$poolId]['node_hosts'])
            || !is_array($router['pools'][$poolId]['node_hosts'])
        ) {
            return;
        }
        foreach ($router['pools'][$poolId]['node_hosts'] as &$h) {
            $h = $ip;
        }
        unset($h);
    }

    /**
     * 测试期：来源池底座钉在武装 IP；非金丝雀用户不得拿到 cur_ip。
     * 金丝雀批（可能已在观察3）override.host 对齐到 canaryIp。
     */
    private function pinDecoyPoolBaseToArmed(
        array &$router,
        string $poolId,
        string $canaryIp = ''
    ): void {
        $domain = $router['decoy']['domains'][$poolId] ?? null;
        if (!is_array($domain)) {
            return;
        }
        $armedIp = (string) ($domain['armed_ip'] ?? '');
        if ($armedIp === '') {
            return;
        }
        $this->applyPoolHost($router, $poolId, $armedIp);

        $canaryIds = [];
        $cur = $domain['current'] ?? null;
        if (is_array($cur)) {
            foreach ($this->normalizeIds($cur['ids'] ?? []) as $uid) {
                $canaryIds[(string) $uid] = true;
            }
        }
        if ($canaryIp === '') {
            $canaryIp = (string) ($domain['cur_ip'] ?? '');
        }
        $now = time();
        foreach ($router['overrides'] ?? [] as $key => &$ov) {
            if (!is_array($ov)) {
                continue;
            }
            if (isset($canaryIds[$key])) {
                if ($canaryIp !== '') {
                    $ov['host'] = $canaryIp;
                    $ov['updated_at'] = $now;
                }
                continue;
            }
            $ovPool = (string) ($ov['pool_id'] ?? '');
            // 仅清理仍挂在来源池的非金丝雀用户
            if ($ovPool !== $poolId) {
                continue;
            }
            $host = (string) ($ov['host'] ?? '');
            if ($host !== '' && $host !== $armedIp) {
                $ov['host'] = '';
                $ov['updated_at'] = $now;
            }
        }
        unset($ov);
    }

    /**
     * 某池在窗口内拉过订阅的用户（排除危险/黑名单）。
     *
     * @return list<int>
     */
    private function collectPoolPullersInWindow(
        array $campaign,
        string $poolId,
        int $windowStart,
        int $now
    ): array {
        $lastMap = $this->poolExposureLastMap($campaign, $poolId);
        $ids = [];
        foreach ($lastMap as $uid => $ts) {
            $uid = (int) $uid;
            $ts = (int) $ts;
            // 含武装当秒的拉取（armed_at 与拉订阅同秒时不能漏）
            if ($uid <= 0 || $ts < $windowStart || $ts > $now) {
                continue;
            }
            $eff = $this->effectivePoolId($campaign, $uid);
            $type = $campaign['router']['pools'][$eff]['type'] ?? '';
            if (in_array($type, ['danger', 'blacklist'], true)) {
                continue;
            }
            $ids[] = $uid;
        }
        return array_values(array_unique($ids));
    }

    /** 兜底：来源池近期拉取用户（二墙曝光窗口无人时用）。 */
    private function seedDecoyDomain(array $campaign, string $poolId): array
    {
        $lastMap = $this->poolExposureLastMap($campaign, $poolId);
        $head = [];
        $tail = [];
        foreach (
            $this->eligibleUsersQuery($campaign['target_group_ids'])
                ->pluck('id')->map('intval')->all() as $uid
        ) {
            if ($this->effectivePoolId($campaign, $uid) !== $poolId) {
                continue;
            }
            $type = $campaign['router']['pools'][
                $this->effectivePoolId($campaign, $uid)
            ]['type'] ?? '';
            if (in_array($type, ['danger', 'blacklist'], true)) {
                continue;
            }
            $note = (string) (
                $campaign['router']['overrides'][(string) $uid]['note'] ?? ''
            );
            if (
                str_contains($note, '金丝雀观察正常')
                || str_contains($note, '诱捕清白收回')
            ) {
                continue;
            }
            if (isset($lastMap[$uid])) {
                $head[$uid] = (int) $lastMap[$uid];
            } else {
                $tail[] = $uid;
            }
        }
        arsort($head);
        shuffle($tail);
        $ordered = array_merge(array_keys($head), $tail);
        $queue = $ordered === [] ? [] : [[
            'ids' => $ordered,
            'batch' => $this->decoyBatchSize(),
            'hold' => $poolId,
        ]];
        return array_merge($this->emptyDecoyDomain(), [
            'phase' => 'testing',
            'armed' => true,
            'queue' => $queue,
        ]);
    }

    /**
     * 收集武装窗口内拉过订阅的嫌疑用户（最近拉取优先）。
     *
     * @return list<int>
     */
    private function collectArmedSuspectIds(
        array $campaign,
        string $poolId,
        int $armedAt,
        int $now
    ): array {
        $lastMap = $this->poolExposureLastMap($campaign, $poolId);
        $head = [];
        foreach ($lastMap as $uid => $ts) {
            $uid = (int) $uid;
            $ts = (int) $ts;
            if ($uid <= 0 || $ts < $armedAt || $ts > $now) {
                continue;
            }
            $eff = $this->effectivePoolId($campaign, $uid);
            $type = $campaign['router']['pools'][$eff]['type'] ?? '';
            if (in_array($type, ['danger', 'blacklist'], true)) {
                continue;
            }
            // 二墙分流：拉过即隔离，不因「观察正常」等旧备注跳过
            $head[$uid] = $ts;
        }
        arsort($head);
        return array_map('intval', array_keys($head));
    }

    /**
     * 二墙开测：前 decoy_batch_size 人进测试队列，其余返回供快隔离。
     *
     * @return array{domain: array, test_ids: list<int>, fast_ids: list<int>, total: int}
     */
    private function seedDecoySuspectsFromArmed(
        array $campaign,
        string $poolId,
        string $armedIp,
        int $armedAt,
        int $now
    ): array {
        $ordered = $this->collectArmedSuspectIds($campaign, $poolId, $armedAt, $now);
        $batch = $this->decoyBatchSize();
        if ($ordered === []) {
            Log::warning('BaitSplit 二墙曝光窗口无人，回退为来源池近期拉取队列', [
                'pool_id' => $poolId,
                'armed_ip' => $armedIp,
            ]);
            $fallback = $this->seedDecoyDomain($campaign, $poolId);
            $fallback['armed_ip'] = $armedIp;
            $fallback['armed_at'] = $armedAt;
            $fallback['dead_ip'] = $armedIp;
            $hold = $this->resolveDecoyIsolatePoolId($campaign['router'] ?? []) ?: $poolId;
            $testIds = $this->normalizeIds($fallback['queue'][0]['ids'] ?? []);
            // 回退时也只测一批，其余快隔离；测试批也挂观察3，避免拖累来源池
            $fastIds = array_slice($testIds, $batch);
            $testIds = array_slice($testIds, 0, $batch);
            $fallback['queue'] = $testIds === [] ? [] : [[
                'ids' => $testIds,
                'batch' => $batch,
                'hold' => $hold,
            ]];
            return [
                'domain' => $fallback,
                'test_ids' => $testIds,
                'fast_ids' => $fastIds,
                'total' => count($testIds) + count($fastIds),
            ];
        }
        $hold = $this->resolveDecoyIsolatePoolId($campaign['router'] ?? []) ?: $poolId;
        $testIds = array_slice($ordered, 0, $batch);
        $fastIds = array_slice($ordered, $batch);
        $domain = array_merge($this->emptyDecoyDomain(), [
            'phase' => 'testing',
            'armed' => true,
            'armed_ip' => $armedIp,
            'armed_at' => $armedAt,
            'dead_ip' => $armedIp,
            'queue' => $testIds === [] ? [] : [[
                'ids' => $testIds,
                'batch' => $batch,
                'hold' => $hold,
            ]],
        ]);
        return [
            'domain' => $domain,
            'test_ids' => $testIds,
            'fast_ids' => $fastIds,
            'total' => count($ordered),
        ];
    }

    /**
     * 把拉订阅嫌疑锁进隔离中转池（默认观察3），白天不自动收回。
     *
     * @param list<int> $ids
     */
    private function fastIsolateSuspects(
        array &$router,
        array $ids,
        int $now,
        string $note = '诱捕墙后拉订阅→观察3',
        string $targetPoolId = ''
    ): array {
        $isolatePoolId = $targetPoolId !== ''
            ? $targetPoolId
            : $this->resolveDecoyIsolatePoolId($router);
        if ($isolatePoolId === '' || $ids === []) {
            return [];
        }
        // 一批越小，命中越集中：按 1/批大小 加权，真内鬼几晚下来会浮到榜首
        $weight = 1 / max(1, count($ids));
        $moved = [];
        foreach ($ids as $uid) {
            $uid = (int) $uid;
            if ($uid <= 0) {
                continue;
            }
            $key = (string) $uid;
            $prevNote = (string) ($router['overrides'][$key]['note'] ?? '');
            if (str_contains($prevNote, '诱捕确认内鬼')) {
                continue;
            }
            $curPool = (string) (
                $router['overrides'][$key]['pool_id']
                ?? ($router['assignments'][$key] ?? '')
            );
            $curType = (string) ($router['pools'][$curPool]['type'] ?? '');
            if (in_array($curType, ['danger', 'blacklist'], true)) {
                continue;
            }
            $router['assignments'][$key] = $isolatePoolId;
            $router['overrides'][$key] = $this->normalizeOverride([
                'pool_id' => $isolatePoolId,
                'locked' => true,
                'note' => $note,
                'updated_at' => $now,
            ]);
            $router['wall_hits'][$key] =
                (int) ($router['wall_hits'][$key] ?? 0) + 1;
            $router['wall_score'][$key] = round(
                (float) ($router['wall_score'][$key] ?? 0) + $weight,
                4
            );
            $router['wall_last'][$key] = $now;
            $router['untested_ids'] = array_values(array_diff(
                $router['untested_ids'] ?? [],
                [$uid]
            ));
            $moved[] = $uid;
        }
        return $moved;
    }

    /**
     * 末级池（没有下游）被墙时的浓缩：本轮拉过订阅的人留下，
     * 其余退回上一级，池子越缩越小，只用两个 IP 也能收敛。
     */
    private function condenseTerminalPool(
        array &$router,
        string $poolId,
        array $pullers,
        string $upstreamPoolId,
        int $now
    ): int {
        // 本轮无人拉订阅还被墙：信息为零，全退回反而丢掉浓缩结果，宁可原地不动
        if (
            $pullers === []
            || $upstreamPoolId === ''
            || !isset($router['pools'][$upstreamPoolId])
        ) {
            return 0;
        }
        $keep = array_flip(array_map('intval', $pullers));
        $weight = 1 / max(1, count($keep));
        foreach (array_keys($keep) as $uid) {
            $key = (string) $uid;
            if ((string) ($router['overrides'][$key]['pool_id'] ?? '') !== $poolId) {
                continue;
            }
            $router['wall_hits'][$key] = (int) ($router['wall_hits'][$key] ?? 0) + 1;
            $router['wall_score'][$key] = round(
                (float) ($router['wall_score'][$key] ?? 0) + $weight,
                4
            );
            $router['wall_last'][$key] = $now;
            $router['overrides'][$key]['note'] = '末级浓缩·连续命中 '
                . (int) $router['wall_hits'][$key] . ' 次';
            $router['overrides'][$key]['updated_at'] = $now;
        }
        $returned = 0;
        foreach ($router['overrides'] as $key => $ov) {
            if ((string) ($ov['pool_id'] ?? '') !== $poolId) {
                continue;
            }
            $uid = (int) $key;
            if ($uid <= 0 || isset($keep[$uid])) {
                continue;
            }
            if (str_contains((string) ($ov['note'] ?? ''), '诱捕确认内鬼')) {
                continue;
            }
            $router['overrides'][$key] = $this->normalizeOverride([
                'pool_id' => $upstreamPoolId,
                'locked' => true,
                'note' => '末级浓缩·本轮未拉订阅退回',
                'updated_at' => $now,
            ]);
            $router['assignments'][$key] = $upstreamPoolId;
            $returned++;
        }
        return $returned;
    }

    /**
     * 搜索槽：每槽一个独立轮换 IP，只装一小组嫌疑人。
     * 嫌疑批均分进槽后，哪个槽被墙就说明内鬼在那一组，再对那组细分。
     */
    private function decoySearchPoolIds(array $router): array
    {
        $ids = [];
        foreach (
            explode(',', (string) ($this->config['decoy_search_pool_ids'] ?? '')) as $ref
        ) {
            $poolId = $this->resolvePoolIdByRef($router, $ref);
            if ($poolId === '' || !isset($router['pools'][$poolId])) {
                continue;
            }
            if (in_array(
                (string) ($router['pools'][$poolId]['type'] ?? ''),
                ['safe', 'danger', 'blacklist'],
                true
            )) {
                continue;
            }
            if (!in_array($poolId, $ids, true)) {
                $ids[] = $poolId;
            }
        }
        return $ids;
    }

    /** 拆到几人就定罪（默认 1 人，绝不冤枉同组）。 */
    private function decoyConvictAt(): int
    {
        return max(1, (int) ($this->config['decoy_convict_at'] ?? 1));
    }

    /**
     * 可用搜索槽：空槽，或者装的全是本批人的槽（被墙的槽要能就地把自己那组拆开）。
     *
     * @param int[] $batch
     * @return string[]
     */
    private function freeSearchSlots(array $router, array $batch = []): array
    {
        $batchMap = array_flip(array_map('intval', $batch));
        $free = [];
        foreach ($this->decoySearchPoolIds($router) as $slot) {
            foreach ($this->poolLockedMemberIds($router, $slot) as $uid) {
                if (!isset($batchMap[$uid])) {
                    continue 2;
                }
            }
            $free[] = $slot;
        }
        return $free;
    }

    /** @return int[] 该池里被覆盖锁定的成员 */
    private function poolLockedMemberIds(array $router, string $poolId): array
    {
        $ids = [];
        foreach ((array) ($router['overrides'] ?? []) as $key => $override) {
            if ((string) ($override['pool_id'] ?? '') !== $poolId) {
                continue;
            }
            $uid = (int) $key;
            if ($uid > 0) {
                $ids[] = $uid;
            }
        }
        return $ids;
    }

    /**
     * 把一批嫌疑人均分到空闲搜索槽。装的全是本批人的槽也算空闲，
     * 这样一个槽被墙后能就地把自己那组再拆开。
     *
     * @return array{dispatched:int,leftover:int[],slots:string[]}
     */
    private function dispatchSuspectBatch(
        array &$router,
        array $ids,
        string $originPoolId,
        int $now,
        bool $proven = false
    ): array {
        $ids = $this->normalizeIds($ids);
        if ($ids === []) {
            return ['dispatched' => 0, 'leftover' => [], 'slots' => []];
        }
        $free = $this->freeSearchSlots($router, $ids);
        if ($free === []) {
            return ['dispatched' => 0, 'leftover' => $ids, 'slots' => []];
        }
        // 正在别的槽里测着的人不重复派发：中途搬走会让那一组的证据作废
        $freeMap = array_flip($free);
        $batchMap = array_flip($ids);
        foreach ($this->decoySearchPoolIds($router) as $slot) {
            if (isset($freeMap[$slot])) {
                continue;
            }
            foreach ($this->poolLockedMemberIds($router, $slot) as $uid) {
                unset($batchMap[$uid]);
            }
        }
        $ids = array_keys($batchMap);
        if ($ids === []) {
            return ['dispatched' => 0, 'leftover' => [], 'slots' => []];
        }

        $groups = array_chunk($ids, (int) ceil(count($ids) / count($free)));
        $dispatched = 0;
        $used = [];
        $leftover = [];
        foreach ($groups as $index => $group) {
            $slot = $free[$index] ?? '';
            if ($slot === '') {
                $leftover = array_merge($leftover, $group);
                continue;
            }
            $weight = 1 / max(1, count($group));
            $placed = 0;
            foreach ($group as $uid) {
                $key = (string) $uid;
                $note = (string) ($router['overrides'][$key]['note'] ?? '');
                if (str_contains($note, '诱捕确认内鬼')) {
                    continue;
                }
                $curPool = (string) (
                    $router['overrides'][$key]['pool_id']
                    ?? ($router['assignments'][$key] ?? '')
                );
                if (in_array(
                    (string) ($router['pools'][$curPool]['type'] ?? ''),
                    ['danger', 'blacklist'],
                    true
                )) {
                    continue;
                }
                $home = $originPoolId !== ''
                    ? $originPoolId
                    : (string) ($router['search_origin'][$key] ?? '');
                if ($home !== '' && !in_array($home, $this->decoySearchPoolIds($router), true)) {
                    $router['search_origin'][$key] = $home;
                }
                $router['overrides'][$key] = $this->normalizeOverride([
                    'pool_id' => $slot,
                    'locked' => true,
                    'note' => '分组测试（本组 ' . count($group) . ' 人）',
                    'updated_at' => $now,
                ]);
                $router['assignments'][$key] = $slot;
                $router['wall_hits'][$key] = (int) ($router['wall_hits'][$key] ?? 0) + 1;
                $router['wall_score'][$key] = round(
                    (float) ($router['wall_score'][$key] ?? 0) + $weight,
                    4
                );
                $router['wall_last'][$key] = $now;
                $router['untested_ids'] = array_values(array_diff(
                    $router['untested_ids'] ?? [],
                    [$uid]
                ));
                $dispatched++;
                $placed++;
            }
            if (!isset($router['decoy']['domains'][$slot])) {
                $router['decoy']['domains'][$slot] = $this->emptyDecoyDomain();
            }
            // 派发即武装：观察窗和「谁拉过本槽 IP」都从这一刻算起。
            // 不武装的话槽的首墙会走「首墙只换 IP」分支，整组白测一轮。
            $slotIp = (string) ($router['pools'][$slot]['host'] ?? '');
            $router['decoy']['domains'][$slot] = array_merge(
                $this->normalizeDecoyDomain($router['decoy']['domains'][$slot]),
                [
                    'phase' => $placed > 0 ? 'armed' : 'idle',
                    'armed' => $placed > 0,
                    'armed_ip' => $slotIp,
                    'armed_at' => $placed > 0 ? $now : 0,
                    'cur_ip' => $slotIp,
                    'dispatched_at' => $placed > 0 ? $now : 0,
                    'group_size' => $placed,
                    // 已取证 = 这组人是从被墙的组里细分出来的，里面一定有内鬼，
                    // 不许被新鲜嫌疑批顶替，必须一路拆到定罪
                    'proven' => $placed > 0 && $proven,
                ]
            );
            $used[] = $slot;
        }
        return [
            'dispatched' => $dispatched,
            'leftover' => array_values($leftover),
            'slots' => $used,
        ];
    }

    /**
     * 腾位子给新鲜嫌疑批：放掉还没取到证据的组。
     * 实测刚被墙时抓到的人 20 分钟内有 6 成会再拉订阅，隔一两个小时的只有 1%，
     * 所以宁可让陈旧的组下车，也要先测最新鲜的那一批。
     */
    private function preemptUnprovenSlots(array &$router, int $now): int
    {
        $evicted = 0;
        foreach ($this->decoySearchPoolIds($router) as $slot) {
            $domain = $router['decoy']['domains'][$slot] ?? [];
            if (!empty($domain['proven'])) {
                continue;
            }
            if ($this->poolLockedMemberIds($router, $slot) === []) {
                continue;
            }
            $evicted += $this->releaseSearchSlot(
                $router,
                $slot,
                $now,
                '分组测试·未取证让位给新鲜嫌疑批',
                false
            );
        }
        return $evicted;
    }

    /** 单人放回来源池；已定罪的不动。 */
    private function releaseSearchSlotUser(
        array &$router,
        int $uid,
        int $now,
        string $note,
        bool $keepOrigin = false,
        bool $graduate = true
    ): int {
        $key = (string) $uid;
        $override = $router['overrides'][$key] ?? null;
        if ($uid <= 0 || !is_array($override)) {
            return 0;
        }
        if (str_contains((string) ($override['note'] ?? ''), '诱捕确认内鬼')) {
            return 0;
        }
        $home = (string) ($router['search_origin'][$key] ?? '');
        $invalid = $home === ''
            || !isset($router['pools'][$home])
            || in_array($home, $this->decoySearchPoolIds($router), true);
        // 判清白才毕业：从嫌疑池（观察3）来的人不退回嫌疑池，否则永远在被反复测。
        // 没测就下车的（让位给新鲜批）必须原路退回，不然嫌疑人会倒灌进默认组
        if ($invalid || ($graduate && in_array($home, $this->decoyIsolateChain($router), true))) {
            $home = $this->poolIdByType($router, 'default');
        }
        if ($home === '') {
            return 0;
        }
        $router['overrides'][$key] = $this->normalizeOverride([
            'pool_id' => $home,
            'locked' => true,
            'note' => $note,
            'updated_at' => $now,
        ]);
        $router['assignments'][$key] = $home;
        if (!$keepOrigin) {
            unset($router['search_origin'][$key]);
        }
        return 1;
    }

    /** 观察期内没墙 = 整组清白，放回原来的池子，槽位腾空复用。 */
    private function releaseSearchSlot(
        array &$router,
        string $slot,
        int $now,
        string $note,
        bool $graduate = true
    ): int {
        $released = 0;
        foreach ($this->poolLockedMemberIds($router, $slot) as $uid) {
            $released += $this->releaseSearchSlotUser(
                $router,
                $uid,
                $now,
                $note,
                false,
                $graduate
            );
        }
        if (isset($router['decoy']['domains'][$slot])) {
            // 槽空了就解除武装，免得空槽的墙被算成一组人的证据
            $router['decoy']['domains'][$slot] = array_merge(
                $this->normalizeDecoyDomain($router['decoy']['domains'][$slot]),
                [
                    'phase' => 'idle',
                    'armed' => false,
                    'armed_at' => 0,
                    'dispatched_at' => 0,
                    'group_size' => 0,
                    'proven' => false,
                ]
            );
        }
        return $released;
    }

    /** 拆到只剩这几个人还被墙：定罪进危险组。 */
    private function convictSuspects(array &$router, array $ids, int $now): array
    {
        $target = $this->resolveDecoyConfirmPoolId($router);
        $ids = $this->normalizeIds($ids);
        if ($target === '' || $ids === []) {
            return [];
        }
        $convicted = [];
        foreach ($ids as $uid) {
            $key = (string) $uid;
            $router['overrides'][$key] = $this->normalizeOverride([
                'pool_id' => $target,
                'locked' => true,
                'note' => '诱捕确认内鬼（分组测试拆到 ' . count($ids) . ' 人仍被墙，跟墙 '
                    . (int) ($router['wall_hits'][$key] ?? 0) . ' 次）',
                'updated_at' => $now,
            ]);
            $router['assignments'][$key] = $target;
            unset($router['search_origin'][$key]);
            $convicted[] = $uid;
        }
        return $convicted;
    }

    /** 隔离链里的上一级；链首返回空串。 */
    private function resolveDecoyUpstreamPoolId(
        array $router,
        string $poolId
    ): string {
        $chain = $this->decoyIsolateChain($router);
        $idx = array_search($poolId, $chain, true);
        if ($idx === false || $idx === 0) {
            return '';
        }
        return (string) $chain[$idx - 1];
    }

    /** 换晚翻篇：日期变了就收回上一夜遗留覆盖并清空探测域。返回是否有变更。 */
    private function decoyRolloverIfNeeded(
        array &$router,
        string $today,
        int $now
    ): bool {
        if (($router['decoy']['active_date'] ?? '') === $today) {
            return false;
        }
        $this->decoyRecallAll($router, $now);
        $router['decoy'] = ['active_date' => $today, 'domains' => []];
        return true;
    }

    /** 从队列抽下一批指向当前金丝雀 IP；current 非空或无 IP/无队列则不动。 */
    private function decoyAdvance(array &$router, string $poolId, int $now): bool
    {
        $domain = $router['decoy']['domains'][$poolId] ?? null;
        if (!is_array($domain) || ($domain['current'] ?? null) !== null) {
            return false;
        }
        $curIp = (string) ($domain['cur_ip'] ?? '');
        if ($curIp === '') {
            return false;
        }
        $queue = $domain['queue'] ?? [];
        while ($queue !== [] && ($queue[0]['ids'] ?? []) === []) {
            array_shift($queue);
        }
        if ($queue === []) {
            $router['decoy']['domains'][$poolId]['queue'] = $queue;
            return false;
        }
        $batch = max(1, (int) ($queue[0]['batch'] ?? 1));
        $hold = (string) ($queue[0]['hold'] ?? $poolId);
        $slice = array_slice($queue[0]['ids'], 0, $batch);
        $queue[0]['ids'] = array_values(array_slice($queue[0]['ids'], $batch));
        if ($queue[0]['ids'] === []) {
            array_shift($queue);
        }
        $router['decoy']['domains'][$poolId]['queue'] = $queue;
        $router['decoy']['domains'][$poolId]['current'] = [
            'ids' => $slice,
            'batch' => $batch,
            'hold' => $hold,
            'started_at' => $now,
        ];
        foreach ($slice as $uid) {
            $key = (string) $uid;
            $router['overrides'][$key] = $this->normalizeOverride([
                'pool_id' => $hold,
                'host' => $curIp,
                'locked' => true,
                'note' => '金丝雀测试批（' . $batch . ' 人一批）',
                'updated_at' => $now,
            ]);
            $router['assignments'][$key] = $hold;
            $router['decoy_nights'][$key] =
                (int) ($router['decoy_nights'][$key] ?? 0) + 1;
        }
        $router['untested_ids'] = array_values(array_diff(
            $router['untested_ids'],
            $slice
        ));
        return true;
    }

    /** 结算当前批：walled=true 记分并（对半拆隔离 / 收敛入危险组）；否则清白收回。 */
    private function decoySettle(
        array &$router,
        string $poolId,
        bool $walled,
        int $now
    ): void {
        $cur = $router['decoy']['domains'][$poolId]['current'] ?? null;
        if (!is_array($cur)) {
            return;
        }
        $ids = $this->normalizeIds($cur['ids'] ?? []);
        $batch = max(1, (int) ($cur['batch'] ?? 1));
        $deadIp = (string) ($router['decoy']['domains'][$poolId]['dead_ip'] ?? '');
        if ($walled && $ids !== []) {
            foreach ($ids as $uid) {
                $key = (string) $uid;
                $router['wall_hits'][$key] =
                    (int) ($router['wall_hits'][$key] ?? 0) + 1;
                $router['wall_last'][$key] = $now;
            }
            $confirmPoolId = $this->resolveDecoyConfirmPoolId($router);
            $minBatch = $this->decoyMinBatch();
            $reachedFloor = $batch <= $minBatch || count($ids) <= $minBatch;
            if ($reachedFloor) {
                // 无危险组时退到隔离池，绝不能无限拆到 1 人却不定罪
                $target = $confirmPoolId !== ''
                    ? $confirmPoolId
                    : $this->resolveDecoyIsolatePoolId($router);
                if ($target === '') {
                    $target = (string) ($cur['hold'] ?? $poolId);
                }
                $notePrefix = $confirmPoolId !== ''
                    ? '诱捕确认内鬼'
                    : '诱捕确认内鬼(无危险组,暂入隔离)';
                foreach ($ids as $uid) {
                    $key = (string) $uid;
                    $router['overrides'][$key] = $this->normalizeOverride([
                        'pool_id' => $target,
                        'locked' => true,
                        'note' => $notePrefix . '（批≤' . $this->decoyMinBatch()
                            . '，跟墙 ' . $router['wall_hits'][$key] . ' 次）',
                        'updated_at' => $now,
                    ]);
                    $router['assignments'][$key] = $target;
                    $router['untested_ids'] = array_values(array_diff(
                        $router['untested_ids'],
                        [$uid]
                    ));
                }
            } else {
                $isolatePoolId = $this->resolveDecoyIsolatePoolId($router);
                $hold = $isolatePoolId !== ''
                    ? $isolatePoolId
                    : (string) ($cur['hold'] ?? $poolId);
                // 嫌疑批插队头，优先续测，避免天亮前收敛不到
                if (!is_array($router['decoy']['domains'][$poolId]['queue'] ?? null)) {
                    $router['decoy']['domains'][$poolId]['queue'] = [];
                }
                array_unshift($router['decoy']['domains'][$poolId]['queue'], [
                    'ids' => $ids,
                    'batch' => max(1, intdiv($batch, 2)),
                    'hold' => $hold,
                ]);
                foreach ($ids as $uid) {
                    $key = (string) $uid;
                    $router['overrides'][$key] = $this->normalizeOverride([
                        'pool_id' => $hold,
                        'host' => $deadIp,
                        'locked' => true,
                        'note' => '诱捕嫌疑隔离（待细分）',
                        'updated_at' => $now,
                    ]);
                    $router['assignments'][$key] = $hold;
                }
            }
        } elseif (!$walled) {
            // 观察期满未墙 → 标记正常；白天收回不删此备注
            foreach ($ids as $uid) {
                $key = (string) $uid;
                $router['overrides'][$key] = $this->normalizeOverride([
                    'pool_id' => $poolId,
                    'host' => '',
                    'locked' => true,
                    'note' => '金丝雀观察正常',
                    'updated_at' => $now,
                ]);
                $router['assignments'][$key] = $poolId;
            }
        }
        $router['decoy']['domains'][$poolId]['current'] = null;
    }

    /**
     * 白天/换晚收回：清诱捕覆盖并归位 assignment。
     * host 仅在「仍等于死 IP / 为空」时提升为 cur_ip，避免盖掉白天已换的新 IP，
     * 同时防止收回后全池掉回死 IP。
     */
    private function decoyRecallAll(array &$router, int $now): int
    {
        $restored = 0;
        $origin = [];
        // 天亮收工：搜索槽里没定罪的一律放回来源池，别把人留在小槽里过白天
        foreach ($this->decoySearchPoolIds($router) as $slot) {
            $this->releaseSearchSlot(
                $router,
                $slot,
                $now,
                '诱捕清白收回·天亮未结论'
            );
        }
        $router['suspect_queue'] = [];
        foreach ((array) ($router['decoy']['domains'] ?? []) as $poolId => $domain) {
            if (!is_array($domain)) {
                continue;
            }
            $curIp = (string) ($domain['cur_ip'] ?? '');
            $deadIp = (string) ($domain['dead_ip'] ?? '');
            if (
                $curIp !== ''
                && isset($router['pools'][$poolId])
            ) {
                $host = (string) ($router['pools'][$poolId]['host'] ?? '');
                if ($host === '' || ($deadIp !== '' && $host === $deadIp)) {
                    $router['pools'][$poolId]['host'] = $curIp;
                    foreach ($router['pools'][$poolId]['node_hosts'] as &$h) {
                        if ($h === '' || ($deadIp !== '' && $h === $deadIp)) {
                            $h = $curIp;
                        }
                    }
                    unset($h);
                    Log::notice('BaitSplit 收回时提升来源池 host', [
                        'pool_id' => $poolId,
                        'from' => $host,
                        'to' => $curIp,
                        'dead_ip' => $deadIp,
                    ]);
                }
            }
            $ids = [];
            if (is_array($domain['current'] ?? null)) {
                $ids = array_merge(
                    $ids,
                    $this->normalizeIds($domain['current']['ids'] ?? [])
                );
            }
            foreach ((array) ($domain['queue'] ?? []) as $item) {
                $ids = array_merge($ids, $this->normalizeIds($item['ids'] ?? []));
            }
            foreach ($ids as $uid) {
                $origin[(string) $uid] = (string) $poolId;
            }
        }
        // 已确认内鬼 / 观察正常 保留；测试中/嫌疑隔离/旧清白收回 清除
        $markers = ['金丝雀测试批', '诱捕嫌疑隔离', '诱捕清白收回', '分组测试'];
        foreach ($router['overrides'] as $key => $ov) {
            $note = (string) ($ov['note'] ?? '');
            if (
                str_contains($note, '诱捕确认内鬼')
                || str_contains($note, '金丝雀观察正常')
            ) {
                continue;
            }
            $isDecoy = false;
            foreach ($markers as $m) {
                if (str_contains($note, $m)) {
                    $isDecoy = true;
                    break;
                }
            }
            if (!$isDecoy) {
                continue;
            }
            unset($router['overrides'][$key]);
            if (isset($origin[$key]) && isset($router['pools'][$origin[$key]])) {
                $router['assignments'][$key] = $origin[$key];
            }
            $restored++;
        }
        return $restored;
    }

    /** 诱捕窗口内 machine：来源池全员跟新 IP；武装期同步 armed_ip。 */
    private function handleDecoyMachineIp(
        array &$router,
        string $poolId,
        string $newIp,
        int $now
    ): array {
        $this->applyPoolHost($router, $poolId, $newIp);
        if (!isset($router['decoy']['domains'][$poolId])) {
            return [
                'reason' => 'machine',
                'mode' => 'decoy',
                'phase' => 'idle',
                'updated_cur_ip' => false,
            ];
        }
        $phase = (string) ($router['decoy']['domains'][$poolId]['phase'] ?? 'idle');
        $router['decoy']['domains'][$poolId]['cur_ip'] = $newIp;
        // 武装期：池 host 可跟机器新 IP，但 armed_ip/armed_at 不动（二墙仍按首墙武装窗口算人）
        // 旧 testing 残留：清金丝雀队列，回到 armed
        if ($phase === 'testing') {
            $armedIp = (string) ($router['decoy']['domains'][$poolId]['armed_ip'] ?? '');
            $router['decoy']['domains'][$poolId]['phase'] = 'armed';
            $router['decoy']['domains'][$poolId]['armed'] = true;
            $router['decoy']['domains'][$poolId]['current'] = null;
            $router['decoy']['domains'][$poolId]['queue'] = [];
            if ($armedIp === '') {
                $router['decoy']['domains'][$poolId]['armed_ip'] = $newIp;
                $router['decoy']['domains'][$poolId]['armed_at'] = $now;
            }
            $phase = 'armed';
        }
        return [
            'reason' => 'machine',
            'mode' => 'decoy',
            'phase' => $phase,
            'updated_cur_ip' => true,
            'retargeted' => 0,
        ];
    }

    /**
     * 来源池两墙分流（不做金丝雀）：
     * - idle→armed：当晚首墙不管，只换 IP
     * - armed→armed：拉过武装 IP 的人全部进观察3，换新 IP 后继续武装
     */
    private function handleDecoyWall(
        array $campaign,
        array &$router,
        string $poolId,
        string $oldIp,
        string $newIp,
        int $now
    ): array {
        $today = now()->format('Y-m-d');
        $this->decoyRolloverIfNeeded($router, $today, $now);
        if (!isset($router['decoy']['domains'][$poolId])) {
            $router['decoy']['domains'][$poolId] = $this->emptyDecoyDomain();
        }
        $router['decoy']['domains'][$poolId] = $this->normalizeDecoyDomain(
            $router['decoy']['domains'][$poolId]
        );

        $phase = (string) ($router['decoy']['domains'][$poolId]['phase'] ?? 'idle');
        $prevHost = (string) ($router['pools'][$poolId]['host'] ?? '');
        $isolated = 0;
        $suspectSeeded = 0;
        $logPhase = $phase;
        $isolateTargetId = '';
        $movedIds = [];

        if ($phase === 'idle') {
            // 第一次墙：不管人，只武装新 IP
            $this->applyPoolHost($router, $poolId, $newIp);
            $dead = $oldIp !== '' ? $oldIp : $prevHost;
            $router['decoy']['domains'][$poolId] = array_merge(
                $this->emptyDecoyDomain(),
                [
                    'phase' => 'armed',
                    'armed' => true,
                    'armed_ip' => $newIp,
                    'armed_at' => $now,
                    'cur_ip' => $newIp,
                    'dead_ip' => $dead,
                ]
            );
            $router['pools'][$poolId]['last_rotation_at'] = $now;
            $phase = 'armed';
            $logPhase = 'armed';
            Log::notice('BaitSplit 首墙：只换 IP，不隔离', [
                'pool_id' => $poolId,
                'new_ip' => $newIp,
            ]);
        } else {
            // 第二次墙（含旧 testing 残留）：武装窗口内拉过订阅的全部进观察3
            $armedIp = (string) ($router['decoy']['domains'][$poolId]['armed_ip'] ?? '');
            $armedAt = (int) ($router['decoy']['domains'][$poolId]['armed_at'] ?? 0);
            if ($armedIp === '') {
                $armedIp = $oldIp !== '' ? $oldIp : $prevHost;
            }
            if ($armedAt <= 0) {
                $armedAt = $now - max(60, (int) ($this->config['wall_lookback_seconds'] ?? 3600));
            }
            $searchSlots = $this->decoySearchPoolIds($router);
            $isolatePoolId = $this->resolveDecoyIsolatePoolId($router, $poolId);
            $pullers = $this->collectPoolPullersInWindow(
                $campaign,
                $poolId,
                $armedAt,
                $now
            );
            $suspectSeeded = count($pullers);
            if (in_array($poolId, $searchSlots, true)) {
                // 搜索槽被墙：内鬼就在本组拉过订阅的人里，同组没拉的当场放回去
                $group = $this->poolLockedMemberIds($router, $poolId);
                $hit = array_values(array_intersect($group, $pullers));
                $clean = array_values(array_diff($group, $hit));
                $cleared = 0;
                foreach ($clean as $uid) {
                    $cleared += $this->releaseSearchSlotUser(
                        $router,
                        (int) $uid,
                        $now,
                        '诱捕清白收回·同组被墙但本轮未拉订阅'
                    );
                }
                if ($hit === []) {
                    // 本组没人拉却被墙：这一轮没信息，换 IP 重开观察窗，人不动也不放
                    $logPhase = 'search_noinfo';
                    $router['decoy']['domains'][$poolId]['dispatched_at'] = $now;
                } elseif (count($hit) <= $this->decoyConvictAt()) {
                    $movedIds = $this->convictSuspects($router, $hit, $now);
                    $isolated = count($movedIds);
                    $isolateTargetId = $this->resolveDecoyConfirmPoolId($router);
                    $logPhase = 'convicted';
                    // 槽已腾空，别让旧的观察计时继续挂着
                    $router['decoy']['domains'][$poolId]['dispatched_at'] = 0;
                    $router['decoy']['domains'][$poolId]['group_size'] = 0;
                    Log::warning('BaitSplit 分组测试定罪', [
                        'pool_id' => $poolId,
                        'ids' => $movedIds,
                        'released_same_group' => $cleared,
                    ]);
                } else {
                    // 别的槽还占着时只剩自己可用，原样放回去这轮就白挨了一次墙。
                    // 留一半在槽里继续测、另一半退回原池排队，保证每次墙都对半收敛
                    $deferred = [];
                    if (count($this->freeSearchSlots($router, $hit)) <= 1) {
                        $half = (int) ceil(count($hit) / 2);
                        $deferred = array_slice($hit, $half);
                        $hit = array_slice($hit, 0, $half);
                        foreach ($deferred as $uid) {
                            $this->releaseSearchSlotUser(
                                $router,
                                (int) $uid,
                                $now,
                                '分组测试待复测·等空槽',
                                true
                            );
                        }
                    }
                    $split = $this->dispatchSuspectBatch($router, $hit, '', $now, true);
                    $pendingIds = array_merge($split['leftover'], $deferred);
                    if ($pendingIds !== []) {
                        $router['suspect_queue'][] = [
                            'ids' => array_values($pendingIds),
                            'origin' => '',
                            'at' => $now,
                        ];
                    }
                    $movedIds = $hit;
                    $isolated = $split['dispatched'];
                    $logPhase = 'split';
                    Log::notice('BaitSplit 分组测试细分', [
                        'pool_id' => $poolId,
                        'hit' => count($hit),
                        'deferred' => count($deferred),
                        'slots' => $split['slots'],
                        'released_same_group' => $cleared,
                    ]);
                }
                $this->applyPoolHost($router, $poolId, $newIp);
                $router['decoy']['domains'][$poolId] = array_merge(
                    $this->normalizeDecoyDomain(
                        $router['decoy']['domains'][$poolId] ?? []
                    ),
                    [
                        'phase' => 'armed',
                        'armed' => true,
                        'armed_ip' => $newIp,
                        'armed_at' => $now,
                        'cur_ip' => $newIp,
                        'dead_ip' => $armedIp,
                    ]
                );
                $router['pools'][$poolId]['last_rotation_at'] = $now;
                $phase = 'armed';
            } elseif ($searchSlots !== []) {
                // 产嫌疑的池：拉过武装 IP 的人扇出均分进搜索槽，池里其余人不动。
                // 刚被墙抓到的人最新鲜（20 分钟内 6 成会再拉订阅），槽不够时
                // 顶掉还没取证的组也要先测这一批；已取证的收敛链受保护
                $evicted = 0;
                $fan = $this->dispatchSuspectBatch($router, $pullers, $poolId, $now);
                if ($fan['dispatched'] === 0) {
                    $evicted = $this->preemptUnprovenSlots($router, $now);
                    if ($evicted > 0) {
                        $fan = $this->dispatchSuspectBatch(
                            $router,
                            $pullers,
                            $poolId,
                            $now
                        );
                    }
                }
                if ($fan['leftover'] !== []) {
                    $router['suspect_queue'][] = [
                        'ids' => $fan['leftover'],
                        'origin' => $poolId,
                        'at' => $now,
                    ];
                }
                $movedIds = $pullers;
                $isolated = $fan['dispatched'];
                $isolateTargetId = (string) ($fan['slots'][0] ?? '');
                $logPhase = $fan['dispatched'] > 0 ? 'fanout' : 'queued';
                $this->applyPoolHost($router, $poolId, $newIp);
                $router['decoy']['domains'][$poolId] = array_merge(
                    $this->normalizeDecoyDomain(
                        $router['decoy']['domains'][$poolId] ?? []
                    ),
                    [
                        'phase' => 'armed',
                        'armed' => true,
                        'armed_ip' => $newIp,
                        'armed_at' => $now,
                        'cur_ip' => $newIp,
                        'dead_ip' => $armedIp,
                    ]
                );
                $router['pools'][$poolId]['last_rotation_at'] = $now;
                $phase = 'armed';
                Log::notice('BaitSplit 二墙扇出：拉订阅者均分进搜索槽', [
                    'pool_id' => $poolId,
                    'pullers' => $suspectSeeded,
                    'dispatched' => $fan['dispatched'],
                    'queued' => count($fan['leftover']),
                    'evicted' => $evicted,
                    'slots' => $fan['slots'],
                    'new_ip' => $newIp,
                ]);
            } elseif ($isolatePoolId === '') {
                // 末级池：本轮拉过的人留下，其余退回上一级，池子越缩越小
                $upstream = $this->resolveDecoyUpstreamPoolId($router, $poolId);
                $returned = $this->condenseTerminalPool(
                    $router,
                    $poolId,
                    $pullers,
                    $upstream,
                    $now
                );
                $this->applyPoolHost($router, $poolId, $newIp);
                $router['decoy']['domains'][$poolId] = array_merge(
                    $this->emptyDecoyDomain(),
                    [
                        'phase' => 'armed',
                        'armed' => true,
                        'armed_ip' => $newIp,
                        'armed_at' => $now,
                        'cur_ip' => $newIp,
                        'dead_ip' => $armedIp,
                    ]
                );
                $router['pools'][$poolId]['last_rotation_at'] = $now;
                $phase = 'armed';
                $logPhase = 'condensed';
                $movedIds = $pullers;
                $isolated = $suspectSeeded;
                Log::notice('BaitSplit 末级浓缩：留下本轮拉订阅者，其余退回上一级', [
                    'pool_id' => $poolId,
                    'kept' => $suspectSeeded,
                    'returned' => $returned,
                    'upstream' => $upstream,
                    'new_ip' => $newIp,
                ]);
            } else {
                $isolateTargetId = $isolatePoolId;
                $targetName = (string) (
                    $router['pools'][$isolatePoolId]['name'] ?? $isolatePoolId
                );
                $movedIds = $this->fastIsolateSuspects(
                    $router,
                    $pullers,
                    $now,
                    '二墙拉订阅→' . $targetName,
                    $isolatePoolId
                );
                $isolated = count($movedIds);
                // 甩完立刻重新武装：新 IP 只有剩下的人能拉到，再墙即可继续甩
                $this->applyPoolHost($router, $poolId, $newIp);
                $router['decoy']['domains'][$poolId] = array_merge(
                    $this->emptyDecoyDomain(),
                    [
                        'phase' => 'armed',
                        'armed' => true,
                        'armed_ip' => $newIp,
                        'armed_at' => $now,
                        'cur_ip' => $newIp,
                        'dead_ip' => $armedIp,
                    ]
                );
                $router['pools'][$poolId]['last_rotation_at'] = $now;
                $phase = 'armed';
                $logPhase = 'dumped';
                Log::notice('BaitSplit 二墙：拉过武装 IP 者全部下沉隔离', [
                    'pool_id' => $poolId,
                    'isolate_name' => $targetName,
                    'armed_ip' => $armedIp,
                    'pullers' => $suspectSeeded,
                    'isolated' => $isolated,
                    'isolate_pool' => $isolatePoolId,
                    'new_ip' => $newIp,
                ]);
            }
        }

        $logEntry = [
            'at' => $now,
            'reason' => 'blocked',
            'mode' => 'decoy',
            'phase' => $logPhase,
            'old_ip' => $oldIp,
            'new_ip' => $newIp,
            'pools' => [[
                'pool_id' => $poolId,
                'pool_name' => (string) ($router['pools'][$poolId]['name'] ?? $poolId),
                'mode' => 'decoy',
                'phase' => $logPhase,
                'suspect_count' => $suspectSeeded,
                'batch' => 0,
                'suspect_seeded' => $suspectSeeded,
                'fast_isolated' => $isolated,
            ]],
            'suspect_count' => $suspectSeeded,
            'scored_count' => $isolated,
            'threshold' => 0,
            'moved_count' => $isolated,
            'moved_ids' => array_slice(array_map('intval', $movedIds), 0, 200),
            'observe_pool_id' => $isolateTargetId !== ''
                ? $isolateTargetId
                : $this->resolveDecoyIsolatePoolId($router, $poolId),
        ];
        $router['wall_log'][] = $logEntry;
        $router['wall_log'] = array_slice($router['wall_log'], -200);

        return [
            'reason' => 'blocked',
            'mode' => 'decoy',
            'phase' => $logPhase,
            'walled' => $logPhase === 'dumped',
            'confirmed' => 0,
            'isolated' => $isolated,
            'next_batch' => 0,
            'drew' => false,
            'mismatch' => false,
            'suspect_seeded' => $suspectSeeded,
        ];
    }

    private function runDecoyLocked(): array
    {
        $state = $this->state();
        $inWindow = $this->decoyInWindow();
        $today = now()->format('Y-m-d');
        $now = time();
        $changed = false;
        $actions = [];
        foreach ($state['campaigns'] as $id => $campaign) {
            if (empty($campaign['router']['enabled'])) {
                continue;
            }
            $router = &$campaign['router'];
            $sourceIds = $this->resolveDecoySourcePoolIds($router);
            if ($sourceIds === []) {
                unset($router);
                continue;
            }

            if (!$inWindow) {
                if (
                    ($router['decoy']['active_date'] ?? '') !== ''
                    || ($router['decoy']['domains'] ?? []) !== []
                ) {
                    $restored = $this->decoyRecallAll($router, $now);
                    $router['decoy'] = ['active_date' => '', 'domains' => []];
                    $router['config_version']++;
                    $changed = true;
                    $actions[] = [
                        'campaign_id' => $id,
                        'action' => 'recall',
                        'restored' => $restored,
                    ];
                    Log::notice('BaitSplit 白天诱捕收回完成', [
                        'campaign_id' => $id,
                        'restored' => $restored,
                    ]);
                }
                // 白天补漏：跟墙分已达标但未隔离的用户
                $reconciled = $this->reconcileWallIsolates($router, $now);
                if ($reconciled > 0) {
                    $router['config_version']++;
                    $changed = true;
                    $actions[] = [
                        'campaign_id' => $id,
                        'action' => 'reconcile_isolate',
                        'moved' => $reconciled,
                    ];
                }
                unset($router);
                $state['campaigns'][$id] = $campaign;
                continue;
            }

            // 窗口内：不再做金丝雀换批；清理旧 testing 残留为 armed，只等二墙 webhook
            $touched = $this->decoyRolloverIfNeeded($router, $today, $now);
            foreach ($sourceIds as $poolId) {
                $domain = $router['decoy']['domains'][$poolId] ?? null;
                if (!is_array($domain)) {
                    continue;
                }
                if ((string) ($domain['phase'] ?? '') !== 'testing') {
                    continue;
                }
                $armedIp = (string) ($domain['armed_ip'] ?? $domain['cur_ip'] ?? '');
                $router['decoy']['domains'][$poolId]['phase'] = 'armed';
                $router['decoy']['domains'][$poolId]['armed'] = true;
                $router['decoy']['domains'][$poolId]['current'] = null;
                $router['decoy']['domains'][$poolId]['queue'] = [];
                if ($armedIp !== '') {
                    $router['decoy']['domains'][$poolId]['armed_ip'] = $armedIp;
                    $this->applyPoolHost($router, $poolId, $armedIp);
                }
                $touched = true;
            }

            // 搜索槽：观察期内没被墙就是清白，放人腾槽；腾出来立刻派发积压嫌疑批
            $observeSeconds = $this->decoyObserveSeconds();
            foreach ($this->decoySearchPoolIds($router) as $slot) {
                $dispatchedAt = (int) (
                    $router['decoy']['domains'][$slot]['dispatched_at'] ?? 0
                );
                if ($dispatchedAt <= 0 || $now - $dispatchedAt < $observeSeconds) {
                    continue;
                }
                // 本组有多少人真的拉过槽 IP：0 说明槽 IP 可能本来就不通，
                // 这一轮等于没测到人，明早看日志才能分清「真清白」和「白跑」
                $pulled = count(array_intersect(
                    $this->poolLockedMemberIds($router, $slot),
                    $this->collectPoolPullersInWindow(
                        $campaign,
                        $slot,
                        $dispatchedAt,
                        $now
                    )
                ));
                $released = $this->releaseSearchSlot(
                    $router,
                    $slot,
                    $now,
                    '诱捕清白收回·分组测试观察期内未被墙'
                );
                if ($released > 0) {
                    $touched = true;
                    $actions[] = [
                        'campaign_id' => $id,
                        'action' => 'search_release',
                        'pool_id' => $slot,
                        'released' => $released,
                        'pulled' => $pulled,
                    ];
                    Log::notice('BaitSplit 分组测试放行：观察期无墙', [
                        'pool_id' => $slot,
                        'released' => $released,
                        'pulled' => $pulled,
                        'slot_ip' => (string) ($router['pools'][$slot]['host'] ?? ''),
                    ]);
                }
            }
            // 队列只是兜底：新鲜的批先测（LIFO），放太久的直接丢。
            // 实测排过两小时的嫌疑批只有 1% 的人会再拉订阅，测了也拿不到证据，
            // 留着只会占住槽位把真正新鲜的批挤掉。
            $queueTtl = max(
                300,
                (int) ($this->config['decoy_queue_ttl_minutes'] ?? 30) * 60
            );
            $pending = array_values(array_filter(
                (array) ($router['suspect_queue'] ?? []),
                fn(array $item): bool => $now - (int) ($item['at'] ?? 0) <= $queueTtl
            ));
            if (count($pending) !== count((array) ($router['suspect_queue'] ?? []))) {
                $touched = true;
            }
            if ($pending !== []) {
                $item = array_pop($pending);
                $result = $this->dispatchSuspectBatch(
                    $router,
                    (array) ($item['ids'] ?? []),
                    (string) ($item['origin'] ?? ''),
                    $now
                );
                if ($result['dispatched'] === 0) {
                    // 放回队尾：它仍是最新鲜的一批，下一分钟还该优先重试
                    $pending[] = $item;
                } else {
                    $touched = true;
                    if ($result['leftover'] !== []) {
                        $pending[] = [
                            'ids' => $result['leftover'],
                            'origin' => (string) ($item['origin'] ?? ''),
                            'at' => (int) ($item['at'] ?? $now),
                        ];
                    }
                    $actions[] = [
                        'campaign_id' => $id,
                        'action' => 'search_dispatch',
                        'dispatched' => $result['dispatched'],
                        'slots' => $result['slots'],
                    ];
                    Log::notice('BaitSplit 队列派发嫌疑批', [
                        'dispatched' => $result['dispatched'],
                        'age_seconds' => $now - (int) ($item['at'] ?? $now),
                        'slots' => $result['slots'],
                        'queue_left' => count($pending),
                    ]);
                }
            }
            $router['suspect_queue'] = array_values($pending);
            if ($touched) {
                $router['config_version']++;
                $changed = true;
                $actions[] = ['campaign_id' => $id, 'action' => 'simplify_no_canary'];
            }
            unset($router);
            $state['campaigns'][$id] = $campaign;
        }
        if ($changed) {
            $this->saveState($state);
        }
        return ['in_window' => $inWindow, 'actions' => $actions];
    }

    /**
     * 处理一次换 IP 事件：
     * - reason=machine（机器挂壁）：只重置该池观察窗口，不记分、不隔离。
     * - reason=blocked（被墙）：把"死 IP 存活期内拉过订阅"的用户记跟墙分，
     *   达到阈值自动锁进观察3，并写入完整事件日志。
     */
    private function processWallEvent(
        array $campaign,
        array &$router,
        array $poolIds,
        string $oldIp,
        string $newIp,
        string $reason
    ): array {
        $now = time();
        $lookback = max(60, (int) ($this->config['wall_lookback_seconds'] ?? 3600));
        $freshMax = max(300, (int) ($this->config['wall_fresh_max_seconds'] ?? 7200));
        $threshold = max(1, (int) ($this->config['wall_hit_threshold'] ?? 2));
        $obsPoolId = $this->resolveWallObservePoolId($router);
        $eventPools = [];
        $suspectIds = [];
        $movedIds = [];
        $scoredCount = 0;

        foreach (array_unique($poolIds) as $poolId) {
            $pool = $router['pools'][$poolId] ?? null;
            if ($pool === null) {
                continue;
            }
            $rawStart = (int) ($pool['last_rotation_at'] ?? 0);
            // 老 IP 首墙：从没换过 IP 或该 IP 存活过久（跨白天），曝光窗口不可信 → 只换 IP 不记分
            $stale = $rawStart <= 0 || ($now - $rawStart) > $freshMax;
            $windowStart = $rawStart > 0 ? $rawStart : ($now - $lookback);
            $exposed = $this->poolExposureLastMap($campaign, $poolId);
            $exposedTotal = count($exposed);
            $poolSuspects = [];
            foreach ($exposed as $userId => $lastAt) {
                if ($lastAt > $windowStart && $lastAt <= $now) {
                    $poolSuspects[] = (int) $userId;
                }
            }
            $router['pools'][$poolId]['last_rotation_at'] = $now;
            $eventPools[] = [
                'pool_id' => $poolId,
                'pool_name' => (string) ($pool['name'] ?? $poolId),
                'mode' => 'exposure',
                'stale' => $stale,
                'window_start' => $windowStart,
                'window_seconds' => $now - $windowStart,
                'exposed_total' => $exposedTotal,
                'suspect_count' => $stale ? 0 : count($poolSuspects),
            ];
            if ($reason !== 'blocked' || $stale || $poolSuspects === []) {
                continue;
            }
            $suspectIds = array_merge($suspectIds, $poolSuspects);
            foreach ($poolSuspects as $userId) {
                $key = (string) $userId;
                $router['wall_hits'][$key] =
                    (int) ($router['wall_hits'][$key] ?? 0) + 1;
                $router['wall_last'][$key] = $now;
                $scoredCount++;
                if ($this->tryAutoIsolateUser($router, $key, $threshold, $obsPoolId, $now)) {
                    $movedIds[] = $userId;
                }
            }
        }
        $suspectIds = array_values(array_unique($suspectIds));

        $logEntry = [
            'at' => $now,
            'reason' => $reason,
            'mode' => 'exposure',
            'old_ip' => $oldIp,
            'new_ip' => $newIp,
            'pools' => $eventPools,
            'suspect_count' => count($suspectIds),
            'scored_count' => $scoredCount,
            'threshold' => $threshold,
            'moved_count' => count($movedIds),
            'moved_ids' => array_slice($movedIds, 0, 200),
            'observe_pool_id' => $obsPoolId,
        ];
        $router['wall_log'][] = $logEntry;
        $router['wall_log'] = array_slice($router['wall_log'], -200);

        return [
            'reason' => $reason,
            'mode' => 'exposure',
            'suspect_count' => count($suspectIds),
            'scored_count' => $scoredCount,
            'moved_count' => count($movedIds),
            'observe_pool_id' => $obsPoolId,
        ];
    }

    private function configBool(string $key, bool $default = false): bool
    {
        $v = $this->config[$key] ?? $default;
        if (is_array($v) && array_key_exists('value', $v)) {
            $v = $v['value'];
        }
        if (is_bool($v)) {
            return $v;
        }
        if (is_string($v)) {
            return in_array(strtolower(trim($v)), ['1', 'true', 'yes', 'on'], true);
        }
        return (bool) $v;
    }

    /**
     * 跟墙分达标则锁定到观察隔离池。跳过危险/封禁/已在隔离池锁定/诱捕进行中的用户。
     */
    private function tryAutoIsolateUser(
        array &$router,
        string $key,
        int $threshold,
        string $obsPoolId,
        int $now
    ): bool {
        if (!$this->configBool('wall_auto_isolate', true) || $obsPoolId === '') {
            return false;
        }
        if ((int) ($router['wall_hits'][$key] ?? 0) < $threshold) {
            return false;
        }
        $note = (string) ($router['overrides'][$key]['note'] ?? '');
        // 诱捕二分进行中或已确认内鬼：不抢流程
        foreach ([
            '金丝雀测试批',
            '诱捕嫌疑隔离',
            '诱捕确认内鬼',
            '诱捕二墙快隔离',
            '诱捕墙后拉订阅',
            '二墙拉订阅',
            '金丝雀观察正常',
            '分组测试',
            '诱捕清白收回',
        ] as $marker) {
            if (str_contains($note, $marker)) {
                return false;
            }
        }
        $currentPoolId = (string) (
            $router['overrides'][$key]['pool_id']
            ?? ($router['assignments'][$key] ?? '')
        );
        $currentType = (string) ($router['pools'][$currentPoolId]['type'] ?? '');
        // 安全组只认人工操作，自动流程一律不碰
        if (in_array($currentType, ['danger', 'blacklist', 'safe'], true)) {
            return false;
        }
        // 已锁在诱捕隔离链或搜索槽里的人归诱捕流程管，
        // 白天补漏不能把正在分组测试的嫌疑再拖回观察3
        if (
            !empty($router['overrides'][$key]['locked'])
            && in_array($currentPoolId, array_merge(
                $this->decoyIsolateChain($router),
                $this->decoySearchPoolIds($router)
            ), true)
        ) {
            return false;
        }
        // 已在隔离池但未锁定 / 备注不是自动隔离 → 仍强制锁定，防止漏人
        $router['assignments'][$key] = $obsPoolId;
        $router['overrides'][$key] = $this->normalizeOverride([
            'pool_id' => $obsPoolId,
            'locked' => true,
            'note' => '自动跟墙隔离（跟墙 '
                . (int) $router['wall_hits'][$key] . ' 次）',
            'updated_at' => $now,
        ]);
        $uid = (int) $key;
        if ($uid > 0) {
            $router['untested_ids'] = array_values(array_diff(
                $router['untested_ids'] ?? [],
                [$uid]
            ));
        }
        return true;
    }

    /** 扫一遍 wall_hits，把已达标但未隔离的用户补隔离（白天 cron / 收回后）。 */
    private function reconcileWallIsolates(array &$router, int $now): int
    {
        $threshold = max(1, (int) ($this->config['wall_hit_threshold'] ?? 2));
        $obsPoolId = $this->resolveWallObservePoolId($router);
        if (!$this->configBool('wall_auto_isolate', true) || $obsPoolId === '') {
            return 0;
        }
        $moved = 0;
        foreach (($router['wall_hits'] ?? []) as $key => $hits) {
            if ((int) $hits < $threshold) {
                continue;
            }
            if ($this->tryAutoIsolateUser($router, (string) $key, $threshold, $obsPoolId, $now)) {
                $moved++;
            }
        }
        if ($moved > 0) {
            Log::notice('BaitSplit 跟墙隔离补漏', [
                'moved' => $moved,
                'observe_pool_id' => $obsPoolId,
                'threshold' => $threshold,
            ]);
        }
        return $moved;
    }

    private function routerPoolExposureKey(array $campaign, string $poolId): string
    {
        return "bait_split:router:{$campaign['id']}:{$campaign['router']['generation']}:pool:{$poolId}";
    }

    private function routerPoolExposureCountKey(array $campaign, string $poolId): string
    {
        return $this->routerPoolExposureKey($campaign, $poolId) . ':counts';
    }

    private function routerPoolExposureLastKey(array $campaign, string $poolId): string
    {
        return $this->routerPoolExposureKey($campaign, $poolId) . ':last';
    }

    private function recordExposure(
        array $campaign,
        array $assignment,
        int $userId
    ): void
    {
        try {
            $key = $this->branchExposureKey($campaign, $assignment['path']);
            Redis::sadd($key, (string) $userId);
            Redis::expire($key, 86400 * 30);
        } catch (\Throwable) {
            // 统计失败不能影响用户获取订阅。
        }
    }

    private function branchExposureUserIds(array $campaign, array $assignment): array
    {
        try {
            $keys = [$this->branchExposureKey($campaign, $assignment['path'])];
            if ($campaign['generation'] === '') {
                $keys[] = $this->legacyExposureKey(
                    $campaign,
                    $assignment['round'],
                    $assignment['bucket']
                );
            }
            $userIds = [];
            foreach ($keys as $key) {
                $userIds = array_merge($userIds, Redis::smembers($key));
            }
            $userIds = $this->normalizeIds($userIds);
            sort($userIds);
            return $userIds;
        } catch (\Throwable) {
            return [];
        }
    }

    private function branchExposureKey(array $campaign, array $path): string
    {
        $generation = $campaign['generation'] !== '' ? $campaign['generation'] : 'legacy';
        return sprintf(
            'bait_split:exposure:%s:%s:branch:%s',
            $campaign['id'],
            $generation,
            implode('.', $path)
        );
    }

    private function legacyExposureKey(array $campaign, int $round, int $bucket): string
    {
        if ($campaign['hash_algo'] === 'bit') {
            return "bait_split:exposure:{$round}:{$this->bucketLabel($bucket)}";
        }
        return "bait_split:exposure:{$campaign['id']}:{$round}:{$bucket}";
    }
}
