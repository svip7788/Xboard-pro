<?php

namespace Plugin\BaitSplit\Services;

use App\Models\Plugin as PluginModel;
use App\Models\Server;
use App\Models\User;
use App\Support\Setting;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
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
        return array_values(array_map(
            fn(array $campaign): array => $this->campaignStatus($campaign),
            $this->state()['campaigns']
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
        $query = $this->eligibleUsersQuery($campaign['target_group_ids']);
        if ($keyword !== '') {
            $query->where(function (Builder $builder) use ($keyword): void {
                if (ctype_digit($keyword)) {
                    $builder->orWhere('id', (int) $keyword);
                }
                $builder->orWhere('email', 'like', '%' . $keyword . '%');
            });
        }

        $userIds = $query->orderBy('id')->pluck('id')->map('intval')
            ->filter(fn(int $id): bool =>
                $this->classifiedPoolId($campaign, $id) === $poolId
            )
            ->values()->all();
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
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $movedCount = $this->movePulledUsersInCampaign(
            $campaign,
            $sourcePoolId,
            $targetPoolId,
            1
        );
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return [
            'campaign' => $this->campaignStatus($campaign),
            'moved_count' => $movedCount,
        ];
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
        $router['pools'] = $snapshot['pools'];
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

        $userIds = $this->eligibleUsersQuery($campaign['target_group_ids'])
            ->pluck('id')->map('intval')
            ->filter(function (int $userId) use ($campaign, $router, $poolId): bool {
                $override = $router['overrides'][(string) $userId] ?? null;
                return $this->classifiedPoolId($campaign, $userId) === $poolId
                    && !$this->overrideBlocksAutomation($override);
            })->values()->all();
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
            $exposedMap = array_flip(
                $this->poolExposureIds($campaign, $node['pool_id'])
            );
            $suspectUserIds = [];
            foreach ($node['user_ids'] as $userId) {
                $override = $router['overrides'][(string) $userId] ?? null;
                if ($this->overrideBlocksAutomation($override)) {
                    $suspectUserIds[] = $userId;
                    continue;
                }
                if (
                    ($router['assignments'][(string) $userId] ?? '')
                    !== $node['pool_id']
                ) {
                    continue;
                }
                if (isset($exposedMap[$userId])) {
                    $suspectUserIds[] = $userId;
                    continue;
                }
                unset($router['assignments'][(string) $userId]);
                $releasedUserIds[] = $userId;
            }
            $router['investigation_nodes'][$nodeId]['user_ids'] =
                array_values(array_unique($suspectUserIds));
            $router['investigation_nodes'][$nodeId]['released_count'] =
                (int) ($node['released_count'] ?? 0)
                + count($releasedUserIds);
            $router['untested_ids'] = $this->normalizeIds(array_merge(
                $router['untested_ids'],
                $releasedUserIds
            ));
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
        if (count($nodeIds) < 2) {
            throw new InvalidArgumentException('至少选择两棵旧排查树');
        }

        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $branches = $this->normalizeInvestigationBranches($router, $branches);
        $sourceRoots = [];
        $sourceNodes = [];
        $userMap = [];
        $releasedUserMap = [];
        $protectedPoolIds = [];
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
                    $this->poolExposureIds($campaign, $node['pool_id'])
                );
                foreach ($node['user_ids'] as $userId) {
                    if (
                        ($router['assignments'][(string) $userId] ?? '')
                        !== $node['pool_id']
                    ) {
                        continue;
                    }
                    $override = $router['overrides'][(string) $userId] ?? null;
                    if ($this->overrideBlocksAutomation($override)) {
                        $protectedPoolIds[$node['pool_id']] = true;
                        continue;
                    }
                    if (isset($exposedMap[$userId])) {
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
            throw new InvalidArgumentException('已拉取用户人数少于新分组数');
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
            $router['investigation_nodes'][$sourceNodeId]['status'] = 'archived';
            $router['investigation_nodes'][$sourceNodeId]['updated_at'] = time();
            if (
                isset($router['pools'][$sourceNode['pool_id']])
                && !isset($protectedPoolIds[$sourceNode['pool_id']])
            ) {
                $router['pools'][$sourceNode['pool_id']]['enabled'] = false;
                $router['pools'][$sourceNode['pool_id']]['status'] = 'blocked';
            }
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
        $state = $this->state();
        $campaign = $this->requireRouterCampaign($state, $campaignId);
        $router = &$campaign['router'];
        $node = $router['investigation_nodes'][$nodeId] ?? null;
        if (
            !$node
            || $node['status'] !== 'safe'
            || $node['children'] !== []
        ) {
            throw new InvalidArgumentException('只能转移已确认安全的叶子节点');
        }
        $targetPool = $router['pools'][$targetPoolId] ?? null;
        if (
            !$targetPool
            || !$this->poolIsUsable($targetPool)
            || !in_array(
                $targetPool['type'],
                ['observation', 'safe', 'custom'],
                true
            )
        ) {
            throw new InvalidArgumentException('请选择可用的观察组、安全组或自定义组');
        }
        $userIds = array_values(array_filter(
            $node['user_ids'],
            function (int $userId) use ($router): bool {
                $override = $router['overrides'][(string) $userId] ?? null;
                return !$this->overrideBlocksAutomation($override);
            }
        ));
        if ($userIds === []) {
            throw new InvalidArgumentException('该节点没有可自动转移的用户');
        }
        $allocations = $this->allocateUsersToPoolChain(
            $campaign,
            $userIds,
            $targetPoolId,
            ['observation', 'safe', 'custom']
        );
        foreach ($allocations as $userId => $allocatedPoolId) {
            unset($router['overrides'][(string) $userId]);
            $router['assignments'][(string) $userId] = $allocatedPoolId;
        }
        $router['untested_ids'] = array_values(array_diff(
            $router['untested_ids'],
            $userIds
        ));
        $router['investigation_nodes'][$nodeId]['status'] = 'archived';
        $router['investigation_nodes'][$nodeId]['updated_at'] = time();
        if (
            isset($router['pools'][$node['pool_id']])
            && $router['pools'][$node['pool_id']]['tree_node_id'] === $nodeId
        ) {
            $router['pools'][$node['pool_id']]['enabled'] = false;
        }
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);
        return [
            'campaign' => $this->campaignStatus($campaign),
            'moved_count' => count($allocations),
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
        $exposed = array_flip($this->poolExposureIds(
            $campaign,
            $node['pool_id']
        ));
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
        app(Setting::class)->set(self::STATE_KEY, $state);
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
        foreach ($router['pools'] as $pool) {
            $poolHosts = array_merge(
                [(string) ($pool['host'] ?? '')],
                array_values((array) ($pool['node_hosts'] ?? []))
            );
            foreach ($poolHosts as $host) {
                if ($host !== '' && isset($hosts[$host])) {
                    throw new InvalidArgumentException(
                        "域名 {$host} 已被其他用户池使用"
                    );
                }
            }
        }
        return $branches;
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
            $overrideDelivered = $host !== '';
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
            if ($selectedPool && !$overrideDelivered) {
                $deliveredPoolIds[$selectedPool['id']] = true;
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
            ? array_slice($router['config_history'], -20)
            : [];
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
        $investigationNodes = [];
        foreach ((array) ($router['investigation_nodes'] ?? []) as $id => $node) {
            if (
                is_array($node)
                && !(bool) ($node['legacy'] ?? false)
                && !str_starts_with((string) ($node['id'] ?? $id), 'legacy-')
            ) {
                $nodeId = (string) ($node['id'] ?? $id);
                $investigationNodes[$nodeId] = $this->normalizeInvestigationNode(
                    $node,
                    $nodeId
                );
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
        $eligibleIds = $this->eligibleUsersQuery($campaign['target_group_ids'])
            ->pluck('id')->map('intval')->values()->all();
        $eligibleMap = array_flip($eligibleIds);
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
                ? $this->poolExposureIds($campaign, $node['pool_id'])
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
                        && isset($exposedMap[$userId])
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
        return (int) $override['expires_at'] <= 0 || (int) $override['expires_at'] > time();
    }

    private function overrideBlocksAutomation(?array $override): bool
    {
        return $override !== null
            && $this->overrideIsActive($override)
            && (bool) $override['locked'];
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

    private function movePulledUsersInCampaign(
        array &$campaign,
        string $sourcePoolId,
        string $targetPoolId,
        int $minimumCount
    ): int {
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
        $userIds = $this->eligibleUsersQuery($campaign['target_group_ids'])
            ->pluck('id')->map('intval')
            ->filter(function (int $userId) use (
                $campaign,
                $router,
                $sourcePoolId,
                $exposedMap
            ): bool {
                $override = $router['overrides'][(string) $userId] ?? null;
                return isset($exposedMap[$userId])
                    && $this->effectivePoolId($campaign, $userId) === $sourcePoolId
                    && !$this->overrideBlocksAutomation($override);
            })->values()->all();
        if (count($userIds) < $minimumCount) {
            throw new InvalidArgumentException('该用户池尚未达到自动分流容量');
        }

        $allowedTypes = ['default', 'probe', 'observation', 'emergency', 'safe', 'custom'];
        $allocations = $this->allocateUsersToPoolChain(
            $campaign,
            $userIds,
            $targetPoolId,
            $allowedTypes
        );
        foreach ($allocations as $userId => $allocatedPoolId) {
            unset($router['overrides'][(string) $userId]);
            $router['assignments'][(string) $userId] = $allocatedPoolId;
        }
        $router['untested_ids'] = array_values(array_diff(
            $router['untested_ids'],
            $userIds
        ));
        return count($allocations);
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
            Cache::lock('bait_split:admin_state', 15)
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
        $router['config_history'][] = [
            'version' => $router['config_version'],
            'pools' => $router['pools'],
            'created_at' => time(),
        ];
        $router['config_history'] = array_slice($router['config_history'], -20);
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
        $eligible = $this->eligibleUsersQuery($campaign['target_group_ids'])
            ->pluck('id')->map('intval')->values()->all();
        $eligibleMap = array_flip($eligible);
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
