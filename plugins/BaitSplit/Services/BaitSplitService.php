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
    private const UPSTREAM_SYNC_MARK = 'bait_split:upstream_sync_at';
    private const WALL_EVIDENCE_KEY = 'bait_split:wall_evidence';
    private const WALL_EVIDENCE_KEEP = 600;
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

        // 管理员主动点「进入树形排查」时，池内锁定用户必须带上。
        // 观察组里的人几乎全是锁定的；若仍按 overrideBlocksAutomation 过滤，
        // 会出现「危险观察1 有人却无法进树」的情况。
        $userIds = $this->normalizeIds($this->poolMemberIds($campaign, $poolId));
        if ($userIds === []) {
            throw new InvalidArgumentException('该用户池没有可进入树形排查的固定用户');
        }
        $type = (string) ($sourcePool['type'] ?? '');
        if (in_array($type, ['danger', 'blacklist'], true)) {
            throw new InvalidArgumentException('危险组 / 封禁组不能进入树形排查');
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
        string $reason = 'blocked',
        string $source = ''
    ): array {
        $oldIp = trim($oldIp);
        $newIp = $this->normalizePublicIp($newIp);
        $targetId = trim($targetId);
        $reason = $this->normalizeRotationReason($reason);
        $isWall = $this->rotationIsWall($reason, $source);
        if ($oldIp !== '') {
            $oldIp = $this->normalizePublicIp($oldIp);
        }
        if ($targetId === '') {
            throw new InvalidArgumentException('必须提供目标唯一标识 target_id');
        }
        if ($oldIp !== '' && $oldIp === $newIp) {
            throw new InvalidArgumentException('新旧 IP 不能相同');
        }

        $state = $this->freshState();
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

        // 墙后二分追猎优先接管：它自带完整的洗清/切分/定罪流程，不走旧车道。
        if (
            $this->huntEnabled()
            && count($targetPoolIds) === 1
            && in_array(
                $targetPoolIds[0],
                $this->huntManagedPoolIds($router),
                true
            )
        ) {
            $poolId = $targetPoolIds[0];
            $nowTs = time();
            // 我们自己请求的换 IP 落地时也会收到通知，绝不能当成墙来算嫌疑。
            $consumed = $this->huntConsumeRequestedRotation(
                $router,
                $poolId,
                $newIp,
                $nowTs
            );
            if ($consumed) {
                $wall = [
                    'reason' => 'hunt_rotation',
                    'mode' => 'hunt',
                    'phase' => 'slot_ip_ready',
                ];
            } elseif ($isWall) {
                $wall = $this->huntHandleWall(
                    $campaign,
                    $router,
                    $poolId,
                    $oldIp,
                    $newIp,
                    $nowTs
                );
                $wall['mode'] = 'hunt';
                $wall['source'] = $source;
                $this->huntLogWall($router, $poolId, $oldIp, $newIp, $wall, $nowTs);
            } else {
                // 扩容、手动、外部接口触发的换址：只把新 IP 换上，不产出嫌疑。
                $this->applyPoolHost($router, $poolId, $newIp);
                $wall = [
                    'reason' => $reason,
                    'mode' => 'hunt',
                    'phase' => 'not_wall',
                    'source' => $source,
                ];
            }
            $router['config_version']++;
            $state['campaigns'][$campaignId] = $campaign;
            $this->saveState($state);
            return [
                'campaign_id' => $campaignId,
                'target_id' => $targetId,
                'reason' => $reason,
                'source' => $source,
                'old_ip' => $oldIp,
                'new_ip' => $newIp,
                'wall' => $wall,
                'hunt' => true,
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
        // 非追猎池同样只认真墙：手动/外部/扩容触发的换址不该记跟墙分。
        $wall = $this->processWallEvent(
            $campaign,
            $router,
            $targetPoolIds,
            $oldIp,
            $newIp,
            $isWall ? 'blocked' : 'machine'
        );
        $router['config_version']++;
        $state['campaigns'][$campaignId] = $campaign;
        $this->saveState($state);

        return [
            'campaign_id' => $campaignId,
            'target_id' => $targetId,
            'reason' => $reason,
            'source' => $source,
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

    /**
     * 进锁之后必须用这个读，不能用 state()。
     *
     * Setting 是 scoped 绑定，一个请求内共用一份 loadedSettings；webhook 在验签
     * 时就已经把快照钉住了，那时锁还没拿到。等真正进锁再调 state()，读回来的仍是
     * 进锁之前的旧快照，于是后进来的那次写会把先落盘的那次整个盖掉——两条换 IP
     * 通知只差一秒时必然发生。丢掉实例强制重载，锁才真正管用。
     */
    private function freshState(): array
    {
        app()->forgetInstance(Setting::class);
        return $this->state();
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
            'wall_evidence_version' => 1,
            'wall_log' => [],
            'decoy_nights' => [],
            'search_origin' => [],
            'queue_strategy_version' => 2,
            'hunt' => $this->emptyHuntState(),
            'fresh_suspects' => [],
            'dormant_suspects' => [],
            'suspect_queue' => [],
            'clean_rounds' => [],
            'positive_rounds' => [],
            'candidate_users' => [],
        ];
    }

    private function normalizeRouter(array $router): array
    {
        $queueStrategyVersion = array_key_exists('queue_strategy_version', $router)
            ? max(1, (int) $router['queue_strategy_version'])
            : 1;
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
        $router['queue_strategy_version'] = $queueStrategyVersion;
        $router['fresh_suspects'] = $this->normalizeIds(
            $router['fresh_suspects'] ?? []
        );
        $queuedUsers = array_fill_keys($router['fresh_suspects'], true);
        $suspectQueue = [];
        foreach ((array) ($router['suspect_queue'] ?? []) as $item) {
            if (!is_array($item)) {
                continue;
            }
            $ids = array_values(array_filter(
                $this->normalizeIds($item['ids'] ?? []),
                function (int $uid) use (&$queuedUsers): bool {
                    if (isset($queuedUsers[$uid])) {
                        return false;
                    }
                    $queuedUsers[$uid] = true;
                    return true;
                }
            ));
            if ($ids === []) {
                continue;
            }
            $suspectQueue[] = [
                'ids' => $ids,
                'origin' => (string) ($item['origin'] ?? ''),
                'at' => (int) ($item['at'] ?? 0),
            ];
        }
        // 批次过多时合并最老部分，不得截掉 FIFO 队首或永久丢失候选。
        if (count($suspectQueue) > 500) {
            $mergeCount = count($suspectQueue) - 499;
            $oldest = array_slice($suspectQueue, 0, $mergeCount);
            $mergedIds = [];
            foreach ($oldest as $item) {
                $mergedIds = array_merge($mergedIds, $item['ids']);
            }
            $suspectQueue = array_merge([[
                'ids' => $this->normalizeIds($mergedIds),
                'origin' => (string) ($oldest[0]['origin'] ?? ''),
                'at' => (int) ($oldest[0]['at'] ?? 0),
            ]], array_slice($suspectQueue, $mergeCount));
        }
        $router['suspect_queue'] = $suspectQueue;
        $dormantSuspects = [];
        foreach ((array) ($router['dormant_suspects'] ?? []) as $userId => $at) {
            $uid = (int) $userId;
            if ($uid > 0) {
                $dormantSuspects[(string) $uid] = max(0, (int) $at);
            }
        }
        $router['dormant_suspects'] = $dormantSuspects;
        foreach (['clean_rounds', 'positive_rounds'] as $counterKey) {
            $normalized = [];
            foreach ((array) ($router[$counterKey] ?? []) as $userId => $count) {
                if ((int) $userId > 0 && (int) $count > 0) {
                    $normalized[(string) (int) $userId] = (int) $count;
                }
            }
            $router[$counterKey] = $normalized;
        }
        if ((int) ($router['wall_evidence_version'] ?? 1) < 2) {
            // 5.1 及以前把来源池的普通拉取者也累计成“跟墙命中”，
            // 数值无法区分真实搜索槽阳性，升级时一次性清掉污染分。
            $router['wall_hits'] = [];
            $router['wall_score'] = [];
            $router['wall_last'] = [];
            $router['wall_evidence_version'] = 2;
        }
        $candidateUsers = [];
        foreach ((array) ($router['candidate_users'] ?? []) as $userId => $item) {
            if ((int) $userId <= 0 || !is_array($item)) {
                continue;
            }
            $candidateUsers[(string) (int) $userId] = [
                'entered_at' => (int) ($item['entered_at'] ?? 0),
                'baseline_pulls' => max(-1, (int) ($item['baseline_pulls'] ?? -1)),
                'ready_at' => (int) ($item['ready_at'] ?? 0),
            ];
        }
        $router['candidate_users'] = $candidateUsers;
        $router['wall_log'] = is_array($router['wall_log'] ?? null)
            ? array_slice(array_values($router['wall_log']), -200)
            : [];
        // 旧诱捕的 decoy 域状态已无人读取，落地时直接丢掉，省下每次保存的体积
        unset($router['decoy']);
        $router['hunt'] = $this->normalizeHuntState($router['hunt'] ?? []);
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
            return $this->guardHuntSlot($router, (string) $override['pool_id'], true);
        }
        $assigned = $router['assignments'][(string) $userId] ?? null;
        return $assigned === null
            ? null
            : $this->guardHuntSlot($router, (string) $assigned, false);
    }

    /**
     * 追猎槽只认追猎自己下的锁定覆盖。
     *
     * 槽里混进零星用户就会拿到测试 IP，二分出来的结论全是污染——过期用户续费后
     * 旧归属复活就是这么漏的。所以非锁定来源指向槽时，一律退回服务池。
     */
    private function guardHuntSlot(
        array $router,
        string $poolId,
        bool $locked
    ): string {
        if ($locked || $poolId === '' || !$this->huntEnabled()) {
            return $poolId;
        }
        if (!in_array($poolId, $this->huntSlotIds($router), true)) {
            return $poolId;
        }
        $service = $this->huntServicePoolIds($router);
        return $service[0] ?? $this->poolIdByType($router, 'default');
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
                    $state = $this->freshState();
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
            $now = time();
            foreach ($poolIds as $poolId) {
                $poolKey = $this->routerPoolExposureKey($campaign, $poolId);
                Redis::sadd($poolKey, (string) $userId);
                Redis::expire($poolKey, 86400 * 90);
                $countKey = $this->routerPoolExposureCountKey($campaign, $poolId);
                $lastKey = $this->routerPoolExposureLastKey($campaign, $poolId);
                Redis::hincrby($countKey, (string) $userId, 1);
                Redis::hset($lastKey, (string) $userId, $now);
                Redis::expire($countKey, 86400 * 90);
                Redis::expire($lastKey, 86400 * 90);

                // 墙事件必须按「实际下发的精确 IP」归因。旧实现只有池级
                // last_at，同一个槽换过几批人后，迟到的墙会算到下一批头上。
                $ip = (string) (
                    $campaign['router']['pools'][(string) $poolId]['host'] ?? ''
                );
                if ($ip !== '') {
                    $ipLastKey = $this->routerPoolIpExposureLastKey(
                        $campaign,
                        (string) $poolId,
                        $ip
                    );
                    Redis::hset($ipLastKey, (string) $userId, $now);
                    Redis::expire($ipLastKey, 86400 * 14);
                }
            }
        } catch (\Throwable) {
            // 统计失败不能影响订阅。
        }
    }

    /** @return array<int,int> 精确到池+IP的用户最后拉取时间 */
    private function poolIpExposureLastMap(
        array $campaign,
        string $poolId,
        string $ip
    ): array {
        if ($ip === '') {
            return [];
        }
        try {
            $raw = Redis::hgetall(
                $this->routerPoolIpExposureLastKey($campaign, $poolId, $ip)
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
        return [
            'events' => $events,
            'top_suspects' => $topSuspects,
            'total_tracked' => count($router['wall_hits'] ?? []),
            'settings' => [
                'auto_isolate' => $this->configBool('wall_auto_isolate', false),
                'hit_threshold' => max(1, (int) ($this->config['wall_hit_threshold'] ?? 2)),
                'lookback_seconds' => max(60, (int) ($this->config['wall_lookback_seconds'] ?? 3600)),
                'fresh_max_seconds' => max(300, (int) ($this->config['wall_fresh_max_seconds'] ?? 7200)),
                'observe_pool_id' => $this->resolveWallObservePoolId($router),
                'observe_pool_raw' => (string) ($this->config['wall_observe_pool_id'] ?? ''),
                'confirm_pool_id' => $this->resolveDecoyConfirmPoolId($router),
                'confirm_pool_raw' => (string) ($this->config['decoy_confirm_pool_id'] ?? ''),
            ],
            'hunt' => $this->huntReport($router, $poolName),
            'pending_ip_rotates' => $this->pendingIpRotateCount(),
        ];
    }

    /** 追猎状态给控制台看：每条链进展、定罪记录、非泄露铁证。 */
    private function huntReport(array $router, callable $poolName): array
    {
        $hunt = $this->normalizeHuntState($router['hunt'] ?? []);
        $chains = [];
        foreach ($hunt['chains'] as $chainId => $chain) {
            $slots = [];
            foreach ($chain['slots'] as $slotId) {
                $slot = $hunt['slots'][$slotId] ?? [];
                $slots[] = [
                    'pool_id' => $slotId,
                    'pool_name' => $poolName($slotId),
                    'host' => (string) ($router['pools'][$slotId]['host'] ?? ''),
                    'members' => count((array) ($slot['members'] ?? [])),
                    'pending' => count((array) ($slot['pending'] ?? [])),
                    'awaiting_ip' => (bool) ($slot['awaiting_ip'] ?? false),
                ];
            }
            $chains[] = [
                'id' => $chainId,
                'round' => $chain['round'],
                'seed_size' => $chain['seed_size'],
                'origin_pool_name' => $poolName($chain['origin_pool_id']),
                'last_event_at' => $chain['last_event_at'],
                'slots' => $slots,
            ];
        }
        $servicePoolIds = $this->huntServicePoolIds($router);
        $slotPoolIds = $this->huntSlotIds($router);
        return [
            'enabled' => $this->huntEnabled(),
            'rotate_api_ready' => $this->huntRotateClient() !== null,
            'service_pool_names' => array_map($poolName, $servicePoolIds),
            'slot_pool_names' => array_map($poolName, $slotPoolIds),
            'chain_capacity' => intdiv(count($slotPoolIds), 2),
            'active_chains' => count($chains),
            'chains' => $chains,
            'convicted' => array_reverse($hunt['convicted']),
            'anomalies' => array_reverse($hunt['anomalies']),
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
                'source' => (string) ($data['source'] ?? ''),
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
                            (string) ($data['reason'] ?? 'blocked'),
                            (string) ($data['source'] ?? '')
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

    /**
     * 这次换 IP 到底算不算被墙。
     *
     * reason 单独不可信：它只表示「换的是 IP 还是机器」。手动、外部接口和地址
     * 自发漂移同样标 blocked，而被墙逼出来的整机重建反倒标 machine。上游的
     * source 才是干净信号：
     *   auto     被墙检测触发的自动换 IP        —— 是墙
     *   rebuild  连续被墙达阈值后销毁重建的新机  —— 是墙
     *   manual / external / launch / sync / drift —— 不是墙
     *
     * 认不出的 source 一律不当墙：上游还在扩充这张表，误判成墙会凭空开一条
     * 追猎链去追不存在的人，代价比漏一次墙大得多。只有 source 整个缺失（旧版
     * 发送端）才退回看 reason，保持改动前的行为。
     */
    private function rotationIsWall(string $reason, string $source): bool
    {
        $source = strtolower(trim($source));
        if ($source === '') {
            return $reason === 'blocked';
        }
        if (in_array($source, ['auto', 'rebuild'], true)) {
            return true;
        }
        if (
            !in_array(
                $source,
                ['manual', 'external', 'launch', 'sync', 'drift'],
                true
            )
        ) {
            Log::warning('BaitSplit 收到未知的换址来源，按非墙处理', [
                'source' => $source,
                'reason' => $reason,
            ]);
        }
        return false;
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

    private function resolveDecoyCandidatePoolId(array $router): string
    {
        $configured = $this->resolvePoolIdByRef(
            $router,
            (string) ($this->config['decoy_candidate_pool_id'] ?? '')
        );
        if (
            $configured !== ''
            && !in_array(
                (string) ($router['pools'][$configured]['type'] ?? ''),
                ['safe', 'danger', 'blacklist'],
                true
            )
        ) {
            return $configured;
        }
        foreach ($router['pools'] as $poolId => $pool) {
            if (
                ($pool['type'] ?? '') === 'observation'
                && preg_match('/观察\s*1/u', (string) ($pool['name'] ?? ''))
            ) {
                return (string) $poolId;
            }
        }
        return '';
    }

    private function resolveDecoyConfirmPoolId(array $router): string
    {
        $ref = $this->resolvePoolIdByRef(
            $router,
            (string) ($this->config['decoy_confirm_pool_id'] ?? '')
        );
        return $ref !== '' ? $ref : $this->poolIdByType($router, 'danger');
    }

    /**
     * 通知丢失时的兜底：主动问上游每个目标的当前地址，本地对不上就补录。
     *
     * 只补地址，不产生墙事件。一次换址算不算墙，只有推送里的 source 说了算，
     * 这里靠"地址变了"反推会凭空开出追猎链，把嫌疑记到无辜的人头上。
     *
     * 需要兜底本身就说明推送链路断了，所以每补一个都记 warning。
     */
    public function reconcileUpstreamHosts(int $interval = 300): array
    {
        $client = IpRotationClient::fromConfig($this->config);
        if ($client === null) {
            return ['skipped' => 'no_api'];
        }
        // add 只在键不存在时成功，天然实现"每 interval 秒最多跑一次"
        if (!Cache::add(self::UPSTREAM_SYNC_MARK, time(), $interval)) {
            return ['skipped' => 'throttled'];
        }

        $targets = [];
        foreach ($this->state()['campaigns'] ?? [] as $campaignId => $campaign) {
            if (empty($campaign['router']['enabled'])) {
                continue;
            }
            foreach ($campaign['router']['pools'] ?? [] as $pool) {
                $targetId = trim((string) ($pool['webhook_id'] ?? ''));
                $host = trim((string) ($pool['host'] ?? ''));
                // 域名池由别处维护，只认地址型的池
                if (
                    $targetId === ''
                    || filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)
                        === false
                ) {
                    continue;
                }
                $targets[] = [
                    'campaign_id' => (string) $campaignId,
                    'target_id' => $targetId,
                    'host' => $host,
                ];
            }
        }

        // 查询在锁外做，别让 HTTP 往返堵住墙处理
        $deadline = microtime(true) + 20;
        $drifted = [];
        foreach ($targets as $target) {
            if (microtime(true) >= $deadline) {
                break;
            }
            $info = $client->instance($target['target_id']);
            // online 为假说明新地址还在验墙，此刻补录会下发一个还没通的地址
            if ($info === null || !$info['online'] || $info['ip'] === $target['host']) {
                continue;
            }
            $drifted[] = $target + ['new_ip' => $info['ip']];
        }
        if (!$drifted) {
            return ['checked' => count($targets), 'fixed' => 0];
        }

        $fixed = 0;
        foreach ($drifted as $item) {
            try {
                Cache::lock(self::STATE_LOCK, 45)->block(
                    10,
                    fn() => $this->rotateCampaignIp(
                        $item['campaign_id'],
                        $item['host'],
                        $item['new_ip'],
                        $item['target_id'],
                        'machine',
                        'drift'
                    )
                );
                $fixed++;
                Log::warning('BaitSplit 上游地址已变但没收到通知，已兜底补录', [
                    'target_id' => $item['target_id'],
                    'old_ip' => $item['host'],
                    'new_ip' => $item['new_ip'],
                ]);
            } catch (\Throwable $exception) {
                Log::error('BaitSplit 兜底补录地址失败', [
                    'target_id' => $item['target_id'],
                    'new_ip' => $item['new_ip'],
                    'error' => $exception->getMessage(),
                ]);
            }
        }
        return ['checked' => count($targets), 'fixed' => $fixed];
    }

    /**
     * 分钟级维护：先消化积压的换 IP 事件，再跑追猎的兜底轮询与空转链解散。
     *
     * 方法名和 bait:decoy 命令名沿用旧称，只因调度里引用着它，改名收益不抵风险。
     */
    public function runDecoyOrchestration(): array
    {
        // 优先消化积压换 IP，避免锁冲突导致永久丢事件
        $drain = $this->drainPendingIpRotates(80);
        // 推送链路断掉时，这是唯一能让下发地址回到正确值的路径，
        // 所以放在追猎开关之前，关了追猎也要跑
        $upstream = $this->reconcileUpstreamHosts();
        if (!$this->huntEnabled()) {
            return [
                'skipped' => 'disabled',
                'drain' => $drain,
                'upstream' => $upstream,
            ];
        }
        try {
            $result = Cache::lock(self::STATE_LOCK, 45)->block(
                12,
                fn(): array => $this->runHuntLocked()
            );
            $result['drain'] = $drain;
            $result['upstream'] = $upstream;
            return $result;
        } catch (LockTimeoutException) {
            Log::warning('BaitSplit bait:decoy 抢锁失败', [
                'pending' => $drain['remaining'] ?? 0,
            ]);
            return [
                'skipped' => 'locked',
                'drain' => $drain,
                'upstream' => $upstream,
            ];
        }
    }

    private function runHuntLocked(): array
    {
        $state = $this->freshState();
        $now = time();
        $changed = false;
        $actions = [];
        foreach ($state['campaigns'] as $id => $campaign) {
            if (empty($campaign['router']['enabled'])) {
                continue;
            }
            $router = &$campaign['router'];
            // 只取算曝光键要用的两个字段，避免把带引用的 $campaign 整个复制进去
            $exposureCtx = [
                'id' => (string) ($campaign['id'] ?? $id),
                'router' => ['generation' => (int) ($router['generation'] ?? 0)],
            ];
            $tick = $this->huntTick($router, $exposureCtx, $now);
            // 漏掉任何一个计数都会让那一轮的改动不落盘，下一分钟原样重做一遍
            if (array_sum(array_map('intval', $tick)) > 0) {
                $router['config_version']++;
                $changed = true;
                $actions[] = [
                    'campaign_id' => $id,
                    'action' => 'hunt_tick',
                ] + $tick;
            }
            unset($router);
            // $campaign 是副本，不写回改动就丢了
            $state['campaigns'][$id] = $campaign;
        }
        if ($changed) {
            $this->saveState($state);
        }
        return ['actions' => $actions];
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

    private function routerUserPoolId(array $router, int $userId): string
    {
        $key = (string) $userId;
        $override = $router['overrides'][$key] ?? null;
        if (
            is_array($override)
            && $this->overrideIsActive($override)
            && (string) ($override['pool_id'] ?? '') !== ''
        ) {
            return (string) $override['pool_id'];
        }
        $assigned = (string) ($router['assignments'][$key] ?? '');
        return $assigned !== ''
            ? $this->guardHuntSlot($router, $assigned, false)
            : $this->poolIdByType($router, 'default');
    }

    // ===================== 墙后二分追猎（v6） =====================
    // 内鬼要上报就必须先刷新订阅拿到新 IP，这一步他绕不过去。所以每次墙都能
    // 拿到一个干净结论：没拉过这个死 IP 的人本轮无罪；拉过的人里必有内鬼。
    // 于是把拉过的人对半切到一条链的两个槽（各自全新 IP），下次哪个槽被墙，
    // 人就在那一半。log2(N) 次墙即可定位到个人。
    //
    // 关键前提：每次换人必须换全新 IP。IP 一旦被上一批人看过就报废，否则
    // 归因会串批——这正是旧逻辑失效的原因。

    private function emptyHuntState(): array
    {
        return [
            'chains' => [],
            'slots' => [],
            'anomalies' => [],
            'convicted' => [],
            'parked' => [],
            'seeded_at' => 0,
        ];
    }

    private function normalizeHuntState(mixed $hunt): array
    {
        $hunt = is_array($hunt) ? $hunt : [];
        $slots = [];
        foreach ((array) ($hunt['slots'] ?? []) as $poolId => $slot) {
            $poolId = (string) $poolId;
            if ($poolId === '' || !is_array($slot)) {
                continue;
            }
            $slots[$poolId] = [
                'chain_id' => (string) ($slot['chain_id'] ?? ''),
                'members' => $this->normalizeIds($slot['members'] ?? []),
                'pending' => $this->normalizeIds($slot['pending'] ?? []),
                'ip' => (string) ($slot['ip'] ?? ''),
                'ip_at' => max(0, (int) ($slot['ip_at'] ?? 0)),
                'awaiting_ip' => (bool) ($slot['awaiting_ip'] ?? false),
                'requested_at' => max(0, (int) ($slot['requested_at'] ?? 0)),
            ];
        }
        $chains = [];
        foreach ((array) ($hunt['chains'] ?? []) as $chainId => $chain) {
            $chainId = (string) $chainId;
            if ($chainId === '' || !is_array($chain)) {
                continue;
            }
            $chainSlots = array_values(array_unique(array_filter(
                array_map('strval', (array) ($chain['slots'] ?? []))
            )));
            if (count($chainSlots) !== 2) {
                continue;
            }
            $chains[$chainId] = [
                'id' => $chainId,
                'slots' => $chainSlots,
                'round' => max(0, (int) ($chain['round'] ?? 0)),
                'seed_size' => max(0, (int) ($chain['seed_size'] ?? 0)),
                'origin_pool_id' => (string) ($chain['origin_pool_id'] ?? ''),
                'created_at' => max(0, (int) ($chain['created_at'] ?? 0)),
                'last_event_at' => max(0, (int) ($chain['last_event_at'] ?? 0)),
            ];
        }
        $anomalies = [];
        foreach ((array) ($hunt['anomalies'] ?? []) as $item) {
            if (is_array($item)) {
                $anomalies[] = [
                    'at' => max(0, (int) ($item['at'] ?? 0)),
                    'pool_id' => (string) ($item['pool_id'] ?? ''),
                    'ip' => (string) ($item['ip'] ?? ''),
                    'kind' => (string) ($item['kind'] ?? ''),
                ];
            }
        }
        $convicted = [];
        foreach ((array) ($hunt['convicted'] ?? []) as $item) {
            if (is_array($item) && (int) ($item['user_id'] ?? 0) > 0) {
                $convicted[] = [
                    'at' => max(0, (int) ($item['at'] ?? 0)),
                    'user_id' => (int) $item['user_id'],
                    'rounds' => max(0, (int) ($item['rounds'] ?? 0)),
                    'seed_size' => max(0, (int) ($item['seed_size'] ?? 0)),
                ];
            }
        }
        $parked = [];
        foreach ((array) ($hunt['parked'] ?? []) as $item) {
            if (!is_array($item)) {
                continue;
            }
            $ids = $this->normalizeIds($item['ids'] ?? []);
            if (count($ids) < 2) {
                continue;
            }
            $parked[] = [
                'ids' => $ids,
                'origin_pool_id' => (string) ($item['origin_pool_id'] ?? ''),
                'round' => max(0, (int) ($item['round'] ?? 0)),
                'from' => (string) ($item['from'] ?? ''),
                'at' => max(0, (int) ($item['at'] ?? 0)),
            ];
        }
        return [
            'chains' => $chains,
            'slots' => $slots,
            'anomalies' => array_slice($anomalies, -50),
            'convicted' => array_slice($convicted, -50),
            'parked' => array_slice($parked, -10),
            'seeded_at' => max(0, (int) ($hunt['seeded_at'] ?? 0)),
        ];
    }

    private function huntEnabled(): bool
    {
        return $this->configBool('hunt_enabled', false);
    }

    /** 逗号分隔的池引用（ID / 接口标识 / 名称）解析为池 ID，保持配置顺序。 */
    private function splitPoolIdList(array $router, string $raw): array
    {
        $ids = [];
        foreach (explode(',', $raw) as $ref) {
            $poolId = $this->resolvePoolIdByRef($router, $ref);
            if ($poolId !== '' && !in_array($poolId, $ids, true)) {
                $ids[] = $poolId;
            }
        }
        return $ids;
    }

    /** 承载大众的服务池：它们被墙时产出的拉取者作为追猎种子。 */
    private function huntServicePoolIds(array $router): array
    {
        return $this->splitPoolIdList(
            $router,
            (string) ($this->config['hunt_service_pool_ids'] ?? '')
        );
    }

    /** 追猎槽：两两组成一条二分链。 */
    private function huntSlotIds(array $router): array
    {
        $service = array_flip($this->huntServicePoolIds($router));
        return array_values(array_filter(
            $this->splitPoolIdList(
                $router,
                (string) ($this->config['hunt_slot_pool_ids'] ?? '')
            ),
            fn(string $poolId): bool => !isset($service[$poolId])
                && !in_array(
                    (string) ($router['pools'][$poolId]['type'] ?? ''),
                    ['safe', 'danger', 'blacklist'],
                    true
                )
        ));
    }

    /** 追猎接管的池子：服务池 + 追猎槽。 */
    private function huntManagedPoolIds(array $router): array
    {
        return array_values(array_unique(array_merge(
            $this->huntServicePoolIds($router),
            $this->huntSlotIds($router)
        )));
    }

    private function huntSlotPairs(array $router): array
    {
        return array_chunk($this->huntSlotIds($router), 2);
    }

    private function huntTargetId(array $router, string $poolId): string
    {
        $webhookId = (string) ($router['pools'][$poolId]['webhook_id'] ?? '');
        return $webhookId !== '' ? $webhookId : $poolId;
    }

    private function huntRotateClient(): ?IpRotationClient
    {
        return IpRotationClient::fromConfig($this->config);
    }

    private function huntSlot(array &$router, string $poolId): array
    {
        if (!isset($router['hunt']['slots'][$poolId])) {
            $router['hunt']['slots'][$poolId] = [
                'chain_id' => '',
                'members' => [],
                'pending' => [],
                'ip' => (string) ($router['pools'][$poolId]['host'] ?? ''),
                'ip_at' => 0,
                'awaiting_ip' => false,
                'requested_at' => 0,
            ];
        }
        return $router['hunt']['slots'][$poolId];
    }

    /**
     * 主入口：某个被追猎接管的池子被墙了。
     *
     * @return array{handled:bool,phase:string,pullers:int,detail:array}
     */
    private function huntHandleWall(
        array $campaign,
        array &$router,
        string $poolId,
        string $oldIp,
        string $newIp,
        int $now
    ): array {
        $deadIp = $oldIp !== ''
            ? $oldIp
            : (string) ($router['pools'][$poolId]['host'] ?? '');
        // 死 IP 的上线时刻，applyPoolHost 会把它覆盖成本次换址时间，先取走。
        $deadIpAt = (int) ($router['pools'][$poolId]['last_rotation_at'] ?? 0);
        // webhook 给的新 IP 已经过国内验墙，可以直接上线。
        $this->applyPoolHost($router, $poolId, $newIp);
        $slot = $this->huntSlot($router, $poolId);
        $router['hunt']['slots'][$poolId]['ip'] = $newIp;
        $router['hunt']['slots'][$poolId]['ip_at'] = $now;
        $router['hunt']['slots'][$poolId]['awaiting_ip'] = false;
        $router['hunt']['slots'][$poolId]['requested_at'] = 0;

        [$pullers, $rawPullerCount, $pullerLastAt] = $this->huntPullersOfDeadIp(
            $campaign,
            $router,
            $poolId,
            $deadIp
        );

        // 无论后面能不能开链，名单都先落盘。开不出链而被丢弃的墙同样是证据，
        // 攒够之后可以离线做交集打分，不占任何槽位。
        $this->huntRecordEvidence(
            $router,
            $poolId,
            $deadIp,
            $deadIpAt,
            $pullerLastAt,
            $rawPullerCount,
            $now
        );

        // 没有任何人拉过这个 IP 却被墙 —— 没人见过它，就没人能上报它。
        // 这是「墙与泄露无关」的铁证，必须立刻叫出来。
        if ($rawPullerCount === 0) {
            $this->huntRecordAnomaly($router, $poolId, $deadIp, $now);
            return [
                'handled' => true,
                'phase' => 'walled_without_pullers',
                'pullers' => 0,
                'detail' => ['dead_ip' => $deadIp],
            ];
        }

        // 有人拉过，但都已经搬走了：这是迟到的墙，不是铁证，别误报。
        if ($pullers === []) {
            Log::warning('BaitSplit 追猎收到迟到的墙，拉取者已全部转移', [
                'pool_id' => $poolId,
                'dead_ip' => $deadIp,
                'raw_pullers' => $rawPullerCount,
            ]);
            return [
                'handled' => true,
                'phase' => 'stale_wall',
                'pullers' => 0,
                'detail' => ['dead_ip' => $deadIp, 'raw' => $rawPullerCount],
            ];
        }

        $chainId = (string) ($slot['chain_id'] ?? '');
        if ($chainId !== '' && isset($router['hunt']['chains'][$chainId])) {
            // 迟到的墙：这个 IP 早就换掉了，现在槽里是下一批人。
            // 拉取者名单仍然准确（按精确 IP 记的），但绝不能拿它去洗清当前批。
            $assignedIp = (string) ($slot['ip'] ?? '');
            if ($assignedIp !== '' && $deadIp !== $assignedIp) {
                Log::warning('BaitSplit 追猎收到迟到的墙，已忽略不动当前批', [
                    'slot' => $poolId,
                    'dead_ip' => $deadIp,
                    'assigned_ip' => $assignedIp,
                    'pullers' => count($pullers),
                ]);
                return [
                    'handled' => true,
                    'phase' => 'stale_wall',
                    'pullers' => count($pullers),
                    'detail' => ['dead_ip' => $deadIp],
                ];
            }
            return $this->huntAdvanceChain(
                $router,
                $chainId,
                $poolId,
                $pullers,
                $now
            );
        }
        return $this->huntSeedChain($router, $poolId, $pullers, $now);
    }

    /**
     * 谁拉过这个死 IP。只认当前确实归属该池的人，避免把已经搬走的历史成员算进来。
     *
     * 同时回传未过滤的原始人数：区分「真的没人见过这个 IP」（非泄露铁证）
     * 和「见过的人已经搬走了」（迟到的墙）——这两件事结论完全相反。
     *
     * 第三个返回值是同一批人的最后拉取时刻，留给证据落盘算「拉完多久墙就来了」。
     *
     * @return array{0:array<int,int>,1:int,2:array<int,int>}
     */
    private function huntPullersOfDeadIp(
        array $campaign,
        array $router,
        string $poolId,
        string $deadIp
    ): array {
        if ($deadIp === '') {
            return [[], 0, []];
        }
        $raw = $this->poolIpExposureLastMap($campaign, $poolId, $deadIp);
        $ids = [];
        $lastAt = [];
        foreach ($raw as $uid => $ts) {
            $uid = (int) $uid;
            if ($uid <= 0) {
                continue;
            }
            $curPool = $this->routerUserPoolId($router, $uid);
            if ($curPool !== $poolId) {
                continue;
            }
            $type = (string) ($router['pools'][$curPool]['type'] ?? '');
            if (in_array($type, ['danger', 'blacklist'], true)) {
                continue;
            }
            $ids[] = $uid;
            $lastAt[$uid] = (int) $ts;
        }
        sort($ids);
        return [$ids, count($raw), $lastAt];
    }

    /** 服务池被墙：拿拉取者开一条新链。 */
    private function huntSeedChain(
        array &$router,
        string $originPoolId,
        array $pullers,
        int $now
    ): array {
        // 已经在别的链里受测（含等新 IP 期间寄放回服务池的人）不能被重复抓走。
        $busy = [];
        foreach ((array) ($router['hunt']['slots'] ?? []) as $slot) {
            foreach (array_merge(
                (array) ($slot['members'] ?? []),
                (array) ($slot['pending'] ?? [])
            ) as $uid) {
                $busy[(int) $uid] = true;
            }
        }
        $pullers = array_values(array_filter(
            $pullers,
            fn(int $uid): bool => !isset($busy[$uid])
        ));
        if ($pullers === []) {
            return [
                'handled' => true,
                'phase' => 'all_pullers_busy',
                'pullers' => 0,
                'detail' => [],
            ];
        }

        $freePair = null;
        foreach ($this->huntSlotPairs($router) as $pair) {
            if (count($pair) !== 2) {
                continue;
            }
            $busy = false;
            foreach ($pair as $slotId) {
                $slot = $this->huntSlot($router, $slotId);
                if (
                    ($slot['chain_id'] ?? '') !== ''
                    || $slot['members'] !== []
                    || $slot['pending'] !== []
                ) {
                    $busy = true;
                    break;
                }
            }
            if (!$busy) {
                $freePair = $pair;
                break;
            }
        }
        if ($freePair === null) {
            // 槽位全忙：本轮拉取者留在服务池，等有空链再抓。
            return [
                'handled' => true,
                'phase' => 'no_free_chain',
                'pullers' => count($pullers),
                'detail' => [],
            ];
        }

        $chainId = 'hunt-' . bin2hex(random_bytes(6));
        $router['hunt']['chains'][$chainId] = [
            'id' => $chainId,
            'slots' => array_values($freePair),
            'round' => 0,
            'seed_size' => count($pullers),
            'origin_pool_id' => $originPoolId,
            'created_at' => $now,
            'last_event_at' => $now,
        ];
        $router['hunt']['seeded_at'] = $now;
        $result = $this->huntSplitInto(
            $router,
            $chainId,
            $pullers,
            $now
        );
        Log::warning('BaitSplit 追猎开链', [
            'chain' => $chainId,
            'origin' => $originPoolId,
            'seed' => count($pullers),
            'slots' => $freePair,
        ]);
        return [
            'handled' => true,
            'phase' => 'chain_seeded',
            'pullers' => count($pullers),
            'detail' => $result,
        ];
    }

    /** 追猎槽被墙：洗清无关的人，把拉取者继续对半切。 */
    private function huntAdvanceChain(
        array &$router,
        string $chainId,
        string $walledSlotId,
        array $pullers,
        int $now
    ): array {
        $chain = $router['hunt']['chains'][$chainId];
        $chain['round'] = (int) $chain['round'] + 1;
        $chain['last_event_at'] = $now;
        $router['hunt']['chains'][$chainId] = $chain;
        $note = '追猎第' . $chain['round'] . '轮洗清（未拉取被墙IP）';

        // 本槽里没拉过死 IP 的人 + 兄弟槽全员，本轮都洗清。
        // 成员以「实际锁在该池的人」为准，不信 hunt 里存的快照——
        // 后台手工挪过人时快照会失真。
        $clear = [];
        foreach ($chain['slots'] as $slotId) {
            $slot = $this->huntSlot($router, $slotId);
            $inSlot = array_values(array_unique(array_merge(
                $this->poolLockedMemberIds($router, $slotId),
                $slot['pending']
            )));
            $clear = array_merge(
                $clear,
                $slotId === $walledSlotId
                    ? array_values(array_diff($inSlot, $pullers))
                    : $inSlot
            );
        }
        $this->huntReleaseToService($router, $clear, $chain, $now, $note);

        $convictAt = max(1, (int) ($this->config['decoy_convict_at'] ?? 1));
        if (count($pullers) <= $convictAt) {
            foreach ($pullers as $uid) {
                $this->huntConvict($router, (int) $uid, $chain, $now);
            }
            $this->huntDissolveChain($router, $chainId, $now, 'convicted');
            Log::warning('BaitSplit 追猎定罪', [
                'chain' => $chainId,
                'round' => $chain['round'],
                'ids' => $pullers,
            ]);
            return [
                'handled' => true,
                'phase' => 'convicted',
                'pullers' => count($pullers),
                'detail' => ['ids' => $pullers],
            ];
        }

        $result = $this->huntSplitInto(
            $router,
            $chainId,
            $pullers,
            $now,
            $walledSlotId
        );
        return [
            'handled' => true,
            'phase' => 'chain_split',
            'pullers' => count($pullers),
            'detail' => $result,
        ];
    }

    /**
     * 把一批人对半切进链的两个槽。
     *
     * $freshSlotId 是刚因被墙换到全新 IP 的槽——那个 IP 刚过验墙、还没发给任何人，
     * 所以可以直接承接一半，省掉一次换 IP 等待。另一半必须等新 IP，因为兄弟槽
     * 当前的 IP 已经被上一批人看过了。
     */
    private function huntSplitInto(
        array &$router,
        string $chainId,
        array $ids,
        int $now,
        string $freshSlotId = ''
    ): array {
        $ids = $this->normalizeIds($ids);
        sort($ids);
        $slots = $router['hunt']['chains'][$chainId]['slots'];
        if ($freshSlotId !== '' && ($slots[1] ?? '') === $freshSlotId) {
            $slots = [$slots[1], $slots[0]];
        }
        [$slotA, $slotB] = $slots;
        // 交错切而不是前后对半：ID 和注册时间强相关，前后切会让「老号」
        // 全落一边，一旦墙其实和号龄有关就会把两个变量混在一起。
        $batchA = [];
        $batchB = [];
        foreach ($ids as $i => $uid) {
            if ($i % 2 === 0) {
                $batchA[] = $uid;
            } else {
                $batchB[] = $uid;
            }
        }

        if ($freshSlotId === $slotA) {
            $this->huntAdoptSlotBatch($router, $slotA, $chainId, $batchA, $now);
        } else {
            $this->huntStageSlotBatch($router, $slotA, $chainId, $batchA, $now);
        }
        $this->huntStageSlotBatch($router, $slotB, $chainId, $batchB, $now);

        return [$slotA => count($batchA), $slotB => count($batchB)];
    }

    /** 槽的 IP 已经是全新的：直接把这批人搬进去。 */
    private function huntAdoptSlotBatch(
        array &$router,
        string $slotId,
        string $chainId,
        array $batch,
        int $now
    ): void {
        $this->huntSlot($router, $slotId);
        $router['hunt']['slots'][$slotId]['chain_id'] = $chainId;
        $router['hunt']['slots'][$slotId]['pending'] = $this->normalizeIds($batch);
        $router['hunt']['slots'][$slotId]['awaiting_ip'] = false;
        $router['hunt']['slots'][$slotId]['requested_at'] = 0;
        $this->huntActivateSlot($router, $slotId, $now);
    }

    /**
     * 把一批人挂到槽上等全新 IP。等待期间人先寄放回服务池，绝不能留在槽里，
     * 否则他们会拿到上一批看过的脏 IP，归因就串批了。
     */
    private function huntStageSlotBatch(
        array &$router,
        string $slotId,
        string $chainId,
        array $batch,
        int $now
    ): void {
        $batch = $this->normalizeIds($batch);
        $this->huntSlot($router, $slotId);
        $router['hunt']['slots'][$slotId]['members'] = [];
        if ($batch === []) {
            $router['hunt']['slots'][$slotId]['chain_id'] = '';
            $router['hunt']['slots'][$slotId]['pending'] = [];
            $router['hunt']['slots'][$slotId]['awaiting_ip'] = false;
            $router['hunt']['slots'][$slotId]['requested_at'] = 0;
            return;
        }
        $router['hunt']['slots'][$slotId]['chain_id'] = $chainId;
        $router['hunt']['slots'][$slotId]['pending'] = $batch;
        $router['hunt']['slots'][$slotId]['awaiting_ip'] = true;
        $router['hunt']['slots'][$slotId]['requested_at'] = $now;

        $chain = $router['hunt']['chains'][$chainId] ?? [];
        $this->huntReleaseToService(
            $router,
            $batch,
            $chain,
            $now,
            '追猎待测：等全新 IP 上线'
        );

        $client = $this->huntRotateClient();
        if ($client === null) {
            // 没配换 IP 接口就退化为直接搬入：能跑，但跨批污染的老问题会回来。
            Log::warning('BaitSplit 追猎未配置换 IP 接口，退化为直接搬入', [
                'slot' => $slotId,
            ]);
            $this->huntActivateSlot($router, $slotId, $now);
            return;
        }
        if (!$client->requestReplace($this->huntTargetId($router, $slotId))) {
            // 换 IP 没受理：保持 awaiting，由分钟级维护重试兜底。
            Log::warning('BaitSplit 追猎换 IP 未受理，等分钟级重试', [
                'slot' => $slotId,
            ]);
        }
    }

    /** 新 IP 已上线：把等待中的人正式搬进槽。 */
    private function huntActivateSlot(
        array &$router,
        string $slotId,
        int $now
    ): int {
        $slot = $this->huntSlot($router, $slotId);
        $batch = $this->normalizeIds($slot['pending'] ?? []);
        $router['hunt']['slots'][$slotId]['pending'] = [];
        $router['hunt']['slots'][$slotId]['awaiting_ip'] = false;
        $router['hunt']['slots'][$slotId]['requested_at'] = 0;
        if ($batch === []) {
            return 0;
        }
        $moved = $this->huntMoveUsers(
            $router,
            $batch,
            $slotId,
            $now,
            '墙后二分追猎：进入测试槽'
        );
        $router['hunt']['slots'][$slotId]['members'] = $moved;
        $router['hunt']['slots'][$slotId]['ip'] =
            (string) ($router['pools'][$slotId]['host'] ?? '');
        $router['hunt']['slots'][$slotId]['ip_at'] = $now;
        return count($moved);
    }

    /** 无条件迁移（已确认内鬼和封禁的人不动）。 */
    private function huntMoveUsers(
        array &$router,
        array $ids,
        string $poolId,
        int $now,
        string $note
    ): array {
        if (!isset($router['pools'][$poolId])) {
            return [];
        }
        $moved = [];
        foreach ($this->normalizeIds($ids) as $uid) {
            $key = (string) $uid;
            $curPool = $this->routerUserPoolId($router, $uid);
            $curType = (string) ($router['pools'][$curPool]['type'] ?? '');
            if (in_array($curType, ['danger', 'blacklist'], true)) {
                continue;
            }
            $router['assignments'][$key] = $poolId;
            $router['overrides'][$key] = $this->normalizeOverride([
                'pool_id' => $poolId,
                'locked' => true,
                'note' => $note,
                'updated_at' => $now,
            ]);
            $router['untested_ids'] = array_values(array_diff(
                $router['untested_ids'] ?? [],
                [$uid]
            ));
            $moved[] = $uid;
        }
        return $moved;
    }

    /** 本轮洗清：放回服务池继续正常用。 */
    private function huntReleaseToService(
        array &$router,
        array $ids,
        array $chain,
        int $now,
        string $note
    ): int {
        $ids = $this->normalizeIds($ids);
        if ($ids === []) {
            return 0;
        }
        $target = (string) ($chain['origin_pool_id'] ?? '');
        if ($target === '' || !isset($router['pools'][$target])) {
            $target = (string) ($this->huntServicePoolIds($router)[0] ?? '');
        }
        if ($target === '') {
            return 0;
        }
        // 回服务池只改 assignment 并删掉覆盖规则。写 locked 覆盖会让
        // 8000 多人全被钉死，状态 blob 也会无止境膨胀。
        $released = 0;
        foreach ($this->normalizeIds($ids) as $uid) {
            $key = (string) $uid;
            $curType = (string) (
                $router['pools'][$this->routerUserPoolId($router, $uid)]['type'] ?? ''
            );
            if (in_array($curType, ['danger', 'blacklist'], true)) {
                continue;
            }
            unset($router['overrides'][$key]);
            $router['assignments'][$key] = $target;
            $released++;
        }
        if ($released > 0) {
            Log::notice('BaitSplit 追猎放人回服务池', [
                'target' => $target,
                'count' => $released,
                'reason' => $note,
            ]);
        }
        return $released;
    }

    private function huntConvict(
        array &$router,
        int $userId,
        array $chain,
        int $now
    ): void {
        $dangerPoolId = $this->resolveDecoyConfirmPoolId($router);
        if ($dangerPoolId === '') {
            // 收敛好几轮才换来的结论，没有落点也必须叫出来，不能悄悄丢
            Log::error('BaitSplit 追猎已定位内鬼但没有确认内鬼池，结论无处安放', [
                'user_id' => $userId,
                'rounds' => (int) ($chain['round'] ?? 0),
            ]);
            return;
        }
        $key = (string) $userId;
        $router['assignments'][$key] = $dangerPoolId;
        $router['overrides'][$key] = $this->normalizeOverride([
            'pool_id' => $dangerPoolId,
            'locked' => true,
            'note' => '墙后二分追猎确认内鬼（第'
                . (int) ($chain['round'] ?? 0) . '轮收敛）',
            'updated_at' => $now,
        ]);
        $router['hunt']['convicted'][] = [
            'at' => $now,
            'user_id' => $userId,
            'rounds' => (int) ($chain['round'] ?? 0),
            'seed_size' => (int) ($chain['seed_size'] ?? 0),
        ];
        $router['hunt']['convicted'] = array_slice(
            $router['hunt']['convicted'],
            -50
        );
    }

    private function huntDissolveChain(
        array &$router,
        string $chainId,
        int $now,
        string $reason
    ): int {
        $chain = $router['hunt']['chains'][$chainId] ?? null;
        if (!is_array($chain)) {
            return 0;
        }
        $ids = [];
        foreach ($chain['slots'] as $slotId) {
            $slot = $this->huntSlot($router, $slotId);
            $ids = array_merge($ids, $slot['members'], $slot['pending']);
            unset($router['hunt']['slots'][$slotId]);
        }
        $released = $this->huntReleaseToService(
            $router,
            $ids,
            $chain,
            $now,
            '追猎链解散（' . $reason . '）'
        );
        unset($router['hunt']['chains'][$chainId]);
        return $released;
    }

    /** 写进 wall_log，复用控制台现有的墙记录展示。 */
    private function huntLogWall(
        array &$router,
        string $poolId,
        string $oldIp,
        string $newIp,
        array $wall,
        int $now
    ): void {
        $detail = (array) ($wall['detail'] ?? []);
        $movedIds = $this->normalizeIds($detail['ids'] ?? []);
        $router['wall_log'][] = [
            'at' => $now,
            'reason' => 'blocked',
            'mode' => 'hunt',
            'phase' => (string) ($wall['phase'] ?? ''),
            'old_ip' => $oldIp,
            'new_ip' => $newIp,
            'pools' => [[
                'pool_id' => $poolId,
                'name' => (string) ($router['pools'][$poolId]['name'] ?? ''),
            ]],
            'suspect_count' => (int) ($wall['pullers'] ?? 0),
            'scored_count' => 0,
            'threshold' => 0,
            'moved_count' => count($movedIds),
            'moved_ids' => $movedIds,
            'hunt_detail' => $detail,
            'observe_pool_id' => '',
        ];
        $router['wall_log'] = array_slice($router['wall_log'], -200);
    }

    /**
     * 把一次墙的拉取者名单落盘。
     *
     * 每条记录存的是「这个 IP 从上线到被墙期间，谁拉过、最后一次拉在什么时候」。
     * 有了拉取时刻才能算出「拉完多久墙就来了」——高频拉取的人总会出现在名单里，
     * 光看出现次数会把他们全冤枉掉，必须靠时间差和对照组把频次的影响除掉。
     *
     * @param array<int,int> $pullerLastAt uid => 最后拉取时刻
     */
    private function huntRecordEvidence(
        array $router,
        string $poolId,
        string $deadIp,
        int $deadIpAt,
        array $pullerLastAt,
        int $rawPullerCount,
        int $now
    ): void {
        if ($pullerLastAt === []) {
            return;
        }
        try {
            $record = [
                'at' => $now,
                'pool_id' => $poolId,
                'pool' => (string) ($router['pools'][$poolId]['name'] ?? $poolId),
                // huntSlot() 会给任何被墙的池惰性建槽记录，服务池也在内，
                // 所以只能按配置判断，否则分不出自然墙和追猎诱发的墙
                'is_slot' => in_array($poolId, $this->huntSlotIds($router), true),
                'ip' => $deadIp,
                'ip_at' => $deadIpAt,
                'raw' => $rawPullerCount,
                'pullers' => $pullerLastAt,
            ];
            Redis::lpush(self::WALL_EVIDENCE_KEY, json_encode(
                $record,
                JSON_UNESCAPED_UNICODE
            ));
            Redis::ltrim(self::WALL_EVIDENCE_KEY, 0, self::WALL_EVIDENCE_KEEP - 1);
            Redis::expire(self::WALL_EVIDENCE_KEY, 86400 * 30);
        } catch (\Throwable $exception) {
            // 证据落盘失败不能影响追猎本身。
            Log::warning('BaitSplit 墙证据落盘失败', [
                'pool_id' => $poolId,
                'error' => $exception->getMessage(),
            ]);
        }
    }

    private function huntRecordAnomaly(
        array &$router,
        string $poolId,
        string $ip,
        int $now
    ): void {
        $router['hunt']['anomalies'][] = [
            'at' => $now,
            'pool_id' => $poolId,
            'ip' => $ip,
            'kind' => 'walled_without_pullers',
        ];
        $router['hunt']['anomalies'] = array_slice(
            $router['hunt']['anomalies'],
            -50
        );
        Log::error('BaitSplit 铁证：IP 无人拉取却被墙，说明墙与订阅泄露无关', [
            'pool_id' => $poolId,
            'ip' => $ip,
            'hint' => '优先排查 AWS IP 段被扫或节点协议特征被识别',
        ]);
    }

    /**
     * 我们主动请求的换 IP 落地了（不是墙）。搬人进槽，不做任何嫌疑计算。
     */
    private function huntConsumeRequestedRotation(
        array &$router,
        string $poolId,
        string $newIp,
        int $now
    ): bool {
        $slot = $router['hunt']['slots'][$poolId] ?? null;
        if (!is_array($slot) || !($slot['awaiting_ip'] ?? false)) {
            return false;
        }
        $this->applyPoolHost($router, $poolId, $newIp);
        $this->huntActivateSlot($router, $poolId, $now);
        Log::notice('BaitSplit 追猎槽已换上全新 IP', [
            'slot' => $poolId,
            'ip' => $newIp,
        ]);
        return true;
    }

    /**
     * 分钟级维护：兜底轮询换 IP 状态（通知可能丢），并解散长时间无进展的链。
     */
    private function huntTick(array &$router, array $campaign, int $now): array
    {
        if (!$this->huntEnabled()) {
            return ['activated' => 0, 'dissolved' => 0];
        }
        $activated = 0;
        $client = $this->huntRotateClient();
        // 整个 tick 在状态锁里跑，每次最多打 2 个槽的接口，剩下的下一分钟再说。
        $probeBudget = 2;
        foreach (array_keys((array) ($router['hunt']['slots'] ?? [])) as $slotId) {
            $slotId = (string) $slotId;
            $slot = $router['hunt']['slots'][$slotId];
            if (!($slot['awaiting_ip'] ?? false) || $client === null) {
                continue;
            }
            if ($probeBudget <= 0) {
                break;
            }
            $probeBudget--;
            // 通知一般几十秒到几分钟到，超过 3 分钟才主动查，避免频繁打接口。
            $waited = $now - (int) ($slot['requested_at'] ?? 0);
            if ($waited < 180) {
                continue;
            }
            $targetId = $this->huntTargetId($router, $slotId);
            $info = $client->instance($targetId);
            if (
                $info !== null
                && $info['online']
                && $info['ip'] !== (string) ($slot['ip'] ?? '')
            ) {
                $this->applyPoolHost($router, $slotId, $info['ip']);
                $activated += $this->huntActivateSlot($router, $slotId, $now);
                continue;
            }
            // 等太久还没换上：可能上次请求没受理，重发一次并重置计时。
            if ($waited >= 600) {
                $client->requestReplace($targetId);
                $router['hunt']['slots'][$slotId]['requested_at'] = $now;
            }
        }

        $idleLimit = max(30, (int) ($this->config['hunt_chain_idle_minutes'] ?? 240)) * 60;
        // 成员迟迟不来拉订阅时的兜底，再值钱的名单也不能永久占着槽
        $maxLife = max(
            $idleLimit,
            max(60, (int) ($this->config['hunt_chain_max_minutes'] ?? 720)) * 60
        );
        $dissolved = 0;
        foreach (array_keys((array) ($router['hunt']['chains'] ?? [])) as $chainId) {
            $chainId = (string) $chainId;
            $chain = $router['hunt']['chains'][$chainId] ?? null;
            if (!is_array($chain)) {
                continue;
            }
            $armedAt = $this->huntChainArmedAt($router, $campaign, $chain);
            if ($armedAt > 0) {
                $since = max((int) $chain['last_event_at'], $armedAt);
                if ($now - $since < $idleLimit) {
                    continue;
                }
            } elseif (
                $now - max(
                    (int) ($chain['created_at'] ?? 0),
                    (int) $chain['last_event_at']
                ) < $maxLife
            ) {
                continue;
            }
            // 已经收敛到很窄的链先把名单记下来。四轮才收敛到十来个人，
            // 直接放回服务池等于把这几轮的成果全扔了，下次得从几千人重来。
            $this->huntParkChain($router, $chainId, $now);
            // 长时间两边都不墙：内鬼当晚没动，放人回去，把槽腾出来。
            $dissolved += $this->huntDissolveChain(
                $router,
                $chainId,
                $now,
                '长时间无墙'
            );
        }

        $resumed = $this->huntResumeParked($router, $now);
        return [
            'activated' => $activated,
            'dissolved' => $dissolved,
            'resumed' => $resumed,
        ];
    }

    /**
     * 链上两个槽的人全部拉到当前 IP 的时刻，还没拉齐返回 0。
     *
     * 「多久没墙」必须从这一刻起算。按 IP 上线时间算的话，成员几小时不来拉
     * 订阅，超时就先到了——一个人都没看过这个地址，却判定成两边都不墙。
     */
    private function huntChainArmedAt(
        array $router,
        array $campaign,
        array $chain
    ): int {
        $armedAt = 0;
        foreach ((array) ($chain['slots'] ?? []) as $slotId) {
            $slotId = (string) $slotId;
            $slot = $router['hunt']['slots'][$slotId] ?? [];
            // 还在等新 IP，或上一轮切进来的人还没落位，实验都没就绪
            if (!empty($slot['awaiting_ip']) || !empty($slot['pending'])) {
                return 0;
            }
            $members = $this->normalizeIds($slot['members'] ?? []);
            if ($members === []) {
                continue;
            }
            $seen = $this->poolIpExposureLastMap(
                $campaign,
                $slotId,
                (string) ($router['pools'][$slotId]['host'] ?? '')
            );
            foreach ($members as $userId) {
                if (!isset($seen[$userId])) {
                    return 0;
                }
                $armedAt = max($armedAt, (int) $seen[$userId]);
            }
        }
        return $armedAt;
    }

    /**
     * 把收敛到很窄的链存档，等槽位空闲时再复活。
     *
     * 只存人不占槽：槽必须腾出来给新的墙，但这批人是几轮筛下来的，
     * 名单本身比槽位值钱得多。
     */
    private function huntParkChain(array &$router, string $chainId, int $now): void
    {
        $chain = $router['hunt']['chains'][$chainId] ?? null;
        if (!is_array($chain)) {
            return;
        }
        $max = max(2, (int) ($this->config['hunt_park_max_members'] ?? 20));
        $ids = [];
        foreach ((array) ($chain['slots'] ?? []) as $slotId) {
            $slot = $router['hunt']['slots'][(string) $slotId] ?? [];
            $ids = array_merge(
                $ids,
                (array) ($slot['members'] ?? []),
                (array) ($slot['pending'] ?? [])
            );
        }
        $ids = $this->normalizeIds($ids);
        // 人还太多说明没筛出什么，存了也没价值
        if (count($ids) < 2 || count($ids) > $max) {
            return;
        }
        $router['hunt']['parked'][] = [
            'ids' => $ids,
            'origin_pool_id' => (string) ($chain['origin_pool_id'] ?? ''),
            'round' => (int) ($chain['round'] ?? 0),
            'from' => $chainId,
            'at' => $now,
        ];
        $router['hunt']['parked'] = array_slice(
            (array) $router['hunt']['parked'],
            -10
        );
        Log::warning('BaitSplit 追猎窄链已挂起，等槽位空闲再续', [
            'chain' => $chainId,
            'round' => $chain['round'] ?? 0,
            'ids' => $ids,
        ]);
    }

    /**
     * 槽位空闲时复活挂起的窄链。
     *
     * 隔一段时间才复活：刚挂起就抢槽会把新墙挤成 no_free_chain，
     * 而新墙的拉取者集合往往比这批陈旧的嫌疑人更值得查。
     */
    private function huntResumeParked(array &$router, int $now): int
    {
        $parked = array_values((array) ($router['hunt']['parked'] ?? []));
        if ($parked === []) {
            return 0;
        }
        $cooldown = max(10, (int) ($this->config['hunt_park_cooldown_minutes'] ?? 120)) * 60;
        // 人少的先复活：越接近收敛，一次墙的信息量越大
        usort($parked, fn(array $a, array $b): int
            => count($a['ids']) <=> count($b['ids']));

        $resumed = 0;
        $keep = [];
        foreach ($parked as $entry) {
            $origin = (string) ($entry['origin_pool_id'] ?? '');
            $ids = [];
            foreach ($this->normalizeIds($entry['ids'] ?? []) as $uid) {
                // 挂起期间被挪走的人（比如已经进危险组）不再参与
                if ($this->routerUserPoolId($router, $uid) === $origin) {
                    $ids[] = $uid;
                }
            }
            if (count($ids) < 2) {
                Log::notice('BaitSplit 挂起的窄链已失效，成员不在原池', [
                    'from' => $entry['from'] ?? '',
                ]);
                continue;
            }
            if (
                $resumed > 0
                || $now - (int) ($entry['at'] ?? 0) < $cooldown
            ) {
                $keep[] = $entry;
                continue;
            }
            $result = $this->huntSeedChain($router, $origin, $ids, $now);
            if ((string) ($result['phase'] ?? '') !== 'chain_seeded') {
                $keep[] = $entry;
                continue;
            }
            $resumed++;
            Log::warning('BaitSplit 挂起的窄链已复活', [
                'from' => $entry['from'] ?? '',
                'round' => $entry['round'] ?? 0,
                'size' => count($ids),
            ]);
        }
        $router['hunt']['parked'] = array_values($keep);
        return $resumed;
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
     * 跟墙分达标则锁定到观察隔离池。默认关闭，开着会和追猎抢人。
     *
     * 只碰完全没人管的用户：任何已锁定的覆盖规则（人工指定、排查树、追猎槽）
     * 一律跳过，否则会把正在分组测试的嫌疑拖走、把排查树拆散。
     */
    private function tryAutoIsolateUser(
        array &$router,
        string $key,
        int $threshold,
        string $obsPoolId,
        int $now
    ): bool {
        if (!$this->configBool('wall_auto_isolate', false) || $obsPoolId === '') {
            return false;
        }
        if ((int) ($router['wall_hits'][$key] ?? 0) < $threshold) {
            return false;
        }
        if (!empty($router['overrides'][$key]['locked'])) {
            return false;
        }
        $currentPoolId = $this->routerUserPoolId($router, (int) $key);
        $currentPool = (array) ($router['pools'][$currentPoolId] ?? []);
        // 安全组只认人工操作，自动流程一律不碰
        if (in_array(
            (string) ($currentPool['type'] ?? ''),
            ['danger', 'blacklist', 'safe'],
            true
        )) {
            return false;
        }
        // 排查树节点和追猎槽各有自己的调度，别插手
        if ((string) ($currentPool['tree_node_id'] ?? '') !== '') {
            return false;
        }
        if (in_array($currentPoolId, $this->huntManagedPoolIds($router), true)) {
            return false;
        }
        if ($currentPoolId === $this->resolveDecoyCandidatePoolId($router)) {
            return false;
        }
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

    private function routerPoolIpExposureLastKey(
        array $campaign,
        string $poolId,
        string $ip
    ): string {
        return $this->routerPoolExposureKey($campaign, $poolId)
            . ':ip:' . sha1($ip) . ':last';
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
