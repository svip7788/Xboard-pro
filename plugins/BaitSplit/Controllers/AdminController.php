<?php

namespace Plugin\BaitSplit\Controllers;

use App\Http\Controllers\PluginController;
use App\Models\Server;
use App\Models\ServerGroup;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Validation\Rule;
use InvalidArgumentException;
use Plugin\BaitSplit\Services\BaitSplitService;
use Throwable;

class AdminController extends PluginController
{
    public function meta(): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->success([
            'groups' => ServerGroup::query()
                ->withCount('users')
                ->orderBy('id')
                ->get(['id', 'name']),
            'servers' => Server::query()
                ->where('enabled', 1)
                ->where('show', 1)
                ->orderBy('sort')
                ->get(['id', 'name', 'type', 'group_ids']),
            'campaigns' => BaitSplitService::fromDatabase()->campaigns(),
        ]);
    }

    public function campaigns(): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->success(BaitSplitService::fromDatabase()->campaigns());
    }

    public function wallLog(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'limit' => ['nullable', 'integer', 'min:1', 'max:200'],
        ]);
        return $this->success(
            BaitSplitService::fromDatabase()->wallReport(
                $campaignId,
                (int) ($data['limit'] ?? 100)
            )
        );
    }

    public function saveWallSettings(Request $request): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'wall_auto_isolate' => ['required', 'boolean'],
            'wall_hit_threshold' => ['required', 'integer', 'min:1', 'max:50'],
            'wall_lookback_seconds' => ['required', 'integer', 'min:60', 'max:86400'],
            'wall_observe_pool_id' => ['nullable', 'string', 'max:64'],
            'decoy_enabled' => ['required', 'boolean'],
            'decoy_source_pool_ids' => ['nullable', 'string', 'max:512'],
            'decoy_pool_ids' => ['nullable', 'string', 'max:512'],
            'decoy_confirm_pool_id' => ['nullable', 'string', 'max:64'],
            'decoy_start' => ['required', 'string', 'regex:/^\d{1,2}:\d{2}$/'],
            'decoy_end' => ['required', 'string', 'regex:/^\d{1,2}:\d{2}$/'],
        ]);
        $service = app(\App\Services\Plugin\PluginConfigService::class);
        $config = $service->getDbConfig('bait_split');
        $config['wall_auto_isolate'] = (bool) $data['wall_auto_isolate'];
        $config['wall_hit_threshold'] = (int) $data['wall_hit_threshold'];
        $config['wall_lookback_seconds'] = (int) $data['wall_lookback_seconds'];
        $config['wall_observe_pool_id'] = trim((string) ($data['wall_observe_pool_id'] ?? ''));
        $config['decoy_enabled'] = (bool) $data['decoy_enabled'];
        $config['decoy_source_pool_ids'] = trim((string) ($data['decoy_source_pool_ids'] ?? ''));
        unset($config['decoy_source_pool_id']);
        $config['decoy_pool_ids'] = trim((string) ($data['decoy_pool_ids'] ?? ''));
        $config['decoy_confirm_pool_id'] = trim((string) ($data['decoy_confirm_pool_id'] ?? ''));
        $config['decoy_start'] = trim((string) $data['decoy_start']);
        $config['decoy_end'] = trim((string) $data['decoy_end']);
        $service->updateConfig('bait_split', $config);
        return $this->success($config);
    }

    public function createPing(Request $request): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'host' => ['required', 'string', 'max:253'],
        ]);
        return $this->executePing(
            fn() => BaitSplitService::fromDatabase()->createPingTask($data['host'])
        );
    }

    public function pingResult(string $taskId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->executePing(
            fn() => BaitSplitService::fromDatabase()->pingTaskResult($taskId)
        );
    }

    public function exposures(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->exposureUsers($campaignId)
        );
    }

    public function saveCampaign(Request $request): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate(
            [
                'campaign_id' => ['nullable', 'string', 'max:64'],
                'name' => ['required', 'string', 'max:50'],
                'target_group_ids' => ['required', 'array', 'min:1'],
                'target_group_ids.*' => [
                    'integer',
                    'distinct',
                    Rule::exists('v2_server_group', 'id'),
                ],
                'excluded_server_ids' => ['nullable', 'array'],
                'excluded_server_ids.*' => ['integer'],
            ],
            [
                'name.required' => '请填写任务名称',
                'target_group_ids.required' => '请至少选择一个用户组',
                'target_group_ids.min' => '请至少选择一个用户组',
                'target_group_ids.*.exists' => '所选用户组不存在',
            ]
        );

        return $this->execute(fn() => BaitSplitService::fromDatabase()->saveCampaign(
            $data['campaign_id'] ?? null,
            $data['name'],
            $data['target_group_ids'],
            $data['excluded_server_ids'] ?? []
        ));
    }

    public function deleteCampaign(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->deleteCampaign($campaignId)
        );
    }

    public function start(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate([
            'domains' => ['required', 'array', 'min:2', 'max:10'],
            'domains.*' => ['required', 'string', 'max:253', 'distinct'],
        ]);

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->startCampaign(
                $campaignId,
                $data['domains']
            )
        );
    }

    public function replaceDomains(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate([
            'domains' => ['required', 'array', 'min:2', 'max:10'],
            'domains.*' => ['required', 'string', 'max:253', 'distinct'],
        ]);

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->replaceDomains(
                $campaignId,
                $data['domains']
            )
        );
    }

    public function result(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate([
            'positive_buckets' => ['present', 'array'],
            'positive_buckets.*' => ['integer', 'min:0', 'max:9', 'distinct'],
        ]);

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->recordResult(
                $campaignId,
                $data['positive_buckets']
            )
        );
    }

    public function disable(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->disableCampaign($campaignId)
        );
    }

    public function reset(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->resetCampaign($campaignId)
        );
    }

    public function initializeRouter(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(fn() => BaitSplitService::fromDatabase()->initializeRouter(
            $campaignId
        ));
    }

    public function savePool(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'id' => ['nullable', 'string', 'max:80'],
            'webhook_id' => [
                'nullable',
                'string',
                'max:100',
                'regex:/^[A-Za-z0-9._:-]+$/',
            ],
            'name' => ['required', 'string', 'max:50'],
            'type' => ['required', Rule::in(['default', 'danger', 'blacklist', 'probe', 'observation', 'emergency', 'safe', 'custom'])],
            'host' => ['nullable', 'string', 'max:253'],
            'node_hosts' => ['nullable', 'array'],
            'node_hosts.*' => ['nullable', 'string', 'max:253'],
            'server_name' => ['nullable', 'string', 'max:253'],
            'transport_host' => ['nullable', 'string', 'max:253'],
            'enabled' => ['required', 'boolean'],
            'status' => ['required', Rule::in(['available', 'active', 'suspected', 'blocked', 'standby'])],
            'capacity' => ['nullable', 'integer', 'min:0'],
            'overflow_pool_id' => ['nullable', 'string', 'max:80'],
            'strategy' => ['nullable', Rule::in(['manual', 'least', 'round_robin'])],
            'note' => ['nullable', 'string', 'max:200'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->savePool($campaignId, $data)
        );
    }

    public function deletePool(string $campaignId, string $poolId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->deletePool($campaignId, $poolId)
        );
    }

    public function toggleRouter(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate(['enabled' => ['required', 'boolean']]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->setRouterEnabled(
                $campaignId,
                (bool) $data['enabled']
            )
        );
    }

    public function syncRouterUsers(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->syncRouterUsers($campaignId)
        );
    }

    public function poolUsers(
        Request $request,
        string $campaignId,
        string $poolId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'q' => ['nullable', 'string', 'max:100'],
            'page' => ['nullable', 'integer', 'min:1'],
            'per_page' => ['nullable', 'integer', 'min:10', 'max:100'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->usersForPool(
                $campaignId,
                $poolId,
                (string) ($data['q'] ?? ''),
                (int) ($data['page'] ?? 1),
                (int) ($data['per_page'] ?? 50)
            )
        );
    }

    public function movePulledPoolUsers(
        Request $request,
        string $campaignId,
        string $poolId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'target_pool_id' => ['required', 'string', 'max:80'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->movePulledPoolUsers(
                $campaignId,
                $poolId,
                $data['target_pool_id']
            )
        );
    }

    public function moveUnpulledPoolUsers(
        Request $request,
        string $campaignId,
        string $poolId
    ): JsonResponse {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'target_pool_id' => ['required', 'string', 'max:80'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->moveUnpulledPoolUsers(
                $campaignId,
                $poolId,
                $data['target_pool_id']
            )
        );
    }

    public function createInvestigationRoot(
        Request $request,
        string $campaignId,
        string $poolId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'name' => ['nullable', 'string', 'max:80'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->createInvestigationRoot(
                $campaignId,
                $poolId,
                (string) ($data['name'] ?? '')
            )
        );
    }

    public function splitInvestigationNode(
        Request $request,
        string $campaignId,
        string $nodeId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'branches' => ['required', 'array', 'min:2', 'max:10'],
            'branches.*.name' => ['nullable', 'string', 'max:50'],
            'branches.*.host' => ['required', 'string', 'max:253'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->splitInvestigationNode(
                $campaignId,
                $nodeId,
                $data['branches']
            )
        );
    }

    public function mergeInvestigationNodes(
        Request $request,
        string $campaignId
    ): JsonResponse {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'node_ids' => ['required', 'array', 'min:1'],
            'node_ids.*' => ['required', 'string', 'max:80', 'distinct'],
            'name' => ['nullable', 'string', 'max:80'],
            'branches' => ['required', 'array', 'min:2', 'max:10'],
            'branches.*.name' => ['nullable', 'string', 'max:50'],
            'branches.*.host' => ['required', 'string', 'max:253'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()
                ->mergeInvestigationNodes(
                    $campaignId,
                    $data['node_ids'],
                    (string) ($data['name'] ?? ''),
                    $data['branches']
                )
        );
    }

    public function setInvestigationNodeStatus(
        Request $request,
        string $campaignId,
        string $nodeId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'status' => ['required', Rule::in(['safe', 'blocked'])],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()
                ->setInvestigationNodeStatus(
                    $campaignId,
                    $nodeId,
                    $data['status']
                )
        );
    }

    public function updateInvestigationNodeHost(
        Request $request,
        string $campaignId,
        string $nodeId
    ): JsonResponse {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'host' => ['required', 'string', 'max:253'],
            'webhook_id' => [
                'nullable',
                'string',
                'max:100',
                'regex:/^[A-Za-z0-9._:-]+$/',
            ],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()
                ->updateInvestigationNodeHost(
                    $campaignId,
                    $nodeId,
                    $data['host'],
                    $data['webhook_id'] ?? ''
                )
        );
    }

    public function deleteInvestigationTree(
        string $campaignId,
        string $nodeId
    ): JsonResponse {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()
                ->deleteInvestigationTree($campaignId, $nodeId)
        );
    }

    public function moveInvestigationNodeUsers(
        Request $request,
        string $campaignId,
        string $nodeId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'target_pool_id' => ['required', 'string', 'max:80'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()
                ->moveInvestigationNodeUsers(
                    $campaignId,
                    $nodeId,
                    $data['target_pool_id']
                )
        );
    }

    public function moveUnpulledInvestigationNodeUsers(
        Request $request,
        string $campaignId,
        string $nodeId
    ): JsonResponse {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'target_pool_id' => ['required', 'string', 'max:80'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()
                ->moveUnpulledInvestigationNodeUsers(
                    $campaignId,
                    $nodeId,
                    $data['target_pool_id']
                )
        );
    }

    public function investigationNodeUsers(
        Request $request,
        string $campaignId,
        string $nodeId
    ): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'q' => ['nullable', 'string', 'max:100'],
            'page' => ['nullable', 'integer', 'min:1'],
            'per_page' => ['nullable', 'integer', 'min:10', 'max:100'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->investigationNodeUsers(
                $campaignId,
                $nodeId,
                (string) ($data['q'] ?? ''),
                (int) ($data['page'] ?? 1),
                (int) ($data['per_page'] ?? 50)
            )
        );
    }

    public function searchUsers(Request $request, string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate(['q' => ['nullable', 'string', 'max:100']]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->searchUsers(
                $campaignId,
                (string) ($data['q'] ?? '')
            )
        );
    }

    public function overrides(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->overrideUsers($campaignId)
        );
    }

    public function saveOverride(Request $request, string $campaignId, int $userId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        $data = $request->validate([
            'pool_id' => ['nullable', 'string', 'max:80'],
            'host' => ['nullable', 'string', 'max:253'],
            'node_hosts' => ['nullable', 'array'],
            'node_hosts.*' => ['nullable', 'string', 'max:253'],
            'server_name' => ['nullable', 'string', 'max:253'],
            'transport_host' => ['nullable', 'string', 'max:253'],
            'locked' => ['required', 'boolean'],
            'note' => ['nullable', 'string', 'max:200'],
            'expires_at' => ['nullable', 'integer', 'min:0'],
        ]);
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->saveUserOverride(
                $campaignId,
                $userId,
                $data
            )
        );
    }

    public function deleteOverride(string $campaignId, int $userId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->deleteUserOverride(
                $campaignId,
                $userId
            )
        );
    }

    public function rollbackRouter(string $campaignId): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }
        return $this->execute(
            fn() => BaitSplitService::fromDatabase()->rollbackRouterConfig($campaignId)
        );
    }

    private function ensureEnabled(): ?JsonResponse
    {
        $error = $this->beforePluginAction();
        return $error ? $this->fail($error) : null;
    }

    private function execute(callable $callback): JsonResponse
    {
        try {
            return Cache::lock('bait_split:admin_state', 10)->block(
                5,
                fn() => $this->success($callback())
            );
        } catch (InvalidArgumentException $exception) {
            return $this->fail([422, $exception->getMessage()]);
        } catch (LockTimeoutException) {
            return $this->fail([423, '其他管理操作正在执行，请稍后重试']);
        }
    }

    private function executePing(callable $callback): JsonResponse
    {
        try {
            return $this->success($callback());
        } catch (InvalidArgumentException $exception) {
            return $this->fail([422, $exception->getMessage()]);
        } catch (Throwable) {
            return $this->fail([502, '拨测服务暂时不可用，请稍后重试']);
        }
    }
}
