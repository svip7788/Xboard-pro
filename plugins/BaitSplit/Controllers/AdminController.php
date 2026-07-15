<?php

namespace Plugin\BaitSplit\Controllers;

use App\Http\Controllers\PluginController;
use App\Models\Server;
use App\Models\ServerGroup;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Validation\Rule;
use InvalidArgumentException;
use Plugin\BaitSplit\Services\BaitSplitService;

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

        $data = $request->validate([
            'campaign_id' => ['nullable', 'string', 'max:64'],
            'name' => ['required', 'string', 'max:50'],
            'target_group_id' => [
                'required',
                'integer',
                Rule::exists('v2_server_group', 'id'),
            ],
            'target_server_ids' => ['required', 'array', 'min:1'],
            'target_server_ids.*' => [
                'integer',
                'distinct',
                Rule::exists('v2_server', 'id')->where(
                    fn($query) => $query
                        ->where('enabled', 1)
                        ->where('show', 1)
                        ->whereJsonContains(
                            'group_ids',
                            (string) $request->integer('target_group_id')
                        )
                ),
            ],
        ]);

        return $this->execute(fn() => BaitSplitService::fromDatabase()->saveCampaign(
            $data['campaign_id'] ?? null,
            $data['name'],
            $data['target_group_id'],
            $data['target_server_ids']
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

    private function ensureEnabled(): ?JsonResponse
    {
        $error = $this->beforePluginAction();
        return $error ? $this->fail($error) : null;
    }

    private function execute(callable $callback): JsonResponse
    {
        try {
            return $this->success($callback());
        } catch (InvalidArgumentException $exception) {
            return $this->fail([422, $exception->getMessage()]);
        }
    }
}
