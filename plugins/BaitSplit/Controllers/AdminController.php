<?php

namespace Plugin\BaitSplit\Controllers;

use App\Http\Controllers\PluginController;
use App\Models\Server;
use App\Models\ServerGroup;
use App\Services\Plugin\PluginConfigService;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Validation\Rule;
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
                ->get(['id', 'name', 'type']),
            'config' => app(PluginConfigService::class)->getDbConfig('bait_split'),
        ]);
    }

    public function saveConfig(Request $request): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate([
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
                    fn($query) => $query->where('enabled', 1)->where('show', 1)
                ),
            ],
        ]);

        app(PluginConfigService::class)->updateConfig('bait_split', $data);
        Cache::forget('plugin_config_bait_split');

        return $this->success([
            'config' => $data,
            'status' => BaitSplitService::fromDatabase()->status(),
        ]);
    }

    public function status(): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->success(BaitSplitService::fromDatabase()->status());
    }

    public function start(Request $request): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate([
            'host_a' => ['required', 'string', 'max:253'],
            'host_b' => ['required', 'string', 'max:253', 'different:host_a'],
            'prefix' => ['nullable', 'string', 'regex:/^[01]*$/', 'max:64'],
        ]);

        return $this->success(BaitSplitService::fromDatabase()->start(
            $data['host_a'],
            $data['host_b'],
            array_key_exists('prefix', $data) ? $data['prefix'] : null
        ));
    }

    public function result(Request $request): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        $data = $request->validate([
            'result' => ['required', Rule::in(['a', 'b', 'both', 'none'])],
        ]);

        return $this->success(
            BaitSplitService::fromDatabase()->recordResult($data['result'])
        );
    }

    public function disable(): JsonResponse
    {
        if ($response = $this->ensureEnabled()) {
            return $response;
        }

        return $this->success(BaitSplitService::fromDatabase()->disable());
    }

    private function ensureEnabled(): ?JsonResponse
    {
        $error = $this->beforePluginAction();
        return $error ? $this->fail($error) : null;
    }
}
