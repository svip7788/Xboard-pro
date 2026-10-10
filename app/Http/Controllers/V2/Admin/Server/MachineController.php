<?php

namespace App\Http\Controllers\V2\Admin\Server;

use App\Exceptions\ApiException;
use App\Http\Controllers\Controller;
use App\Jobs\MachineInstallJob;
use App\Models\Server;
use App\Models\ServerMachine;
use App\Models\ServerMachineLoadHistory;
use App\Models\ServerMachineTask;
use App\Services\Machine\CloudflareService;
use App\Services\Machine\MachineCommandService;
use App\Services\Machine\V2bXSettings;
use App\Services\NodeSyncService;
use Illuminate\Http\Request;
use Illuminate\Support\Str;

class MachineController extends Controller
{
    /**
     * 获取机器列表（附带关联节点数）
     */
    public function fetch(Request $request)
    {
        ServerMachineTask::expireStale();
        $activeTasks = ServerMachineTask::whereIn('status', ServerMachineTask::ACTIVE_STATUSES)
            ->orderByDesc('id')
            ->get(['id', 'machine_id', 'type', 'step'])
            ->unique('machine_id')
            ->keyBy('machine_id');

        $machines = ServerMachine::withCount('servers')
            ->orderBy('id')
            ->get()
            ->map(function (ServerMachine $machine) use ($activeTasks) {
                return [
                    'id' => $machine->id,
                    'name' => $machine->name,
                    'host' => $machine->host,
                    'ssh_port' => $machine->ssh_port,
                    'ssh_user' => $machine->ssh_user,
                    'notes' => $machine->notes,
                    'is_active' => $machine->is_active,
                    'last_seen_at' => $machine->last_seen_at,
                    'load_status' => $machine->load_status,
                    'v2bx_status' => $machine->v2bx_status,
                    'v2bx_version' => $machine->v2bx_version,
                    'ws_online' => MachineCommandService::isOnline($machine),
                    'active_task' => $activeTasks->get($machine->id),
                    'servers_count' => $machine->servers_count,
                    'created_at' => $machine->created_at,
                    'updated_at' => $machine->updated_at,
                ];
            });

        return $this->success($machines);
    }

    /**
     * 创建 / 更新机器
     */
    public function save(Request $request)
    {
        $params = $request->validate([
            'id' => 'nullable|integer|exists:v2_server_machine,id',
            'name' => 'required|string|max:255',
            'host' => 'nullable|string|max:255',
            'ssh_port' => 'nullable|integer|min:1|max:65535',
            'ssh_user' => 'nullable|string|max:64',
            'notes' => 'nullable|string',
            'is_active' => 'nullable|boolean',
        ]);

        $attributes = array_filter([
            'name' => $params['name'],
            'host' => array_key_exists('host', $params) ? trim((string) $params['host']) : null,
            'ssh_port' => $params['ssh_port'] ?? null,
            'ssh_user' => isset($params['ssh_user']) ? trim($params['ssh_user']) : null,
        ], fn($v) => $v !== null);
        if (array_key_exists('notes', $params)) {
            $attributes['notes'] = $params['notes'];
        }
        if (array_key_exists('is_active', $params)) {
            $attributes['is_active'] = $params['is_active'];
        }

        if (!empty($params['id'])) {
            ServerMachine::find($params['id'])->update($attributes);
            return $this->success(true);
        }

        $machine = ServerMachine::create($attributes + [
            'is_active' => true,
            'token' => ServerMachine::generateToken(),
        ]);

        return $this->success([
            'id' => $machine->id,
            'token' => $machine->token,
            'install_command' => $this->buildInstallCommand($machine),
        ]);
    }

    /**
     * 重置机器 Token
     */
    public function resetToken(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
        ]);

        $machine = ServerMachine::find($params['id']);
        $token = ServerMachine::generateToken();
        $machine->update(['token' => $token]);

        return $this->success(['token' => $token]);
    }

    /**
     * 获取机器 Token
     */
    public function getToken(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
        ]);

        return $this->success(['token' => ServerMachine::find($params['id'])->token]);
    }

    /**
     * 获取 V2bX 机器模式一键安装命令
     */
    public function installCommand(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
        ]);

        return $this->success([
            'command' => $this->buildInstallCommand(ServerMachine::find($params['id'])),
        ]);
    }

    /**
     * 删除机器（自动解除关联节点）
     */
    public function drop(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
        ]);

        $machine = ServerMachine::find($params['id']);
        $machineId = $machine->id;

        Server::where('machine_id', $machineId)->update(['machine_id' => null]);
        $machine->delete();

        NodeSyncService::notifyMachineNodesChanged($machineId);

        return $this->success(true);
    }

    /**
     * 获取机器下的节点列表
     */
    public function nodes(Request $request)
    {
        $params = $request->validate([
            'machine_id' => 'required|integer|exists:v2_server_machine,id',
        ]);

        $nodes = Server::where('machine_id', $params['machine_id'])
            ->orderBy('sort')
            ->get(['id', 'name', 'type', 'host', 'port', 'show', 'enabled', 'sort']);

        return $this->success($nodes);
    }

    /**
     * 获取机器负载历史
     */
    public function history(Request $request)
    {
        $params = $request->validate([
            'machine_id' => 'required|integer|exists:v2_server_machine,id',
            'limit' => 'nullable|integer|min:10|max:1440',
            'range_hours' => 'nullable|integer|min:1|max:24',
        ]);

        $query = ServerMachineLoadHistory::query()
            ->where('machine_id', $params['machine_id']);

        if (!empty($params['range_hours'])) {
            $query->where('recorded_at', '>=', now()->subHours((int) $params['range_hours'])->timestamp);
        }

        $history = $query
            ->orderByDesc('recorded_at')
            ->limit((int) ($params['limit'] ?? 60))
            ->get(['cpu', 'mem_total', 'mem_used', 'disk_total', 'disk_used', 'net_in_speed', 'net_out_speed', 'recorded_at'])
            ->reverse()
            ->values();

        return $this->success($history);
    }

    /**
     * 一次性 SSH 安装 V2bX（凭据只随加密队列任务传递，不入库）
     */
    public function sshInstall(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
            'host' => 'required|string|max:255',
            'port' => 'nullable|integer|min:1|max:65535',
            'user' => 'nullable|string|max:64',
            'password' => 'nullable|string|max:1024',
            'key' => 'nullable|string|max:16384',
            'bbr' => 'nullable|boolean',
            'core' => 'nullable|in:auto,sing,xray',
            'version' => 'nullable|string|max:32',
        ]);
        if (empty($params['password']) && empty($params['key'])) {
            throw new ApiException('请填写 SSH 密码或私钥');
        }
        if (!preg_match('#^https?://#', V2bXSettings::panelUrl())) {
            throw new ApiException('请先在系统配置中设置站点网址（app_url），V2bX 需要用它连接面板');
        }

        $machine = ServerMachine::find($params['id']);
        $this->ensureNoActiveTask($machine);

        try {
            $version = V2bXSettings::resolveVersion($params['version'] ?? null);
        } catch (\Throwable $e) {
            throw new ApiException($e->getMessage());
        }

        $task = ServerMachineTask::create([
            'machine_id' => $machine->id,
            'machine_name' => $machine->name,
            'type' => ServerMachineTask::TYPE_INSTALL,
            'params' => ['version' => $version, 'host' => $params['host']],
            'status' => 'pending',
            'step' => '排队中',
        ]);

        MachineInstallJob::dispatch($task->id, [
            'host' => trim($params['host']),
            'port' => (int) ($params['port'] ?? 22),
            'user' => trim($params['user'] ?? '') ?: 'root',
            'password' => $params['password'] ?? '',
            'key' => $params['key'] ?? '',
        ], [
            'bbr' => (bool) ($params['bbr'] ?? true),
            'core' => $params['core'] ?? 'auto',
        ]);

        return $this->success(['task_id' => $task->id]);
    }

    /**
     * 向机器下发运维命令
     */
    public function command(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
            'action' => 'required|in:' . implode(',', ServerMachineTask::COMMANDS),
            'version' => 'nullable|string|max:32',
            'lines' => 'nullable|integer|min:10|max:2000',
        ]);

        $machine = ServerMachine::find($params['id']);
        $task = MachineCommandService::dispatch($machine, $params['action'], $this->commandParams($params));

        return $this->success(['task_id' => $task->id, 'status' => $task->status, 'message' => $task->message]);
    }

    /**
     * 批量下发运维命令（如批量升级）
     */
    public function batchCommand(Request $request)
    {
        $params = $request->validate([
            'ids' => 'required|array|min:1',
            'ids.*' => 'integer',
            'action' => 'required|in:' . implode(',', ServerMachineTask::COMMANDS),
            'version' => 'nullable|string|max:32',
        ]);

        $commandParams = $this->commandParams($params);
        $batch = (string) Str::uuid();
        $results = ServerMachine::whereIn('id', $params['ids'])->get()
            ->map(fn(ServerMachine $m) => MachineCommandService::dispatch($m, $params['action'], $commandParams, $batch))
            ->countBy('status');

        return $this->success([
            'batch' => $batch,
            'sent' => $results->get('running', 0),
            'failed' => $results->get('failed', 0),
        ]);
    }

    /**
     * 任务列表（不含日志）
     */
    public function tasks(Request $request)
    {
        $params = $request->validate([
            'machine_id' => 'nullable|integer',
            'batch' => 'nullable|string|max:36',
            'limit' => 'nullable|integer|min:1|max:200',
        ]);

        ServerMachineTask::expireStale();
        $tasks = ServerMachineTask::query()
            ->when($params['machine_id'] ?? null, fn($q, $id) => $q->where('machine_id', $id))
            ->when($params['batch'] ?? null, fn($q, $batch) => $q->where('batch', $batch))
            ->orderByDesc('id')
            ->limit($params['limit'] ?? 50)
            ->get(['id', 'batch', 'machine_id', 'machine_name', 'type', 'status', 'step', 'message', 'params', 'started_at', 'finished_at', 'created_at']);

        return $this->success($tasks);
    }

    /**
     * 任务详情（含日志）
     */
    public function task(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine_task,id',
        ]);

        ServerMachineTask::expireStale();
        return $this->success(ServerMachineTask::find($params['id']));
    }

    public function settings()
    {
        return $this->success(V2bXSettings::toArray());
    }

    public function saveSettings(Request $request)
    {
        $params = $request->validate([
            'repo' => ['nullable', 'string', 'max:128', 'regex:#^[\w.-]+/[\w.-]+$#'],
            'version' => 'nullable|string|max:32',
        ], [
            'repo.regex' => '仓库格式应为 用户名/仓库名',
        ]);
        V2bXSettings::save($params);
        return $this->success(true);
    }

    public function saveCloudflare(Request $request)
    {
        $params = $request->validate([
            'token' => 'nullable|string|max:256',
            'email' => 'nullable|email|max:128',
        ]);
        $token = trim($params['token'] ?? '');
        $email = trim($params['email'] ?? '');
        if ($token !== '') {
            try {
                (new CloudflareService($token, $email))->verify();
            } catch (\Throwable $e) {
                throw new ApiException('凭据校验失败：' . $e->getMessage());
            }
        }
        V2bXSettings::saveCloudflare($token, $email);
        return $this->success(true);
    }

    /**
     * 把域名解析到机器地址（Cloudflare）
     */
    public function dnsResolve(Request $request)
    {
        $params = $request->validate([
            'id' => 'required|integer|exists:v2_server_machine,id',
            'domain' => 'required|string|max:255',
            'proxied' => 'nullable|boolean',
        ]);

        $machine = ServerMachine::find($params['id']);
        if (empty($machine->host)) {
            throw new ApiException('请先在机器信息中填写机器地址');
        }
        try {
            $result = CloudflareService::fromSettings()->upsert($params['domain'], $machine->host, (bool) ($params['proxied'] ?? false));
        } catch (\Throwable $e) {
            throw new ApiException($e->getMessage());
        }

        return $this->success($result);
    }

    private function commandParams(array $params): array
    {
        return match ($params['action']) {
            'upgrade' => (function () use ($params) {
                try {
                    return ['version' => V2bXSettings::resolveVersion($params['version'] ?? null), 'repo' => V2bXSettings::repo()];
                } catch (\Throwable $e) {
                    throw new ApiException($e->getMessage());
                }
            })(),
            'logs' => ['lines' => (int) ($params['lines'] ?? 200)],
            default => [],
        };
    }

    private function ensureNoActiveTask(ServerMachine $machine): void
    {
        ServerMachineTask::expireStale();
        if (ServerMachineTask::where('machine_id', $machine->id)->where('type', ServerMachineTask::TYPE_INSTALL)
            ->whereIn('status', ServerMachineTask::ACTIVE_STATUSES)->exists()) {
            throw new ApiException('该机器已有安装任务在执行');
        }
    }

    private function buildInstallCommand(ServerMachine $machine): string
    {
        $repo = V2bXSettings::repo();
        $installer = sprintf('https://raw.githubusercontent.com/%s/HEAD/install/install.sh', $repo);

        return sprintf(
            'curl -fsSL %s -o /tmp/v2bx-install.sh && bash /tmp/v2bx-install.sh --panel %s --machine-id %d --token %s --version %s --repo %s',
            $installer,
            escapeshellarg(V2bXSettings::panelUrl()),
            $machine->id,
            escapeshellarg($machine->token),
            escapeshellarg(V2bXSettings::version()),
            escapeshellarg($repo)
        );
    }
}
