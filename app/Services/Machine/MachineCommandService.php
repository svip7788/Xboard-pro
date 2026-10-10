<?php

namespace App\Services\Machine;

use App\Models\ServerMachine;
use App\Models\ServerMachineTask;
use App\Services\NodeSyncService;
use App\WebSocket\NodeWorker;
use Illuminate\Support\Facades\Cache;

/**
 * 经 WS 向 V2bX（机器模式）下发白名单运维命令，并处理回执
 */
class MachineCommandService
{
    public static function isOnline(ServerMachine $machine): bool
    {
        return Cache::has(NodeWorker::HEARTBEAT_CACHE_KEY) && Cache::has(ServerMachine::wsAliveKey($machine->id));
    }

    public static function dispatch(ServerMachine $machine, string $action, array $params = [], ?string $batch = null): ServerMachineTask
    {
        $task = ServerMachineTask::create([
            'batch' => $batch,
            'machine_id' => $machine->id,
            'machine_name' => $machine->name,
            'type' => $action,
            'params' => $params ?: null,
            'status' => 'running',
            'step' => '已下发',
            'started_at' => time(),
        ]);

        if (!$machine->is_active || !self::isOnline($machine)) {
            $task->forceFill([
                'status' => 'failed',
                'step' => '未下发',
                'message' => $machine->is_active ? '机器不在线（V2bX 未连接面板 WebSocket）' : '机器已停用',
                'finished_at' => time(),
            ])->save();
            return $task;
        }

        NodeSyncService::pushMachine($machine->id, 'machine.command', [
            'id' => (string) $task->id,
            'action' => $action,
            'params' => (object) $params,
        ]);

        return $task;
    }

    /**
     * WS 进程收到 machine.command.progress / machine.command.result
     */
    public static function handleEvent(int $machineId, string $event, array $data): void
    {        $task = ServerMachineTask::where('id', (int) ($data['id'] ?? 0))
            ->where('machine_id', $machineId)
            ->whereIn('status', ServerMachineTask::ACTIVE_STATUSES)
            ->first();
        if (!$task) {
            return;
        }

        $output = (string) ($data['output'] ?? '');
        if ($output !== '') {
            $task->appendLog(str_ends_with($output, "\n") ? $output : "{$output}\n");
            $task->message = mb_strimwidth(V2bXScripts::lastLine($output), 0, 500);
        }

        if ($event === 'machine.command.result') {
            $ok = (bool) ($data['ok'] ?? false);
            $task->status = $ok ? 'success' : 'failed';
            $task->step = $ok ? '已完成' : '失败';
            $task->finished_at = time();
            if ($ok && $task->type === 'upgrade' && !empty($task->params['version'])) {
                ServerMachine::whereKey($machineId)->update(['v2bx_version' => $task->params['version']]);
            }
        } else {
            $task->step = '执行中';
        }
        $task->save();
    }
}
