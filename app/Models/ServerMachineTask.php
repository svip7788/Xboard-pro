<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

/**
 * 机器运维任务：WS 下发给 V2bX 的命令，或首次 SSH 安装
 *
 * @property int $id
 * @property string|null $batch
 * @property int|null $machine_id
 * @property string $machine_name
 * @property string $type
 * @property string $status pending|running|success|failed
 * @property string $step
 * @property string $message
 * @property string|null $log
 * @property array|null $params
 * @property int|null $started_at
 * @property int|null $finished_at
 */
class ServerMachineTask extends Model
{
    public const TYPE_INSTALL = 'install';

    /** V2bX 机器模式支持的远程命令 */
    public const COMMANDS = ['status', 'restart', 'logs', 'upgrade', 'bbr'];

    public const ACTIVE_STATUSES = ['pending', 'running'];

    /** 超过该时长仍未结束的任务视为超时 */
    public const TIMEOUT = [
        self::TYPE_INSTALL => 900,
        'upgrade' => 600,
        'default' => 60,
    ];

    protected $table = 'v2_server_machine_task';

    protected $guarded = ['id'];

    protected $casts = [
        'params' => 'array',
        'started_at' => 'integer',
        'finished_at' => 'integer',
        'created_at' => 'timestamp',
        'updated_at' => 'timestamp',
    ];

    public function machine(): BelongsTo
    {
        return $this->belongsTo(ServerMachine::class, 'machine_id');
    }

    /**
     * 把超时未完成的任务标记为失败
     */
    public static function expireStale(): void
    {
        foreach (self::TIMEOUT as $type => $seconds) {
            $query = self::whereIn('status', self::ACTIVE_STATUSES)
                ->where('created_at', '<', now()->subSeconds($seconds));
            $type === 'default'
                ? $query->whereNotIn('type', array_keys(self::TIMEOUT))
                : $query->where('type', $type);
            $query->update(['status' => 'failed', 'message' => '任务超时，未收到机器回执', 'finished_at' => time()]);
        }
    }

    public function appendLog(string $text): void
    {
        $log = ($this->log ?? '') . $text;
        if (strlen($log) > 60000) {
            $log = mb_strcut($log, strlen($log) - 60000);
        }
        $this->log = $log;
    }
}
