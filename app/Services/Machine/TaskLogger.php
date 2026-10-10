<?php

namespace App\Services\Machine;

use App\Models\ServerMachineTask;

/**
 * 缓冲 SSH 输出，按时间/字节合批写入任务日志
 */
class TaskLogger
{
    private const FLUSH_BYTES = 2048;
    private const FLUSH_SECONDS = 1.0;

    private string $buffer = '';
    private float $lastFlush;

    public function __construct(private readonly ServerMachineTask $task)
    {
        $this->lastFlush = microtime(true);
    }

    public function log(string $text): void
    {
        $this->buffer .= $text;
        if (strlen($this->buffer) >= self::FLUSH_BYTES || microtime(true) - $this->lastFlush >= self::FLUSH_SECONDS) {
            $this->flush();
        }
    }

    public function line(string $text): void
    {
        $this->log(">>> {$text}\n");
    }

    public function step(string $step): void
    {
        $this->flush(['step' => $step]);
    }

    public function flush(array $extra = []): void
    {
        if ($this->buffer !== '') {
            $this->task->appendLog($this->buffer);
            if (!isset($extra['message']) && ($last = V2bXScripts::lastLine($this->buffer)) !== '') {
                $extra['message'] = mb_strimwidth($last, 0, 500);
            }
            $this->buffer = '';
        }
        $this->lastFlush = microtime(true);
        $this->task->forceFill($extra);
        if ($this->task->isDirty()) {
            $this->task->save();
        }
    }
}
