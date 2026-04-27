<?php

namespace App\Jobs;

use App\Services\TelegramService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class SendTelegramJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    protected $telegramId;
    protected $text;

    public $tries = 1;
    public $timeout = 20;

    public function __construct(int $telegramId, string $text)
    {
        $this->onQueue('send_telegram');
        $this->telegramId = $telegramId;
        $this->text = $text;
    }

    public function handle()
    {
        $telegramService = new TelegramService();
        $telegramService->sendMessage($this->telegramId, $this->text, 'markdown');
    }

    /**
     * 失败时记录 warning 并自删 failed_jobs 记录，避免长期堆积。
     */
    public function failed(\Throwable $e): void
    {
        \Log::warning('SendTelegramJob failed (dropped)', [
            'telegram_id' => $this->telegramId,
            'error' => $e->getMessage(),
        ]);
        try {
            if ($this->job && ($uuid = $this->job->uuid())) {
                app('queue.failer')->forget($uuid);
            }
        } catch (\Throwable $ignore) {
        }
    }
}
