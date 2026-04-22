<?php

namespace App\Jobs;

use App\Services\MailService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class SendEmailJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    protected $params;

    public $tries = 1;
    public $timeout = 10;
    public $failOnTimeout = true;
    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct($params, $queue = 'send_email')
    {
        $this->onQueue($queue);
        $this->params = $params;
    }

    /**
     * Execute the job.
     *
     * 发送失败不重试、不记录到 failed_jobs。
     *
     * @return void
     */
    public function handle()
    {
        MailService::sendEmail($this->params);
    }

    /**
     * 失败时吞掉异常，避免写入 failed_jobs。
     */
    public function failed(\Throwable $e): void
    {
        \Log::warning('SendEmailJob failed (dropped)', [
            'email' => $this->params['email'] ?? null,
            'error' => $e->getMessage(),
        ]);
    }
}
