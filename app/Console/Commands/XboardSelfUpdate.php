<?php

namespace App\Console\Commands;

use App\Services\UpdateService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Cache;

class XboardSelfUpdate extends Command
{
    protected $signature = 'xboard:self-update {--finish : 拉取代码后的收尾阶段（内部使用）} {--from=}';

    protected $description = '后台执行面板更新（由后台更新按钮触发）';

    public function handle(UpdateService $updateService): int
    {
        if (!Cache::get(UpdateService::CACHE_UPDATE_LOCK)) {
            $this->error('未持有更新锁，请从后台发起更新');
            return self::FAILURE;
        }

        $output = fn (string $line) => $this->line($line);
        if ($this->option('finish')) {
            $updateService->finishUpdate($output, (string) $this->option('from'));
            return self::SUCCESS;
        }

        Cache::put(UpdateService::CACHE_UPDATE_LOCK, getmypid(), now()->addSeconds(UpdateService::LOCK_SECONDS));
        $result = $updateService->runUpdate($output);
        return $result['success'] ? self::SUCCESS : self::FAILURE;
    }
}
