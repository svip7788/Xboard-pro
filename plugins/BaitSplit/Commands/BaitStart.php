<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitStart extends Command
{
    protected $signature = 'bait:start
        {campaign=legacy : 排查任务 ID}
        {--domain=* : 分组域名或 IP，可重复 2 至 10 次}
        {--host-a= : 兼容旧命令的 A 组域名}
        {--host-b= : 兼容旧命令的 B 组域名}';

    protected $description = '启动指定任务的一轮多域名分组';

    public function handle(): int
    {
        try {
            $domains = (array) $this->option('domain');
            if ($domains === [] && $this->option('host-a') && $this->option('host-b')) {
                $domains = [
                    (string) $this->option('host-a'),
                    (string) $this->option('host-b'),
                ];
            }
            $status = BaitSplitService::fromDatabase()->startCampaign(
                (string) $this->argument('campaign'),
                $domains
            );

            $this->info("第 {$status['round']} 轮已启动");
            foreach ($status['bucket_labels'] as $index => $label) {
                $this->line("{$label} 组：{$status['bucket_counts'][$index]} 人");
            }
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
