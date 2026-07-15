<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitStart extends Command
{
    protected $signature = 'bait:start
        {--host-a= : A 组使用的新域名或 IP}
        {--host-b= : B 组使用的新域名或 IP}
        {--prefix= : 可选，手动指定二进制分支前缀}';

    protected $description = '启动一轮订阅诱饵 A/B 分组';

    public function handle(): int
    {
        try {
            $hostA = (string) $this->option('host-a');
            $hostB = (string) $this->option('host-b');
            if ($hostA === '' || $hostB === '') {
                $this->error('必须同时提供 --host-a 和 --host-b');
                return self::FAILURE;
            }

            $prefix = $this->option('prefix');
            $status = BaitSplitService::fromDatabase()->start(
                $hostA,
                $hostB,
                $prefix === null ? null : (string) $prefix
            );

            $this->info("第 {$status['round']} 轮已启动");
            $this->line("当前分支：{$status['active_prefix']}");
            $this->line("A 组：{$status['group_counts']['A']} 人");
            $this->line("B 组：{$status['group_counts']['B']} 人");
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
