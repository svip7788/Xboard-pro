<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitResult extends Command
{
    protected $signature = 'bait:result
        {result : A、B、both（都被墙）或 none（都未被墙）}';

    protected $description = '记录当前诱饵轮次结果并选择下一分支';

    public function handle(): int
    {
        try {
            $status = BaitSplitService::fromDatabase()
                ->recordResult((string) $this->argument('result'));

            $this->info('结果已记录，域名替换已自动暂停');
            $this->line("下一分支：{$status['active_prefix']}");
            $this->line("候选用户：{$status['candidate_count']} 人");
            $this->line('请准备两个新域名/IP 后执行 bait:start');

            if ($status['findings'] !== []) {
                $this->warn('已有单用户候选，请查看 bait:status 并重复验证');
            }
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
