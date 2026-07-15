<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitDisable extends Command
{
    protected $signature = 'bait:disable {campaign=legacy : 排查任务 ID}';

    protected $description = '立即停止替换订阅节点域名';

    public function handle(): int
    {
        try {
            BaitSplitService::fromDatabase()->disableCampaign(
                (string) $this->argument('campaign')
            );
            $this->info('订阅诱饵分组已停止');
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
