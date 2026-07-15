<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitDisable extends Command
{
    protected $signature = 'bait:disable';

    protected $description = '立即停止替换订阅节点域名';

    public function handle(): int
    {
        try {
            BaitSplitService::fromDatabase()->disable();
            $this->info('订阅诱饵分组已停止');
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
