<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitDecoy extends Command
{
    protected $signature = 'bait:decoy';

    protected $description = '夜间分组诱捕编排：到点打散嫌疑人到独立 IP 组，白天收回';

    public function handle(): int
    {
        try {
            $result = BaitSplitService::fromDatabase()->runDecoyOrchestration();
            if (isset($result['skipped'])) {
                $this->info('诱捕跳过：' . $result['skipped']);
                return self::SUCCESS;
            }
            foreach ($result['actions'] ?? [] as $action) {
                $this->info(json_encode($action, JSON_UNESCAPED_UNICODE));
            }
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
