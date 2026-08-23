<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitDecoy extends Command
{
    protected $signature = 'bait:decoy';

    protected $description = '分钟级维护：消化积压换 IP，并跟上游对一遍当前地址';

    public function handle(): int
    {
        // 大状态落盘需要足够内存
        @ini_set('memory_limit', '1024M');

        try {
            $result = BaitSplitService::fromDatabase()->runDecoyOrchestration();
            if (isset($result['drain'])) {
                $d = $result['drain'];
                $this->info(sprintf(
                    'pending换IP: processed=%d failed=%d remaining=%d',
                    (int) ($d['processed'] ?? 0),
                    (int) ($d['failed'] ?? 0),
                    (int) ($d['remaining'] ?? 0)
                ));
            }
            $this->info('上游对账: ' . json_encode(
                $result['upstream'] ?? ['skipped' => 'none'],
                JSON_UNESCAPED_UNICODE
            ));
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
