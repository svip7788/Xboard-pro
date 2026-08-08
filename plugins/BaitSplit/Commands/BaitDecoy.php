<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitDecoy extends Command
{
    protected $signature = 'bait:decoy';

    protected $description = '分钟级维护：消化积压换 IP，轮询追猎槽新 IP 上线，解散空转追猎链';

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
            if (isset($result['skipped'])) {
                $this->info('追猎维护跳过：' . $result['skipped']);
                // locked 时仍算成功：pending 可能已消化一部分，下分钟再试
                return self::SUCCESS;
            }
            foreach ($result['actions'] ?? [] as $action) {
                $this->info(json_encode($action, JSON_UNESCAPED_UNICODE));
            }
            if (($result['actions'] ?? []) === []) {
                $this->info('无动作');
            }
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
