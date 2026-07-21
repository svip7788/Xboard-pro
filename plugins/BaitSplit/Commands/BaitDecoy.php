<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitDecoy extends Command
{
    protected $signature = 'bait:decoy';

    protected $description = '夜间分组诱捕编排：到点打散嫌疑人到独立 IP 组，白天收回；并消费积压换 IP';

    public function handle(): int
    {
        // 大状态落盘需要足够内存，避免白天收回 OOM
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
                $this->info('诱捕跳过：' . $result['skipped']);
                // locked 时仍算成功：pending 可能已消化一部分，下分钟再试收回
                return self::SUCCESS;
            }
            foreach ($result['actions'] ?? [] as $action) {
                $this->info(json_encode($action, JSON_UNESCAPED_UNICODE));
            }
            if (($result['actions'] ?? []) === []) {
                $this->info(json_encode([
                    'in_window' => $result['in_window'] ?? null,
                    'actions' => [],
                ], JSON_UNESCAPED_UNICODE));
            }
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
