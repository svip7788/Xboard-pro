<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitStatus extends Command
{
    protected $signature = 'bait:status';

    protected $description = '查看订阅诱饵分组状态';

    public function handle(): int
    {
        try {
            $campaigns = BaitSplitService::fromDatabase()->campaigns();
            if ($campaigns === []) {
                $this->warn('尚未创建排查任务');
                return self::SUCCESS;
            }

            $this->table(
                ['任务 ID', '名称', '状态', '组数', '轮次', '候选用户', '当前分支'],
                array_map(fn(array $campaign): array => [
                    $campaign['id'],
                    $campaign['name'],
                    $campaign['enabled'] ? '运行中' : '已暂停',
                    $campaign['bucket_count'],
                    $campaign['round'],
                    $campaign['candidate_count'],
                    $campaign['active_path_label'],
                ], $campaigns)
            );
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
