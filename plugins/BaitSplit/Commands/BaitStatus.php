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
            $status = BaitSplitService::fromDatabase()->status();

            $this->table(
                ['项目', '值'],
                [
                    ['状态', $status['enabled'] ? '运行中' : '已暂停'],
                    ['轮次', $status['round']],
                    ['当前分支', $status['active_prefix'] === '' ? '根分支' : $status['active_prefix']],
                    ['候选用户', $status['candidate_count']],
                    ['A 组人数/已拉取', "{$status['group_counts']['A']} / {$status['exposed_counts']['A']}"],
                    ['B 组人数/已拉取', "{$status['group_counts']['B']} / {$status['exposed_counts']['B']}"],
                    ['待排查分支', implode(', ', $status['positive_queue']) ?: '无'],
                    ['延后复查分支', implode(', ', $status['deferred_queue']) ?: '无'],
                    ['目标权限组 ID', $status['target_group_id']],
                    ['目标节点 ID', implode(', ', $status['target_server_ids']) ?: '未配置'],
                    ['A 域名', $status['host_a'] ?: '未配置'],
                    ['B 域名', $status['host_b'] ?: '未配置'],
                ]
            );

            if ($status['findings'] !== []) {
                $this->warn('单用户候选：');
                $this->table(['用户 ID', '命中次数'], array_map(
                    fn(array $finding): array => [
                        $finding['user_id'],
                        $finding['confirmations'],
                    ],
                    $status['findings']
                ));
            }

            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }
}
