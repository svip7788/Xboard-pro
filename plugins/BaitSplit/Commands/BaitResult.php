<?php

namespace Plugin\BaitSplit\Commands;

use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

class BaitResult extends Command
{
    protected $signature = 'bait:result
        {campaignOrResult : 排查任务 ID，或兼容旧命令的 A/B/both/none}
        {--campaign=legacy : 旧命令结果对应的任务 ID}
        {--bucket=* : 被墙分组字母，可重复；不填写表示都未被墙}';

    protected $description = '记录当前诱饵轮次结果并选择下一分支';

    public function handle(): int
    {
        try {
            $campaignOrResult = strtolower((string) $this->argument('campaignOrResult'));
            $legacyResults = [
                'a' => [0],
                'b' => [1],
                'both' => [0, 1],
                'none' => [],
            ];
            $campaignId = array_key_exists($campaignOrResult, $legacyResults)
                ? (string) $this->option('campaign')
                : (string) $this->argument('campaignOrResult');
            $buckets = array_key_exists($campaignOrResult, $legacyResults)
                ? $legacyResults[$campaignOrResult]
                : array_map(function ($label): int {
                $label = strtoupper(trim((string) $label));
                if (!preg_match('/^[A-J]$/', $label)) {
                    throw new \InvalidArgumentException('分组只能填写 A 至 J');
                }
                return ord($label) - 65;
            }, (array) $this->option('bucket'));

            $status = BaitSplitService::fromDatabase()
                ->recordResult(
                    $campaignId,
                    $buckets
                );

            $this->info('结果已记录，域名替换已自动暂停');
            $this->line("下一分支：{$status['active_path_label']}");
            $this->line("候选用户：{$status['candidate_count']} 人");
            $this->line('请准备新域名/IP 后执行 bait:start');

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
