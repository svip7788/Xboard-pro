<?php

namespace Plugin\BaitSplit\Commands;

use App\Models\User;
use Illuminate\Console\Command;
use Plugin\BaitSplit\Services\BaitSplitService;

/**
 * 按名单重排牺牲组的静态归属。
 *
 * 排查是在面板上一轮轮做的：可疑的人放一组、其余放另一组，看哪组被墙；
 * 干净的那组整批挪去正常分组，再对剩下的对半分，直到剩下招墙的那个人。
 * 每轮都要动上千人的归属，手动点不动，所以有这个命令。
 */
class BaitNightRegroup extends Command
{
    protected $signature = 'bait:night-regroup
        {--suspect= : 可疑名单，逗号分隔的 uid}
        {--from= : 从 JSON 文件读 uids 字段当可疑名单}
        {--to=cs4 : 可疑的人挪进哪个池，填接口标识或池名}
        {--rest= : 其余人挪进哪个池，留空则让他们留在原组}
        {--scope= : 只在这些池的现有成员里重排，逗号分隔；留空则等于 to 和 rest}
        {--half : 把 scope 里的人对半分到 to 和 rest，忽略可疑名单，用于逐轮二分}
        {--dry : 只看会怎么动，不写入}';

    protected $description = '重排牺牲组的静态归属：可疑的进一个组，其余进另一个组';

    public function handle(): int
    {
        try {
            $service = BaitSplitService::fromDatabase();
            $campaignId = $this->campaignId($service);
            $to = trim((string) $this->option('to'));
            $rest = trim((string) $this->option('rest'));
            $scope = $this->scopePools($to, $rest);
            $this->guard($to, $rest, $scope);
            $members = [];
            foreach ($scope as $ref) {
                foreach ($service->poolMembers($campaignId, $ref) as $uid) {
                    $members[$uid] = true;
                }
            }
            if ($members === []) {
                $this->warn('这些池里没有生效用户：' . implode('、', $scope));
                return self::SUCCESS;
            }

            [$hit, $miss] = $this->split(array_keys($members));
            $this->line(sprintf(
                '范围内 %d 人 → %s 组 %d 人，%s',
                count($members),
                $to,
                count($hit),
                $rest === ''
                    ? sprintf('其余 %d 人留在原组', count($miss))
                    : sprintf('%s 组 %d 人', $rest, count($miss))
            ));

            if ($this->option('dry')) {
                $this->preview($to, $rest, $hit, $miss);
                $this->warn('这是预演，没有写入。去掉 --dry 才真的改');
                return self::SUCCESS;
            }

            $plan = [$to => $hit];
            if ($rest !== '') {
                $plan[$rest] = $miss;
            }
            foreach ($service->reassignUsers($campaignId, $plan) as $result) {
                $this->line(sprintf(
                    '  %s：挪入 %d 人，本来就在 %d 人%s%s',
                    $result['pool_name'],
                    $result['moved'],
                    $result['already'],
                    $result['locked'] > 0 ? sprintf('，跳过人工锁定 %d 人', $result['locked']) : '',
                    $result['invalid'] > 0 ? sprintf('，无效 %d 人', $result['invalid']) : ''
                ));
            }
            $this->info('重排完成，面板刷新后人数即为新的分组');
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }

    private function campaignId(BaitSplitService $service): string
    {
        foreach ($service->campaigns() as $campaign) {
            if (!empty($campaign['router'])) {
                return (string) $campaign['id'];
            }
        }
        throw new \RuntimeException('没有启用了域名调度的排查任务');
    }

    /** @return string[] */
    private function scopePools(string $to, string $rest): array
    {
        $raw = (string) $this->option('scope');
        if ($raw !== '') {
            return array_values(array_unique(array_filter(array_map('trim', explode(',', $raw)))));
        }
        return array_values(array_unique(array_filter([$to, $rest])));
    }

    /**
     * 挡住那些跑完才发现白跑一趟的组合。
     *
     * @param string[] $scope
     */
    private function guard(string $to, string $rest, array $scope): void
    {
        if ($to === '') {
            throw new \RuntimeException('--to 不能为空');
        }
        if ($rest === $to) {
            throw new \RuntimeException('--to 和 --rest 是同一个池，挪了等于没挪');
        }
        if ($this->option('half') && $rest === '') {
            throw new \RuntimeException('--half 要把人分到两个池，得同时给 --rest');
        }
        if ($this->option('half') || $this->suspectIds() !== []) {
            return;
        }
        // 既没名单也不是二分，那就是整批挪走，范围必须是别的池，否则原地打转
        if ($scope === [$to]) {
            throw new \RuntimeException(
                '整批挪走要用 --scope 指定挪哪个池的人，比如 --scope=cs2 --to=aq1'
            );
        }
    }

    /**
     * @param int[] $members
     * @return array{0: int[], 1: int[]} 命中名单的、其余的
     */
    private function split(array $members): array
    {
        sort($members);
        if ($this->option('half')) {
            // 逐轮二分：按 uid 排序切两半，跟名单无关
            $mid = (int) ceil(count($members) / 2);
            return [array_slice($members, 0, $mid), array_slice($members, $mid)];
        }
        $ids = $this->suspectIds();
        if ($ids === []) {
            // 没给名单就是整批挪走：一个组整晚没被墙，直接搬去正常分组
            return [$members, []];
        }
        $suspect = array_flip($ids);
        $hit = $miss = [];
        foreach ($members as $uid) {
            if (isset($suspect[$uid])) {
                $hit[] = $uid;
            } else {
                $miss[] = $uid;
            }
        }
        return [$hit, $miss];
    }

    /** @return int[] */
    private function suspectIds(): array
    {
        if ($path = (string) $this->option('from')) {
            if (!is_file($path)) {
                throw new \RuntimeException("找不到文件：{$path}");
            }
            $data = json_decode((string) file_get_contents($path), true);
            if (!is_array($data) || !is_array($data['uids'] ?? null)) {
                throw new \RuntimeException("{$path} 里没有 uids 数组");
            }
            return $this->parse(implode(',', $data['uids']));
        }
        return $this->parse((string) $this->option('suspect'));
    }

    /** @return int[] */
    private function parse(string $raw): array
    {
        $ids = [];
        foreach (explode(',', $raw) as $piece) {
            $uid = (int) trim($piece);
            if ($uid > 0) {
                $ids[$uid] = $uid;
            }
        }
        sort($ids);
        return array_values($ids);
    }

    /**
     * @param int[] $hit
     * @param int[] $miss
     */
    private function preview(string $to, string $rest, array $hit, array $miss): void
    {
        $rows = [];
        $groups = [["进 {$to}", $hit]];
        if ($rest !== '') {
            $groups[] = ["进 {$rest}", $miss];
        }
        foreach ($groups as [$label, $ids]) {
            $emails = User::query()
                ->whereIn('id', array_slice($ids, 0, 5))
                ->orderBy('id')
                ->pluck('email', 'id');
            foreach ($emails as $id => $email) {
                $rows[] = [$label, $id, $email];
            }
        }
        if ($rows !== []) {
            $this->table(['去向', 'uid', '邮箱'], $rows);
        }
    }
}
