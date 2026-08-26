<?php

namespace Plugin\BaitSplit\Commands;

use App\Models\User;
use App\Services\Plugin\PluginConfigService;
use Illuminate\Console\Command;

class BaitNightRisk extends Command
{
    protected $signature = 'bait:night-risk
        {--set= : 逗号分隔的 uid，覆盖现有名单}
        {--from= : 从 JSON 文件读 uids 字段，配合离线分析脚本用}
        {--clear : 清空名单，收敛退回按 uid 均分}
        {--show=20 : 列出前若干个 uid 的邮箱}';

    protected $description = '维护夜间高危名单：名单里的人全压第一个牺牲池，其余进第二个';

    public function handle(): int
    {
        try {
            $service = app(PluginConfigService::class);
            $config = $service->getDbConfig('bait_split');

            $next = $this->resolveInput();
            if ($next !== null) {
                $config['night_risk_uids'] = implode(',', $next);
                $service->updateConfig('bait_split', $config);
                $this->info(sprintf('名单已更新：%d 人', count($next)));
            }

            $uids = $this->parse((string) ($config['night_risk_uids'] ?? ''));
            if ($uids === []) {
                $this->warn('名单为空，收敛按 uid 均分（两个牺牲池人群同质，死哪个都读不出信息）');
                return self::SUCCESS;
            }

            $this->info(sprintf('当前名单 %d 人，窗口内全部压第一个牺牲池', count($uids)));
            $limit = max(0, (int) $this->option('show'));
            if ($limit > 0) {
                $rows = User::query()
                    ->whereIn('id', array_slice($uids, 0, $limit))
                    ->orderBy('id')
                    ->get(['id', 'email']);
                $this->table(
                    ['uid', '邮箱'],
                    $rows->map(fn(User $u): array => [$u->id, $u->email])->all()
                );
            }
            return self::SUCCESS;
        } catch (\Throwable $exception) {
            $this->error($exception->getMessage());
            return self::FAILURE;
        }
    }

    /** @return int[]|null null 表示这次只看不改 */
    private function resolveInput(): ?array
    {
        if ($this->option('clear')) {
            return [];
        }
        if ($path = (string) $this->option('from')) {
            if (!is_file($path)) {
                throw new \RuntimeException("找不到文件：{$path}");
            }
            $data = json_decode((string) file_get_contents($path), true);
            if (!is_array($data) || !isset($data['uids']) || !is_array($data['uids'])) {
                throw new \RuntimeException("{$path} 里没有 uids 数组");
            }
            return $this->parse(implode(',', $data['uids']));
        }
        $set = $this->option('set');
        return $set === null ? null : $this->parse((string) $set);
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
}
