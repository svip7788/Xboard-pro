<?php

namespace App\Jobs;

use App\Models\StatUser;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

/**
 * 节点流量上报核心 Job（合并了扣费 + 用户维度统计）。
 *
 * 合并动机：
 *   原流水线每个 chunk 同时 dispatch 3 个 Job（TrafficFetch / StatUser / StatServer），
 *   每个 Job 都把 1000 条明细数据各自序列化进 Redis，payload 在队列里重复 3 份。
 *   扣费 (v2_user) 和按用户维度的每日统计 (v2_stat_user) 使用的是完全相同的
 *   input data，拿同一份数据做两次 DB 操作即可，合并后：
 *     - Redis 网络/序列化开销 -33%
 *     - Horizon worker 取出并 unserialize 的次数 -33%
 *     - 每 chunk 只需一把 traffic_fetch 锁，不再跨队列抢
 */
class TrafficFetchJob implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $data;
    protected $server;
    protected $protocol;
    protected $timestamp;

    public $tries = 1;
    public $timeout = 120;

    public function __construct(array $server, array $data, $protocol, int $timestamp)
    {
        $this->onQueue('traffic_fetch');
        $this->server = $server;
        $this->data = $data;
        $this->protocol = $protocol;
        $this->timestamp = $timestamp;
    }

    public function uniqueId(): string
    {
        return 'tf-' . ($this->server['id'] ?? 0)
            . '-' . $this->protocol
            . '-' . $this->timestamp
            . '-' . crc32(serialize(array_keys($this->data)));
    }

    public function uniqueFor(): int
    {
        return 180;
    }

    public function handle(): void
    {
        $rate = (float) ($this->server['rate'] ?? 1.0);

        $casesU = [];
        $casesD = [];
        $ids    = [];
        $statRows = [];
        $now    = time();
        $recordAt = strtotime(date('Y-m-d'));

        foreach ($this->data as $uid => $v) {
            $uid = (int) $uid;
            if ($uid <= 0) continue;

            $up   = isset($v[0]) ? (int) $v[0] : 0;
            $down = isset($v[1]) ? (int) $v[1] : 0;

            $du = (int) round($up * $rate);
            $dd = (int) round($down * $rate);
            if ($du === 0 && $dd === 0) continue;

            $casesU[] = "WHEN {$uid} THEN u + {$du}";
            $casesD[] = "WHEN {$uid} THEN d + {$dd}";
            $ids[]    = $uid;

            $statRows[] = [
                'user_id'     => $uid,
                'server_rate' => $this->server['rate'],
                'record_at'   => $recordAt,
                'record_type' => 'd',
                'u'           => $du,
                'd'           => $dd,
                'created_at'  => $now,
                'updated_at'  => $now,
            ];
        }

        if (empty($ids)) return;

        $inList = implode(',', $ids);
        $sqlU   = implode(' ', $casesU);
        $sqlD   = implode(' ', $casesD);

        // 1) 扣费：更新 v2_user.u / v2_user.d
        DB::update(
            "UPDATE v2_user
             SET u = CASE id {$sqlU} ELSE u END,
                 d = CASE id {$sqlD} ELSE d END,
                 t = {$now}
             WHERE id IN ({$inList})"
        );

        // 2) 日流量统计 upsert v2_stat_user（合并自原 StatUserJob）
        try {
            $this->upsertStatUser($statRows, $now);
        } catch (\Throwable $e) {
            // 统计失败不影响扣费；记录后不再抛出，避免 Job 整体失败。
            Log::error('TrafficFetchJob stat_user upsert failed: ' . $e->getMessage(), [
                'server_id' => $this->server['id'] ?? null,
                'rows'      => count($statRows),
            ]);
        }

        // 3) 推入超额检查队列，check:traffic-exceeded 会每分钟消费一次
        Redis::sadd('traffic:pending_check', ...$ids);
    }

    /**
     * v2_stat_user 批量 upsert，支持 pgsql 和 mysql（与原 StatUserJob 对齐）。
     */
    protected function upsertStatUser(array $rows, int $now): void
    {
        if (empty($rows)) return;

        $driver = config('database.default');
        if ($driver === 'pgsql') {
            $table = (new StatUser())->getTable();
            $placeholders = [];
            $bindings = [];
            foreach ($rows as $r) {
                $placeholders[] = '(?, ?, ?, ?, ?, ?, ?, ?)';
                array_push(
                    $bindings,
                    $r['user_id'], $r['server_rate'], $r['record_at'], $r['record_type'],
                    $r['u'], $r['d'], $r['created_at'], $r['updated_at']
                );
            }
            $sql = "INSERT INTO {$table} (user_id, server_rate, record_at, record_type, u, d, created_at, updated_at)
                    VALUES " . implode(',', $placeholders) . "
                    ON CONFLICT (user_id, server_rate, record_at)
                    DO UPDATE SET
                        u = {$table}.u + EXCLUDED.u,
                        d = {$table}.d + EXCLUDED.d,
                        updated_at = EXCLUDED.updated_at";
            DB::statement($sql, $bindings);
            return;
        }

        StatUser::upsert(
            $rows,
            ['user_id', 'server_rate', 'record_at', 'record_type'],
            [
                'u'          => DB::raw('u + VALUES(u)'),
                'd'          => DB::raw('d + VALUES(d)'),
                'updated_at' => $now,
            ]
        );
    }
}
