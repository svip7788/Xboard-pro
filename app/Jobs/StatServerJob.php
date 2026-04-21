<?php


namespace App\Jobs;

use App\Models\Server;
use App\Models\StatServer;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * 节点维度的流量汇总 Job。
 *
 * Breaking change (intentional)：构造函数第二参 $data 语义变了：
 *   旧版：用户明细 data（[uid => [up, down], ...]，1000 用户就 1000 条）
 *   新版：聚合后的 ['u' => int, 'd' => int]
 *
 * 动机：原来 UserService::trafficFetch 按 chunk(1000) 分发，一次 5000 用户的上报
 *   会产生 5 个 StatServerJob，各自对 v2_server / v2_stat_server 同一行做 upsert，
 *   行锁下串行化，5 次 upsert 之间互相等待，payload 里还带着 5×1000 条明细数据
 *   白白占 Redis。现在改成：trafficFetch 一次性聚合，只 dispatch 1 个 StatServerJob
 *   且 payload 只有两个整数。
 *
 * 同时实现 ShouldBeUnique 防止 Horizon 重试风暴时 v2_server / v2_stat_server
 *  被重复累加。
 */
class StatServerJob implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /** @var array{u:int,d:int} */
    protected array $data;
    protected array $server;
    protected string $protocol;
    protected string $recordType;
    protected int $timestamp;

    public $tries = 3;
    public $timeout = 60;
    public $maxExceptions = 3;

    /**
     * Calculate the number of seconds to wait before retrying the job.
     */
    public function backoff(): array
    {
        return [1, 5, 10];
    }

    /**
     * @param array $server
     * @param array $data 兼容两种形态：
     *                    - 新格式：['u' => int, 'd' => int]
     *                    - 旧格式：[uid => [up, down], ...]（内部会聚合）
     */
    public function __construct(array $server, array $data, $protocol, string $recordType = 'd', ?int $timestamp = null)
    {
        $this->onQueue('stat');
        $this->server = $server;
        $this->protocol = $protocol;
        $this->recordType = $recordType;
        $this->timestamp = $timestamp ?? (int) (
            $recordType === 'm' ? strtotime(date('Y-m-01')) : strtotime(date('Y-m-d'))
        );

        // 归一化为 ['u' => .., 'd' => ..]，兼容历史调用方
        if (array_key_exists('u', $data) && array_key_exists('d', $data)) {
            $this->data = ['u' => (int) $data['u'], 'd' => (int) $data['d']];
        } else {
            $u = $d = 0;
            foreach ($data as $traffic) {
                if (!is_array($traffic)) continue;
                $u += (int) ($traffic[0] ?? 0);
                $d += (int) ($traffic[1] ?? 0);
            }
            $this->data = ['u' => $u, 'd' => $d];
        }
    }

    public function uniqueId(): string
    {
        return 'ss-' . ($this->server['id'] ?? 0)
            . '-' . $this->protocol
            . '-' . $this->recordType
            . '-' . $this->timestamp
            . '-' . $this->data['u']
            . '-' . $this->data['d'];
    }

    public function uniqueFor(): int
    {
        return 180;
    }

    public function handle(): void
    {
        $recordAt = $this->recordType === 'm'
            ? strtotime(date('Y-m-01'))
            : strtotime(date('Y-m-d'));

        $u = (int) ($this->data['u'] ?? 0);
        $d = (int) ($this->data['d'] ?? 0);
        if ($u === 0 && $d === 0) return;

        try {
            $this->processServerStat($u, $d, $recordAt);
            $this->updateServerTraffic($u, $d);
        } catch (\Exception $e) {
            Log::error('StatServerJob failed for server ' . $this->server['id'] . ': ' . $e->getMessage());
            throw $e;
        }
    }

    protected function updateServerTraffic(int $u, int $d): void
    {
        DB::table('v2_server')
            ->where('id', $this->server['id'])
            ->incrementEach(
                ['u' => $u, 'd' => $d],
                ['updated_at' => Carbon::now()]
            );
    }

    protected function processServerStat(int $u, int $d, int $recordAt): void
    {
        $driver = config('database.default');
        if ($driver === 'sqlite') {
            $this->processServerStatForSqlite($u, $d, $recordAt);
        } elseif ($driver === 'pgsql') {
            $this->processServerStatForPostgres($u, $d, $recordAt);
        } else {
            $this->processServerStatForOtherDatabases($u, $d, $recordAt);
        }
    }

    protected function processServerStatForSqlite(int $u, int $d, int $recordAt): void
    {
        DB::transaction(function () use ($u, $d, $recordAt) {
            $existingRecord = StatServer::where([
                'record_at' => $recordAt,
                'server_id' => $this->server['id'],
                'server_type' => $this->protocol,
                'record_type' => $this->recordType,
            ])->first();

            if ($existingRecord) {
                $existingRecord->update([
                    'u' => $existingRecord->u + $u,
                    'd' => $existingRecord->d + $d,
                    'updated_at' => time(),
                ]);
            } else {
                StatServer::create([
                    'record_at' => $recordAt,
                    'server_id' => $this->server['id'],
                    'server_type' => $this->protocol,
                    'record_type' => $this->recordType,
                    'u' => $u,
                    'd' => $d,
                    'created_at' => time(),
                    'updated_at' => time(),
                ]);
            }
        }, 3);
    }

    protected function processServerStatForOtherDatabases(int $u, int $d, int $recordAt): void
    {
        StatServer::upsert(
            [
                'record_at' => $recordAt,
                'server_id' => $this->server['id'],
                'server_type' => $this->protocol,
                'record_type' => $this->recordType,
                'u' => $u,
                'd' => $d,
                'created_at' => time(),
                'updated_at' => time(),
            ],
            ['server_id', 'server_type', 'record_at', 'record_type'],
            [
                'u' => DB::raw("u + VALUES(u)"),
                'd' => DB::raw("d + VALUES(d)"),
                'updated_at' => time(),
            ]
        );
    }

    /**
     * PostgreSQL upsert with arithmetic increments using ON CONFLICT ... DO UPDATE
     */
    protected function processServerStatForPostgres(int $u, int $d, int $recordAt): void
    {
        $table = (new StatServer())->getTable();
        $now = time();

        // Use parameter binding to avoid SQL injection and keep maintainability
        $sql = "INSERT INTO {$table} (record_at, server_id, server_type, record_type, u, d, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (server_id, server_type, record_at)
                DO UPDATE SET
                    u = {$table}.u + EXCLUDED.u,
                    d = {$table}.d + EXCLUDED.d,
                    updated_at = EXCLUDED.updated_at";

        DB::statement($sql, [
            $recordAt,
            $this->server['id'],
            $this->protocol,
            $this->recordType,
            $u,
            $d,
            $now,
            $now,
        ]);
    }
}
