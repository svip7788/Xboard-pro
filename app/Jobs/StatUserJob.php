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

class StatUserJob implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected array $data;
    protected array $server;
    protected string $protocol;
    protected string $recordType;

    public $tries = 1;
    public $timeout = 120;
    public $maxExceptions = 1;

    public function __construct(array $server, array $data, string $protocol, string $recordType = 'd')
    {
        $this->onQueue('stat');
        $this->data = $data;
        $this->server = $server;
        $this->protocol = $protocol;
        $this->recordType = $recordType;
    }

    public function uniqueId(): string
    {
        $ts = $this->recordType === 'm' ? date('Ym') : date('Ymd');
        return 'su-' . ($this->server['id'] ?? 0) . '-' . $this->protocol . '-' . $this->recordType . '-' . $ts . '-' . crc32(serialize(array_keys($this->data)));
    }

    public function uniqueFor(): int
    {
        return 180;
    }

    public function handle(): void
    {
        if (empty($this->data)) return;

        $recordAt = $this->recordType === 'm'
            ? strtotime(date('Y-m-01'))
            : strtotime(date('Y-m-d'));

        $rate = (float) ($this->server['rate'] ?? 1.0);
        $now  = time();

        $rows = [];
        foreach ($this->data as $uid => $v) {
            $uid = (int) $uid;
            if ($uid <= 0) continue;

            $u = (int) round(((int) ($v[0] ?? 0)) * $rate);
            $d = (int) round(((int) ($v[1] ?? 0)) * $rate);
            if ($u === 0 && $d === 0) continue;

            $rows[] = [
                'user_id'     => $uid,
                'server_rate' => $this->server['rate'],
                'record_at'   => $recordAt,
                'record_type' => $this->recordType,
                'u'           => $u,
                'd'           => $d,
                'created_at'  => $now,
                'updated_at'  => $now,
            ];
        }

        if (empty($rows)) return;

        try {
            $driver = config('database.default');
            if ($driver === 'pgsql') {
                $this->batchUpsertPostgres($rows, $now);
            } else {
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
        } catch (\Throwable $e) {
            Log::error('StatUserJob batch upsert failed: ' . $e->getMessage(), [
                'server_id' => $this->server['id'] ?? null,
                'rows'      => count($rows),
            ]);
            throw $e;
        }
    }

    protected function batchUpsertPostgres(array $rows, int $now): void
    {
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
    }
}
