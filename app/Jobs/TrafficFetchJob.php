<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;

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
        $ids = [];

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
        }

        if (empty($ids)) return;

        $now    = time();
        $inList = implode(',', $ids);
        $sqlU   = implode(' ', $casesU);
        $sqlD   = implode(' ', $casesD);

        DB::update(
            "UPDATE v2_user
             SET u = CASE id {$sqlU} ELSE u END,
                 d = CASE id {$sqlD} ELSE d END,
                 t = {$now}
             WHERE id IN ({$inList})"
        );

        Redis::sadd('traffic:pending_check', ...$ids);
    }
}
