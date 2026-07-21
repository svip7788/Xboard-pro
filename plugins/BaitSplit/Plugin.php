<?php

namespace Plugin\BaitSplit;

use App\Models\User;
use App\Services\Plugin\AbstractPlugin;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use Plugin\BaitSplit\Services\BaitSplitService;

class Plugin extends AbstractPlugin
{
    public function boot(): void
    {
        $this->filter('client.subscribe.servers', [$this, 'replaceServerHosts'], 10);
    }

    public function update(string $oldVersion, string $newVersion): void
    {
        (new BaitSplitService($this->getConfig()))->persistMigration();
    }

    public function schedule(\Illuminate\Console\Scheduling\Schedule $schedule): void
    {
        // withoutOverlapping(12)：单次 drain+收回可能较长；过期太短会叠跑抢锁
        $schedule->command('bait:decoy')
            ->everyMinute()
            ->withoutOverlapping(12)
            ->runInBackground();
    }

    public function replaceServerHosts(
        array $servers,
        User $user,
        Request $request
    ): array {
        try {
            return (new BaitSplitService($this->getConfig()))
                ->filterServers($servers, $user);
        } catch (\Throwable $exception) {
            Log::error('订阅诱饵分组处理失败', [
                'user_id' => $user->id,
                'error' => $exception->getMessage(),
            ]);
            try {
                return (new BaitSplitService($this->getConfig()))
                    ->failClosedServers($servers, $user);
            } catch (\Throwable) {
                return [];
            }
        }
    }
}
