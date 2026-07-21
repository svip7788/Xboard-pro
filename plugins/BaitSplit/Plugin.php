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
        // withoutOverlapping(5)：防重叠锁最多 5 分钟过期，避免进程被杀后整天跳过
        $schedule->command('bait:decoy')
            ->everyMinute()
            ->withoutOverlapping(5)
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
