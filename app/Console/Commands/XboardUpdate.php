<?php

namespace App\Console\Commands;

use App\Services\ThemeService;
use App\Services\UpdateService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Process;
use App\Services\Plugin\PluginManager;

class XboardUpdate extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'xboard:update';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'xboard 更新';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $this->info('正在导入数据库请稍等...');
        Artisan::call("migrate", ['--force' => true]);
        $this->info(Artisan::output());
        $this->info('正在检查并安装默认插件...');
        PluginManager::installDefaultPlugins();
        $this->info('默认插件检查完成');
        $updateService = new UpdateService();
        $updateService->updateVersionCache();
        $themeService = app(ThemeService::class);
        $themeService->refreshCurrentTheme();
        if (config('queue.default') === 'sync') {
            $this->info('horizon:terminate skipped (sync queue, no workers to terminate).');
        } else {
            try {
                Artisan::call('horizon:terminate');
            } catch (\Throwable $e) {
                $this->warn('horizon:terminate skipped: ' . $e->getMessage());
            }
        }
        $this->restartWsServer();
        $this->info('更新完毕，队列服务已重启，你无需进行任何操作。');
    }

    /**
     * Workerman 的 stop/restart 会 exit 当前进程，必须在子进程里执行。
     * 被 supervisor 等守护时只 stop 由其拉起新进程；以 -d 守护化运行（父进程为 1）时原地 restart。
     */
    private function restartWsServer(): void
    {
        $pidFile = storage_path('logs/xboard-ws-server.pid');
        $pid = is_file($pidFile) ? (int) trim((string) file_get_contents($pidFile)) : 0;
        if ($pid <= 0 || !function_exists('posix_kill') || !posix_kill($pid, 0)) {
            return;
        }

        $ppid = (int) trim((string) Process::run(['ps', '-o', 'ppid=', '-p', (string) $pid])->output());
        $args = $ppid === 1 ? ['restart', '--d'] : ['stop'];
        $result = Process::path(base_path())->timeout(60)
            ->run(array_merge([PHP_BINARY, 'artisan', 'ws-server'], $args));
        $this->info($result->successful()
            ? 'WebSocket 服务已重启（' . implode(' ', $args) . '）'
            : 'WebSocket 服务重启失败: ' . trim($result->errorOutput() ?: $result->output()));
    }
}
