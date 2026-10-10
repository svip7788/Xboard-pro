<?php

namespace App\Jobs;

use App\Models\ServerMachineTask;
use App\Services\Machine\SshClient;
use App\Services\Machine\TaskLogger;
use App\Services\Machine\V2bXScripts;
use App\Services\Machine\V2bXSettings;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeEncrypted;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

/**
 * 通过一次性 SSH 凭据为机器安装 V2bX（机器模式），凭据不落库
 */
class MachineInstallJob implements ShouldQueue, ShouldBeEncrypted
{
    use Dispatchable, InteractsWithQueue, Queueable;

    public const QUEUE = 'machine_ops';

    private const DEFAULT_FILES = ['dns', 'route', 'custom_inbound', 'custom_outbound', 'sing_origin'];

    public $tries = 1;
    public $timeout = 570;
    public $failOnTimeout = true;

    public function __construct(
        public int $taskId,
        public array $ssh,
        public array $options = [],
    ) {
        $this->onQueue(self::QUEUE);
    }

    public function handle(): void
    {
        $task = ServerMachineTask::find($this->taskId);
        if (!$task || $task->status !== 'pending' || !($machine = $task->machine)) {
            return;
        }
        $task->forceFill(['status' => 'running', 'started_at' => time()])->save();
        $logger = new TaskLogger($task);
        $ssh = new SshClient(
            $this->ssh['host'],
            (int) $this->ssh['port'],
            $this->ssh['user'],
            $this->ssh['password'] ?: null,
            $this->ssh['key'] ?: null,
        );

        try {
            $logger->step('连接服务器');
            $logger->line("连接 {$this->ssh['host']}:{$this->ssh['port']} ...");
            $ssh->connect();
            $logger->line('连接成功');

            $version = $task->params['version'] ?? V2bXSettings::resolveVersion();
            $logger->step('安装 V2bX');
            $logger->line("安装 V2bX {$version}（" . V2bXSettings::repo() . '）');
            $code = $ssh->runScript(V2bXScripts::install(V2bXSettings::repo(), $version), fn($o) => $logger->log($o), 480);
            if ($code !== 0) {
                throw new \RuntimeException("安装脚本退出码: {$code}");
            }

            if ($this->options['bbr'] ?? true) {
                $logger->step('BBR 优化');
                $ssh->runScript(V2bXScripts::bbr(), fn($o) => $logger->log($o), 120);
            }

            $logger->step('写入配置');
            $ssh->exec(V2bXScripts::ENSURE_CONFIG_DIR, 120);
            if (preg_match('/BACKED_UP:(\S+)/', $ssh->exec(V2bXScripts::BACKUP_CONFIG, 30)[1], $m)) {
                $logger->line("已备份旧配置到 {$m[1]}");
            }
            $ssh->writeFile('/etc/V2bX/config.json', V2bXSettings::machineConfig($machine->id, $machine->token, $this->options['core'] ?? 'auto'), 0600);
            $logger->line('写入 /etc/V2bX/config.json（机器模式）');
            foreach (self::DEFAULT_FILES as $name) {
                $path = "/etc/V2bX/{$name}.json";
                if ($ssh->exec('test -s ' . escapeshellarg($path), 10)[0] !== 0) {
                    $ssh->writeFile($path, V2bXScripts::resource("{$name}.json"));
                    $logger->line("写入默认 {$path}");
                }
            }
            $ssh->exec(V2bXScripts::REMOVE_LEGACY_AGENT, 30);

            $logger->step('启动服务');
            $status = V2bXScripts::lastLine($ssh->exec(V2bXScripts::RESTART_AND_WAIT, 90)[1]);
            $logger->line("服务状态: {$status}");
            if ($status !== 'active') {
                $logger->log(">>> 最近日志:\n" . trim($ssh->exec(V2bXScripts::RECENT_LOGS, 30)[1]) . "\n");
                throw new \RuntimeException("V2bX 启动失败（{$status}）");
            }

            $machine->forceFill([
                'host' => $this->ssh['host'],
                'ssh_port' => (int) $this->ssh['port'],
                'ssh_user' => $this->ssh['user'],
                'v2bx_status' => 'active',
                'v2bx_version' => $version,
            ])->save();
            $logger->line('安装完成，V2bX 将自动连接面板并拉取节点');
            $logger->flush(['status' => 'success', 'step' => '已完成', 'finished_at' => time()]);
        } catch (\Throwable $e) {
            $logger->log(">>> 失败: {$e->getMessage()}\n");
            $logger->flush(['status' => 'failed', 'message' => mb_strimwidth($e->getMessage(), 0, 500), 'finished_at' => time()]);
        } finally {
            $ssh->disconnect();
        }
    }

    public function failed(?\Throwable $e): void
    {
        ServerMachineTask::whereKey($this->taskId)
            ->whereIn('status', ServerMachineTask::ACTIVE_STATUSES)
            ->update([
                'status' => 'failed',
                'message' => mb_strimwidth('任务中断: ' . ($e?->getMessage() ?? '超时'), 0, 500),
                'finished_at' => time(),
            ]);
    }
}
