<?php

namespace App\Services;

use App\Utils\CacheKey;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Process;
use Illuminate\Support\Facades\File;

class UpdateService
{
    const UPDATE_CHECK_INTERVAL = 86400; // 24 hours
    const GITHUB_API_URL = 'https://api.github.com/repos/svip7788/Xboard-pro/commits';
    const CACHE_UPDATE_INFO = 'UPDATE_INFO';
    const CACHE_LAST_CHECK = 'LAST_UPDATE_CHECK';
    const CACHE_UPDATE_LOCK = 'UPDATE_LOCK';
    const CACHE_VERSION = 'CURRENT_VERSION';
    const CACHE_VERSION_DATE = 'CURRENT_VERSION_DATE';
    const CACHE_UPDATE_RESULT = 'UPDATE_RESULT';
    const CHECK_CACHE_SECONDS = 600;
    const LOCK_SECONDS = 3600;
    
    /**
     * Get current version from cache or generate new one
     */
    public function getCurrentVersion(): string
    {
        $date = Cache::get(self::CACHE_VERSION_DATE) ?? date('Ymd');
        $hash = Cache::rememberForever(self::CACHE_VERSION, function () {
            return $this->getCurrentCommit();
        });
        return $date . '-' . $hash;
    }

    /**
     * Update version cache
     */
    public function updateVersionCache(): void
    {
        try {
            $result = Process::run('git log -1 --format=%cd:%H --date=format:%Y%m%d');
            if ($result->successful()) {
                list($date, $hash) = explode(':', trim($result->output()));
                Cache::forever(self::CACHE_VERSION_DATE, $date);
                Cache::forever(self::CACHE_VERSION, substr($hash, 0, 7));
                // Log::info('Version cache updated: ' . $date . '-' . substr($hash, 0, 7));
                return;
            }
        } catch (\Exception $e) {
            Log::error('Failed to get version with date: ' . $e->getMessage());
        }

        // Fallback
        Cache::forever(self::CACHE_VERSION_DATE, date('Ymd'));
        $fallbackHash = $this->getCurrentCommit();
        Cache::forever(self::CACHE_VERSION, $fallbackHash);
        Log::info('Version cache updated (fallback): ' . date('Ymd') . '-' . $fallbackHash);
    }

    public function checkForUpdates(bool $force = false): array
    {
        if (!$force) {
            $cached = Cache::get(self::CACHE_UPDATE_INFO);
            $fresh = now()->timestamp - (int) $this->getLastCheckTime() < self::CHECK_CACHE_SECONDS;
            if ($cached && $fresh && ($cached['current_version'] ?? null) === $this->getCurrentCommit()) {
                return $cached;
            }
        }
        try {
            // Get current version commit
            $currentCommit = $this->getCurrentCommit();
            if ($currentCommit === 'unknown') {
                // If unable to get current commit, try to get the first commit
                $currentCommit = $this->getFirstCommit();
            }
            // Get local git logs
            $localLogs = $this->getLocalGitLogs();
            if (empty($localLogs)) {
                Log::error('Failed to get local git logs');
                return $this->getCachedUpdateInfo();
            }

            // Get remote latest commits
            $response = Http::withHeaders([
                'Accept' => 'application/vnd.github.v3+json',
                'User-Agent' => 'XBoard-Update-Checker'
            ])->timeout(15)->get(self::GITHUB_API_URL . '?per_page=50');

            if ($response->successful()) {
                $commits = $response->json();
                
                if (empty($commits) || !is_array($commits)) {
                    Log::error('Invalid GitHub response format');
                    return $this->getCachedUpdateInfo();
                }
                
                $latestCommit = $this->formatCommitHash($commits[0]['sha']);
                $currentIndex = -1;
                $updateLogs = [];
                
                // First, find the current version position in remote commit history
                foreach ($commits as $index => $commit) {
                    $shortSha = $this->formatCommitHash($commit['sha']);
                    if ($shortSha === $currentCommit) {
                        $currentIndex = $index;
                        break;
                    }
                }
                
                // Check local version status
                $isLocalNewer = false;
                if ($currentIndex === -1) {
                    // Current version not found in remote history, check local commits
                    foreach ($localLogs as $localCommit) {
                        $localHash = $this->formatCommitHash($localCommit['hash']);
                        // If latest remote commit found, local is not newer
                        if ($localHash === $latestCommit) {
                            $isLocalNewer = false;
                            break;
                        }
                        // Record additional local commits
                        $updateLogs[] = [
                            'version' => $localHash,
                            'message' => $localCommit['message'],
                            'author' => $localCommit['author'],
                            'date' => $localCommit['date'],
                            'is_local' => true
                        ];
                        $isLocalNewer = true;
                    }
                }
                
                // If local is not newer, collect commits that need to be updated
                if (!$isLocalNewer && $currentIndex > 0) {
                    $updateLogs = [];
                    // Collect all commits between current version and latest version
                    for ($i = 0; $i < $currentIndex; $i++) {
                        $commit = $commits[$i];
                        $updateLogs[] = [
                            'version' => $this->formatCommitHash($commit['sha']),
                            'message' => $commit['commit']['message'],
                            'author' => $commit['commit']['author']['name'],
                            'date' => $commit['commit']['author']['date'],
                            'is_local' => false
                        ];
                    }
                }

                $hasUpdate = !$isLocalNewer && $currentIndex > 0;
                
                $updateInfo = [
                    'has_update' => $hasUpdate,
                    'is_local_newer' => $isLocalNewer,
                    'latest_version' => $isLocalNewer ? $currentCommit : $latestCommit,
                    'current_version' => $currentCommit,
                    'update_logs' => $updateLogs,
                    'download_url' => $commits[0]['html_url'] ?? '',
                    'published_at' => $commits[0]['commit']['author']['date'] ?? '',
                    'author' => $commits[0]['commit']['author']['name'] ?? '',
                ];

                // Cache check results
                $this->setLastCheckTime();
                Cache::put(self::CACHE_UPDATE_INFO, $updateInfo, now()->addHours(24));

                return $updateInfo;
            }
            
            return $this->getCachedUpdateInfo();
        } catch (\Exception $e) {
            Log::error('Update check failed: ' . $e->getMessage());
            return $this->getCachedUpdateInfo();
        }
    }

    public function logPath(): string
    {
        return storage_path('logs/self-update.log');
    }

    /**
     * 校验后在后台启动 xboard:self-update，避免更新过程被 HTTP 超时打断
     */
    public function startUpdate(): array
    {
        $updateInfo = $this->checkForUpdates(true);
        if (!empty($updateInfo['is_local_newer'])) {
            return ['success' => false, 'message' => __('update.local_newer')];
        }
        if (empty($updateInfo['has_update'])) {
            return ['success' => false, 'message' => __('update.already_latest')];
        }
        if (!Cache::add(self::CACHE_UPDATE_LOCK, true, now()->addSeconds(self::LOCK_SECONDS))) {
            return ['success' => false, 'message' => __('update.process_running')];
        }

        Cache::forget(self::CACHE_UPDATE_RESULT);
        File::put($this->logPath(), sprintf("[%s] 开始更新 %s -> %s\n", date('H:i:s'), $updateInfo['current_version'], $updateInfo['latest_version']));

        $result = Process::path(base_path())->run(sprintf(
            'nohup %s artisan xboard:self-update < /dev/null >> %s 2>&1 &',
            escapeshellarg($this->phpBinary()),
            escapeshellarg($this->logPath())
        ));
        if (!$result->successful()) {
            Cache::forget(self::CACHE_UPDATE_LOCK);
            return ['success' => false, 'message' => __('update.failed', ['error' => $result->errorOutput()])];
        }

        return [
            'success' => true,
            'message' => '更新已开始',
            'from_version' => $updateInfo['current_version'],
            'to_version' => $updateInfo['latest_version'],
        ];
    }

    public function getUpdateStatus(): array
    {
        $log = '';
        $path = $this->logPath();
        if (is_file($path)) {
            $size = filesize($path);
            $fp = fopen($path, 'r');
            if ($size > 32768) {
                fseek($fp, -32768, SEEK_END);
            }
            $log = (string) stream_get_contents($fp);
            fclose($fp);
        }

        $lock = Cache::get(self::CACHE_UPDATE_LOCK);
        if (is_int($lock) && function_exists('posix_kill') && !posix_kill($lock, 0) && posix_get_last_error() !== 1) {
            $this->saveResult(false, '更新进程意外退出，请查看日志或 SSH 处理');
            $lock = null;
        }

        return [
            'running' => (bool) $lock,
            'result' => Cache::get(self::CACHE_UPDATE_RESULT),
            'current_version' => $this->getCurrentCommit(),
            'log' => $log,
        ];
    }

    /**
     * 由 xboard:self-update 调用，调用方须已持有更新锁
     */
    public function runUpdate(callable $output): array
    {
        $step = function (string $msg) use ($output) {
            $output(sprintf('[%s] %s', date('H:i:s'), $msg));
        };

        $from = $this->getCurrentCommit();
        $rollbackPoint = null;
        $head = Process::path(base_path())->run('git rev-parse HEAD');
        if ($head->successful()) {
            $rollbackPoint = trim($head->output());
        }

        try {
            Process::run(sprintf('git config --global --add safe.directory %s', escapeshellarg(base_path())));

            $artisan = escapeshellarg($this->phpBinary()) . ' artisan ';

            $step('备份数据库...');
            $this->runStep($artisan . 'backup:database', 900, $output);

            $step('拉取最新代码...');
            $this->runStep('git fetch origin master', 120, $output);
            $this->runStep('git reset --hard origin/master', 60, $output);

            $step('安装依赖...');
            $this->runStep($this->composerCommand() . ' install --optimize-autoloader --no-interaction', 900, $output);

            // 代码和 vendor 已替换，当前进程不能再加载新类，收尾交给新进程
            $this->runStep($artisan . 'xboard:self-update --finish --from=' . escapeshellarg($from), 1200, $output);
            return ['success' => true];
        } catch (\Throwable $e) {
            $step('更新失败: ' . $e->getMessage());

            $rollbackMsg = '';
            if ($rollbackPoint) {
                try {
                    $step('回滚到 ' . substr($rollbackPoint, 0, 7) . '...');
                    $this->runStep(sprintf('git reset --hard %s', escapeshellarg($rollbackPoint)), 60, $output);
                    $this->runStep($this->composerCommand() . ' install --optimize-autoloader --no-interaction', 900, $output);
                    $this->updateVersionCache();
                    $rollbackMsg = '（已自动回滚到 ' . substr($rollbackPoint, 0, 7) . '）';
                } catch (\Throwable $rollbackErr) {
                    $rollbackMsg = '（回滚也失败: ' . $rollbackErr->getMessage() . '，请 SSH 处理）';
                }
            }

            $message = '更新失败: ' . $e->getMessage() . $rollbackMsg;
            $step($message);
            return $this->saveResult(false, $message);
        }
    }

    /**
     * 新代码进程中执行：迁移、清缓存、重载服务并记录结果
     */
    public function finishUpdate(callable $output, string $from): array
    {
        $step = function (string $msg) use ($output) {
            $output(sprintf('[%s] %s', date('H:i:s'), $msg));
        };
        $artisan = escapeshellarg($this->phpBinary()) . ' artisan ';

        $step('迁移数据库并刷新插件/主题...');
        $this->runStep($artisan . 'xboard:update', 600, $output);

        $step('清理缓存...');
        foreach (['config:clear', 'view:clear', 'route:clear'] as $cmd) {
            $this->runStep($artisan . $cmd, 120, $output);
        }

        $this->createUpdateFlag();
        $this->restartOctane();

        $message = __('update.success', ['from' => $from, 'to' => $this->getCurrentCommit()]);
        $step($message);
        return $this->saveResult(true, $message);
    }

    protected function saveResult(bool $success, string $message): array
    {
        $result = ['success' => $success, 'message' => $message, 'finished_at' => now()->timestamp];
        Cache::forget(self::CACHE_UPDATE_INFO);
        Cache::put(self::CACHE_UPDATE_RESULT, $result, now()->addDay());
        Cache::forget(self::CACHE_UPDATE_LOCK);
        return $result;
    }

    protected function runStep(string $command, int $timeout, callable $output): void
    {
        $result = Process::path(base_path())
            ->timeout($timeout)
            ->env(['COMPOSER_ALLOW_SUPERUSER' => '1', 'COMPOSER_HOME' => getenv('COMPOSER_HOME') ?: storage_path('composer')])
            ->run($command, function (string $type, string $buffer) use ($output) {
                foreach (preg_split('/\r?\n/', rtrim($buffer)) as $line) {
                    if ($line !== '') {
                        $output('  ' . $line);
                    }
                }
            });
        if (!$result->successful()) {
            throw new \RuntimeException(sprintf('%s 退出码 %d', $command, $result->exitCode()));
        }
    }

    protected function phpBinary(): string
    {
        $bin = PHP_BINARY;
        if ($bin === '' || str_contains(basename($bin), 'fpm')) {
            $cli = PHP_BINDIR . '/php';
            return is_executable($cli) ? $cli : 'php';
        }
        return $bin;
    }

    protected function composerCommand(): string
    {
        $phar = base_path('composer.phar');
        return is_file($phar)
            ? escapeshellarg($this->phpBinary()) . ' ' . escapeshellarg($phar)
            : 'composer';
    }

    protected function getCurrentCommit(): string
    {
        try {
            // Ensure git configuration is correct
            Process::run(sprintf('git config --global --add safe.directory %s', base_path()));
            $result = Process::run('git rev-parse HEAD');
            $fullHash = trim($result->output());
            return $fullHash ? $this->formatCommitHash($fullHash) : 'unknown';
        } catch (\Exception $e) {
            Log::error('Failed to get current commit: ' . $e->getMessage());
            return 'unknown';
        }
    }

    protected function getFirstCommit(): string
    {
        try {
            // Get first commit hash
            $result = Process::run('git rev-list --max-parents=0 HEAD');
            $fullHash = trim($result->output());
            return $fullHash ? $this->formatCommitHash($fullHash) : 'unknown';
        } catch (\Exception $e) {
            Log::error('Failed to get first commit: ' . $e->getMessage());
            return 'unknown';
        }
    }

    protected function formatCommitHash(string $hash): string
    {
        // Use 7 characters for commit hash
        return substr($hash, 0, 7);
    }

    protected function createUpdateFlag(): void
    {
        try {
            // Create update flag file for external script to detect and restart container
            $flagFile = storage_path('update_pending');
            File::put($flagFile, date('Y-m-d H:i:s'));
        } catch (\Exception $e) {
            Log::error('Failed to create update flag: ' . $e->getMessage());
            throw new \Exception(__('update.flag_create_failed', ['error' => $e->getMessage()]));
        }
    }

    protected function restartOctane(): void
    {
        try {
            if (!config('octane.server')) {
                return;
            }

            // Check Octane running status
            $statusResult = Process::run('php artisan octane:status');
            if (!$statusResult->successful()) {
                Log::info('Octane is not running, skipping restart.');
                return;
            }

            $output = $statusResult->output();
            if (str_contains($output, 'Octane server is running')) {
                Log::info('Reloading Octane server after update...');
                $this->updateVersionCache();
                // reload 是无缝热重启（worker 平滑替换），比 stop 更稳
                Process::run('php artisan octane:reload');
                Log::info('Octane server reloaded successfully.');
            } else {
                Log::info('Octane is not running, skipping restart.');
            }
        } catch (\Exception $e) {
            Log::error('Failed to restart Octane server: ' . $e->getMessage());
            // Non-fatal error, don't throw exception
        }
    }

    public function getLastCheckTime()
    {
        return Cache::get(self::CACHE_LAST_CHECK, null);
    }

    protected function setLastCheckTime(): void
    {
        Cache::put(self::CACHE_LAST_CHECK, now()->timestamp, now()->addDays(30));
    }

    public function getCachedUpdateInfo(): array
    {
        return Cache::get(self::CACHE_UPDATE_INFO, [
            'has_update' => false,
            'latest_version' => $this->getCurrentCommit(),
            'current_version' => $this->getCurrentCommit(),
            'update_logs' => [],
            'download_url' => '',
            'published_at' => '',
            'author' => '',
        ]);
    }

    protected function getLocalGitLogs(int $limit = 50): array
    {
        try {
            // 获取本地git log
            $result = Process::run(
                sprintf('git log -%d --pretty=format:"%%H||%%s||%%an||%%ai"', $limit)
            );

            if (!$result->successful()) {
                return [];
            }

            $logs = [];
            $lines = explode("\n", trim($result->output()));
            foreach ($lines as $line) {
                $parts = explode('||', $line);
                if (count($parts) === 4) {
                    $logs[] = [
                        'hash' => $parts[0],
                        'message' => $parts[1],
                        'author' => $parts[2],
                        'date' => $parts[3]
                    ];
                }
            }
            return $logs;
        } catch (\Exception $e) {
            Log::error('Failed to get local git logs: ' . $e->getMessage());
            return [];
        }
    }
} 
