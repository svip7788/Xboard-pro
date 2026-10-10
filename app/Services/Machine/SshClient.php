<?php

namespace App\Services\Machine;

use Illuminate\Support\Str;
use phpseclib3\Crypt\PublicKeyLoader;
use phpseclib3\Net\SFTP;

class SshClient
{
    private ?SFTP $conn = null;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly string $user,
        private readonly ?string $password,
        private readonly ?string $key,
    ) {
    }

    public function connect(int $timeout = 15): self
    {
        if ($this->conn?->isConnected()) {
            return $this;
        }

        $conn = new SFTP($this->host, $this->port, $timeout);
        $credentials = [];
        if ($this->key) {
            try {
                $credentials[] = PublicKeyLoader::load($this->key, $this->password ?: false);
            } catch (\Throwable) {
                throw new SshException('私钥格式不支持或口令错误');
            }
        }
        if ($this->password) {
            $credentials[] = $this->password;
        }
        if (!$credentials) {
            throw new SshException('未配置 SSH 密码或私钥');
        }

        try {
            $ok = $conn->login($this->user, ...$credentials);
        } catch (\Throwable $e) {
            throw new SshException("无法连接 {$this->host}:{$this->port}：" . $e->getMessage());
        }
        if (!$ok) {
            throw new SshException('SSH 认证失败，请检查用户名、密码或私钥');
        }

        $this->conn = $conn;
        return $this;
    }

    /**
     * @return array{0:int,1:string} [exit code, stdout+stderr]
     */
    public function exec(string $command, int $timeout = 300): array
    {
        $conn = $this->connect()->conn;
        $conn->setTimeout($timeout);
        $output = (string) $conn->exec(self::wrap($command));
        if ($conn->isTimeout()) {
            return [-1, $output . "\n>>> 命令执行超时\n"];
        }
        return [$this->exitStatus(), $output];
    }

    public function execChecked(string $command, int $timeout = 300): string
    {
        [$code, $output] = $this->exec($command, $timeout);
        if ($code !== 0) {
            throw new SshException("远程命令失败 (exit code: {$code})\n" . trim($output));
        }
        return $output;
    }

    /**
     * 流式执行，输出分片回调
     */
    public function stream(string $command, callable $onOutput, int $timeout = 600): int
    {
        $conn = $this->connect()->conn;
        $conn->setTimeout($timeout);
        $conn->exec(self::wrap($command), function ($chunk) use ($onOutput) {
            $onOutput((string) $chunk);
        });
        if ($conn->isTimeout()) {
            $onOutput("\n>>> 命令执行超时，强制终止\n");
            return -1;
        }
        return $this->exitStatus();
    }

    private static function wrap(string $command): string
    {
        return 'sh -c ' . escapeshellarg($command);
    }

    private function exitStatus(): int
    {
        $status = $this->conn?->getExitStatus();
        return $status === false || $status === null ? -1 : (int) $status;
    }

    /**
     * 上传脚本到 /tmp 执行后删除
     */
    public function runScript(string $script, callable $onOutput, int $timeout = 600): int
    {
        $tmp = '/tmp/.xb_' . Str::random(10) . '.sh';
        $this->upload($tmp, $script, 0700);
        return $this->stream(sprintf('sh %1$s; c=$?; rm -f %1$s; exit $c', $tmp), $onOutput, $timeout);
    }

    /**
     * 写远程文件；SFTP 无权限时写 /tmp 再 sudo mv（兼容非 root 用户）
     */
    public function writeFile(string $path, string $content, int $mode = 0644): void
    {
        $conn = $this->connect()->conn;
        if ($conn->put($path, $content)) {
            $conn->chmod($mode, $path);
            return;
        }

        $tmp = '/tmp/.xb_upload_' . Str::random(10);
        $this->upload($tmp, $content, 0600);
        $this->execChecked(sprintf(
            'sudo mkdir -p %s && sudo mv %s %s && sudo chmod %o %s',
            escapeshellarg(dirname($path)),
            escapeshellarg($tmp),
            escapeshellarg($path),
            $mode,
            escapeshellarg($path)
        ), 30);
    }

    public function disconnect(): void
    {
        $this->conn?->disconnect();
        $this->conn = null;
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    private function upload(string $path, string $content, int $mode): void
    {
        $conn = $this->connect()->conn;
        if (!$conn->put($path, $content)) {
            throw new SshException("上传文件失败：{$path}");
        }
        $conn->chmod($mode, $path);
    }
}
