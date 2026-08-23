<?php

namespace Plugin\BaitSplit\Services;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * 外部换 IP 服务客户端。
 *
 * 换 IP 是异步的：POST 立即返回受理结果，新地址要先通过国内验墙才会上线，
 * 通常几十秒到几分钟。上线后对方会推 ip-rotate 通知，这里的查询接口只作为
 * 通知丢失时的兜底轮询。
 */
class IpRotationClient
{
    /**
     * 这些调用发生在持有状态锁的墙处理路径上，超时必须短。
     * 请求没打出去不要紧，分钟级维护会重试。
     */
    public function __construct(
        private readonly string $baseUrl,
        private readonly string $token,
        private readonly int $timeout = 8
    ) {
    }

    public static function fromConfig(array $config): ?self
    {
        // 旧键是追猎时代留下的，线上存量配置还在用，读取时一并认
        $baseUrl = rtrim(trim((string) (
            $config['ip_rotate_api'] ?? $config['hunt_rotate_api'] ?? ''
        )), '/');
        $token = trim((string) (
            $config['ip_rotate_token'] ?? $config['hunt_rotate_token'] ?? ''
        ));
        if ($baseUrl === '' || $token === '') {
            return null;
        }
        return new self($baseUrl, $token);
    }

    /**
     * 请求给指定目标换一个全新 IP。返回是否已被受理。
     */
    public function requestReplace(string $targetId): bool
    {
        if (trim($targetId) === '') {
            return false;
        }
        try {
            $response = $this->request()->post(
                $this->baseUrl . '/api/external/replace-ip',
                ['target_id' => $targetId]
            );
        } catch (\Throwable $exception) {
            Log::warning('BaitSplit 换 IP 请求失败', [
                'target_id' => $targetId,
                'error' => $exception->getMessage(),
            ]);
            return false;
        }
        if (!$response->successful()) {
            Log::warning('BaitSplit 换 IP 请求被拒绝', [
                'target_id' => $targetId,
                'status' => $response->status(),
                'body' => mb_substr($response->body(), 0, 300),
            ]);
            return false;
        }
        Log::notice('BaitSplit 已请求换 IP', [
            'target_id' => $targetId,
            'body' => mb_substr($response->body(), 0, 300),
        ]);
        return true;
    }

    /**
     * 查询目标当前状态。online 为 true 表示已验墙通过并上线解析。
     *
     * @return array{ip:string,online:bool,status:string}|null
     */
    public function instance(string $targetId): ?array
    {
        if (trim($targetId) === '') {
            return null;
        }
        try {
            $response = $this->request()->get(
                $this->baseUrl . '/api/external/instance',
                ['target_id' => $targetId]
            );
        } catch (\Throwable $exception) {
            Log::warning('BaitSplit 查询换 IP 状态失败', [
                'target_id' => $targetId,
                'error' => $exception->getMessage(),
            ]);
            return null;
        }
        if (!$response->successful()) {
            return null;
        }
        $data = $response->json();
        if (!is_array($data)) {
            return null;
        }
        $ip = trim((string) ($data['ip'] ?? ''));
        if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) === false) {
            return null;
        }
        return [
            'ip' => $ip,
            'online' => (bool) ($data['online'] ?? false),
            'status' => (string) ($data['status'] ?? ''),
        ];
    }

    private function request(): \Illuminate\Http\Client\PendingRequest
    {
        return Http::withToken($this->token)
            ->acceptJson()
            ->asJson()
            ->timeout($this->timeout)
            ->connectTimeout(min(4, $this->timeout));
    }
}
