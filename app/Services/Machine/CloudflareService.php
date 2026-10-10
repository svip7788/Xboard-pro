<?php

namespace App\Services\Machine;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Http;

/**
 * Cloudflare DNS：支持 API Token（Bearer）与 Global API Key（37 位 hex + 邮箱）
 */
class CloudflareService
{
    private const API = 'https://api.cloudflare.com/client/v4';

    public function __construct(private readonly string $secret, private readonly string $email = '')
    {
        if ($secret === '') {
            throw new \RuntimeException('尚未配置 Cloudflare 凭据');
        }
        if (self::isGlobalKey($secret) && $email === '') {
            throw new \RuntimeException('使用 Global API Key 时必须同时配置邮箱');
        }
    }

    public static function fromSettings(): self
    {
        $cf = V2bXSettings::cloudflare();
        return new self($cf['token'], $cf['email']);
    }

    public static function isGlobalKey(string $secret): bool
    {
        return (bool) preg_match('/^[a-f0-9]{37}$/i', $secret);
    }

    public function verify(): int
    {
        return (int) data_get($this->request('GET', '/zones', ['per_page' => 1]), 'result_info.total_count', 0);
    }

    /**
     * target 为 IPv4 → A，IPv6 → AAAA，否则 → CNAME；自动清理同名冲突类型
     *
     * @return array{action:string,type:string,zone:string,record:string,target:string}
     */
    public function upsert(string $domain, string $target, bool $proxied = false): array
    {
        $domain = strtolower(rtrim(trim($domain), '.'));
        $target = rtrim(trim($target), '.');
        if ($domain === '' || $target === '') {
            throw new \RuntimeException('域名或目标为空');
        }
        $type = match (true) {
            (bool) filter_var($target, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) => 'A',
            (bool) filter_var($target, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) => 'AAAA',
            default => 'CNAME',
        };
        if ($type === 'CNAME' && !str_contains($target, '.')) {
            throw new \RuntimeException("目标 {$target} 既不是 IP 也不像域名");
        }

        [$zoneId, $zoneName] = $this->findZone($domain);
        $records = collect(data_get($this->request('GET', "/zones/{$zoneId}/dns_records", ['name' => $domain, 'per_page' => 50]), 'result', []))
            ->whereIn('type', ['A', 'AAAA', 'CNAME']);
        $same = $records->where('type', $type)->values();
        $diff = $records->where('type', '!=', $type)->values();

        foreach ($diff as $record) {
            $this->request('DELETE', "/zones/{$zoneId}/dns_records/{$record['id']}");
        }

        $payload = ['type' => $type, 'name' => $domain, 'content' => $target, 'ttl' => $proxied ? 1 : 60, 'proxied' => $proxied];
        $result = ['type' => $type, 'zone' => $zoneName, 'record' => $domain, 'target' => $target];

        if ($primary = $same->shift()) {
            foreach ($same as $extra) {
                $this->request('DELETE', "/zones/{$zoneId}/dns_records/{$extra['id']}");
            }
            if (rtrim($primary['content'] ?? '', '.') === $target && ($primary['proxied'] ?? false) === $proxied && $diff->isEmpty()) {
                return ['action' => 'noop'] + $result;
            }
            $this->request('PUT', "/zones/{$zoneId}/dns_records/{$primary['id']}", $payload);
            return ['action' => 'updated'] + $result;
        }

        $this->request('POST', "/zones/{$zoneId}/dns_records", $payload);
        return ['action' => 'created'] + $result;
    }

    private function findZone(string $domain): array
    {
        $parts = explode('.', $domain);
        for ($i = 0; $i < count($parts) - 1; $i++) {
            $candidate = implode('.', array_slice($parts, $i));
            $zone = data_get($this->request('GET', '/zones', ['name' => $candidate]), 'result.0');
            if ($zone) {
                return [$zone['id'], $zone['name']];
            }
        }
        throw new \RuntimeException("Cloudflare 账号下找不到包含 {$domain} 的域名（是否托管在 CF？）");
    }

    private function request(string $method, string $path, array $data = []): array
    {
        $response = $this->client()->send($method, self::API . $path, $method === 'GET' ? ['query' => $data] : ['json' => $data]);
        $body = $response->json();
        if (!is_array($body)) {
            throw new \RuntimeException("Cloudflare API 非 JSON 响应 ({$response->status()})");
        }
        if (empty($body['success'])) {
            $msg = collect($body['errors'] ?? [])->map(fn($e) => ($e['code'] ?? '') . ': ' . ($e['message'] ?? ''))->implode('; ');
            throw new \RuntimeException('Cloudflare: ' . ($msg ?: "HTTP {$response->status()}"));
        }
        return $body;
    }

    private function client(): PendingRequest
    {
        $headers = self::isGlobalKey($this->secret)
            ? ['X-Auth-Email' => $this->email, 'X-Auth-Key' => $this->secret]
            : ['Authorization' => "Bearer {$this->secret}"];
        return Http::withHeaders($headers)->acceptJson()->timeout(15);
    }
}
