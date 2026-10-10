<?php

namespace App\Services\Machine;

use Illuminate\Contracts\Encryption\DecryptException;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Crypt;
use Illuminate\Support\Facades\Http;

/**
 * V2bX 机器管理配置（存于 admin_setting，前缀 v2bx_）
 */
class V2bXSettings
{
    public static function repo(): string
    {
        return trim((string) admin_setting('v2bx_repo', '')) ?: 'svip7788/V2bX';
    }

    public static function version(): string
    {
        return trim((string) admin_setting('v2bx_version', '')) ?: 'latest';
    }

    public static function panelUrl(): string
    {
        return rtrim(trim((string) admin_setting('app_url', '')), '/');
    }

    public static function cloudflare(): array
    {
        $raw = (string) admin_setting('v2bx_cf_token', '');
        $token = '';
        if ($raw !== '') {
            try {
                $token = Crypt::decryptString($raw);
            } catch (DecryptException) {
            }
        }
        return ['token' => $token, 'email' => (string) admin_setting('v2bx_cf_email', '')];
    }

    public static function toArray(): array
    {
        $cf = self::cloudflare();
        return [
            'repo' => self::repo(),
            'version' => self::version(),
            'panel_url' => self::panelUrl(),
            'cf_configured' => $cf['token'] !== '',
            'cf_token_preview' => $cf['token'] ? substr($cf['token'], 0, 4) . '***' . substr($cf['token'], -4) : '',
            'cf_email' => $cf['email'],
        ];
    }

    public static function save(array $data): void
    {
        $save = [];
        foreach (['repo', 'version'] as $key) {
            if (array_key_exists($key, $data)) {
                $save["v2bx_{$key}"] = trim((string) $data[$key]);
            }
        }
        if ($save) {
            admin_setting($save);
        }
    }

    public static function saveCloudflare(string $token, string $email): void
    {
        admin_setting([
            'v2bx_cf_token' => $token === '' ? '' : Crypt::encryptString($token),
            'v2bx_cf_email' => $token === '' ? '' : $email,
        ]);
    }

    /**
     * 解析目标版本（latest → GitHub 最新 release tag）
     */
    public static function resolveVersion(?string $version = null): string
    {
        $version = $version ?: self::version();
        if ($version !== 'latest') {
            return $version;
        }
        $repo = self::repo();
        return Cache::remember("v2bx_latest_version:{$repo}", 600, function () use ($repo) {
            $headers = ['Accept' => 'application/vnd.github+json', 'User-Agent' => 'Xboard'];
            try {
                $tag = Http::withHeaders($headers)->timeout(15)
                    ->get("https://api.github.com/repos/{$repo}/releases/latest")->json('tag_name');
                if ($tag) {
                    return $tag;
                }
            } catch (\Throwable) {
            }
            $location = Http::withHeaders($headers)->timeout(15)->withoutRedirecting()
                ->get("https://github.com/{$repo}/releases/latest")->header('Location');
            if ($location && preg_match('#/tag/([^/?]+)#', $location, $m)) {
                return $m[1];
            }
            throw new \RuntimeException('获取 V2bX 最新版本失败，请检查网络或在设置中指定版本');
        });
    }

    /**
     * 首装写入的 config.json（机器模式）
     */
    public static function machineConfig(int $machineId, string $token, string $core = 'auto'): string
    {
        return json_encode([
            'Log' => ['Level' => 'error', 'Output' => ''],
            'Machine' => [
                'ApiHost' => self::panelUrl(),
                'MachineID' => $machineId,
                'Token' => $token,
                'Core' => $core,
                'Timeout' => 30,
            ],
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) . "\n";
    }
}
