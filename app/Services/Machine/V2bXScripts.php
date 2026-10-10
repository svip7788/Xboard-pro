<?php

namespace App\Services\Machine;

/**
 * 远程 V2bX 管理命令（兼容 systemd / Alpine OpenRC，非 root 自动 sudo）
 */
class V2bXScripts
{
    private const DETECT = <<<'SH'
if [ "$(id -u)" -ne 0 ]; then S=sudo; else S=''; fi
if [ -f /etc/alpine-release ]; then M=alpine; elif command -v systemctl >/dev/null 2>&1; then M=systemd; else M=unknown; fi
SH;

    public const STATUS_PROBE = self::DETECT . "\n" . <<<'SH'
if [ "$M" = alpine ]; then
  $S service V2bX status 2>/dev/null | grep -q started && ST=active || ST=inactive
elif [ "$M" = systemd ]; then
  ST=$($S systemctl is-active V2bX 2>/dev/null || true); [ -n "$ST" ] || ST=inactive
else
  ST=unknown
fi
[ -x /usr/local/V2bX/V2bX ] && IN=yes || IN=no
VER=$(sed -n '1p' /etc/V2bX/.panel_version 2>/dev/null || sed -n '1p' /usr/local/V2bX/.version 2>/dev/null || true)
echo "__STATUS__:$ST"
echo "__INSTALLED__:$IN"
echo "__VERSION__:$VER"
SH;

    public const RESTART_AND_WAIT = self::DETECT . "\n" . <<<'SH'
if [ "$M" = alpine ]; then
  $S service V2bX restart >/dev/null 2>&1 || $S service V2bX start >/dev/null 2>&1 || true
  for i in $(seq 1 15); do $S service V2bX status 2>/dev/null | grep -q started && break; sleep 2; done
  $S service V2bX status 2>/dev/null | grep -q started && echo active || echo inactive
elif [ "$M" = systemd ]; then
  $S systemctl restart V2bX 2>/dev/null || true
  fail=0
  for i in $(seq 1 12); do
    sleep 2
    if [ "$($S systemctl is-active V2bX 2>/dev/null)" = active ]; then
      sleep 2
      [ "$($S systemctl is-active V2bX 2>/dev/null)" = active ] && echo active && exit 0
    fi
    fail=$((fail+1)); [ "$fail" -ge 3 ] && echo inactive && exit 0
  done
  echo inactive
else
  echo unsupported
fi
SH;

    public const STATUS_DETAIL = self::DETECT . "\n" . <<<'SH'
if [ "$M" = alpine ]; then $S service V2bX status 2>&1 || true
elif [ "$M" = systemd ]; then $S systemctl status V2bX --no-pager -l 2>&1 | sed -n '1,20p'
else echo '无法识别 V2bX 服务管理器'; fi
SH;

    public const RECENT_LOGS = self::DETECT . "\n" . <<<'SH'
if [ "$M" = alpine ]; then $S service V2bX status 2>&1 || true
elif command -v journalctl >/dev/null 2>&1; then $S journalctl -u V2bX --no-pager -n 100 --output cat 2>/dev/null
else echo '无法获取服务日志'; fi
SH;

    public const ENSURE_CONFIG_DIR = <<<'SH'
if [ "$(id -u)" -ne 0 ]; then S=sudo; else S=''; fi
$S mkdir -p /etc/V2bX/cert
for f in geoip.dat geosite.dat; do
  [ -f "/etc/V2bX/$f" ] && continue
  if [ -f "/usr/local/V2bX/$f" ]; then $S cp "/usr/local/V2bX/$f" /etc/V2bX/; continue; fi
  if [ "$f" = geoip.dat ]; then u=https://github.com/v2fly/geoip/releases/latest/download/geoip.dat
  else u=https://github.com/v2fly/domain-list-community/releases/latest/download/geosite.dat; fi
  curl -fsSL --connect-timeout 8 -o "/tmp/$f" "$u" 2>/dev/null || curl -fsSL --connect-timeout 15 -o "/tmp/$f" "https://ghfast.top/$u" 2>/dev/null || true
  [ -s "/tmp/$f" ] && $S mv "/tmp/$f" "/etc/V2bX/$f" || rm -f "/tmp/$f"
done
SH;

    public const BACKUP_CONFIG = <<<'SH'
if [ "$(id -u)" -ne 0 ]; then S=sudo; else S=''; fi
if [ -f /etc/V2bX/config.json ]; then
  D="/etc/V2bX/backup/$(date +%Y%m%d_%H%M%S)"
  $S mkdir -p "$D" && $S cp /etc/V2bX/*.json "$D/" 2>/dev/null
  ls -1d /etc/V2bX/backup/* 2>/dev/null | sort -r | tail -n +6 | while read -r old; do $S rm -rf "$old"; done
  echo "BACKED_UP:$D"
fi
SH;

    /** 清理 V2bX-Panel 的旧心跳 cron */
    public const REMOVE_LEGACY_AGENT = <<<'SH'
if [ "$(id -u)" -ne 0 ]; then S=sudo; else S=''; fi
if command -v crontab >/dev/null 2>&1 && $S crontab -l 2>/dev/null | grep -q '/etc/V2bX/heartbeat.sh'; then
  $S crontab -l 2>/dev/null | grep -v '/etc/V2bX/heartbeat.sh' | $S crontab -
fi
$S rm -f /etc/V2bX/heartbeat.sh /etc/V2bX/agent.env
SH;

    public static function install(string $repo, string $version): string
    {
        return strtr(self::resource('install.sh'), ['__REPO__' => $repo, '__VERSION__' => $version]);
    }

    public static function bbr(): string
    {
        return self::resource('bbr.sh');
    }

    public static function resource(string $name): string
    {
        return (string) file_get_contents(resource_path('v2bx/' . $name));
    }

    /**
     * @return array{status:string,installed:bool,version:string}
     */
    public static function parseProbe(string $output): array
    {
        $result = ['status' => 'unknown', 'installed' => false, 'version' => ''];
        foreach (preg_split('/\r?\n/', $output) as $line) {
            $line = trim($line);
            if (str_starts_with($line, '__STATUS__:')) {
                $result['status'] = strtolower(trim(substr($line, 11))) ?: 'unknown';
            } elseif (str_starts_with($line, '__INSTALLED__:')) {
                $result['installed'] = trim(substr($line, 14)) === 'yes';
            } elseif (str_starts_with($line, '__VERSION__:')) {
                $result['version'] = trim(substr($line, 12));
            }
        }
        return $result;
    }

    public static function lastLine(string $output): string
    {
        $lines = array_values(array_filter(array_map('trim', preg_split('/\r?\n/', $output)), 'strlen'));
        return $lines ? end($lines) : '';
    }
}
