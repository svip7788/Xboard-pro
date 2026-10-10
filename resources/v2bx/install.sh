#!/bin/sh
# V2bX 安装/升级：临时目录解压，失败自动回滚。占位符由面板替换：__REPO__ __VERSION__
set -eu

if [ "$(id -u)" -ne 0 ]; then
    SUDO="sudo"
    sudo -n true 2>/dev/null || { echo ">>> 错误: 当前用户无 sudo 免密权限，请配置 sudoers 或使用 root 登录"; exit 1; }
else
    SUDO=""
fi

REPO="__REPO__"
V2BX_VERSION="__VERSION__"

log() {
    echo ">>> $1"
}

detect_release() {
    if [ -f /etc/redhat-release ]; then
        RELEASE="centos"
    elif grep -Eqi "alpine" /etc/issue 2>/dev/null || grep -Eqi "alpine" /proc/version 2>/dev/null || [ -f /etc/alpine-release ]; then
        RELEASE="alpine"
    elif grep -Eqi "debian" /etc/issue 2>/dev/null || grep -Eqi "debian" /proc/version 2>/dev/null; then
        RELEASE="debian"
    elif grep -Eqi "ubuntu" /etc/issue 2>/dev/null || grep -Eqi "ubuntu" /proc/version 2>/dev/null; then
        RELEASE="ubuntu"
    elif grep -Eqi "centos|red hat|redhat|rocky|alma|oracle linux" /etc/issue 2>/dev/null || grep -Eqi "centos|red hat|redhat|rocky|alma|oracle linux" /proc/version 2>/dev/null; then
        RELEASE="centos"
    elif grep -Eqi "arch" /proc/version 2>/dev/null; then
        RELEASE="arch"
    else
        RELEASE="unknown"
    fi
}

ensure_base_tools() {
    need_install=0
    for cmd in unzip tar file curl; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            need_install=1
            break
        fi
    done
    if [ "$need_install" -eq 0 ]; then
        log "基础依赖已存在"
        return 0
    fi

    log "安装基础依赖..."
    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        $SUDO apt-get update -y >/dev/null 2>&1 || true
        $SUDO apt-get install -y -q wget curl unzip tar file ca-certificates socat cron >/dev/null 2>&1
    elif command -v dnf >/dev/null 2>&1; then
        $SUDO dnf install -y wget curl unzip tar file ca-certificates socat cronie >/dev/null 2>&1
    elif command -v yum >/dev/null 2>&1; then
        $SUDO yum install -y wget curl unzip tar file ca-certificates socat cronie >/dev/null 2>&1
    elif command -v apk >/dev/null 2>&1; then
        $SUDO apk add --no-cache wget curl unzip tar file ca-certificates socat >/dev/null 2>&1
    elif command -v pacman >/dev/null 2>&1; then
        $SUDO pacman -Sy --noconfirm --needed wget curl unzip tar file ca-certificates socat cronie >/dev/null 2>&1
    else
        echo ">>> 错误: 未识别的包管理器，且缺少安装依赖所需命令"
        exit 1
    fi

    if command -v update-ca-certificates >/dev/null 2>&1; then
        $SUDO update-ca-certificates >/dev/null 2>&1 || true
    elif command -v update-ca-trust >/dev/null 2>&1; then
        $SUDO update-ca-trust force-enable >/dev/null 2>&1 || true
    fi

    for cmd in unzip tar file curl; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            echo ">>> 错误: 缺少依赖 $cmd"
            exit 1
        fi
    done
}

download_file() {
    url="$1"
    dest="$2"
    ct="${3:-8}"
    mt="${4:-$((ct * 4))}"
    curl -fsSL --retry 2 --retry-delay 2 \
        --connect-timeout "$ct" --max-time "$mt" \
        --speed-time 30 --speed-limit 10240 \
        -o "$dest" "$url" && [ -s "$dest" ]
}

download_with_mirrors() {
    original_url="$1"
    dest="$2"
    ct="${3:-8}"
    mt="${4:-$((ct * 4))}"
    gh_path=$(echo "$original_url" | sed 's|https://github.com/||')
    for mirror_url in \
        "https://github.com/$gh_path" \
        "https://ghfast.top/https://github.com/$gh_path" \
        "https://gh-proxy.com/https://github.com/$gh_path" \
        "https://ghproxy.net/https://github.com/$gh_path"; do
        log "下载: $mirror_url"
        rm -f "$dest"
        if download_file "$mirror_url" "$dest" "$ct" "$mt"; then
            return 0
        fi
        log "失败，尝试下一个镜像..."
    done
    return 1
}

get_free_kb() {
    free_kb=$(df -Pk "${1:-/}" 2>/dev/null | awk 'NR==2 {print $4}')
    echo "${free_kb:-0}"
}

get_dir_kb() {
    if [ ! -e "$1" ]; then
        echo 0
        return 0
    fi
    size_kb=$(du -sk "$1" 2>/dev/null | awk 'NR==1 {print $1}')
    echo "${size_kb:-0}"
}

deep_cleanup_space() {
    log "深度清理磁盘空间（系统日志/归档）..."
    if [ -d /var/log ]; then
        $SUDO find /var/log -type f \( -name '*.gz' -o -name '*.old' -o -name '*.[0-9]' -o -name '*.[0-9].gz' \) -exec rm -f {} \; 2>/dev/null || true
    fi
    for dir in /var/log/V2bX /usr/local/V2bX /etc/V2bX; do
        [ -d "$dir" ] || continue
        for f in "$dir"/*.log "$dir"/*/*.log; do
            [ -f "$f" ] || continue
            size=$(wc -c < "$f" 2>/dev/null || echo 0)
            [ "${size:-0}" -gt 5242880 ] && : > "$f" 2>/dev/null || true
        done
    done
    if command -v journalctl >/dev/null 2>&1; then
        $SUDO journalctl --vacuum-size=100M >/dev/null 2>&1 || true
    fi
}

cleanup_old_backups() {
    for dir in /usr/local/V2bX.backup-*; do
        [ -d "$dir" ] || continue
        log "清理旧备份: $dir"
        $SUDO rm -rf "$dir" 2>/dev/null || true
    done
}

ensure_install_space() {
    need_kb=$(( $(get_dir_kb "$STAGE_DIR") + 16384 ))
    avail_kb=$(get_free_kb /usr/local)
    if [ "${avail_kb:-0}" -lt "$need_kb" ]; then
        deep_cleanup_space
        avail_kb=$(get_free_kb /usr/local)
    fi
    if [ "${avail_kb:-0}" -lt "$need_kb" ]; then
        cleanup_old_backups
        avail_kb=$(get_free_kb /usr/local)
    fi
    if [ "${avail_kb:-0}" -lt "$need_kb" ]; then
        echo ">>> 错误: 磁盘可用空间不足，当前 ${avail_kb:-0}KB，至少需要 ${need_kb}KB"
        exit 1
    fi
    log "磁盘可用空间满足安装要求 (${avail_kb}KB)"
}

setup_service() {
    if [ "$RELEASE" = "alpine" ]; then
        log "配置 OpenRC 服务..."
        $SUDO sh -c "cat > /etc/init.d/V2bX" <<'SVCEOF'
#!/sbin/openrc-run

name="V2bX"
description="V2bX"

command="/usr/local/V2bX/V2bX"
command_args="server"
command_user="root"

pidfile="/run/V2bX.pid"
command_background="yes"

depend() {
        need net
}
SVCEOF
        $SUDO chmod +x /etc/init.d/V2bX
        $SUDO rc-update add V2bX default >/dev/null 2>&1 || true
    elif command -v systemctl >/dev/null 2>&1; then
        log "配置 systemd 服务..."
        $SUDO sh -c "cat > /etc/systemd/system/V2bX.service" <<'SVCEOF'
[Unit]
Description=V2bX Service
After=network.target nss-lookup.target
Wants=network.target

[Service]
User=root
Group=root
Type=simple
LimitAS=infinity
LimitRSS=infinity
LimitCORE=infinity
LimitNOFILE=999999
WorkingDirectory=/usr/local/V2bX/
ExecStart=/usr/local/V2bX/V2bX server
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SVCEOF
        $SUDO systemctl daemon-reload
        $SUDO systemctl enable V2bX >/dev/null 2>&1 || true
    else
        log "警告: 未检测到 systemd/OpenRC，跳过服务配置"
    fi
}

service_is_active() {
    if [ "$RELEASE" = "alpine" ]; then
        $SUDO service V2bX status >/dev/null 2>&1
    elif command -v systemctl >/dev/null 2>&1; then
        systemctl is-active --quiet V2bX 2>/dev/null
    else
        return 1
    fi
}

start_service() {
    if [ "$RELEASE" = "alpine" ]; then
        $SUDO service V2bX start >/dev/null 2>&1
    elif command -v systemctl >/dev/null 2>&1; then
        $SUDO systemctl start V2bX >/dev/null 2>&1
    else
        return 1
    fi
}

stop_old_service() {
    log "停止旧服务..."
    if [ "$RELEASE" = "alpine" ]; then
        $SUDO service V2bX stop 2>/dev/null || true
    elif command -v systemctl >/dev/null 2>&1; then
        $SUDO systemctl stop V2bX 2>/dev/null || true
        $SUDO systemctl stop v2bx 2>/dev/null || true
    fi
    sleep 2
    for attempt in 1 2 3; do
        PIDS=$(ps -eo pid=,comm= 2>/dev/null | awk '$2=="V2bX" {print $1}')
        [ -z "$PIDS" ] && break
        log "第 $attempt 次终止残留进程: $PIDS"
        $SUDO kill -9 $PIDS 2>/dev/null || true
        sleep 2
    done
    PIDS=$(ps -eo pid=,comm= 2>/dev/null | awk '$2=="V2bX" {print $1}')
    if [ -n "$PIDS" ]; then
        echo ">>> 错误: 仍有 V2bX 残留进程未退出: $PIDS"
        exit 1
    fi
}

CURRENT_DIR="/usr/local/V2bX"
BACKUP_DIR=""
STAGE_DIR=""
RELEASE="unknown"
SERVICE_WAS_ACTIVE=0

on_exit() {
    exit_code=$?
    trap - EXIT HUP INT TERM
    if [ "$exit_code" -ne 0 ]; then
        log "安装失败，准备回滚"
        if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
            $SUDO rm -rf "$CURRENT_DIR" 2>/dev/null || true
            $SUDO mv "$BACKUP_DIR" "$CURRENT_DIR" 2>/dev/null && log "已回滚到升级前版本" || log "警告: 回滚目录恢复失败，请手动检查"
        fi
        if [ "$SERVICE_WAS_ACTIVE" -eq 1 ]; then
            start_service && log "回滚后服务已恢复" || log "警告: 回滚后服务未能自动恢复"
        fi
    fi
    [ -n "$STAGE_DIR" ] && rm -rf "$STAGE_DIR" 2>/dev/null || true
    exit "$exit_code"
}
trap on_exit EXIT HUP INT TERM

detect_release

ARCH=$(uname -m)
case "$ARCH" in
    x86_64|x64|amd64) ARCH_NAME="64" ;;
    aarch64|arm64) ARCH_NAME="arm64-v8a" ;;
    s390x) ARCH_NAME="s390x" ;;
    *) echo ">>> 错误: 当前架构暂不支持自动安装: $ARCH"; exit 1 ;;
esac
log "系统: $RELEASE / $ARCH"

ensure_base_tools
rm -rf /tmp/v2bx-install.* 2>/dev/null || true

log "版本: $V2BX_VERSION"
DOWNLOAD_URL="https://github.com/$REPO/releases/download/$V2BX_VERSION/V2bX-linux-$ARCH_NAME.zip"
STAGE_DIR=$(mktemp -d /tmp/v2bx-install.XXXXXX)
ZIP_FILE="$STAGE_DIR/V2bX-linux.zip"
if ! download_with_mirrors "$DOWNLOAD_URL" "$ZIP_FILE" 15 300; then
    echo ">>> 错误: 所有下载源均失败，请检查服务器网络"
    exit 1
fi

log "校验压缩包..."
if ! unzip -tq "$ZIP_FILE" >/dev/null 2>&1; then
    echo ">>> 错误: 压缩包损坏或下载不完整"
    exit 1
fi
(cd "$STAGE_DIR" && unzip -oq "$ZIP_FILE" >/dev/null 2>&1) || { echo ">>> 错误: 解压失败"; exit 1; }
rm -f "$ZIP_FILE"
[ -f "$STAGE_DIR/V2bX" ] || { echo ">>> 错误: 解压后未找到 V2bX 二进制文件"; exit 1; }
chmod +x "$STAGE_DIR/V2bX"
case "$(file -b "$STAGE_DIR/V2bX" | head -1)" in
    *ELF*) log "二进制验证通过" ;;
    *) echo ">>> 错误: V2bX 不是有效的二进制文件"; exit 1 ;;
esac

ensure_install_space
if service_is_active; then SERVICE_WAS_ACTIVE=1; fi
stop_old_service

if [ -d "$CURRENT_DIR" ]; then
    cleanup_old_backups
    BACKUP_DIR="/usr/local/V2bX.backup-$(date +%Y%m%d_%H%M%S)"
    $SUDO mv "$CURRENT_DIR" "$BACKUP_DIR"
    log "已备份旧目录到 $BACKUP_DIR"
fi

$SUDO mkdir -p "$CURRENT_DIR" /etc/V2bX
$SUDO cp -a "$STAGE_DIR"/. "$CURRENT_DIR"/
$SUDO chmod +x "$CURRENT_DIR/V2bX"
printf '%s\n' "$V2BX_VERSION" | $SUDO tee "$CURRENT_DIR/.version" >/dev/null
printf '%s\n' "$V2BX_VERSION" | $SUDO tee /etc/V2bX/.panel_version >/dev/null
for f in geoip.dat geosite.dat; do
    [ -f "$CURRENT_DIR/$f" ] && $SUDO cp "$CURRENT_DIR/$f" /etc/V2bX/ || true
done

setup_service

log "下载 V2bX 管理脚本..."
TMP_SCRIPT="/tmp/V2bX.sh.$$"
for ref in master main; do
    if download_file "https://raw.githubusercontent.com/$REPO/$ref/install/V2bX.sh" "$TMP_SCRIPT" \
        || download_file "https://ghfast.top/https://raw.githubusercontent.com/$REPO/$ref/install/V2bX.sh" "$TMP_SCRIPT"; then
        $SUDO install -m 755 "$TMP_SCRIPT" /usr/bin/V2bX
        $SUDO ln -sf /usr/bin/V2bX /usr/bin/v2bx 2>/dev/null || true
        break
    fi
done
rm -f "$TMP_SCRIPT"

log "安装完成"
