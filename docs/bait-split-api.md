# BaitSplit 接口文档

本文只说明 BaitSplit 插件里和“通过接口更换分组 IP/域名”相关的接口。

## 外部换 IP Webhook

用于 AWS、探测程序或外部脚本通知某个用户池更换 IP/域名。

```http
POST /api/v2/plugin/bait-split/hooks/ip-rotate
Content-Type: application/json
X-Bait-Timestamp: <当前 Unix 时间戳>
X-Bait-Signature: <签名>
```

### 签名

插件配置里必须设置 `ip_webhook_secret`，长度至少 32 位。

签名算法：

```text
signature = HMAC_SHA256(timestamp + "\n" + raw_json_body, ip_webhook_secret)
```

注意：

- `raw_json_body` 必须是实际发送的原始 JSON 字符串。
- 时间戳有效期为 5 分钟。
- 签名结果为 64 位小写十六进制字符串。

### 请求参数

```json
{
  "event_id": "unique-event-001",
  "campaign_id": "任务ID",
  "instance_id": "可选实例ID",
  "target_id": "用户池接口标识",
  "old_ip": "1.1.1.1",
  "new_ip": "2.2.2.2",
  "reason": "blocked",
  "source": "auto"
}
```

字段说明：

- `event_id`：必填，事件唯一 ID，重复提交会去重。
- `campaign_id`：必填，BaitSplit 任务 ID。
- `instance_id`：可选，外部实例 ID，仅用于记录。
- `target_id`：必填，用户池的 `webhook_id`；未配置时可用池 ID。
- `old_ip`：可选，旧 IP/域名。建议传，便于精确记录拿到过旧地址的用户。
- `new_ip`：必填，新 IP/域名。
- `reason`：可选，`blocked` 表示被墙，`machine` 表示机器故障。
- `source`：可选，建议传 `auto`。只有 `auto` / `rebuild` 会按真实被墙处理。

### 被墙判定

`source` 优先级高于 `reason`：

- `source=auto`：真实被墙。
- `source=rebuild`：真实被墙。
- `source=manual` / `external` / `launch` / `sync` / `drift`：不按被墙处理。
- `source` 为空时，才根据 `reason` 判断。

只有真实被墙时，才会：

- 写入换 IP 事件日志。
- 统计拿到过旧 IP 的用户。
- 如果开启“墙后重置 token/uuid”，自动重置命中用户的 `token` 和 `uuid`。

### 成功响应

```json
{
  "ok": true,
  "data": {
    "campaign_id": "任务ID",
    "target_id": "aq1",
    "reason": "blocked",
    "source": "auto",
    "old_ip": "1.1.1.1",
    "new_ip": "2.2.2.2",
    "already": false,
    "updated_pool_ids": ["pool-id"],
    "config_version": 12,
    "wall": {
      "reason": "blocked",
      "mode": "exposure",
      "suspect_count": 20,
      "exact_count": 10,
      "credential_reset_count": 10
    },
    "event_id": "unique-event-001",
    "instance_id": "",
    "duplicate": false
  }
}
```

如果系统正忙，会先入队：

```json
{
  "ok": true,
  "queued": true,
  "data": {
    "event_id": "unique-event-001",
    "pending": 1
  }
}
```

### curl 示例

```bash
BODY='{"event_id":"test-001","campaign_id":"CAMPAIGN_ID","target_id":"aq1","old_ip":"1.1.1.1","new_ip":"2.2.2.2","reason":"blocked","source":"auto"}'
TS=$(date +%s)
SECRET='替换为 ip_webhook_secret'
SIG=$(printf "%s\n%s" "$TS" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $2}')

curl -X POST 'https://你的面板域名/api/v2/plugin/bait-split/hooks/ip-rotate' \
  -H 'Content-Type: application/json' \
  -H "X-Bait-Timestamp: $TS" \
  -H "X-Bait-Signature: $SIG" \
  --data "$BODY"
```

## 管理后台保存用户池 IP/域名

需要管理员登录 token。适合后台程序直接修改用户池配置。

```http
POST /api/v2/{后台路径}/plugin/bait-split/campaigns/{campaignId}/pools
Authorization: Bearer <管理员 token>
Content-Type: application/json
```

请求示例：

```json
{
  "id": "pool-id",
  "webhook_id": "aq1",
  "name": "安全组1",
  "type": "safe",
  "host": "2.2.2.2",
  "node_hosts": {},
  "server_name": "",
  "transport_host": "",
  "enabled": true,
  "status": "available",
  "capacity": 0,
  "overflow_pool_id": "",
  "note": ""
}
```

字段说明：

- `id`：用户池 ID。新增时可为空，修改时必须传原 ID。
- `webhook_id`：外部接口使用的目标标识。
- `name`：用户池名称。
- `type`：用户池类型，可选 `default`、`danger`、`blacklist`、`probe`、`observation`、`emergency`、`safe`、`custom`。
- `host`：统一域名/IP。
- `node_hosts`：按节点单独覆盖域名/IP，可为空对象。
- `server_name`：SNI 覆盖，可为空。
- `transport_host`：传输层 Host 覆盖，可为空。
- `enabled`：是否启用。
- `status`：可选 `available`、`active`、`suspected`、`blocked`、`standby`。
- `capacity`：容量，`0` 表示不限。
- `overflow_pool_id`：满员后自动转入的池 ID，可为空。
- `note`：备注。

## 修改排查树分支 IP/域名

需要管理员登录 token。用于修改某个排查树节点的统一域名/IP。

```http
POST /api/v2/{后台路径}/plugin/bait-split/campaigns/{campaignId}/investigations/{nodeId}/host
Authorization: Bearer <管理员 token>
Content-Type: application/json
```

请求示例：

```json
{
  "host": "2.2.2.2",
  "webhook_id": "branch-a"
}
```

字段说明：

- `host`：新的统一域名/IP。
- `webhook_id`：外部接口标识，可为空。

## 常见注意事项

- 外部自动换 IP 推荐使用 webhook 接口，不需要管理员 token。
- 后台保存用户池接口会直接改配置，适合人工或内部管理脚本。
- 如果是被墙检测触发，`source` 必须传 `auto` 或 `rebuild`。
- 如果只是手动换 IP，不想记录为被墙，`source` 传 `manual` 或 `external`。
- 开启“墙后重置 token/uuid”后，真实被墙事件会重置拿到过旧 IP 的用户凭据。
