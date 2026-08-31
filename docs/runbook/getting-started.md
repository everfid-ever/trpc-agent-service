# trpc-agent-service 启动与配置指南

本文面向第一次使用仓库的人，说明如何启动本地 WebUI、填写 DeepSeek API Key、运行基础设施验证，以及如何理解生产多角色配置。

## 1. 先选择启动方式

| 目标 | 使用入口 | 是否自动初始化 | 适用场景 |
|---|---|---|---|
| 在本机直接聊天并验证完整内部链路 | `webui-local` Compose profile | 是 | 首次体验、开发联调、无 Feishu/WeCom 凭据 |
| 验证 PostgreSQL、Redis 和故障恢复契约 | `runtime-test` Compose profile | 测试脚本创建并删除随机测试库 | CI、后端契约、故障恢复验证 |
| 独立启动 Gateway 和 Worker | `gateway-worker` Compose profile | 否 | 已具备控制面数据、scoped Secret 和对象存储的集成环境 |
| 部署全部生产角色 | `trpc-service <role>` | 否 | Kubernetes、独立进程或正式部署系统 |

如果只是想确认项目可以正常工作，请从第 2 节的 WebUI 开始。不要从生产多角色模式开始。

## 2. WebUI 本地一键启动

### 2.1 前置条件

需要：

- Docker Desktop 或兼容的 Docker Engine；
- Docker Compose v2，即命令形式为 `docker compose`；
- 一个可用的 DeepSeek API Key；
- 本机端口 `58081`、`55432` 和 `56379` 未被占用。

在仓库根目录执行命令。可以用下面的命令确认当前位置：

```bash
pwd
test -f deploy/compose/docker-compose.m2.yml
```

### 2.2 创建本地配置

```bash
cp deploy/compose/.env.m2.example deploy/compose/.env.m2
mkdir -p deploy/compose/secrets
chmod 700 deploy/compose/secrets
```

`.env.m2` 只保存非密钥配置。不要把 DeepSeek API Key 写进 `.env.m2`。

### 2.3 写入 DeepSeek API Key

推荐先在仓库外创建一个权限为 `0600` 的纯文本文件。文件内容只能是 API Key 本身，不能带引号、变量名、空行或多行内容。例如：

```text
sk-xxxxxxxxxxxxxxxx
```

然后复制为 Docker secret：

```bash
install -m 600 /absolute/path/to/your/deepseek-api-key \
  deploy/compose/secrets/deepseek-api-key
```

检查文件是否存在及权限是否正确，但不要输出文件内容：

```bash
test -s deploy/compose/secrets/deepseek-api-key
ls -l deploy/compose/secrets/deepseek-api-key
```

应看到只有文件所有者可读写。该路径已被 Git 忽略，但仍不得主动执行 `git add -f`。

如果 API Key 曾出现在截图、日志、终端录屏或提交记录中，应先去 DeepSeek 控制台撤销并创建新 Key。

### 2.4 启动

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui up --build
```

首次运行需要拉取镜像并下载 Go 依赖，耗时取决于网络。终端出现类似下面的日志表示应用已监听：

```text
WebUI local ready: http://localhost:8080/webui/ route="local-webui" account="local-webui" model=deepseek
```

宿主机访问地址不是容器日志中的 `8080`，而是：

```text
http://localhost:58081/webui/
```

若希望后台运行：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui up -d --build
```

查看日志：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui logs -f webui-local
```

### 2.5 WebUI 输入参数

页面默认值如下：

| 页面字段 | 默认值 | 含义 |
|---|---|---|
| Route Key | `local-webui` | 查找公开 Channel route 的不透明定位值 |
| Account ID | `local-webui` | 本地 Channel 外部账号标识 |
| User ID | `local-user` | 模拟外部用户；不同值形成不同内部用户身份 |
| Chat ID | `local-chat` | 模拟会话定位值 |
| Channel Token | `local-webui-token-change-me` | WebUI HMAC 验签材料，至少 16 字符 |

点击连接后输入纯文本消息。WebUI 本地 profile 不支持图片和文件。

消息仍会经过公共链路：

```text
WebUI callback
→ candidate/scoped verification
→ durable Inbox + PreprocessJob
→ PrepareDispatch + Redis Broker
→ Worker + DeepSeek
→ CommitTurn + Reply Outbox
→ Reply Queue + Delivery Ledger
→ WebUI mailbox
```

WebUI mailbox 只保存投递元数据；回复正文位于加密 ResultStore。

### 2.6 判断是否启动成功

浏览器可以发送并收到回复，同时下面两个地址应返回成功：

```bash
curl -i http://localhost:58081/livez
curl -i http://localhost:58081/readyz
```

- `/livez` 返回 `200`：进程存活；
- `/readyz` 返回 `200`：PostgreSQL、Redis、控制面和本地 Secret 已准备好；
- `/readyz` 返回 `503`：进程仍在运行，但依赖或配置未通过门禁。

### 2.7 停止、重启与清空数据

停止但保留 PostgreSQL/Redis 数据：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui down
```

再次启动：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui up -d --build
```

只有在本地测试数据可以全部删除时才执行：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui down -v
```

`-v` 会删除该 Compose 环境的 PostgreSQL、Redis 和 MinIO volume，无法通过普通重启恢复。生产环境禁止使用这条命令。

更换 `TRPC_WEBUI_LOCAL_TOKEN` 或不可变 Secret generation 后，既有本地控制面可能与新配置不兼容；本地环境可使用 `down -v` 后重新初始化。

### 2.8 修改本地默认值

编辑 `deploy/compose/.env.m2`：

```dotenv
TRPC_WEBUI_LOCAL_ROUTE_KEY=local-webui
TRPC_WEBUI_LOCAL_TOKEN=replace-with-at-least-16-random-characters
M2_WEBUI_PORT=58081
```

- Route Key 不能为空，首尾不能有空格；
- Token 首尾不能有空格，长度至少 16；
- 修改宿主机端口后访问 `http://localhost:<M2_WEBUI_PORT>/webui/`；
- 页面填写值必须与 `.env.m2` 一致。

## 3. 运行 PostgreSQL/Redis Runtime Slice

该模式不需要 DeepSeek Key，用于验证 PostgreSQL 16、Redis 7、Gateway/Worker、lease/fence、重投和 Delivery Ledger 语义：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  up -d postgres redis

docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile runtime-test run --rm runtime-test
```

测试入口会创建随机测试数据库并在完成后删除。不要把测试环境变量指向生产数据库。

停止基础设施：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml down
```

## 4. 可执行角色

统一二进制的调用方式：

```text
trpc-service artifact
trpc-service preprocess
trpc-service channel
trpc-service channel-delivery
trpc-service gateway
trpc-service worker
trpc-service webui-local
```

不传 role 时默认启动 `artifact`。生产部署应始终显式指定 role。

| Role | 主要职责 | 核心依赖 |
|---|---|---|
| `artifact` | ObjectStore readiness、upload-intent 和 retention 回收 | PostgreSQL、Redis、S3、ClamAV、DLP、Secret |
| `preprocess` | 媒体下载、扫描、PreparedInput 和 Dispatch | PostgreSQL、S3、ClamAV、DLP、Secret |
| `channel` | Feishu/WeCom callback 和可选 WebUI ingress | PostgreSQL、Secret |
| `channel-delivery` | Reply Queue、reclaim、Delivery Ledger 和 Provider sender | PostgreSQL、Redis、Secret |
| `gateway` | 认证 HTTP、OpenAI-compatible、A2A、status/cancel、SSE | PostgreSQL、Secret |
| `worker` | Redis 消费、lease/fence、Runner、DeepSeek、CommitTurn | PostgreSQL、Redis、S3、Secret |
| `webui-local` | 本地组合式全链路 | PostgreSQL、Redis、DeepSeek Key 文件 |

## 5. 生产模式的重要限制

`gateway-worker` Compose profile 不是一键初始化环境。它只提供接近生产的进程拓扑，启动前必须已经完成：

1. 通过受控部署步骤应用完整 PostgreSQL schema；
2. 发布 Tenant、Agent App、Agent Revision、ConfigSnapshot 和 DeepSeek ModelProfile；
3. 创建 ChannelBinding、公开 route 和所需 BackendBinding；
4. 创建并开启 versioning 的 S3 bucket；
5. 按完整 scope 生成 `TRPC_SECRET_ROOT` 下的 Secret 文件；
6. 签发 Gateway 使用的 `gw1.*` bearer token；
7. 为 HTTPS Gateway 和 Channel callback 配置 TLS ingress。

当前仓库没有面向生产的一键 bootstrap CLI，也没有最终用户 Gateway token 签发 CLI。因此，只有已经具备控制面部署器和 Secret 投影流程的环境才能直接使用 `gateway-worker` profile。`webui-local` 是唯一会自动建立隔离 schema、控制面和本地 Secret 的入口。

不要使用测试数据库 runner 对生产数据库执行初始化，也不要复制 `webui-local` 的固定 Tenant、Token 或派生 Secret 到生产环境。

## 6. 生产角色必填环境变量

### 6.1 通用变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TRPC_LISTEN_ADDRESS` | `:8080` | HTTP health/API 监听地址 |
| `TRPC_POSTGRES_DSN` | 无 | PostgreSQL DSN；所有生产角色必填 |
| `TRPC_SECRET_ROOT` | 无 | scoped Secret 文件只读根目录；所有生产角色必填 |
| `TRPC_PROBE_TIMEOUT` | `5s` | 单次依赖探测超时 |
| `TRPC_PROBE_INTERVAL` | `15s` | readiness 探测周期 |
| `TRPC_SHUTDOWN_TIMEOUT` | 一般 `30s`；Gateway/Worker `45s` | 总关闭时间预算 |

### 6.2 按角色必填矩阵

| Role | 必填变量 |
|---|---|
| `artifact` | `TRPC_POSTGRES_DSN`、`TRPC_REDIS_ADDRESS`、`TRPC_SECRET_ROOT`、`TRPC_S3_REGION`、`TRPC_S3_BUCKET`、`TRPC_CLAMAV_ADDRESS`、`TRPC_DLP_ENDPOINT`、`TRPC_DLP_PROBE_TENANT_ID`、`TRPC_DLP_SECRET_REF`、`TRPC_DLP_SECRET_VERSION`、`TRPC_DLP_BACKEND_VERSION`、`TRPC_PAYLOAD_KEY_REF`、`TRPC_PAYLOAD_KEY_VERSION` |
| `preprocess` | `artifact` 的全部非 Redis 变量，再加 `TRPC_PREPROCESS_ARTIFACT_RETENTION` |
| `channel` | `TRPC_POSTGRES_DSN`、`TRPC_SECRET_ROOT`、`TRPC_CHANNEL_PROBE_TENANT_ID`、`TRPC_PAYLOAD_KEY_REF`、`TRPC_PAYLOAD_KEY_VERSION` |
| `channel-delivery` | `TRPC_POSTGRES_DSN`、`TRPC_REDIS_ADDRESS`、`TRPC_SECRET_ROOT`、`TRPC_REDIS_ENVIRONMENT`、`TRPC_CHANNEL_DELIVERY_GROUP`、`TRPC_CHANNEL_PROBE_TENANT_ID`、`TRPC_PAYLOAD_KEY_REF`、`TRPC_PAYLOAD_KEY_VERSION` |
| `gateway` | `TRPC_POSTGRES_DSN`、`TRPC_SECRET_ROOT`、`TRPC_GATEWAY_PROBE_TENANT_ID`、`TRPC_GATEWAY_AUTH_SECRET_REF`、`TRPC_GATEWAY_AUTH_SECRET_VERSION`、`TRPC_GATEWAY_PUBLIC_BASE_URL`、`TRPC_PAYLOAD_KEY_REF`、`TRPC_PAYLOAD_KEY_VERSION` |
| `worker` | `TRPC_POSTGRES_DSN`、`TRPC_REDIS_ADDRESS`、`TRPC_SECRET_ROOT`、`TRPC_REDIS_ENVIRONMENT`、`TRPC_WORKER_GROUP`、`TRPC_WORKER_CONTROL_GROUP`、`TRPC_WORKER_PROBE_TENANT_ID`、`TRPC_S3_REGION`、`TRPC_S3_BUCKET`、`TRPC_PAYLOAD_KEY_REF`、`TRPC_PAYLOAD_KEY_VERSION` |
| `audit-relay` | `TRPC_POSTGRES_DSN`（源业务库）、`TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN`（独立合规库；不得与源 DSN 相同） |
| `audit-compliance-migrate` | `TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN`；仅用于受控部署步骤，完成后退出 |

空字符串等同于未配置。版本和 generation 必须是大于等于 1 的十进制整数。

## 7. 参数参考

### 7.1 PostgreSQL、Redis 和 S3

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_REDIS_PASSWORD` | 空 | Redis 密码，不应写入普通 ConfigMap |
| `TRPC_REDIS_DB` | `0` | 非负整数 |
| `TRPC_REDIS_ENVIRONMENT` | 无 | Redis key 环境隔离前缀，Worker/Delivery 必填 |
| `TRPC_S3_ENDPOINT` | AWS 默认 | 自建 S3-compatible endpoint |
| `TRPC_S3_PATH_STYLE` | `false` | MinIO 通常设为 `true` |
| `TRPC_S3_ALLOW_INSECURE` | `false` | 只有隔离本地 HTTP MinIO 才允许 `true` |
| `TRPC_ARTIFACT_MAX_BYTES` | `16777216` | Artifact 和媒体最大字节数，必须大于 0 |

S3 lifecycle 依赖精确 VersionID，bucket 未开启 versioning 时 readiness 保持失败。

Audit Relay 的两个 PostgreSQL DSN 必须使用不同数据库和最小权限账号。先以
`audit-compliance-migrate` 对目标库应用独立 schema，再启动 Relay；Relay readiness
同时检查源业务 schema 和目标 compliance schema checksum。

### 7.2 Artifact lifecycle

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_ARTIFACT_PUT_TIMEOUT` | `30s` | 至少 `1s` |
| `TRPC_ARTIFACT_UPLOAD_PROTECTION` | `2m` | 必须长于 put timeout |
| `TRPC_ARTIFACT_UPLOAD_CLAIM_TTL` | `1m` | 至少 `1s` |
| `TRPC_ARTIFACT_RETENTION_CLAIM_TTL` | `1m` | 至少 `1s` |
| `TRPC_ARTIFACT_UPLOAD_POLL_INTERVAL` | `1m` | 至少 `1ms` |
| `TRPC_ARTIFACT_RETENTION_POLL_INTERVAL` | `1m` | 至少 `1ms` |
| `TRPC_ARTIFACT_ORPHAN_GRACE` | `24h` | 至少 `1m` |
| `TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE` | `100` | `1–1000` |
| `TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS` | `8` | `1–100` |

### 7.3 Preprocess

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_PREPROCESS_ARTIFACT_RETENTION` | 无 | 必填；整秒且至少 `1s` |
| `TRPC_PREPROCESS_BATCH_SIZE` | `100` | `1–1000` |
| `TRPC_PREPROCESS_MAX_ATTEMPTS` | `8` | `1–100` |
| `TRPC_PREPROCESS_LEASE_TTL` | `30s` | 至少 `1s` |
| `TRPC_PREPROCESS_RETRY_DELAY` | `1s` | 至少 `1ms` |
| `TRPC_PREPROCESS_POLL_INTERVAL` | `1s` | 至少 `1ms` |
| `TRPC_PREPROCESS_MEDIA_FETCH_TIMEOUT` | `15s` | 至少 `1ms` |
| `TRPC_PREPROCESS_MEDIA_ALLOWED_HOSTS` | 空 | 逗号分隔、转为小写；空表示禁止通用 HTTPS 媒体下载 |

`TRPC_CLAMAV_ADDRESS` 是 clamd TCP 地址，例如 `clamav:3310`。DLP endpoint 生产环境必须使用 HTTPS；只有隔离本地 smoke 才可设置 `TRPC_DLP_ALLOW_INSECURE=true`。

### 7.4 Channel callback

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_WEBUI_ENABLED` | `false` | 必须在 `channel` 和 `channel-delivery` 两个角色同时开启 |
| `TRPC_CHANNEL_CANDIDATE_TTL` | `30s` | `1s–10m` |
| `TRPC_CHANNEL_CALLBACK_MAX_BODY` | `1048576` | `1–16777216` 字节 |

Provider callback 地址：

```text
https://im.example.test/callbacks/feishu?route_key=<opaque-route>
https://im.example.test/callbacks/wecom?route_key=<opaque-route>
```

公开 `route_key` 只是候选定位值，不是 Tenant ID、Binding ID 或 Secret。

### 7.5 Channel delivery

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_CHANNEL_DELIVERY_REFRESH` | `15s` | 至少 `1ms` |
| `TRPC_CHANNEL_REPLY_READ_BLOCK` | `1s` | 至少 `1ms` |
| `TRPC_CHANNEL_REPLY_RECLAIM_IDLE` | `30s` | 至少 `1ms` |
| `TRPC_CHANNEL_REPLY_RECLAIM_INTERVAL` | `5s` | 至少 `1ms` |
| `TRPC_CHANNEL_REPLY_RECLAIM_LIMIT` | `100` | `1–1000` |
| `TRPC_CHANNEL_DELIVERY_CLAIM_TTL` | `30s` | 必须长于 renew |
| `TRPC_CHANNEL_DELIVERY_CLAIM_RENEW` | `10s` | 必须短于 claim TTL |
| `TRPC_CHANNEL_DELIVERY_RETRY_DELAY` | `1s` | 至少 `1ms` |
| `TRPC_CHANNEL_DELIVERY_MAX_RETRY` | `1m` | 至少 `1ms` |
| `TRPC_CHANNEL_DELIVERY_MAX_ATTEMPTS` | `8` | `1–1000` |
| `TRPC_CHANNEL_DELIVERY_MAX_RECONCILE` | `8` | `1–1000` |
| `TRPC_CHANNEL_PROVIDER_TIMEOUT` | `15s` | 至少 `1ms` |

### 7.6 Worker

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_WORKER_ID` | 进程生成 | 多实例建议显式设置唯一值 |
| `TRPC_WORKER_SHARD_COUNT` | `1` | `1–4096` |
| `TRPC_WORKER_SHARDS` | `0..count-1` | 逗号分隔、不可重复、每项小于 shard count |
| `TRPC_WORKER_LEASE_TTL` | `30s` | 至少 `1s` |
| `TRPC_WORKER_LEASE_RENEW` | `10s` | 必须短于 lease TTL |
| `TRPC_WORKER_RETRY_WAIT` | `100ms` | 至少 `1ms` |
| `TRPC_WORKER_RECLAIM_INTERVAL` | `5s` | 至少 `1ms` |
| `TRPC_WORKER_RECLAIM_LIMIT` | `100` | `1–1000` |
| `TRPC_WORKER_CANCEL_POLL` | `100ms` | 至少 `1ms` |
| `TRPC_WORKER_DRAIN_TIMEOUT` | `30s` | 至少 `1s` |
| `TRPC_WORKER_BUNDLE_FAILURE_BACKOFF` | `250ms` | 至少 `1ms` |
| `TRPC_WORKER_BUNDLE_CLOSE_TIMEOUT` | `5s` | 至少 `1ms` |

`drain timeout + bundle close timeout + 5s HTTP 预算` 不得超过 `TRPC_SHUTDOWN_TIMEOUT`。

### 7.7 Gateway

| 变量 | 默认值 | 约束 |
|---|---|---|
| `TRPC_GATEWAY_AUTH_CLOCK_SKEW` | `30s` | 至少 `1s`，且小于 shutdown timeout |
| `TRPC_GATEWAY_MAX_BODY` | `1048576` | `1–16777216` 字节 |
| `TRPC_GATEWAY_SSE_POLL_INTERVAL` | `1s` | 至少 `1ms` |
| `TRPC_GATEWAY_SSE_REPLAY_LIMIT` | `64` | `1–256` |
| `TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS` | `128` | `1–10000` |
| `TRPC_GATEWAY_PROTOCOL_TIMEOUT` | `2m` | `1s–30m` |
| `TRPC_GATEWAY_PUBLIC_BASE_URL` | 无 | 必须是无 userinfo/query/fragment 的 HTTPS origin |

公开接口包括：

```text
POST /v1/agent-runs
GET  /v1/agent-runs/{request_id}
GET  /v1/agent-runs/{request_id}/events
POST /v1/agent-runs/{request_id}:cancel
POST /v1/chat/completions
```

请求必须携带有效 `gw1.*` bearer token 和相应幂等信息。仓库当前不提供生产 token 签发 CLI。

### 7.8 WebUI local 专用变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TRPC_WEBUI_LOCAL_ROUTE_KEY` | `local-webui` | 本地公开 route key |
| `TRPC_WEBUI_LOCAL_TOKEN` | `local-webui-token-change-me` | 本地 HMAC token，至少 16 字符 |
| `TRPC_WEBUI_DEEPSEEK_KEY_FILE` | `/run/secrets/deepseek_api_key` | 容器内 DeepSeek Key 绝对路径 |
| `TRPC_WEBUI_LOCAL_SECRET_ROOT` | `/tmp/trpc-webui-secrets` | 自动生成 scoped Secret 的绝对目录 |
| `TRPC_REDIS_ENVIRONMENT` | `m2-webui-local` | WebUI local Redis key 前缀 |

这些默认值只属于 `webui-local`。生产角色不会自动生成控制面或 Secret。

## 8. Secret 配置

### 8.1 为什么不能直接用 SecretRef 当文件名

生产角色不会读取 `TRPC_SECRET_ROOT/my-secret`。文件名由完整授权坐标计算：

```text
tenant_id
+ subject
+ purpose
+ resource_id
+ resource_version
+ secret_ref
+ secret_ref_version
→ StableFilename
```

这可以防止一个知道 SecretRef 的角色越权读取其他 Tenant 或 purpose 的密钥。

生产部署器必须调用 `trpcservice/secrets/filesystem.StableFilename` 生成文件名，并把文件以 `0400` 或 `0600` 权限投影到 `TRPC_SECRET_ROOT`。根目录不得允许 group/world 写入，进程内同一 generation 的文件内容不得变化。

当前仓库没有通用的 Secret 投影命令行工具；生产部署需要由 Kubernetes CSI、部署生成器或控制面完成该步骤。`webui-local` 会自动完成自己的隔离 Secret 投影。

### 8.2 常用 Secret scope

| 用途 | Subject | Resource ID | Resource version |
|---|---|---|---|
| payload encryption | Tenant ID | `messaging-payload` | payload key generation |
| Gateway auth | `gateway` | `gateway-auth` | Gateway auth generation |
| DeepSeek model call | `worker-model` | Model Profile ID | Model Profile version |
| Channel verify | Channel Binding ID | Channel Binding ID | Config version |
| Channel send | Channel Binding ID | Channel Binding ID | Config version |
| tenant identity/session | Tenant ID | Tenant ID | SecretRef version |
| DLP backend | Tenant ID | `http-dlp` | DLP backend version |

Payload encryption key 必须恰好为 32 bytes。API key、token 和 Secret JSON 不得进入日志、`.env`、ConfigMap 或命令行参数。

### 8.3 Provider Secret JSON

Feishu 发送凭据：

```json
{"app_id":"cli_xxx","app_secret":"..."}
```

WeCom Agent 发送凭据：

```json
{"corp_id":"ww_xxx","corp_secret":"...","agent_id":1000002}
```

Feishu callback 验证材料使用严格 JSON：

```json
{"EncryptKey":"...","VerificationToken":"...","AppID":"cli_xxx","BotOpenID":"ou_xxx"}
```

WeCom callback 验证材料使用严格 JSON：

```json
{"token":"...","encoding_aes_key":"...","receive_id":"ww_xxx","agent_id":1000002}
```

发送与验签必须使用不同 SecretRef 和 purpose，不能互相回退；未知字段会被拒绝。

## 9. Compose 宿主机参数

这些变量控制本地容器和端口，不会直接替代生产 `TRPC_*` 配置：

| 变量 | 默认值 |
|---|---|
| `M2_POSTGRES_USER` | `postgres` |
| `M2_POSTGRES_PASSWORD` | `postgres` |
| `M2_POSTGRES_DB` | `trpc_agent_service_test` |
| `M2_POSTGRES_PORT` | `55432` |
| `M2_REDIS_PORT` | `56379` |
| `M2_MINIO_ROOT_USER` | `minioadmin` |
| `M2_MINIO_ROOT_PASSWORD` | `minioadmin` |
| `M2_MINIO_PORT` | `59000` |
| `M2_MINIO_CONSOLE_PORT` | `59001` |
| `M2_GATEWAY_PORT` | `58080` |
| `M2_WEBUI_PORT` | `58081` |

默认密码、默认 Token 和 `TRPC_*_ALLOW_INSECURE=true` 只允许隔离本地环境。

## 10. 真实 Feishu/WeCom 验证

真实 Provider smoke 会发送可见消息。只允许对授权测试账号执行。

```bash
cp deploy/compose/.env.m3-im.example deploy/compose/.env.m3-im
chmod 600 /absolute/path/to/feishu.json /absolute/path/to/wecom.json
bash scripts/m3_im_provider_smoke_test.sh
```

`.env.m3-im` 只填写绝对凭据文件路径和非密钥 locator。可以通过 `TRPC_M3_IM_PROVIDERS=feishu`、`wecom` 或 `feishu,wecom` 选择 Provider。

WebUI 成功不能替代这项验证。WeCom Bot/WebSocket 和群聊 mention 不属于 WeCom Agent Adapter 支持范围。

## 11. 常见问题

### 11.1 `DeepSeek API key file is missing or invalid`

检查：

- `deploy/compose/secrets/deepseek-api-key` 存在且非空；
- 文件只有一行 Key，没有 `DEEPSEEK_API_KEY=`、引号或额外空行；
- Compose 使用了 `--profile webui`；
- secret 文件权限为 `0600`。

### 11.2 `config tenant scope mismatch`

通常表示本地 volume 中存在与新 Token、Route 或 seed 版本不匹配的旧控制面数据。确认这是可删除的本地环境后执行：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui down -v
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui up --build
```

不要在生产环境通过删除 volume 处理 scope mismatch；生产必须检查 Tenant、ConfigVersion、Binding 和 Secret generation。

### 11.3 浏览器打不开 `localhost:58081`

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui ps
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui logs webui-local
```

确认 `webui-local` 没有退出，并检查 `M2_WEBUI_PORT` 是否被其他进程占用。

### 11.4 `/livez` 成功但 `/readyz` 返回 503

这是设计行为，表示进程存活但依赖未就绪。查看日志中的具体 dependency，并检查：

- PostgreSQL schema checksum；
- Redis 连接；
- S3 bucket versioning；
- ClamAV engine version；
- DLP authorizer；
- probe Tenant 对应的 payload/auth Secret generation。

### 11.5 页面可以打开但发送失败

确认页面中的 Route Key、Account ID 和 Channel Token 与 `.env.m2` 一致。修改本地不可变 Token 后需要清空一次性 volume 再启动。

### 11.6 DeepSeek 返回错误

先检查 Key 是否仍有效、账户是否有额度、宿主机是否能访问 `https://api.deepseek.com`。生产 ModelProfile 只允许 Catalog 注册的 DeepSeek endpoint、model 和 option；不会回退到 mock model。

### 11.7 Docker API permission denied

确认 Docker Desktop 已启动，并且当前用户可以执行：

```bash
docker info
docker compose version
```

### 11.8 如何确认没有把 Key 提交到 Git

```bash
git status --short
git check-ignore -v deploy/compose/secrets/deepseek-api-key
```

不要执行会输出 Key 内容的检查命令。

## 12. 相关文档

- [生产角色与 Artifact 生命周期 Runbook](artifact-lifecycle.md)
- [能力与兼容边界](../design/8.capability-boundaries.md)
- [验证与发布规范](../design/7.verification-release-design.md)
- [Docker Compose 说明](../../deploy/compose/README.md)
