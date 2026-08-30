# Production role runbook

本地 M2 验收的 Docker 基础设施与应用镜像说明见
[`deploy/compose/README.md`](../../deploy/compose/README.md)。Compose 只负责可重复启动
PostgreSQL/Redis/MinIO 和角色容器，不自动迁移或伪造控制面、Secret、Gateway 身份。

`cmd/trpc-service artifact` 组合 Artifact lifecycle、依赖 readiness 和 health listener；`cmd/trpc-service preprocess` 组合持久 Preprocess Worker、媒体 staging、Artifact、扫描器和 `PrepareDispatch`；`cmd/trpc-service channel` 组合 Feishu/WeCom callback、可选 WebUI callback、持久候选验签、身份映射、加密 payload 和 durable Inbox/PreprocessJob；`cmd/trpc-service channel-delivery` 组合 Reply Queue/Delivery Ledger 与各 Channel Adapter；`cmd/trpc-service worker` 组合持久 Task/Session/Bundle/Runner 与 DeepSeek Model client；`cmd/trpc-service gateway` 组合签名认证、durable run、SSE 和 OpenAI façade。各 role 共享只读配置/Secret 约束，但进程生命周期与租约 owner 独立；均不会创建 InMemory fallback。

## 启动前置条件

必须先通过受控部署步骤应用仓库全部 PostgreSQL migration。进程不会自动执行 migration；readiness 会逐项核验 `schema_migrations` 的版本与 checksum，缺失或漂移时保持 503。

必填配置：

| 环境变量 | 用途 |
|---|---|
| `TRPC_POSTGRES_DSN` | PostgreSQL 连接串 |
| `TRPC_REDIS_ADDRESS` | Redis 地址（仅 `artifact` role） |
| `TRPC_S3_REGION` / `TRPC_S3_BUCKET` | Artifact bucket |
| `TRPC_CLAMAV_ADDRESS` | clamd TCP 地址 |
| `TRPC_DLP_ENDPOINT` | DLP HTTPS base URL |
| `TRPC_SECRET_ROOT` | 只读 CSI Secret Store 挂载目录 |
| `TRPC_DLP_SECRET_REF` / `TRPC_DLP_SECRET_VERSION` | DLP credential 的不可变引用与版本 |
| `TRPC_DLP_BACKEND_VERSION` | DLP backend profile 的固定版本 |
| `TRPC_PAYLOAD_KEY_REF` / `TRPC_PAYLOAD_KEY_VERSION` | tenant-scoped payload AES-256 key 的稳定引用与 generation |
| `TRPC_DLP_PROBE_TENANT_ID` | readiness 使用的授权探测 tenant |

`channel` role 只需要 PostgreSQL、`TRPC_SECRET_ROOT`、`TRPC_PAYLOAD_KEY_REF/VERSION` 和 `TRPC_CHANNEL_PROBE_TENANT_ID`；它不要求 Redis、S3、ClamAV 或 DLP。公开入口固定为 `/callbacks/feishu` 与 `/callbacks/wecom`，普通 callback 仅在 payload 写入及 Inbox/PreprocessJob 原子 claim 成功后返回 provider ACK；URL challenge 复用同一 opaque route、candidate 和 scoped verifier，但不创建 Inbox。`TRPC_CHANNEL_CANDIDATE_TTL` 默认 `30s`，`TRPC_CHANNEL_CALLBACK_MAX_BODY` 默认 `1 MiB`，分别限制候选有效期与 callback body。

`channel-delivery` role 需要 PostgreSQL、Redis、`TRPC_SECRET_ROOT`、`TRPC_PAYLOAD_KEY_REF/VERSION`、`TRPC_CHANNEL_PROBE_TENANT_ID`、`TRPC_REDIS_ENVIRONMENT` 和 `TRPC_CHANNEL_DELIVERY_GROUP`，但不要求 S3、ClamAV 或 DLP。它从所有未 disabled tenant 的历史 published binding 发现账号 stream，动态启动 Redis consumer/reclaim，并以 ReplyEvent 固定的 config version 解析 `channel_send` Secret；配置轮换不得删除旧 generation，直到旧 reply/pending 已完成。`TRPC_CHANNEL_DELIVERY_REFRESH`、`TRPC_CHANNEL_REPLY_READ_BLOCK`、`TRPC_CHANNEL_REPLY_RECLAIM_IDLE/INTERVAL/LIMIT`、`TRPC_CHANNEL_DELIVERY_CLAIM_TTL/CLAIM_RENEW`、`TRPC_CHANNEL_DELIVERY_RETRY_DELAY/MAX_RETRY/MAX_ATTEMPTS/MAX_RECONCILE` 和 `TRPC_CHANNEL_PROVIDER_TIMEOUT` 控制发现、领取、重试和 provider deadline；claim renew 必须短于 claim TTL。

`worker` role 需要 PostgreSQL、Redis、versioned S3、`TRPC_SECRET_ROOT`、`TRPC_PAYLOAD_KEY_REF/VERSION`、`TRPC_REDIS_ENVIRONMENT`、`TRPC_WORKER_GROUP`、`TRPC_WORKER_CONTROL_GROUP` 和 `TRPC_WORKER_PROBE_TENANT_ID`。Worker 从 PostgreSQL 读取 Envelope 固定的 Agent/Config/Model 版本，以 `deepseek` Catalog schema v1 构造官方 OpenAI-compatible client；DeepSeek API key 不读取环境变量，必须按 ModelProfile 的 `secret_ref + secret_version` 投影到 CSI SecretProvider，并授权 `purpose=model_call`、`subject=worker-model`、`resource=profile_id`、`resource_version=profile_version`。缺少 key、generation 不匹配、非官方 endpoint 或未实现的 Tool/Skill/Checkpoint resolver 均保持 fail closed。

`gateway` role 需要 PostgreSQL、`TRPC_SECRET_ROOT`、`TRPC_PAYLOAD_KEY_REF/VERSION`、`TRPC_GATEWAY_PROBE_TENANT_ID`、`TRPC_GATEWAY_AUTH_SECRET_REF/VERSION`。Gateway auth secret 使用 `purpose=gateway_auth`、`subject=gateway`、`resource=gateway-auth` 和固定 generation 的 scoped SecretProvider 坐标；它是签发 `gw1.<payload>.<hmac>` bearer token 的进程级验签密钥，不能放在环境变量或日志。token 必须携带 tenant/version、principal/user、agent app/session、权限和 `iat/exp/jti`；Gateway 回查当前 Tenant，disabled、版本漂移、过期、签名错误或缺少 Idempotency-Key 均 fail closed；suspended tenant 仍可读取状态/终态，但不能创建新执行。Gateway 公开 `/v1/agent-runs`、`/v1/agent-runs/{request}/events` 和 OpenAI `/v1/chat/completions`，body/header 中的 tenant/user/session 只作待校验 locator，不能覆盖 token 的 canonical identity。OpenAI façade 通过 `ProtocolInvocationMiddleware + CanonicalRunner + GatewayRunnerBridge` 提交共享 durable Inbox，执行仍由 Worker 负责。

Worker 的 shard 由 `TRPC_WORKER_SHARD_COUNT` 与可选 `TRPC_WORKER_SHARDS` 指定；省略 shard 列表时接管 `0..count-1`，多实例部署应显式分配不重叠的 shard。`TRPC_WORKER_LEASE_RENEW` 必须短于 `TRPC_WORKER_LEASE_TTL`，`TRPC_WORKER_DRAIN_TIMEOUT + TRPC_WORKER_BUNDLE_CLOSE_TIMEOUT` 必须不超过 `TRPC_SHUTDOWN_TIMEOUT`（还应为 HTTP shutdown 留出余量）。

`preprocess` role 另外要求 `TRPC_PREPROCESS_ARTIFACT_RETENTION`（整秒、至少 1 秒），并支持 `TRPC_PREPROCESS_BATCH_SIZE`、`TRPC_PREPROCESS_LEASE_TTL`、`TRPC_PREPROCESS_RETRY_DELAY`、`TRPC_PREPROCESS_POLL_INTERVAL`、`TRPC_PREPROCESS_MAX_ATTEMPTS` 和 `TRPC_PREPROCESS_MEDIA_FETCH_TIMEOUT`。未配置 provider-specific token resolver 时，Feishu/WeCom opaque media ID 不会被当作 URL；只有显式安装的 generic HTTPS host 白名单（`TRPC_PREPROCESS_MEDIA_ALLOWED_HOSTS`）可下载通用 URL，其他媒体 fail closed。

AWS 凭据由官方 SDK default credential chain 解析，优先使用 Kubernetes Web Identity、实例角色或其他 workload identity。不得把凭据写入普通 ConfigMap、命令行参数或日志。

## M3 外部 Provider 最终验收

真实 Feishu/WeCom 凭据与外部 API 验证按 [M2-SD-003](../design/8.m2-scope-decisions.md) 移到 M3 最终验收，不阻塞无企业资源环境中的 M2 仓库内闭环。该 smoke 会产生可见消息，只能对授权测试消息和测试用户执行。凭据必须分别放在权限不宽于 `0600` 的绝对路径文件中：Feishu 文件使用严格 `{"app_id":"...","app_secret":"..."}`，WeCom Agent 文件使用严格 `{"corp_id":"...","corp_secret":"...","agent_id":123}`。环境变量只携带文件路径与非密钥 locator，不得携带 Secret 值。

推荐复制模板后一键执行；本机没有 Go 时脚本自动使用 Docker：

```bash
cp deploy/compose/.env.m3-im.example deploy/compose/.env.m3-im
chmod 600 /absolute/secret/feishu.json /absolute/secret/wecom.json
bash scripts/m3_im_provider_smoke_test.sh
```

`.env.m3-im` 只填写 secret 文件绝对路径、App/Corp ID、飞书已有授权消息 ID 和企微测试用户 ID。`TRPC_M3_IM_PROVIDERS` 可取 `feishu`、`wecom` 或 `feishu,wecom`。测试只有在所有选中 Provider 返回有效 provider message ID 时通过。该入口验证真实 provider credential/API，不替代 production PostgreSQL binding locator 与 CSI Secret scope contract；后者继续由组合根和 repository/credential tests 验收。WeCom Bot/WebSocket 与群聊 mention 已按 [M2-SD-001](../design/8.m2-scope-decisions.md)正式移出 M2，不能用本 smoke 宣称 Bot 能力。

## 本地 WebUI Channel 验收

没有 Feishu/WeCom 企业凭据时，可在 `channel` 与 `channel-delivery` role 同时设置 `TRPC_WEBUI_ENABLED=true`，发布正常的 `channel=webui` ChannelBinding 后访问 `/webui/`。verification material 必须是严格 JSON：`{"token":"至少 16 字符的随机值","external_account_id":"local-webui"}`。浏览器只在页面内存保存 token，并以时间戳、nonce 和 HMAC-SHA256 签名请求；服务端仍使用 opaque route、candidate scoped verifier、durable callback、身份/Session、Preprocess、Reply Queue 与 Delivery Ledger，回复正文仍从加密 ResultStore 读取，WebUI mailbox 不保存明文。

本地一键验收可使用 Compose 的 `webui` profile。先将 DeepSeek API key 单独写入被 git 忽略的 `deploy/compose/secrets/deepseek-api-key`，再执行：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml --profile webui up --build
```

访问 `http://localhost:58081/webui/`，默认 Route Key/Account ID 为 `local-webui`，Channel Token 为 `local-webui-token-change-me`。本 profile 的 `webui-local` 组合根只用于隔离本地环境：它自动应用 embedded migrations、幂等发布本地 tenant/DeepSeek profile/Agent App/ChannelBinding，并组合既有 Preprocess、Dispatch/Wakeup/Reply Relay、Redis Worker、Reply Queue、Delivery Ledger 和 `webui.Adapter`。更换 token 或 Secret generation 前应使用 `docker compose -f deploy/compose/docker-compose.m2.yml --profile webui down -v` 清理一次性本地数据；生产环境不得自动 migration/seed，也不得使用默认 token。

该验收用于证明 provider-neutral 内部链路、重启恢复与重复投递语义，不代表 Feishu/WeCom 的真实凭据、外部网络和 provider API 已通过。真实 provider smoke 仍按上一节在授权环境执行，范围口径见 [M2-SD-002/M2-SD-003](../design/8.m2-scope-decisions.md)。

DLP bearer 不再接受明文环境变量。部署生成器必须调用 `trpcservice/secrets/filesystem.StableFilename`，为每个获授权的完整坐标 `(tenant, subject, purpose, resource, resource_version, ref, ref_version)` 在 `TRPC_SECRET_ROOT` 投影一个文件；DLP 使用 `subject=tenant`、`purpose=backend_connect`、`resource=http-dlp`，payload key 使用 `purpose=payload_encrypt`、`resource=messaging-payload` 且 `resource_version=TRPC_PAYLOAD_KEY_VERSION`。文件名是不透明 SHA-256 标识，SecretRef 不会被当作路径。目录不得 group/world-writable，文件必须为普通、非空、至多 64 KiB 且权限为 `0400` 或 `0600`；volume 必须只读挂载。

同一 `ref_version` 的内容是不可变事实：轮换时先投影新版本文件，再发布新的 `TRPC_DLP_SECRET_VERSION` 并滚动实例，最后在旧 generation 排空后移除旧文件。不得原地改写同版本文件；进程会检测运行期间的内容漂移并以 version mismatch fail closed。每个业务 tenant 需要自己的授权坐标文件；缺少该文件即视为未授权，不回退到 probe tenant 或全局 token。

常用可选配置：

| 环境变量 | 默认值 |
|---|---|
| `TRPC_LISTEN_ADDRESS` | `:8080` |
| `TRPC_REDIS_DB` | `0` |
| `TRPC_S3_ENDPOINT` | AWS SDK 默认 endpoint |
| `TRPC_S3_PATH_STYLE` | `false` |
| `TRPC_ARTIFACT_MAX_BYTES` | `16777216` |
| `TRPC_PROBE_TIMEOUT` / `TRPC_PROBE_INTERVAL` | `5s` / `15s` |
| `TRPC_ARTIFACT_PUT_TIMEOUT` | `30s` |
| `TRPC_ARTIFACT_UPLOAD_PROTECTION` | `2m`，必须长于 put timeout |
| `TRPC_ARTIFACT_ORPHAN_GRACE` | `24h` |
| `TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE` | `100` |
| `TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS` | `8` |
| `TRPC_SHUTDOWN_TIMEOUT` | `30s`（`worker` role 默认 `45s`） |
| `TRPC_PREPROCESS_MEDIA_ALLOWED_HOSTS` | 空；generic HTTPS 媒体默认禁用 |
| `TRPC_GATEWAY_PROBE_TENANT_ID` | Gateway readiness 的 Secret/payload-key 探测 tenant |
| `TRPC_GATEWAY_AUTH_SECRET_REF` / `TRPC_GATEWAY_AUTH_SECRET_VERSION` | `purpose=gateway_auth` HMAC key 的稳定引用与 generation |
| `TRPC_GATEWAY_MAX_BODY` | `1048576`；run request body 上限，最大 16 MiB |
| `TRPC_GATEWAY_SSE_POLL_INTERVAL` / `TRPC_GATEWAY_SSE_REPLAY_LIMIT` | `1s` / `64`；SSE 轮询与单页回放上限 |
| `TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS` | `128`；单实例 SSE 并发上限 |

`TRPC_REDIS_ADDRESS` 只对 `artifact` role 必填；`preprocess` 和 `channel` 不直接依赖 Redis。Channel route/binding 必须由受控控制面预先写入 `channel_public_route`、`channel_binding_locator`；callback 进程不会根据公开 query 或环境变量猜测 tenant/binding，也不会自动创建路由。

只有本地 MinIO/DLP smoke 可显式设置 `TRPC_S3_ALLOW_INSECURE=true` 或 `TRPC_DLP_ALLOW_INSECURE=true`；生产保持 false。

## 健康与关闭

- `/livez`：进程可运行时返回 200。
- `/readyz`：Lifecycle ready 且角色声明的 PostgreSQL、全部 migration checksum、外部 provider、Secret scope 和 payload-key exact generation 最近一次全部成功时返回 200，否则 503。Gateway 额外要求 `gateway_auth` Secret scope 和 probe tenant payload key；Artifact/Worker/Channel/Preprocess/Delivery 只检查各自声明的依赖，不把未使用的 provider 强行纳入 readiness。
- SIGTERM/SIGINT：立即进入 draining 并使 readiness 变为 503；`artifact` 随后停止两个 reconciler，`preprocess` 停止新的 preprocess/dispatch claim，关闭 HTTP 并等待后台退出，均受 `TRPC_SHUTDOWN_TIMEOUT` 限制。
- `worker` 在 draining 后停止新的 Broker/control 消费，等待已接收 delivery 在有效 lease/fence 下完成或到达有界 drain deadline，再关闭 HTTP、RuntimeBundle 和后台 goroutine；execution-control hint 丢失不影响正确性，Consumer 始终回查 PostgreSQL `CancelRequested`。
- `channel` 在 draining 后不再接受新的 callback/challenge 处理，关闭 HTTP 并等待 readiness monitor；未完成的数据库事务由平台重试，candidate 仍受一次性/过期边界保护。
- `gateway` 在 draining 后立即使 `/readyz` 返回 503 并拒绝新的 `POST /v1/agent-runs`；已建立的 status/SSE 请求按 HTTP shutdown deadline 收敛，OpenAI bridge 只关闭事件订阅，不取消已持久化执行。

依赖故障不会通过退出进程触发重启风暴；实例保持 live、ready=false，并在下一 probe 周期自动恢复。

## 隔离处置

对象 digest/version 不一致会立即进入 `quarantined`，临时删除失败达到最大次数后也会隔离。当前组合根记录 tenant/artifact 标识供告警采集，但尚未实现正式告警 sink 或自动解隔离。处置前不得手工删除对象或修改 digest；先核对 `media_artifact`、`artifact_object_upload` 和对象精确 VersionID。

迁移前的 Artifact 默认 `retention_managed=false`，不会自动回收。历史 backfill 必须在能够重建明确引用和保留期限后通过单独受控任务执行。
