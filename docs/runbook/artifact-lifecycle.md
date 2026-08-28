# Artifact lifecycle role runbook

`cmd/trpc-service` 当前只组合 Artifact lifecycle、依赖 readiness 和 health listener。它不会挂载 IM callback、Gateway、Worker 或 Admin 路由，也不会创建 InMemory fallback。

## 启动前置条件

必须先通过受控部署步骤应用仓库全部 PostgreSQL migration。进程不会自动执行 migration；readiness 会逐项核验 `schema_migrations` 的版本与 checksum，缺失或漂移时保持 503。

必填配置：

| 环境变量 | 用途 |
|---|---|
| `TRPC_POSTGRES_DSN` | PostgreSQL 连接串 |
| `TRPC_REDIS_ADDRESS` | Redis 地址 |
| `TRPC_S3_REGION` / `TRPC_S3_BUCKET` | Artifact bucket |
| `TRPC_CLAMAV_ADDRESS` | clamd TCP 地址 |
| `TRPC_DLP_ENDPOINT` | DLP HTTPS base URL |
| `TRPC_SECRET_ROOT` | 只读 CSI Secret Store 挂载目录 |
| `TRPC_DLP_SECRET_REF` / `TRPC_DLP_SECRET_VERSION` | DLP credential 的不可变引用与版本 |
| `TRPC_DLP_BACKEND_VERSION` | DLP backend profile 的固定版本 |
| `TRPC_DLP_PROBE_TENANT_ID` | readiness 使用的授权探测 tenant |

AWS 凭据由官方 SDK default credential chain 解析，优先使用 Kubernetes Web Identity、实例角色或其他 workload identity。不得把凭据写入普通 ConfigMap、命令行参数或日志。

DLP bearer 不再接受明文环境变量。部署生成器必须调用 `trpcservice/secrets/filesystem.StableFilename`，为每个获授权的完整坐标 `(tenant, subject, purpose, resource, resource_version, ref, ref_version)` 在 `TRPC_SECRET_ROOT` 投影一个文件；DLP 使用 `subject=tenant`、`purpose=backend_connect`、`resource=http-dlp`。文件名是不透明 SHA-256 标识，SecretRef 不会被当作路径。目录不得 group/world-writable，文件必须为普通、非空、至多 64 KiB 且权限为 `0400` 或 `0600`；volume 必须只读挂载。

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
| `TRPC_SHUTDOWN_TIMEOUT` | `30s` |

只有本地 MinIO/DLP smoke 可显式设置 `TRPC_S3_ALLOW_INSECURE=true` 或 `TRPC_DLP_ALLOW_INSECURE=true`；生产保持 false。

## 健康与关闭

- `/livez`：进程可运行时返回 200。
- `/readyz`：Lifecycle ready 且 PostgreSQL、全部 migration checksum、Redis、versioned S3、ClamAV engine version、probe tenant 的精确 Secret scope 解析、授权 DLP probe 最近一次全部成功时返回 200，否则 503。
- SIGTERM/SIGINT：立即进入 draining 并使 readiness 变为 503；随后停止两个 reconciler 的新领取、关闭 HTTP 并等待后台退出，受 `TRPC_SHUTDOWN_TIMEOUT` 限制。

依赖故障不会通过退出进程触发重启风暴；实例保持 live、ready=false，并在下一 probe 周期自动恢复。

## 隔离处置

对象 digest/version 不一致会立即进入 `quarantined`，临时删除失败达到最大次数后也会隔离。当前组合根记录 tenant/artifact 标识供告警采集，但尚未实现正式告警 sink 或自动解隔离。处置前不得手工删除对象或修改 digest；先核对 `media_artifact`、`artifact_object_upload` 和对象精确 VersionID。

迁移前的 Artifact 默认 `retention_managed=false`，不会自动回收。历史 backfill 必须在能够重建明确引用和保留期限后通过单独受控任务执行。
