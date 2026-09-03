# 本地 Docker Desktop 验收指南

本仓库只提供本地 Docker Desktop 验收入口。PostgreSQL、Redis、Vault、Qdrant、Jaeger 和 OpenTelemetry Collector 都运行在本机容器中；不需要 Kubernetes、云主机、Prometheus Alertmanager 或外部运维资源。

DeepSeek 是唯一默认启用的外部调用。API Key 只用于本机容器中的真实模型调用，绝不能提交到仓库。

## 1. 前置条件

- Docker Desktop（包含 `docker compose` v2）；
- 一个可用的 DeepSeek API Key；
- 本机端口 `55432`、`56379`、`58081`、`56686`、`59464` 未被占用。

## 2. 启动本地 WebUI

在仓库根目录执行：

```bash
mkdir -p deploy/compose/secrets
install -m 600 /absolute/path/to/deepseek-api-key \
  deploy/compose/secrets/deepseek-api-key
./start.sh
```

或直接使用 Compose：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui up --build
```

访问：

- WebUI：<http://localhost:58081/webui/>；
- Jaeger Trace：<http://localhost:56686/>；
- Collector Prometheus 格式指标：<http://localhost:59464/metrics>。

页面默认 Route Key 和 Account ID 都是 `local-webui`，Channel Token 是 `local-webui-token-change-me`。点击连接后，可发送：

```text
创建一条标题为验收、内容为 WebUI durable confirmation 的笔记
```

页面应展示持久化的“批准一次 / 拒绝”卡片。批准后，Graph 从 Redis checkpoint 恢复并只执行一次本地 note Tool；刷新页面不会重复执行。消息链路为：

```text
WebUI → HMAC callback → Inbox / Preprocess → Redis dispatch
→ Worker + DeepSeek → Result / Reply Outbox → Delivery Ledger → WebUI mailbox
```

WebUI 仅验证 provider-neutral 的本地链路；Feishu 和 WeCom 的 adapter 在本仓库保留离线协议与契约测试，但没有真实账号验收承诺。

## 3. 多节点 Docker 验收

多租户/节点化代码不会因本地验收而被简化。`webui-multinode` profile 先建立同一套本地 tenant、ConfigSnapshot、Graph 与 scoped secret fixture，再启动两个独立容器；两个容器共享 PostgreSQL 和 Redis、分别使用唯一 Worker/relay/delivery consumer ID。

```bash
bash scripts/local_multinode_smoke.sh
```

该 smoke 使用随机 Compose 项目和随机本机端口。它先在临时 PostgreSQL 16 数据库中创建两个 tenant、运行两个 Redis-backed Worker 并验证 tenant scope，再依次确认 node A、node B 的 `/readyz`，停止 node A，确认 node B 保持 ready。成功后自动删除本次容器和卷；失败日志路径会打印到终端。若要手动查看两个节点页面：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui-multinode up --build
```

默认访问地址为 `http://localhost:58083/webui/` 和 `http://localhost:58084/webui/`。

停止本地环境但保留数据：

```bash
./stop.sh
```

需要丢弃所有本地测试数据、修改 Token 或更换 Key 时：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile webui down -v
```

## 4. 本地验证命令

```bash
# 纯 Go 静态、单元与 race 检查（Go 1.21）
bash scripts/ci_admission.sh
bash scripts/ci_admission.sh --race

# Disposable PostgreSQL 16、Redis 7、Qdrant、Vault 真适配器 smoke
bash scripts/minimal_backend_smoke.sh

# PostgreSQL / Redis runtime slice
docker compose -f deploy/compose/docker-compose.m2.yml up -d postgres redis
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile runtime-test run --rm runtime-test
```

`minimal_backend_smoke.sh` 成功后会删除它创建的容器和卷；失败时会保留临时 Compose 日志路径。所有验收都只针对本机 Docker 环境，不得把 WebUI、fixture 或 fake adapter 的成功表述为真实 Feishu/WeCom、云对象存储、DLP 或生产集群已通过。

## 5. 本地边界

- 结构化 JSON 日志默认脱敏；`TRPC_LOG_LEVEL` 可设 `debug|info|warn|error`，`TRPC_LOG_MASKING_LEVEL` 可设 `none|basic|strict`。
- Collector 或 Jaeger 停止不会阻断本地业务链路；Trace 可见性是本地诊断辅助，而非远端 SLO 告警。
- MinIO、ClamAV、DLP、Feishu、WeCom 和 Kubernetes 不属于本地闭环必需项；相关 adapter 与代码级测试保留，外部 smoke 和运维资产不再维护。
- `deploy/compose/secrets/` 已被 Git 忽略；不得用 `git add -f` 加入 API Key 或其他凭据。
