# 本地 Docker Desktop 验收指南

本仓库只提供本地 Docker Desktop 验收入口。PostgreSQL、Redis、Vault、Qdrant、Jaeger 和 OpenTelemetry Collector 都运行在本机容器中；不需要 Kubernetes、云主机、Prometheus Alertmanager 或外部运维资源。

DeepSeek 是唯一默认启用的外部调用。API Key 只用于本机容器中的真实模型调用，绝不能提交到仓库。可选的 Feishu 与 WeCom smoke 会使用开发者自行创建的应用和临时 HTTPS tunnel；它们同样只服务于本机 Docker 验收。

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

WebUI 验证 provider-neutral 的本地链路。Feishu 与 WeCom 另有下面各自的可选真实账号 smoke。

`webui-local`、`feishu-local` 和 `wecom-local` 都是同一套本地 PostgreSQL/Redis composition 的完整 Worker runtime，不能并行启动。它们会通过 PostgreSQL advisory lock 拒绝并发启动，避免跨渠道争抢 durable work 或读取不到对方的 scoped secret。切换 profile 前先停止当前的 standalone local service：

```bash
docker compose -f deploy/compose/docker-compose.m2.yml stop \
  webui-local feishu-local wecom-local
```

`webui-multinode` 使用独立 Compose project 与显式节点 ID，不受此限制。

## 3. 可选：真实 Feishu 本地验收

此 smoke 运行完整的 Feishu Webhook、验签、durable ingress、Redis Worker、DeepSeek 调用和 Feishu Reply API。数据库、Worker 和可观测性组件仍全部运行在 Docker Desktop；唯一的外部依赖是开发者自己的 Feishu 应用、DeepSeek Key 和临时 HTTPS tunnel。

先创建仅供本机使用的凭据文件：

```bash
mkdir -p deploy/compose/secrets
cp deploy/compose/feishu.env.example deploy/compose/secrets/feishu.env
chmod 600 deploy/compose/secrets/feishu.env
```

编辑 `deploy/compose/secrets/feishu.env`，填入同一个 Feishu 应用的 `FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`FEISHU_VERIFICATION_TOKEN` 和 `FEISHU_ENCRYPT_KEY`。`FEISHU_BOT_OPEN_ID` 对私聊可留空；要验收群聊 @mention 时必须填入该机器人的 Open ID。不要给值添加引号或反引号。这个文件被 Git 忽略，严禁强制加入版本库。

启动本地服务：

```bash
M2_FEISHU_LOCAL_PORT=58086 \
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile feishu-local up -d --build

curl --fail http://localhost:58086/readyz
```

然后用开发者选择的临时 HTTPS tunnel 将宿主机 `58086` 暴露出去。例如，已安装 Docker Desktop 时可运行一次性 Cloudflare Quick Tunnel：

```bash
docker run --rm --name trpc-feishu-tunnel \
  cloudflare/cloudflared:latest tunnel --no-autoupdate \
  --url http://host.docker.internal:58086
```

从 tunnel 输出中复制 `https://<temporary-host>.trycloudflare.com`。在 Feishu 开放平台的“事件与回调”中选择 **Webhook 接收事件**，而不是“长连接接收事件”，并配置：

```text
https://<temporary-host>.trycloudflare.com/callbacks/feishu?route_key=local-feishu
```

使用同一份 `Verification Token` 和 `Encrypt Key` 完成 URL 验证，订阅 `im.message.receive_v1`，开启机器人能力及 `im:message` 权限，并在每次变更后发布应用版本。先私聊机器人发送一条新消息；成功时机器人会调用 DeepSeek 并回复。群聊验收通过“群设置 → 群机器人 → 添加机器人”将机器人加入群，再从 @ 菜单选择机器人发送消息；普通未 @ 的群消息会被安全地忽略。

排查顺序：

1. `docker logs trpc-agent-m2-feishu-local-1`：`app secret invalid` 表示 `FEISHU_APP_SECRET` 与 App ID 不匹配；更换 Secret 后必须重建服务。
2. `docker logs trpc-feishu-tunnel`：Quick Tunnel 重启会生成新域名，必须把新 URL 重新保存到 Feishu。
3. 若私聊成功但群聊无响应，确认使用 @ 菜单选择机器人，且 `FEISHU_BOT_OPEN_ID` 为当前机器人 Open ID。

App Secret 更新后的本地重启命令：

```bash
M2_FEISHU_LOCAL_PORT=58086 \
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile feishu-local up -d --force-recreate feishu-local
```

## 4. 可选：真实 WeCom 本地验收

此 smoke 运行完整的企业微信回调验签（GET URL 验证与加密消息）、durable ingress、Redis Worker、DeepSeek 调用和企业微信 `message/send` Reply API。数据库、Worker 和可观测性组件仍全部运行在 Docker Desktop；唯一的外部依赖是开发者自己的企业微信自建应用、DeepSeek Key 和临时 HTTPS tunnel。

先创建仅供本机使用的凭据文件：

```bash
mkdir -p deploy/compose/secrets
cp deploy/compose/wecom.env.example deploy/compose/secrets/wecom.env
chmod 600 deploy/compose/secrets/wecom.env
```

编辑 `deploy/compose/secrets/wecom.env`，填入同一个企业微信自建应用的 `WECOM_CORP_ID`、`WECOM_AGENT_ID`、`WECOM_CALLBACK_TOKEN`、`WECOM_ENCODING_AES_KEY` 和 `WECOM_APP_SECRET`。`WECOM_AGENT_ID` 必须为正整数，且四项凭据必须属于同一应用；`WECOM_AGENT_ID` 启动时会强校验。不要给值添加引号或反引号。这个文件被 Git 忽略，严禁强制加入版本库。

启动本地服务：

```bash
M2_WECOM_LOCAL_PORT=58087 \
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile wecom-local up -d --build

curl --fail http://localhost:58087/readyz
```

企业微信回调 URL 只接受 80/443 端口，因此同样需要临时 HTTPS tunnel 将宿主机 `58087` 暴露出去：

```bash
docker run --rm --name trpc-wecom-tunnel \
  cloudflare/cloudflared:latest tunnel --no-autoupdate \
  --url http://host.docker.internal:58087
```

从 tunnel 输出中复制 `https://<temporary-host>.trycloudflare.com`。在企业微信管理后台该应用的“接收消息 → 设置 API 接收”中配置：

```text
https://<temporary-host>.trycloudflare.com/callbacks/wecom?route_key=local-wecom
```

`Token` 与 `EncodingAESKey` 必须与 `wecom.env` 一致；保存时企业微信会发起 GET URL 验证，本地服务完成回调验签并返回 echostr 后握手成功。先在手机端给该应用发一条消息；成功时应用会调用 DeepSeek 并通过官方 Reply API 回复。

排查顺序：

1. `docker logs trpc-agent-m2-wecom-local-1`：`WeCom local configuration is incomplete` 表示 `wecom.env` 缺字段或 `WECOM_AGENT_ID` 不是正整数；`existing WeCom local control plane is incompatible; recreate the Compose volume` 表示已持久化的绑定与当前 Corp ID 冲突，需按第 5 节清理卷后重建。
2. `docker logs trpc-wecom-tunnel`：Quick Tunnel 重启会生成新域名，必须把新 URL 重新保存到企业微信。
3. 回复失败且日志出现 `60020` 等 errcode：在企业微信管理后台“企业可信 IP”中放行本机出口 IP，否则 `gettoken` 会被拒绝。

App Secret 更新后的本地重启命令：

```bash
M2_WECOM_LOCAL_PORT=58087 \
docker compose -f deploy/compose/docker-compose.m2.yml \
  --profile wecom-local up -d --force-recreate wecom-local
```

## 5. 多节点 Docker 验收

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

## 6. 本地验证命令

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

`minimal_backend_smoke.sh` 成功后会删除它创建的容器和卷；失败时会保留临时 Compose 日志路径。除第 3、4 节中开发者亲自完成的 Feishu 与 WeCom real-account smoke 外，所有验收都只针对本机 Docker 环境，不得把 WebUI、fixture 或 fake adapter 的成功表述为云对象存储、DLP 或生产集群已通过。

## 7. 本地边界

- 结构化 JSON 日志默认脱敏；`TRPC_LOG_LEVEL` 可设 `debug|info|warn|error`，`TRPC_LOG_MASKING_LEVEL` 可设 `none|basic|strict`。
- 节点故障、IM 重试、PostgreSQL/Redis 短断、模型/Tool 超时的降级策略，以及灰度、回滚、容量和生产推荐拓扑，统一见 [`reliability-release-capacity.md`](reliability-release-capacity.md)。
- Collector 或 Jaeger 停止不会阻断本地业务链路；Trace 可见性是本地诊断辅助，而非远端 SLO 告警。
- MinIO、ClamAV、DLP 和 Kubernetes 不属于本地闭环必需项；相关 adapter 与代码级测试保留，外部 smoke 和运维资产不再维护。Feishu 与 WeCom 只分别支持第 3、4 节所述的开发者自有账号、本地 Docker 和临时 tunnel smoke，不承诺生产可用性或 tunnel 的稳定域名。
- `deploy/compose/secrets/` 已被 Git 忽略；不得用 `git add -f` 加入 API Key 或其他凭据。
