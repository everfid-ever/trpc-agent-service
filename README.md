# tRPC Agent Service

本项目是一个可在 **Docker Desktop** 上完整运行和验证的多租户、节点化 Agent 服务。它将 tRPC-Agent-Go 的 Runner 接入租户配置、共享持久状态、治理与审计，并提供 WebUI、飞书和企业微信 Channel Adapter。

交付运行边界是本机 Docker：不依赖 Kubernetes、云账号或托管运维资源。真实 DeepSeek、飞书和企业微信账号可由开发者按需接入，用于本机端到端验证。

## 已实现能力

- 多租户配置、密钥作用域、会话、Inbox/Outbox、审计与合规账本均持久化于 PostgreSQL；Redis 提供共享调度、租约和跨节点协调。
- Gateway、Worker、relay、delivery 与 wakeup consumer 可部署为独立节点；本地 smoke 覆盖两个租户、两个 Worker 及单节点故障后的持续服务。
- WebUI、飞书、企业微信回调均经过验签、去重、身份映射、耐久化 ingress、预处理、异步 Runner 和官方回复 API。
- 文本、图片和文件附件走受限大小的预处理与租户隔离 artifact 流；本地 ClamAV 扫描后，以 DeepSeek 多模态模型处理可访问的媒体内容。
- 模型、工具、知识、治理策略和通道绑定均按租户/版本解析；危险操作支持 durable confirmation。
- OpenTelemetry、Jaeger、Prometheus 指标、审计查询/保留/销毁及本地恢复演练均可在 Docker Desktop 中验证。

## 快速开始

前置条件：Docker Desktop（含 Docker Compose v2），以及一个可消费的 DeepSeek API Key。

~~~
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

mkdir -p deploy/compose/secrets
install -m 600 /absolute/path/to/deepseek-api-key \
  deploy/compose/secrets/deepseek-api-key

./start.sh
~~~

打开：

- WebUI：[http://localhost:58081/webui/](http://localhost:58081/webui/)
- Jaeger：[http://localhost:56686/](http://localhost:56686/)

停止服务但保留本地验证数据：

~~~
./stop.sh
~~~

本地数据重置是显式操作，执行前请确认不需要当前 Docker 卷：

~~~
docker compose -f deploy/compose/docker-compose.local.yml --profile webui down -v
~~~

## 本地配置

可从示例创建本机配置；配置文件仅存 locator、端口和非敏感开关，API Key 与 IM 密钥必须放在忽略的 owner-only 文件中。

~~~
cp deploy/compose/.env.local.example deploy/compose/.env.local
~~~

| 场景 | 必需的本机资源 | 启动入口 |
| --- | --- | --- |
| WebUI + DeepSeek 多模态对话 | secrets/deepseek-api-key | ./start.sh |
| 飞书单聊、群聊、图片 | DeepSeek Key、secrets/feishu.env、临时公网 HTTPS tunnel | docker compose -f deploy/compose/docker-compose.local.yml --profile feishu-local up -d --build |
| 企业微信回调与回复 | DeepSeek Key、secrets/wecom.env、临时公网 HTTPS tunnel | docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-local up -d --build |
| 两租户、两节点连续性 | DeepSeek Key | bash scripts/local_multinode_smoke.sh |
| PostgreSQL/Redis 短断恢复 | DeepSeek Key | bash scripts/local_dependency_recovery_smoke.sh |
| PostgreSQL、Redis、Qdrant、Vault adapter | Docker Desktop | bash scripts/backend_adapter_smoke.sh |

完整的密钥文件格式、飞书/企业微信回调地址、群聊验证、媒体限制和排障步骤见 [本地运行手册](docs/runbook/getting-started.md)。

## 验证入口

~~~
# 静态检查、单测、迁移与依赖边界
bash scripts/ci_admission.sh

# race 检查
bash scripts/ci_admission.sh --race

# 独立后端 adapter smoke（会自动清理自己的容器与卷）
bash scripts/backend_adapter_smoke.sh

# 两租户、两个 WebUI/Worker 节点和节点故障连续性
bash scripts/local_multinode_smoke.sh

# PostgreSQL / Redis 短断后无需重启的恢复能力
bash scripts/local_dependency_recovery_smoke.sh
~~~

真实 IM 和模型调用只在开发者显式提供本机密钥后运行；测试密钥、图片和回调内容不得提交至仓库。每项验证的成功证据与资源边界见 [可靠性、发布与容量手册](docs/runbook/reliability-release-capacity.md)。

## 架构与交付文档

- [架构总览](docs/design/0.architecture.md)：组件拓扑、租户隔离、请求时序和跨节点职责。
- [租户根设计](docs/design/1.tenant-root-design.md)：配置、版本、密钥和授权边界。
- [存储与数据一致性](docs/design/3.storage-data-consistency-design.md)：Inbox/Outbox、幂等、relay、迁移和多后端路由。
- [可观测、审计与运维](docs/design/5.observability-audit-devops-design.md)：trace、指标、审计、保留策略、故障处理、容量与回滚。
- [本地 Compose 说明](deploy/compose/README.md)：profile 与运行面职责。
- [本地运行手册](docs/runbook/getting-started.md)：命令、密钥、IM 回调和故障排查。

## 代码布局

~~~
cmd/trpc-service/     服务进程与本地运行角色
trpcservice/          租户、gateway、worker、channel、存储、治理、审计等实现
migrations/           主业务库的追加式 PostgreSQL migration
compliancemigrations/ 独立合规账本的追加式 migration
deploy/compose/       Docker Desktop 运行面、示例配置和 secrets 目录
scripts/              Admission、adapter、多节点与恢复验证入口
docs/                 架构设计和运行手册
~~~

迁移编号、合规账本及 durable 数据是历史契约，不应通过删除或改名“清理”。本项目当前交付的是可复现的本地 Docker 验证环境；生产基础设施和外部托管能力不在该交付范围内。
