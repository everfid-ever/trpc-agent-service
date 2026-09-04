# 文档目录

[`design/README.md`](./design/README.md) 是多租户、多节点服务技术方案的入口。设计文档描述系统当前采用的架构、领域模型、协议、数据一致性、安全边界、运行能力和验证规范，不记录开发轮次、里程碑、提交或迁移文件编号。

第一次启动请阅读 [`runbook/getting-started.md`](./runbook/getting-started.md)。可靠性故障策略、灰度/回滚、容量报告与最小/生产推荐拓扑见 [`runbook/reliability-release-capacity.md`](./runbook/reliability-release-capacity.md)。本仓库的可执行验收边界是 Docker Desktop：本地 Compose、单元/契约测试与最小后端 smoke；不提供 Kubernetes 发布、远端告警或生产 Provider 验收资产。

设计以 `trpc-agent-service` 为独立服务边界，只依赖 `trpc-agent-go` 的公共 API；任何生产代码和技术方案都不得依赖其他 Go module 的 `internal` 包。
