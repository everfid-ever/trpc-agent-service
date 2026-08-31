# 文档目录

[`design/README.md`](./design/README.md) 是多租户、多节点服务技术方案的入口。设计文档描述系统当前采用的架构、领域模型、协议、数据一致性、安全边界、运行能力和验证规范，不记录开发轮次、里程碑、提交或迁移文件编号。

第一次启动请阅读 [`runbook/getting-started.md`](./runbook/getting-started.md)。生产角色与故障处置见 [`runbook/artifact-lifecycle.md`](./runbook/artifact-lifecycle.md)，Kubernetes 发布、drain 与回滚见 [`../deploy/runbooks/kubernetes-rollout.md`](../deploy/runbooks/kubernetes-rollout.md)。运行手册可以记录真实命令、环境变量和脚本，但不承担实现进度记录。

设计以 `trpc-agent-service` 为独立服务边界，只依赖 `trpc-agent-go` 的公共 API；任何生产代码和技术方案都不得依赖其他 Go module 的 `internal` 包。
