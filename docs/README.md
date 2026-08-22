# 文档目录

[`design/README.md`](./design/README.md) 是多租户、多节点服务设计的入口。该目录中的文档定义本仓库的规范性架构、跨组件契约、实现阶段和验收门禁。

设计以 `trpc-agent-service` 为独立服务边界，只依赖 `trpc-agent-go` 的公共 API；任何设计不得依赖另一个 Go module 的 `internal` 包。
