# 本地 Compose 环境

这是仓库唯一受支持的运行环境：Docker Desktop 本地验收，而非生产部署模板。

完整启动、验证与清理步骤见 [`docs/runbook/getting-started.md`](../../docs/runbook/getting-started.md)。

- `webui` profile：PostgreSQL、Redis、Jaeger、OTel Collector 和 `webui-local`；需要本机 Docker secret 中的 DeepSeek Key。
- `webui-multinode` profile：先运行一次性 `webui-local-bootstrap`，再启动两个独立的 WebUI composition node；它们共享 PostgreSQL/Redis，使用不同 Worker、relay、delivery 和 wakeup consumer ID。运行 `bash scripts/local_multinode_smoke.sh` 会先执行真实 PostgreSQL/Redis 的两租户、两 Worker 集成 slice，再验证一个 WebUI 节点停止后另一个节点仍然 ready。
- `runtime-test` profile：本地 PostgreSQL/Redis migration 与恢复契约。
- `docker-compose.minimal.yml`：由 `bash scripts/minimal_backend_smoke.sh` 创建并自动销毁的 PostgreSQL、Redis、Qdrant、Vault smoke 环境。

Compose 环境的数据只用于开发和测试。不要把默认 Token、默认密码或容器卷复制到真实环境。
