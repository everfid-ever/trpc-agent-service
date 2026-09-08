# Kubernetes base

此目录是 `gateway` 与 `worker` 两个现有二进制 role 的最小 Kustomize
组合根，不是带凭据的生产发布配置。

- Overlay 必须将 `ghcr.io/liuzengh/trpc-agent-service:replace-me` 替换为已审核
  的不可变镜像 digest，并提供 `trpc-service-runtime` ConfigMap 与
  `trpc-service-runtime-secrets` Secret/CSI projection。
- `worker` 没有 Service，且通过 `trpc-worker-deny-ingress` 拒绝 Pod ingress；
  它只从 Redis broker 消费工作。依赖出口策略应由环境 overlay 按 PostgreSQL、Redis、
  ObjectStore、模型、MCP 与 DNS 的实际地址显式放行。
- 两个 Deployment 都以无 shell 的 `prestop` role 对 PID 1 发送 `SIGTERM`，等待
  readiness 进入 draining；120 秒 termination grace 覆盖现有有界 shutdown/drain。
- 不在这里定义 `schema-migrate` Job。生产 schema migration 必须由唯一的 Helm
  `pre-install`/`pre-upgrade` hook 承载，固定镜像 digest 与 expected-current/target，
  不能同时维护裸 Job 和 chart hook 两条发布路径。

本地验证渲染：

```bash
kubectl kustomize deploy/kubernetes/base
```
