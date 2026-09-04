# 本地验证矩阵

本文是当前交付物的可执行验证索引。所有命令以仓库根目录为工作目录；除明确标为“真实外部”的行外，验证仅使用 Docker Desktop 本地资源。真实凭据、图片和回调 payload 均为本机私有数据，严禁提交。

## 判定规则

- **通过**：命令退出码为零，且表中对应的成功证据可见。
- **真实外部**：需要开发者自有账号、可消费 API Key 或临时 HTTPS tunnel；本地 WebUI、fixture 和 mock 不能替代该证据。
- **隔离性**：带“自动清理”的命令只操作其创建的 Compose project；其他命令保留本地卷，重置必须由开发者显式执行。

| 验证项 | 命令或操作 | 所需资源 | 通过证据 | 范围 |
| --- | --- | --- | --- | --- |
| Compose 契约 | `docker compose -f deploy/compose/docker-compose.local.yml config --quiet` | Docker Compose v2 | 退出码 0 | 本地 |
| Admission | `bash scripts/ci_admission.sh`；race 使用 `--race` | Go 1.21.x；或等价 Go 1.21 容器/CI runner | format、依赖边界、build、vet、test 全部通过 | 本地/CI |
| 后端 adapter | `bash scripts/backend_adapter_smoke.sh` | Docker Desktop | PostgreSQL、Redis、Qdrant、Vault contract 通过；临时资源自动清理 | 本地 |
| PostgreSQL/Redis runtime slice | `docker compose -f deploy/compose/docker-compose.local.yml up -d postgres redis` 后执行 `--profile runtime-test run --rm runtime-test` | Docker Desktop | migration 与真实 PostgreSQL/Redis slice 通过 | 本地 |
| WebUI + 模型 | `./start.sh`，打开 `http://localhost:58081/webui/` | Docker Desktop、DeepSeek Key | `/readyz` 为 200；一次文本对话得到回复；confirmation 只执行一次 | 本地 + 真实模型 |
| 多租户、多节点 | `bash scripts/local_multinode_smoke.sh` | Docker Desktop、DeepSeek Key | 两租户、两 Worker 通过；停止 node A 后 node B 保持 ready | 本地 + 真实模型 |
| 依赖短断恢复 | `bash scripts/local_dependency_recovery_smoke.sh` | Docker Desktop、DeepSeek Key | PostgreSQL/Redis 中断期间节点 unready，恢复后无需重启重新 ready | 本地 + 真实模型 |
| Feishu 文本/群聊 | 依据 [getting-started.md](getting-started.md) 第 3 节启动 `feishu-local` 并配置 tunnel | DeepSeek Key、`feishu.env`、Feishu 应用、临时 HTTPS URL | callback 验签成功；私聊收到回复；群聊仅 @ 机器人时处理 | 真实外部 |
| Feishu 图片 | 在同一 Feishu p2p 会话发送一张新的 JPEG/PNG/GIF/WebP（≤10 MiB） | 同上、ClamAV healthy、视觉模型 | 回复基于图片内容；日志显示媒体下载、扫描和 prepared input | 真实外部 |
| WeCom 文本/图片 | 依据 [getting-started.md](getting-started.md) 第 4 节启动 `wecom-local` 并配置 tunnel | DeepSeek Key、`wecom.env`、WeCom 自建应用、临时 HTTPS URL | callback URL 验证成功，消息收到官方 Reply API 回复；图片经扫描后进入视觉模型 | 真实外部 |
| 可观测性 | 打开 `http://localhost:56686/` 与 `http://localhost:59464/metrics` | 本地 Compose profile | Jaeger 可查询 trace，metrics endpoint 返回 200 | 本地 |

## 已知边界

- Feishu 的群聊文本需要可靠的机器人 mention；**media-only 群消息会被忽略**。图片/文件的真实验收只以 p2p 为准。
- WeCom 支持的是自建应用 Agent callback，不包含智能机器人长连接或 Bot 流式协议。
- 非图片文件会下载、扫描和审计，但当前本地视觉模型不宣称理解 PDF 或 Office 内容。
- 审计查询、保留和销毁的角色、权限与不可变约束见 [能力与兼容边界](../design/8.capability-boundaries.md)；破坏性 purge 不暴露 HTTP 端点。

完整的启动、密钥格式、回调地址和故障排查步骤见 [本地 Docker Desktop 验收指南](getting-started.md)。
