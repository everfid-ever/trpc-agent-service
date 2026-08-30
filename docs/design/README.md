# trpc-agent-service 多租户多节点设计文档

本目录是 `trpc-agent-service` 的规范性设计入口。实现、评审和验收均以这里冻结的契约为准。

## 文档导航

| 文档 | 负责回答的问题 |
|---|---|
| [architecture.md](0.architecture.md) | 租户模型、组件拓扑、关键契约、端到端交互及异常恢复时序 |
| [tenant-root-design.md](1.tenant-root-design.md) | Tenant、Agent App、Model/Backend Profile 与 Provider Catalog 的控制面领域模型、发布/回滚、运行时绑定及事务性审计 |
| [gateway-worker-design.md](2.gateway-worker-design.md) | 请求如何分发，任意 Worker 如何安全执行和故障接管 |
| [storage-data-consistency-design.md](3.storage-data-consistency-design.md) | 数据如何隔离、原子提交、跨节点可见和迁移 |
| [channel-governance-security-design.md](4.channel-governance-security-design.md) | WeCom/Feishu 如何接入，权限、确认、密钥和脱敏如何实施 |
| [observability-audit-devops-design.md](5.observability-audit-devops-design.md) | trace、指标、审计、部署、灰度和故障恢复如何落地 |
| [module-boundaries.md](6.module-boundaries.md) | A/B/C/D 内部模块边界、`trpc-agent-go` 公共 API 依赖和上游能力缺口 |
| [implementation-roadmap.md](7.implementation-roadmap.md) | 实现顺序、2026-08-21 至 2026-09-11 排期、里程碑和交付门禁 |
| [m2-scope-decisions.md](8.m2-scope-decisions.md) | M2 范围调整的原因、影响、替代验收、owner 与复查日期 |

## 推荐阅读顺序

第一次阅读不需要从头到尾逐页下钻。先读总体架构和模块边界，再围绕一条业务链进入 A/B/C/D：

```text
0 总体架构 / 6 模块边界
→ C：外部事件如何成为可信请求
→ A：请求如何调度、执行和故障接管
→ B：哪些状态被原子提交并成为事实
→ C：结果如何治理并投递回 IM
→ D：如何观测、告警、发布和恢复
```

每份模块文档的 §0 都是“一页入口”，统一说明核心问题、输入输出、状态所有权、正常主链路和按角色阅读路径；§1 以后保留规范性契约与实现细节。评审架构时先检查 §0/§1，编码时再进入接口、状态机、DDL 和测试章节。

## 实现状态口径

设计基线与当前实现必须分别陈述。每份模块文档和每个实施切片使用以下统一状态，不得用“已设计”代替“已实现”，也不得用 fake/in-memory 测试代替生产验证：

| 状态 | 含义 | 必需证据 |
|---|---|---|
| `contract_frozen` | 字段、状态机、原子性、错误和兼容规则已冻结 | 规范章节与兼容性说明 |
| `implemented` | 生产代码路径已实现，不再只是接口、fake 或占位包 | 代码落点与单元测试 |
| `contract_tested` | fake/in-memory 与生产 adapter 通过同一契约套件 | contract suite 结果 |
| `integration_tested` | 真实 PostgreSQL/Redis/Provider fixture 完成集成验证 | 可复现命令与测试记录 |
| `production_verified` | 故障注入、容量、安全、观测和回滚门禁通过 | 验收报告与 Runbook |
| `blocked` | 外部依赖或上游能力尚未闭环，生产路径保持 fail closed | blocker、owner、替代门禁和复查日期 |

状态按能力逐项记录，不能给整个模块笼统标记“完成”。文档中的 `设计状态=实施基线` 只表示契约有效，不表示对应生产代码已经达到 `production_verified`。

| 需要回答的问题 | 权威模块 | 最短阅读路径 |
|---|---|---|
| 请求由谁接收，何时算可靠接受？ | C + B | C §0/§3–4 → B §0/§6 |
| 多 Worker 如何避免乱序和旧 owner 覆盖？ | A + B | A §0/§5–9 → B §8/§16 |
| 配置、Session 和回复的事实在哪里？ | B | B §0/§2/§7–10 |
| IM 协议、治理和确认如何组合？ | C | C §0/§5–10/§17–18 |
| 故障如何发现、告警、回滚和验收？ | D | D §0/§5–8/§16–17 |

## 核心交付物覆盖

| 交付物 | 规范落点 | 覆盖口径 |
|---|---|---|
| 系统架构图 | [架构文档（§2）](0.architecture.md) | Gateway、Worker、Channel Adapter、Storage Adapter、Worker-local Governance/Filter（Plugin/Guardrail hooks）、Model/Tool、Telemetry 与后端拓扑 |
| 核心时序图 | [架构文档（§12）](0.architecture.md) | IM callback、租户/版本固定、Runner、Model、Tool 授权与调用、Session/Memory、CommitTurn、IM 回复和 Trace 闭环 |
| 数据模型与多后端 | [Tenant/Agent App 文档（第一部分 §5、第二部分 §4）](1.tenant-root-design.md)、[Storage 文档（§2/§7/§14/§18）](3.storage-data-consistency-design.md) | 原始要求的最小模型和生产基线 DDL；其余完整 migration 按路线图分阶段实现；Redis/SQL/向量库/对象存储能力与一致性取舍明确 |
| 风险清单 | [Observability 文档（§19）](5.observability-audit-devops-design.md) | 12 项生产风险、缓解措施和可验证验收口径 |

## 模块划分

- A：多租户、多节点 Runtime，包括 Gateway、Worker、Broker、协调与业务 Relay。
- B：Storage 与数据一致性，包括 Router、Session/Memory/Artifact/Knowledge、Inbox/Outbox 与迁移。
- C：IM、治理与安全，包括 Channel Framework、WeCom、Feishu、Policy、确认和密钥。
- D：Observability、Audit 与 DevOps，包括 OTel、审计、部署、容量、灰度和 Runbook。

模块表示代码与架构边界，不表示人员分工；整个系统可由同一实现者按模块依赖顺序完成。

## 全局不变量

1. `TenantContext` 只能由服务端根据认证凭据、Channel Binding 或管理面身份构造，不能信任客户端提交的 `tenant_id`。
2. 所有持久化键、缓存键、消息、日志和审计事件都显式携带 `tenant_id`；跨租户访问必须在存储层再次拒绝。
3. Worker 无状态且不需要 sticky session；Session、Memory、Artifact、执行状态和回复状态均由共享后端保存。
4. Broker 分片只控制同 session 消息的可见顺序；真正的单 session 互斥由 lease 和单调 fence 保证。
5. `input_seq` 按 `PrepareDispatch` 成功提交顺序分配，不按 Inbox claim、IM 时间戳或 request ID 排序。
6. event、state、终态、`next_input_seq` 和 Reply/Wakeup Outbox 必须原子提交。
7. 消息投递采用 at-least-once，业务效果依靠 Inbox、Commit 和 Delivery Ledger 实现幂等。
8. 密钥只以 `SecretRef` 出现在配置和消息中，明文不得进入日志、trace、审计或错误报告。
9. 分布式模式必须 fail closed：后端不支持原子提交、fence 或租户作用域时不得启动多 Worker。
10. `trpc-agent-service` 不导入上游 `internal`；ExecutionProfile 由 service 自有实现，IM Provider 则复用并扩展获批的 OpenClaw 公共 Channel 契约。独立产品模块未通过 toolchain/依赖门禁前不得进入生产依赖图。
11. `PrepareDispatch` 成功提交才表示任务被执行系统接受；仅完成 ClaimInbox 的消息仍必须通过 tenant 状态门禁。
12. tenant 状态变化、审计事实以及缓存失效/取消 Outbox 必须在同一事务中提交。
13. `agent_app_revision` 是 Agent 定义的唯一发布版本；ExecutionProfileSnapshot 只是 App Revision、ConfigSnapshot 和 PolicySnapshot 的运行时投影。
14. 发布后的 Agent App Revision 及拓扑、Tool/Skill/Knowledge 子记录不可修改；发布、回滚和状态变化与审计/失效 Outbox 原子提交。
15. `TenantContext` 只表达可信身份；固定 App Revision、Config 和 Policy 版本由 `PrepareDispatch` 生成 ExecutionBinding，并由 Worker 从 TaskStore 复核。
16. 公开 Channel route、外部账号或 callback payload 在验签前只能得到不含 `tenant_id`、`app_id`、`secret_ref` 和 Secret 值的短期候选能力；只有验签成功后才能建立 `TenantContext` 和调用 `ClaimInbox`。
17. Secret 解析必须同时绑定可信 scope、purpose 和 credential version；禁止提供可由任意调用方执行的全局 `Resolve(SecretRef)` 生产入口。
18. ModelProfile 与 BackendProfile 是租户内版本化、无密钥、严格 schema 化的控制面对象；它们可以被 Revision/Binding 引用，但不得成为第二套 Agent 定义版本或绕过 Config/Policy 固定。
19. 验签前只允许生成 `ingress_attempt_id` 和 trace；稳定业务 `request_id` 只能由 `ClaimInbox` 原子生成，重复回调必须返回同一 request ID。
20. IM Adapter 必须复用并扩展 OpenClaw 公共 Channel 生命周期和 sender 契约；兼容接口不等于直接代码复用，公共包未通过 toolchain、依赖与 contract 门禁前真实 IM 保持 blocker。
21. LLM/Graph/Chain/Parallel/Cycle 必须由同一版本化 AgentSpec 发布并映射到上游公共 Agent 构造器；实施分期不能形成第二套临时定义。
22. OpenAI、AG-UI、A2A 和 tRPC-Agent 协议 façade 只能通过 GatewayRunnerBridge 进入共享 Inbox/Task/Event Store；不得创建旁路本地 Runner 或 InMemory 权威状态。
23. Knowledge/Skill 复用必须固定 tenant、版本和 digest；框架 scope/filter 只能收窄能力，不能替代平台最终授权或 tenant filter。
24. Plugin/callback/OTel hook 用于扩展和观测，平台 mandatory Guard、CommitTurn 和 Audit Outbox 始终位于不可绕过的外层。

## 需求追踪

| 需求 | 权威文档 | 主要实现包 | 核心验收 |
|---|---|---|---|
| 租户模型与隔离 | tenant-root、architecture、storage、channel | `tenant/`、`config/`、`storage/` | 状态门禁、复合外键及相同资源 ID 跨租户隔离 |
| Agent App 发布与回滚 | agent-app、architecture、gateway-worker | `agentapp/`、`profile/`、`storage/postgres/agentapp/` | Revision 不可变、并发发布、失败保持旧版本和在途版本固定 |
| Agent 编排与 Skill | tenant-root、module-boundaries | `agentapp/`、`profile/`、`skills/` | 五类 Agent schema/拓扑、精确子 Revision、Skill digest/scope 与上游构造器兼容测试 |
| Gateway/Worker 多节点 | gateway-worker | `gateway/`、`worker/`、`broker/` | Worker kill 后安全接管 |
| Session 一致性 | storage | `storage/session/` | stale fence、CAS、重投测试 |
| 多数据后端与迁移 | storage | `storage/router/`、`migration/` | 双写、对账、切换、回滚 |
| Knowledge 复用 | storage、tenant-root、module-boundaries | `knowledge/`、`storage/knowledge/` | 强制 tenant/version filter、摄取发布、跨租户返回负向测试 |
| Model/Backend Profile | tenant-root（第三部分）、gateway-worker、storage | `modelprofile/`、`backendprofile/`、`provider/`、`profile/` | schema 拒绝、digest、跨租户引用、Secret 负向测试 |
| WeCom 与 Feishu | channel | `channels/wecom/`、`channels/feishu/` | 公共 contract + 官方协议测试 |
| OpenClaw Channel 复用 | channel、module-boundaries、roadmap | `channels/contract/`、`channels/openclawcompat/` | compile-time assertion、compatibility suite、产品运行时/internal 负向依赖检查 |
| 治理与安全 | channel | `governance/`、`secrets/` | 越权、预算、确认、secret canary |
| 协议 Server 复用 | gateway-worker、module-boundaries | `gateway/serverbridge/`、`serverfacade/` | OpenAI/A2A/tRPC-Agent 兼容测试；AG-UI 版本门禁；无本地 Runner/InMemory 旁路 |
| Plugin/Callbacks | channel、observability、module-boundaries | `governance/`、`telemetry/` | mandatory Guard 顺序、组合 Agent 覆盖、panic/timeout、无重复审计/计费 |
| Trace、审计、部署 | observability | `telemetry/`、`audit/`、`deploy/` | 全链 trace、审计重投、故障演练 |

## 文档变更规则

- 修改全局不变量、AgentAppRevision、Envelope、CommitTurn、ChannelEvent、ReplyEvent 或 AuditEvent 时，必须同步检查全部受影响模块的接口、迁移和契约测试。
- 兼容变更增加 `schema_version`；破坏性变更必须提供双读/双写和回滚窗口。
- 设计中的接口是逻辑契约，代码可以拆分，但不得改变原子性、可信边界和幂等语义。
- 每个实现切片必须对应设计章节、测试和观测指标。
- 每个实现切片必须更新实现状态台账；只有 `production_verified` 才能在对外文档中表述为生产完成。
