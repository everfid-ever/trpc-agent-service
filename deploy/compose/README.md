# M2 Docker environment

For a complete Chinese quick-start, parameter reference, Secret explanation,
health checks, and troubleshooting guide, see
[`docs/runbook/getting-started.md`](../../docs/runbook/getting-started.md).

This Compose file is a local acceptance harness, not a production deployment.
It provides PostgreSQL 16, Redis 7, and an optional MinIO S3-compatible store.
The application image contains the same multi-role binary used by the
production entrypoints (`gateway`, `worker`, `channel`, and so on).

## Runtime slice (environment 1)

From the repository root:

```bash
docker compose -f deploy/compose/docker-compose.m2.yml up -d postgres redis
docker compose -f deploy/compose/docker-compose.m2.yml --profile runtime-test run --rm runtime-test
```

`runtime-test` creates and drops a random PostgreSQL database through the
existing migration matrix and then runs the opt-in Gateway/Worker/Reply Queue
slice. Use a disposable Compose volume; the test user must be allowed to
create databases (the default `postgres` image user is).

## 0909 minimal backend smoke

```bash
bash scripts/minimal_backend_smoke.sh
```

This runs isolated PostgreSQL 16, Redis 7, Qdrant and Vault KV v2 containers;
it validates the migration matrix, Redis recovery contracts and real
Vault/Qdrant adapters. It removes containers and volumes on success and keeps
Compose logs in a temporary directory on failure. No external credentials are
used and it does not test IM, model, S3, OTEL or Kubernetes integrations.

## Gateway/Worker (environment 2)

Build and start the infrastructure plus application containers with:

```bash
cp deploy/compose/.env.m2.example deploy/compose/.env.m2
mkdir -p deploy/compose/secrets
docker compose -f deploy/compose/docker-compose.m2.yml --profile gateway-worker up --build
```

Before starting the application profile, apply migrations, seed Tenant/App/
Config/ModelProfile rows, create the MinIO bucket, and project the scoped
`payload_encrypt`, `gateway_auth`, and `model_call` secret files under
`deploy/compose/secrets`. The Gateway requires an HTTPS public base URL; put a
TLS reverse proxy in front of port 58080 or use an already deployed HTTPS
Gateway. DeepSeek API keys must be mounted as scoped files and are never placed
in `.env.m2`.

Graph continuations use the same authenticated Worker Redis deployment. Their
keys are isolated by tenant, immutable ConfigSnapshot version, and declared
checkpoint namespace, and expire after `TRPC_WORKER_GRAPH_CHECKPOINT_TTL`
(default `168h`). Keep that TTL longer than the maximum confirmation window.

After the Gateway is healthy, run the semantic vertical smoke from the host:

```bash
TRPC_M2_DEEPSEEK_SMOKE=1 \
TRPC_M2_GATEWAY_URL=https://gateway.example.test \
TRPC_M2_GATEWAY_BEARER_TOKEN='do-not-log-this' \
TRPC_M2_DEEPSEEK_PROFILE_ID=profile_xxx \
bash scripts/m2_deepseek_vertical_smoke_test.sh
```

The Compose file deliberately does not auto-run migrations or seed control
plane data. This keeps the production rule—migrations are applied by a
controlled deployment step—and prevents a sample stack from fabricating
tenants, credentials, or a Gateway authentication authority.

The `gateway-worker` profile also starts the independent `audit-relay` role.
It claims only `kind=audit` Outbox rows, resolves tenant-scoped durable facts,
and idempotently exports them to a physically separate compliance PostgreSQL
database. Apply its deliberately small schema as an explicit deployment step:

```bash
docker compose -f deploy/compose/docker-compose.m2.yml --profile audit-migration run --rm audit-compliance-migrate
```

The normal application migrations still apply only to the source database.
The compliance database contains immutable `audit_event` facts and durable
`quarantine_alert` rows; the Relay refuses to start if its DSN equals the
source DSN, if runtime database identities are equal, or if its schema checksum is absent.
Its health endpoints use port `58082` by default. To run it independently
after applying migrations:

```bash
docker compose -f deploy/compose/docker-compose.m2.yml --profile audit up --build postgres compliance-postgres audit-relay
curl -fsS http://localhost:58082/readyz
curl -fsS http://localhost:58082/metrics
```

The Audit Relay polls a PostgreSQL aggregate that contains no tenant, user,
session, request, payload, or secret labels. Its defaults alert when the oldest
active Audit Outbox row reaches 5 minutes, active rows reach 10,000, or any row
is dead-lettered. Override these with `M2_AUDIT_LAG_POLL_INTERVAL`,
`M2_AUDIT_LAG_ALERT_AGE`, and `M2_AUDIT_LAG_ALERT_COUNT`; the poll interval must
remain below the age threshold. Prometheus-compatible rules live in
`deploy/alerts/audit-relay.rules.yml`, with recovery steps in
`deploy/runbooks/audit-relay.md`. A stale or missing backlog snapshot is an
alert condition, not permission to discard or manually publish Outbox rows.

The `gateway-worker`, `audit`, and `webui` profiles also include an optional
OTLP/HTTP Collector and Jaeger trace UI. Applications use the internal
`http://otel-collector:4318` endpoint with bounded SDK queues; Collector or
Jaeger downtime does not block a request, relay claim, Worker commit, or Reply
delivery. Jaeger is exposed at `http://localhost:${M2_JAEGER_PORT:-56686}` and
the Collector Prometheus endpoint at `http://localhost:${M2_OTEL_PROMETHEUS_PORT:-59464}`.
Set `TRPC_OTEL_ENDPOINT` only when an external deployment supplies its own
Collector; leave it empty to use the no-op provider.

The operation-level SLO recording and alert rules are in
`deploy/alerts/operations.rules.yml`. Import them into Prometheus alongside
`audit-relay.rules.yml`; the response procedure is in
`deploy/runbooks/operations-slo.md`.

## Local WebUI channel

For the one-command local acceptance profile, first place only your DeepSeek
API key in the ignored Docker secret file, then start the profile:

```bash
cp deploy/compose/.env.m2.example deploy/compose/.env.m2
mkdir -p deploy/compose/secrets
install -m 600 /absolute/path/to/your/deepseek-api-key deploy/compose/secrets/deepseek-api-key
docker compose -f deploy/compose/docker-compose.m2.yml --profile webui up --build
```

Open <http://localhost:58081/webui/> and enter the defaults from `.env.m2`:

| Field | Default |
|---|---|
| Route Key | `local-webui` |
| Account ID | `local-webui` |
| User ID | `local-user` |
| Chat ID | `local-chat` |
| Channel Token | `local-webui-token-change-me` |

After connecting, send the following deterministic acceptance request:

```text
创建一条标题为验收、内容为 WebUI durable confirmation 的笔记
```

The assistant must request `webui_create_note@1` instead of claiming success.
The timeline then presents a durable confirmation card. Choose **批准一次** to
resume the suspended execution and receive the created note result, or choose
**拒绝** to reach a terminal denial without calling the tool. Reconnecting or
refreshing the page must not execute an approved call a second time; the same
durable reply/confirmation state is read from PostgreSQL-backed authorities.

The `webui-local` command is an explicitly local composition root. It applies
the embedded migrations and idempotently publishes one isolated tenant,
DeepSeek profile, checkpointed Graph root with a frozen LLM child, WebUI
ChannelBinding, public route, and scoped
secret projections. It also registers the code-owned, tenant-bound,
exact-version `webui_create_note@1` local acceptance tool and upgrades older
disposable WebUI control-plane state idempotently. Approval resumes the Graph
from its Redis checkpoint; it does not restart the root workflow. It then runs the existing Preprocess Worker,
Dispatch/Reply relays, Redis Worker, Reply Queue, Delivery Ledger and
`webui.Adapter` in one container. PostgreSQL and Redis remain the durable
authorities; no alternate IM state machine is used. Media is intentionally not
enabled in this text-only acceptance profile. The note tool has no external
provider side effect and is not registered by the production `worker` role.

Changing the DeepSeek key or the local token changes immutable local secret
material. Reset the disposable environment before restarting with new values:

```bash
docker compose -f deploy/compose/docker-compose.m2.yml --profile webui down -v
```

Do not use this profile or its default token as a production deployment.

Set `TRPC_WEBUI_ENABLED=true` on both the `channel` and `channel-delivery`
roles to expose `/webui/` and install the `webui.Adapter` in the existing
delivery catalog. This is not a shortcut around the Channel framework: inbound
messages still pass through the shared callback endpoint, durable Inbox and
Preprocess pipeline, while replies still use Reply Queue and Delivery Ledger.

Publish a normal `webui` ChannelBinding with an opaque public route, identity
and session references, payload key generation, and strict verification
material shaped as:

```json
{"token":"at-least-16-random-characters","external_account_id":"local-webui"}
```

The browser keeps the token in page memory and signs callback and mailbox
requests with HMAC-SHA256; the token is never placed in a URL. Mailbox rows
contain only delivery metadata, and reply plaintext remains in the encrypted
ResultStore. A successful WebUI exercise proves the provider-neutral internal
Channel/Gateway/Worker/delivery path. It does not prove that real Feishu or
WeCom credentials and external APIs work.

## M3 real IM provider final acceptance

Real Feishu/WeCom credentials are not an M2 release prerequisite. When an
authorized provider environment becomes available in M3, copy the locator-only
template and run the final provider smoke:

```bash
cp deploy/compose/.env.m3-im.example deploy/compose/.env.m3-im
# Create the credential JSON files referenced by .env.m3-im, then:
chmod 600 /absolute/path/to/feishu.json /absolute/path/to/wecom.json
bash scripts/m3_im_provider_smoke_test.sh
```

Set `TRPC_M3_IM_PROVIDERS=feishu` or `wecom` to validate one provider first;
`feishu,wecom` validates both. The script uses the host Go toolchain when
available and otherwise runs the test in `golang:1.25-bookworm`. Credential
values are mounted/read from owner-only files and are never passed as
environment values or command arguments. The smoke intentionally sends a
visible random nonce: Feishu replies to the configured existing message ID,
while WeCom sends an Agent application message to the configured test user.

The production inbound entrypoints are already part of the multi-role binary:

```bash
trpc-service channel
trpc-service channel-delivery
```

Before starting them, apply migrations and publish the Tenant, Agent App,
ConfigSnapshot, ChannelBinding and opaque public-route records. Project
separate immutable `channel_verify`, `channel_send`, tenant identity/session
and payload-encryption SecretRef generations below `TRPC_SECRET_ROOT`. Expose
the channel role through HTTPS and configure provider callbacks as:

```text
https://im.example.test/callbacks/feishu?route_key=<opaque-route>
https://im.example.test/callbacks/wecom?route_key=<opaque-route>
```

Feishu `channel_send` material is strict JSON
`{"app_id":"cli_xxx","app_secret":"..."}`. WeCom Agent `channel_send`
material is strict JSON
`{"corp_id":"ww_xxx","corp_secret":"...","agent_id":1000002}`. Callback
verification uses a different SecretRef: Feishu requires `EncryptKey`,
`VerificationToken`, `AppID`, and `BotOpenID`; WeCom requires `token`,
`encoding_aes_key`, `receive_id`, and `agent_id`. Do not reuse send credentials
as callback verification material.
