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

The `webui-local` command is an explicitly local composition root. It applies
the embedded migrations and idempotently publishes one isolated tenant,
DeepSeek profile, Agent App, WebUI ChannelBinding, public route, and scoped
secret projections. It then runs the existing Preprocess Worker,
Dispatch/Reply relays, Redis Worker, Reply Queue, Delivery Ledger and
`webui.Adapter` in one container. PostgreSQL and Redis remain the durable
authorities; no alternate IM state machine is used. Media is intentionally not
enabled in this text-only acceptance profile.

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
