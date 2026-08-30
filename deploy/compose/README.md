# M2 Docker environment

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
