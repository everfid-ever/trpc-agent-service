#!/usr/bin/env bash
set -euo pipefail
for name in TRPC_MIGRATION_TEST TRPC_POSTGRES_ADMIN_DSN TRPC_REDIS_TEST_ADDR TRPC_QDRANT_TEST_ENDPOINT TRPC_VAULT_TEST_ENDPOINT TRPC_VAULT_TEST_TOKEN; do
  [[ -n "${!name:-}" ]] || { echo "${name} is required" >&2; exit 2; }
done

# The disposable smoke image starts with an empty module cache. A transient
# proxy EOF must not be reported as a backend-adapter regression; populate the
# complete module graph before the test matrix and retry only this network step.
download_modules() {
  local attempt
  for attempt in 1 2 3; do
    if go mod download; then
      return 0
    fi
    if [[ ${attempt} -lt 3 ]]; then
      echo "go mod download failed (attempt ${attempt}/3); retrying" >&2
      sleep "${attempt}"
    fi
  done
  return 1
}

# The Qdrant image has no shell HTTP client, so readiness is asserted here
# rather than by a container healthcheck.
for _ in $(seq 1 60); do
  curl -fsS "${TRPC_QDRANT_TEST_ENDPOINT}/healthz" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "${TRPC_QDRANT_TEST_ENDPOINT}/healthz" >/dev/null

# Seed the disposable Vault dev server with the synthetic secret that the
# integration test reads back (KV v2 path secret/data/model).
curl -fsS -X POST \
  -H "X-Vault-Token: ${TRPC_VAULT_TEST_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"data":{"value":"integration-secret"}}' \
  "${TRPC_VAULT_TEST_ENDPOINT}/v1/secret/data/model" >/dev/null

download_modules
go run ./cmd/postgres-migration-test
go test -count=1 ./trpcservice/broker/redis ./trpcservice/coordination/redis ./trpcservice/relay/redis
TRPC_RUNTIME_TEST=1 go run ./cmd/postgres-migration-test
go test -count=1 ./trpcservice/secrets/vault ./trpcservice/storage/knowledge/qdrant
