#!/usr/bin/env bash
set -euo pipefail
suite="${TRPC_E2E_SUITE:-all}"
case "${suite}" in
  all|migration|migration-coverage|runtime|storage|artifact) ;;
  *) echo "TRPC_E2E_SUITE must be all, migration, migration-coverage, runtime, storage, or artifact" >&2; exit 2 ;;
esac

for name in TRPC_MIGRATION_TEST TRPC_POSTGRES_ADMIN_DSN; do
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

download_modules

run_migration() {
  # The Compose image also serves runtime-e2e. Explicitly suppress its opt-in
  # Redis slice here so migration-e2e remains a PostgreSQL-only dependency.
  TRPC_RUNTIME_TEST=0 go run ./cmd/postgres-migration-test
  # The operator role stays bounded and needs no production credentials in this
  # disposable environment. Its transition/journal suite is nevertheless a
  # required real-backend gate, never an optional developer-only check.
  go test -count=1 ./cmd/trpc-service ./trpcservice/migration/...
}

run_migration_coverage() {
  # This is intentionally opt-in: the normal migration job remains a fast
  # contract gate. The migration runner creates the random database, keeps it
  # alive for the contract matrix, and attributes those executions to the
  # adapter packages before it tears the database down.
  TRPC_RUNTIME_TEST=0 TRPC_POSTGRES_ADAPTER_COVERAGE=1 go run ./cmd/postgres-migration-test
}

run_runtime() {
  [[ -n "${TRPC_REDIS_TEST_ADDR:-}" ]] || { echo "TRPC_REDIS_TEST_ADDR is required" >&2; exit 2; }
  # Do not permit opt-in contracts to turn into green builds because a Redis
  # address or the runtime slice was accidentally omitted.
  bash scripts/lib/test-no-skip.sh ./trpcservice/broker/redis ./trpcservice/coordination/redis ./trpcservice/relay/redis
  TRPC_RUNTIME_TEST=1 go run ./cmd/postgres-migration-test
  go test -count=1 ./trpcservice/admin ./trpcservice/agent ./trpcservice/agent/condition
}

run_storage() {
  for name in TRPC_QDRANT_TEST_ENDPOINT TRPC_MINIO_TEST_ENDPOINT TRPC_VAULT_TEST_ENDPOINT TRPC_VAULT_TEST_TOKEN; do
    [[ -n "${!name:-}" ]] || { echo "${name} is required" >&2; exit 2; }
  done
  # Qdrant has no shell HTTP client, so readiness is asserted by the smoke
  # runner. MinIO and Vault receive the same real protocol checks.
  for _ in $(seq 1 60); do
    curl -fsS "${TRPC_QDRANT_TEST_ENDPOINT}/healthz" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "${TRPC_QDRANT_TEST_ENDPOINT}/healthz" >/dev/null
  for _ in $(seq 1 60); do
    curl -fsS "${TRPC_MINIO_TEST_ENDPOINT}/minio/health/live" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "${TRPC_MINIO_TEST_ENDPOINT}/minio/health/live" >/dev/null
  curl -fsS -X POST -H "X-Vault-Token: ${TRPC_VAULT_TEST_TOKEN}" -H "Content-Type: application/json" \
    -d '{"data":{"value":"integration-secret"}}' "${TRPC_VAULT_TEST_ENDPOINT}/v1/secret/data/model" >/dev/null
  bash scripts/lib/test-no-skip.sh ./trpcservice/secrets/vault ./trpcservice/storage/knowledge/qdrant ./trpcservice/storage/objectstore/s3
  go test -count=1 ./trpcservice/skill ./trpcservice/storage/knowledge ./trpcservice/tool/codeexec
}

run_artifact() {
  for name in TRPC_S3_ENDPOINT TRPC_S3_BUCKET TRPC_S3_ACCESS_KEY TRPC_S3_SECRET_KEY; do
    [[ -n "${!name:-}" ]] || { echo "${name} is required" >&2; exit 2; }
  done
  # Artifact metadata is schema-owned by this repository, so migrate the
  # disposable PostgreSQL database before composing it with real MinIO.
  TRPC_RUNTIME_TEST=0 go run ./cmd/postgres-migration-test
  TRPC_ARTIFACT_E2E=1 go test -count=1 -run '^TestComposeArtifactObjectStoreTenantIsolation$' ./trpcservice/integration
}

case "${suite}" in
  migration) run_migration ;;
  migration-coverage) run_migration_coverage ;;
  runtime) run_runtime ;;
  storage) run_storage ;;
  artifact) run_artifact ;;
  all) run_migration; run_runtime; run_storage; run_artifact ;;
esac
