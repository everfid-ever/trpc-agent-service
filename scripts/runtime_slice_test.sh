#!/usr/bin/env bash
set -euo pipefail

if [[ "${TRPC_MIGRATION_TEST:-}" != "1" ]]; then
  echo "refusing: set TRPC_MIGRATION_TEST=1" >&2
  exit 1
fi
if [[ -z "${TRPC_POSTGRES_ADMIN_DSN:-}" ]]; then
  echo "TRPC_POSTGRES_ADMIN_DSN is required" >&2
  exit 1
fi
if [[ -z "${TRPC_REDIS_TEST_ADDR:-}" ]]; then
  echo "TRPC_REDIS_TEST_ADDR is required" >&2
  exit 1
fi

go test -count=1 ./trpcservice/broker/redis ./trpcservice/coordination/redis ./trpcservice/relay/redis
TRPC_RUNTIME_TEST=1 go run ./cmd/postgres-migration-test
