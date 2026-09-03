#!/usr/bin/env bash
# Verifies two tenant-scoped Worker paths on local PostgreSQL/Redis, two WebUI
# composition nodes sharing those authorities, and single-node continuity.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.m2.yml"
secret_file="${repo_root}/deploy/compose/secrets/deepseek-api-key"
project="trpc-local-multinode-${RANDOM}${RANDOM}"
# RANDOM is at most 32767, keeping all seven allocated ports below 65535.
port_base="$((20000 + RANDOM))"
port_postgres="${port_base}"
port_redis="$((port_base + 1))"
port_jaeger="$((port_base + 2))"
port_otel="$((port_base + 3))"
port_metrics="$((port_base + 4))"
port_a="$((port_base + 5))"
port_b="$((port_base + 6))"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-local-multinode.XXXXXX")"

command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
test -s "${secret_file}" || { echo "DeepSeek key file is required at ${secret_file}" >&2; exit 2; }

compose() {
  M2_POSTGRES_PORT="${port_postgres}" M2_REDIS_PORT="${port_redis}" \
    M2_JAEGER_PORT="${port_jaeger}" M2_OTEL_HTTP_PORT="${port_otel}" M2_OTEL_PROMETHEUS_PORT="${port_metrics}" \
    M2_MULTINODE_NODE_A_PORT="${port_a}" M2_MULTINODE_NODE_B_PORT="${port_b}" \
    docker compose --project-name "${project}" -f "${compose_file}" --profile webui-multinode "$@"
}

compose_runtime() {
  M2_POSTGRES_PORT="${port_postgres}" M2_REDIS_PORT="${port_redis}" \
    M2_JAEGER_PORT="${port_jaeger}" M2_OTEL_HTTP_PORT="${port_otel}" M2_OTEL_PROMETHEUS_PORT="${port_metrics}" \
    docker compose --project-name "${project}" -f "${compose_file}" --profile runtime-test "$@"
}

cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then
    compose logs --no-color >"${diagnostics}/compose.log" || true
    echo "local multi-node smoke failed; diagnostics retained at ${diagnostics}/compose.log" >&2
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  [[ ${status} -ne 0 ]] || rmdir "${diagnostics}" || true
  exit "${status}"
}
trap cleanup EXIT

wait_ready() {
  local port="$1"
  for _ in $(seq 1 90); do
    if curl --fail --silent "http://127.0.0.1:${port}/readyz" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

compose up --detach --build
wait_ready "${port_a}"
wait_ready "${port_b}"
# runtime-test creates a fresh disposable PostgreSQL database. Its integration
# slice provisions tenant A and tenant B, runs two Redis-backed Workers, and
# rejects cross-tenant state while exercising real PostgreSQL/Redis adapters.
compose_runtime run --rm runtime-test
compose stop webui-node-a
wait_ready "${port_b}"
echo "local multi-tenant, multi-node smoke passed: two tenant Worker slice passed; node-a stopped; node-b remained ready"
