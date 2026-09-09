#!/usr/bin/env bash
set -euo pipefail
suite="${1:-all}"
case "${suite}" in
  all|migration|runtime|storage|artifact) ;;
  *) echo "usage: $0 [all|migration|runtime|storage|artifact]" >&2; exit 2 ;;
esac
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.backend-smoke.yml"
project="trpc-${suite}-e2e-${RANDOM}${RANDOM}"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-backend-smoke.XXXXXX")"
command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then docker compose --project-name "${project}" -f "${compose_file}" logs --no-color >"${diagnostics}/compose.log" || true; echo "backend adapter smoke failed; diagnostics retained at ${diagnostics}/compose.log" >&2; fi
  docker compose --project-name "${project}" -f "${compose_file}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  [[ ${status} -ne 0 ]] || rmdir "${diagnostics}" || true
  exit "${status}"
}
trap cleanup EXIT

compose() {
  TRPC_E2E_SUITE="${suite}" docker compose --project-name "${project}" -f "${compose_file}" "$@"
}

wait_healthy() {
  local service="$1" container state
  container="$(compose ps -q "${service}")"
  [[ -n "${container}" ]] || { echo "${service} container was not created" >&2; return 1; }
  for _ in $(seq 1 60); do
    state="$(docker inspect -f '{{.State.Health.Status}}' "${container}")"
    [[ "${state}" == "healthy" ]] && return 0
    sleep 1
  done
  echo "${service} did not become healthy" >&2
  return 1
}

case "${suite}" in
  migration)
    compose up --detach postgres
    wait_healthy postgres
    compose run --rm --no-deps smoke
    ;;
  runtime)
    compose up --detach postgres redis
    wait_healthy postgres
    wait_healthy redis
    compose run --rm --no-deps smoke
    ;;
  storage)
    compose up --detach qdrant minio vault
    wait_healthy vault
    compose run --rm --no-deps smoke
    ;;
  artifact)
    compose up --detach postgres minio
    wait_healthy postgres
    compose run --rm --no-deps smoke
    ;;
  all)
    # Do not pass --abort-on-container-exit: vault-init is a one-shot container
    # and would abort the whole stack the moment it exits, before smoke starts.
    compose up --exit-code-from smoke
    ;;
esac
