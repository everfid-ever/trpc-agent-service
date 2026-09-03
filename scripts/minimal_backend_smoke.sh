#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.minimal.yml"
project="trpc-minimal-${RANDOM}${RANDOM}"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-minimal-smoke.XXXXXX")"
command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then docker compose --project-name "${project}" -f "${compose_file}" logs --no-color >"${diagnostics}/compose.log" || true; echo "minimal smoke failed; diagnostics retained at ${diagnostics}/compose.log" >&2; fi
  docker compose --project-name "${project}" -f "${compose_file}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  [[ ${status} -ne 0 ]] || rmdir "${diagnostics}" || true
  exit "${status}"
}
trap cleanup EXIT
# Do not pass --abort-on-container-exit: vault-init is a one-shot container and
# would abort the whole stack the moment it exits, before smoke even starts.
docker compose --project-name "${project}" -f "${compose_file}" up --exit-code-from smoke
