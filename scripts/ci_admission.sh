#!/usr/bin/env bash
# Runs the repository-only admission gate with disposable Go caches. The
# backend smoke has its own Docker job because it needs a Docker daemon.
set -euo pipefail

case "${1:-}" in
  ""|--race) ;;
  *) echo "usage: $0 [--race]" >&2; exit 2 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

go_version="$(go env GOVERSION)"
if [[ "${go_version}" != go1.21.* ]]; then
  echo "Go 1.21.x is required for the CI compatibility gate; found ${go_version}" >&2
  exit 2
fi

# RUNNER_TEMP is job-private on GitHub Actions.  Fall back to TMPDIR for local
# execution, but never touch a shared Go cache.  Some integration helpers can
# leave files owned by another user in this directory; cleanup is best-effort
# and must not turn a successful admission run into a failed job.
temp_parent="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
cache_root="$(mktemp -d "${temp_parent%/}/trpc-ci-go.XXXXXX")"
cleanup() {
  local status=$?
  trap - EXIT
  if ! rm -rf -- "${cache_root}"; then
    echo "warning: could not fully remove disposable Go cache ${cache_root}; the ephemeral runner will discard it" >&2
  fi
  exit "${status}"
}
trap cleanup EXIT
export GOMODCACHE="${cache_root}/mod"
export GOCACHE="${cache_root}/build"
export GOPATH="${cache_root}/path"
mkdir -p "${GOMODCACHE}" "${GOCACHE}" "${GOPATH}"

go mod download
go mod verify

if [[ "${1:-}" == "--race" ]]; then
  go test -count=1 -race ./...
  exit 0
fi

while IFS= read -r -d '' script; do
  bash -n "${script}"
done < <(find scripts -type f -name '*.sh' -print0)

bash scripts/check-format.sh
bash scripts/check-dependency-boundaries.sh
go build ./...
go vet ./...
go test -count=1 ./...
git diff --check
