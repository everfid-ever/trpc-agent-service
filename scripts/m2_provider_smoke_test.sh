#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${TRPC_PROVIDER_SMOKE_CONFIG_FILE:-}"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'USAGE'
Usage: bash scripts/m2_provider_smoke_test.sh [config-file]

Runs the opt-in S3/ClamAV/DLP provider smoke. The optional owner-only config
file can contain provider credentials and is never printed. See
deploy/smoke/object-scan.env.example.
USAGE
  exit 0
fi
if [[ $# -gt 1 ]]; then
  echo "usage: $0 [config-file]" >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then
  config_file="$1"
fi
if [[ -n "${config_file}" ]]; then
  if [[ ! -f "${config_file}" ]]; then
    echo "config file not found" >&2
    exit 2
  fi
  mode="$(stat -f '%Lp' "${config_file}" 2>/dev/null || stat -c '%a' "${config_file}")"
  mode="${mode#0}"
  if (( (8#${mode} & 077) != 0 )); then
    echo "config file must not be group/world-readable; run chmod 600 on it" >&2
    exit 2
  fi
  set -a
  source "${config_file}"
  set +a
fi

if [[ "${TRPC_PROVIDER_SMOKE:-}" != "1" ]]; then
  echo "refusing: set TRPC_PROVIDER_SMOKE=1 for explicit real provider smoke" >&2
  exit 2
fi

required=(
  TRPC_S3_ENDPOINT
  TRPC_S3_BUCKET
  TRPC_S3_ACCESS_KEY
  TRPC_S3_SECRET_KEY
  TRPC_CLAMAV_ADDR
  TRPC_DLP_ENDPOINT
  TRPC_DLP_BEARER_TOKEN
  TRPC_DLP_REJECT_SAMPLE
  TRPC_DLP_UNKNOWN_SAMPLE
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "refusing: ${name} is required" >&2
    exit 2
  fi
done

go -C "${repo_root}" test -count=1 -run TestM2RealObjectStoreAndScannerProviderSmoke ./trpcservice/integration
