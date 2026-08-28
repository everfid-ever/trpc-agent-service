#!/usr/bin/env bash
set -euo pipefail

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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go -C "${repo_root}" test -count=1 -run TestM2RealObjectStoreAndScannerProviderSmoke ./trpcservice/integration
