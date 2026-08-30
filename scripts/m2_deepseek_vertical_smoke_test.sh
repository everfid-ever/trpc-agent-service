#!/usr/bin/env bash
set -euo pipefail

if [[ "${TRPC_M2_DEEPSEEK_SMOKE:-}" != "1" ]]; then
  echo "refusing: set TRPC_M2_DEEPSEEK_SMOKE=1 for the real Gateway/Worker/DeepSeek smoke" >&2
  exit 2
fi

required=(
  TRPC_M2_GATEWAY_URL
  TRPC_M2_GATEWAY_BEARER_TOKEN
  TRPC_M2_DEEPSEEK_PROFILE_ID
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "refusing: ${name} is required" >&2
    exit 2
  fi
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go -C "${repo_root}" test -count=1 -run TestM2GatewayWorkerDeepSeekSmoke ./trpcservice/integration
