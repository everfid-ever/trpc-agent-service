#!/usr/bin/env bash
set -euo pipefail

if [[ "${TRPC_M2_IM_PROVIDER_SMOKE:-}" != "1" ]]; then
  echo "refusing: set TRPC_M2_IM_PROVIDER_SMOKE=1; this test sends visible Feishu and WeCom messages" >&2
  exit 1
fi

required=(
  TRPC_M2_FEISHU_SECRET_FILE
  TRPC_M2_FEISHU_APP_ID
  TRPC_M2_FEISHU_MESSAGE_ID
  TRPC_M2_WECOM_SECRET_FILE
  TRPC_M2_WECOM_CORP_ID
  TRPC_M2_WECOM_USER_ID
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "${name} is required" >&2
    exit 1
  fi
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go -C "${repo_root}" test -count=1 -run TestM2RealFeishuAndWeComCredentialSmoke ./trpcservice/integration
