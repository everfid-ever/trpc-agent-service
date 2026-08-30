#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "M2 real-provider smoke moved to the M3 final acceptance entrypoint" >&2

export TRPC_M3_IM_PROVIDER_SMOKE="${TRPC_M3_IM_PROVIDER_SMOKE:-${TRPC_M2_IM_PROVIDER_SMOKE:-}}"
export TRPC_M3_IM_PROVIDERS="${TRPC_M3_IM_PROVIDERS:-feishu,wecom}"
export TRPC_M3_FEISHU_SECRET_FILE="${TRPC_M3_FEISHU_SECRET_FILE:-${TRPC_M2_FEISHU_SECRET_FILE:-}}"
export TRPC_M3_FEISHU_APP_ID="${TRPC_M3_FEISHU_APP_ID:-${TRPC_M2_FEISHU_APP_ID:-}}"
export TRPC_M3_FEISHU_MESSAGE_ID="${TRPC_M3_FEISHU_MESSAGE_ID:-${TRPC_M2_FEISHU_MESSAGE_ID:-}}"
export TRPC_M3_WECOM_SECRET_FILE="${TRPC_M3_WECOM_SECRET_FILE:-${TRPC_M2_WECOM_SECRET_FILE:-}}"
export TRPC_M3_WECOM_CORP_ID="${TRPC_M3_WECOM_CORP_ID:-${TRPC_M2_WECOM_CORP_ID:-}}"
export TRPC_M3_WECOM_USER_ID="${TRPC_M3_WECOM_USER_ID:-${TRPC_M2_WECOM_USER_ID:-}}"

exec bash "${repo_root}/scripts/m3_im_provider_smoke_test.sh" "$@"
