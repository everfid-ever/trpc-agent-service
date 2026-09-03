#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_config="${repo_root}/deploy/smoke/m3-im.env"
config_file="${TRPC_M3_IM_CONFIG_FILE:-${default_config}}"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'USAGE'
Usage: bash scripts/m3_im_provider_smoke_test.sh [config-file]

Runs the opt-in M3 real Feishu/WeCom delivery smoke. The test sends a visible
nonce message to each selected provider. Copy deploy/smoke/m3-im.env.example
to an owner-only file, fill only locators and absolute credential-file paths,
then run this script.
USAGE
  exit 0
fi
if [[ $# -gt 1 ]]; then
  echo "usage: $0 [config-file]" >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then
  config_file="$1"
  if [[ ! -f "${config_file}" ]]; then
    echo "config file not found: ${config_file}" >&2
    exit 1
  fi
fi
if [[ -f "${config_file}" ]]; then
  mode="$(stat -f '%Lp' "${config_file}" 2>/dev/null || stat -c '%a' "${config_file}")"
  mode="${mode#0}"
  if (( (8#${mode} & 077) != 0 )); then
    echo "config file must not be group/world-readable; run chmod 600 on it" >&2
    exit 1
  fi
  set -a
  # This is a local operator-owned shell environment file. It must contain
  # locators and credential-file paths only, never credential values.
  source "${config_file}"
  set +a
elif [[ -z "${TRPC_M3_IM_PROVIDER_SMOKE:-}" ]]; then
  echo "missing ${default_config}; copy deploy/smoke/m3-im.env.example to an owner-only file and fill it first" >&2
  exit 1
fi

if [[ "${TRPC_M3_IM_PROVIDER_SMOKE:-}" != "1" ]]; then
  echo "refusing: set TRPC_M3_IM_PROVIDER_SMOKE=1; this test sends visible provider messages" >&2
  exit 1
fi

providers="${TRPC_M3_IM_PROVIDERS:-all}"
case "${providers}" in
  all|feishu,wecom|wecom,feishu)
    use_feishu=1
    use_wecom=1
    providers="feishu,wecom"
    ;;
  feishu)
    use_feishu=1
    use_wecom=0
    ;;
  wecom)
    use_feishu=0
    use_wecom=1
    ;;
  *)
    echo "TRPC_M3_IM_PROVIDERS must be feishu, wecom, or feishu,wecom" >&2
    exit 1
    ;;
esac
export TRPC_M3_IM_PROVIDERS="${providers}"

require_value() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "${name} is required" >&2
    exit 1
  fi
}

validate_secret_file() {
  local name="$1"
  local path="${!name:-}"
  require_value "${name}"
  if [[ "${path}" != /* || ! -f "${path}" ]]; then
    echo "${name} must name an existing absolute regular file" >&2
    exit 1
  fi
  local mode
  if mode="$(stat -f '%Lp' "${path}" 2>/dev/null)"; then
    :
  else
    mode="$(stat -c '%a' "${path}")"
  fi
  mode="${mode#0}"
  if (( (8#${mode} & 077) != 0 )); then
    echo "${name} must not be group/world-readable; run: chmod 600 ${path}" >&2
    exit 1
  fi
}

if [[ "${use_feishu}" == "1" ]]; then
  validate_secret_file TRPC_M3_FEISHU_SECRET_FILE
  require_value TRPC_M3_FEISHU_APP_ID
  require_value TRPC_M3_FEISHU_MESSAGE_ID
fi
if [[ "${use_wecom}" == "1" ]]; then
  validate_secret_file TRPC_M3_WECOM_SECRET_FILE
  require_value TRPC_M3_WECOM_CORP_ID
  require_value TRPC_M3_WECOM_USER_ID
fi

test_pattern='^TestM3RealIMCredentialSmoke$'
if command -v go >/dev/null 2>&1; then
  exec go -C "${repo_root}" test -count=1 -run "${test_pattern}" ./trpcservice/integration
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "Go or Docker is required to run the provider smoke" >&2
  exit 1
fi

docker_args=(run --rm -v "${repo_root}:/src:ro" -w /src
  -e TRPC_M3_IM_PROVIDER_SMOKE -e TRPC_M3_IM_PROVIDERS)
if [[ "${use_feishu}" == "1" ]]; then
  docker_args+=(-v "${TRPC_M3_FEISHU_SECRET_FILE}:/run/secrets/m3-feishu.json:ro"
    -e TRPC_M3_FEISHU_SECRET_FILE=/run/secrets/m3-feishu.json
    -e TRPC_M3_FEISHU_APP_ID -e TRPC_M3_FEISHU_MESSAGE_ID)
fi
if [[ "${use_wecom}" == "1" ]]; then
  docker_args+=(-v "${TRPC_M3_WECOM_SECRET_FILE}:/run/secrets/m3-wecom.json:ro"
    -e TRPC_M3_WECOM_SECRET_FILE=/run/secrets/m3-wecom.json
    -e TRPC_M3_WECOM_CORP_ID -e TRPC_M3_WECOM_USER_ID)
fi
exec docker "${docker_args[@]}" golang:1.25-bookworm \
  go test -count=1 -run "${test_pattern}" ./trpcservice/integration
