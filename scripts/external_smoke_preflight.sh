#!/usr/bin/env bash
# Records whether external release smoke is deliberately skipped, blocked, or
# ready. It never contacts providers and never emits configuration values.
set -euo pipefail

usage() {
  echo "usage: $0 [--config FILE] [--evidence-dir DIRECTORY]" >&2
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${TRPC_EXTERNAL_SMOKE_CONFIG_FILE:-${repo_root}/deploy/smoke/external-smoke.env}"
config_explicit=false
[[ -n "${TRPC_EXTERNAL_SMOKE_CONFIG_FILE:-}" ]] && config_explicit=true
evidence_dir=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      config_file="$2"
      config_explicit=true
      shift 2
      ;;
    --evidence-dir)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      evidence_dir="$2"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

if [[ -f "${config_file}" ]]; then
  mode="$(stat -f '%Lp' "${config_file}" 2>/dev/null || stat -c '%a' "${config_file}")"
  mode="${mode#0}"
  if (( (8#${mode} & 077) != 0 )); then
    echo "external smoke config must not be group/world-readable" >&2
    exit 2
  fi
  set -a
  source "${config_file}"
  set +a
elif [[ "${config_explicit}" == true ]]; then
  echo "external smoke config file not found" >&2
  exit 2
fi

if [[ -z "${evidence_dir}" ]]; then
  evidence_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-external-smoke.XXXXXX")"
else
  mkdir -p "${evidence_dir}"
fi
report="${evidence_dir}/external-smoke-status.txt"

approved="${TRPC_EXTERNAL_SMOKE_APPROVED:-0}"
if [[ "${approved}" != "0" && "${approved}" != "1" ]]; then
  echo "TRPC_EXTERNAL_SMOKE_APPROVED must be 0 or 1" >&2
  exit 2
fi

requirement_for() {
  case "$1" in
    deepseek) echo TRPC_EXTERNAL_DEEPSEEK_CONFIG_FILE ;;
    object_scan) echo TRPC_EXTERNAL_OBJECT_SCAN_CONFIG_FILE ;;
    im) echo TRPC_EXTERNAL_IM_CONFIG_FILE ;;
    otel) echo TRPC_EXTERNAL_OTEL_EVIDENCE_REF ;;
    kubernetes) echo TRPC_EXTERNAL_KUBERNETES_EVIDENCE_REF ;;
    *) return 1 ;;
  esac
}

status=0
overall="READY"
{
  printf 'revision=%s\n' "$(git -C "${repo_root}" rev-parse HEAD)"
  printf 'approval=%s\n' "${approved}"
  if [[ "${approved}" == "0" ]]; then
    overall="SKIPPED"
    printf 'overall=SKIPPED\n'
    printf 'deepseek=SKIPPED:not_approved\n'
    printf 'object_scan=SKIPPED:not_approved\n'
    printf 'im=SKIPPED:not_approved\n'
    printf 'otel=SKIPPED:not_approved\n'
    printf 'kubernetes=SKIPPED:not_approved\n'
  else
    for target in deepseek object_scan im otel kubernetes; do
      requirement="$(requirement_for "${target}")"
      if [[ -z "${!requirement:-}" ]]; then
        printf '%s=BLOCKED:missing_%s\n' "${target}" "${requirement}"
        status=1
        overall="BLOCKED"
      elif [[ "${target}" == deepseek || "${target}" == object_scan || "${target}" == im ]] && [[ ! -f "${!requirement}" ]]; then
        printf '%s=BLOCKED:unreadable_%s\n' "${target}" "${requirement}"
        status=1
        overall="BLOCKED"
      else
        printf '%s=READY:operator_action_required\n' "${target}"
      fi
    done
    printf 'overall=%s\n' "${overall}"
  fi
} >"${report}"

if [[ "${overall}" == "BLOCKED" ]]; then
  echo "external smoke is blocked; status retained at ${report}" >&2
  exit 3
fi
echo "external smoke ${overall}; status retained at ${report}"
