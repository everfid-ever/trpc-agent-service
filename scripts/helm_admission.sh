#!/usr/bin/env bash
# Renders the non-secret production example with pinned, disposable tool
# containers. The output is review evidence, not an authorization to deploy.
set -euo pipefail

usage() {
  echo "usage: $0 [--output-dir DIRECTORY]" >&2
}

evidence_dir=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      evidence_dir="$2"
      shift 2
      ;;
    *) usage; exit 2 ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
chart="deploy/helm/trpc-agent-service"
values="${chart}/values.production.example.yaml"
helm_image="alpine/helm:3.17.0@sha256:93aaa4b514d91720861dd6c9af51359013b178963a47ab421b7fa648ccc7de80"
kubeconform_image="ghcr.io/yannh/kubeconform:v0.6.7@sha256:0925177fb05b44ce18574076141b5c3d83235e1904d3f952182ac99ddc45762c"
roles=(gateway worker channel channel-delivery preprocess artifact audit-relay audit-query audit-purge business-audit-purge)

command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 2; }
docker version >/dev/null 2>&1 || { echo "a running Docker daemon is required" >&2; exit 2; }

created_evidence_dir=false
if [[ -z "${evidence_dir}" ]]; then
  evidence_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-helm-evidence.XXXXXX")"
  created_evidence_dir=true
else
  mkdir -p "${evidence_dir}"
fi

cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then
    echo "Helm admission failed; evidence retained at ${evidence_dir}" >&2
  elif [[ "${created_evidence_dir}" == true ]]; then
    echo "Helm admission passed; evidence retained at ${evidence_dir}"
  fi
  exit "${status}"
}
trap cleanup EXIT

run_helm() {
  docker run --rm \
    --mount "type=bind,src=${repo_root},dst=/work,readonly" \
    --workdir /work "${helm_image}" "$@"
}

run_helm version --short >"${evidence_dir}/tool-versions.txt"
docker run --rm "${kubeconform_image}" -v >>"${evidence_dir}/tool-versions.txt"
run_helm lint "${chart}" --strict --values "${values}" >"${evidence_dir}/helm-lint.txt"
run_helm template trpc-agent "${chart}" --namespace trpc-agent --values "${values}" >"${evidence_dir}/rendered.yaml"

if grep -Eq '^kind: (Secret|ConfigMap)$' "${evidence_dir}/rendered.yaml"; then
  echo "rendered production example must not create Secret or ConfigMap" >&2
  exit 1
fi

for role in "${roles[@]}"; do
  if ! grep -Fq "args: [\"${role}\"]" "${evidence_dir}/rendered.yaml"; then
    echo "rendered chart is missing production role ${role}" >&2
    exit 1
  fi
done

readiness_count="$(grep -Fc 'path: /readyz' "${evidence_dir}/rendered.yaml" || true)"
liveness_count="$(grep -Fc 'path: /livez' "${evidence_dir}/rendered.yaml" || true)"
prestop_count="$(grep -Fc 'command: ["/trpc-service", "prestop"]' "${evidence_dir}/rendered.yaml" || true)"
if [[ "${readiness_count}" -ne "${#roles[@]}" || "${liveness_count}" -lt "${#roles[@]}" || "${prestop_count}" -ne "${#roles[@]}" ]]; then
  echo "rendered role lifecycle contract mismatch: ready=${readiness_count} live=${liveness_count} prestop=${prestop_count} roles=${#roles[@]}" >&2
  exit 1
fi
printf 'roles=%s\nreadyz=%s\nlivez=%s\nprestop=%s\n' "${#roles[@]}" "${readiness_count}" "${liveness_count}" "${prestop_count}" >"${evidence_dir}/role-lifecycle.txt"

docker run --rm -i "${kubeconform_image}" \
  -strict -summary -kubernetes-version 1.31.0 \
  <"${evidence_dir}/rendered.yaml" >"${evidence_dir}/kubeconform.txt"

{
  printf 'revision=%s\n' "$(git -C "${repo_root}" rev-parse HEAD)"
  printf 'helm_image=%s\n' "${helm_image}"
  printf 'kubeconform_image=%s\n' "${kubeconform_image}"
  printf 'values=%s\n' "${values}"
  printf 'sha256_rendered_yaml=%s\n' "$(shasum -a 256 "${evidence_dir}/rendered.yaml" | awk '{print $1}')"
} >"${evidence_dir}/manifest.txt"
