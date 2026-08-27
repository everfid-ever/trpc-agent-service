#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

required_module="trpc.group/trpc-go/trpc-agent-go"
required_version="v1.11.2"
required_feishu_module="github.com/larksuite/oapi-sdk-go/v3"
required_feishu_version="v3.10.0"

actual_version="$(go list -m -f '{{.Version}}' "$required_module")"
if [[ "$actual_version" != "$required_version" ]]; then
  echo "dependency baseline mismatch: $required_module=$actual_version, want $required_version" >&2
  exit 1
fi

actual_feishu_version="$(go list -m -f '{{.Version}}' "$required_feishu_module")"
if [[ "$actual_feishu_version" != "$required_feishu_version" ]]; then
  echo "dependency baseline mismatch: $required_feishu_module=$actual_feishu_version, want $required_feishu_version" >&2
  exit 1
fi

if go list -m all | awk '{print $1}' | grep -Eq '^trpc\.group/trpc-go/trpc-agent-go/openclaw$'; then
  echo "forbidden module dependency: trpc-agent-go/openclaw" >&2
  exit 1
fi

forbidden_imports='"trpc\.group/trpc-go/trpc-agent-go/(openclaw|internal)(/|\")'

scan_forbidden_imports() {
  if command -v rg >/dev/null 2>&1; then
    rg -n --glob '*.go' "$forbidden_imports" cmd trpcservice
    return
  fi

  local found=1
  local file
  while IFS= read -r -d '' file; do
    if grep -nEH "$forbidden_imports" "$file"; then
      found=0
    fi
  done < <(find cmd trpcservice -type f -name '*.go' -print0)
  return "$found"
}

if scan_forbidden_imports; then
  echo "service code must not import trpc-agent-go/openclaw or trpc-agent-go/internal" >&2
  exit 1
fi

if ! grep -Eq '^go 1\.21([[:space:]]|$)' go.mod; then
  echo "go.mod must retain the Go 1.21 baseline" >&2
  exit 1
fi
