#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

bash scripts/check-format.sh
bash scripts/check-dependency-boundaries.sh
go vet ./...
if command -v golangci-lint >/dev/null 2>&1; then
  golangci-lint run ./...
fi
