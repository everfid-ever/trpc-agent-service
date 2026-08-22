#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
  echo "Go files must be formatted with gofmt:" >&2
  echo "$unformatted" >&2
  exit 1
fi
