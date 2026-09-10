#!/usr/bin/env bash
# Coverage is a ratchet: repository coverage may not fall beneath the current
# baseline, and pull requests must cover at least 80% of changed executable Go
# lines.  The latter is intentionally scoped to PRs because push/workflow runs
# do not have a trustworthy base revision.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

minimum_total="${TRPC_MIN_TOTAL_COVERAGE:-42.0}"
base_sha="${TRPC_DIFF_BASE_SHA:-}"
coverage_file="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage.XXXXXX")"
changed_file="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage-diff.XXXXXX")"
lines_file="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage-lines.XXXXXX")"
test_log="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage-test.XXXXXX")"
cleanup() {
  local status=$?
  rm -f -- "$coverage_file" "$changed_file" "$lines_file" "$test_log"
  exit "$status"
}
trap cleanup EXIT

if ! go test -count=1 -covermode=atomic -coverprofile="$coverage_file" ./... >"$test_log" 2>&1; then
  cat "$test_log" >&2
  exit 1
fi
total="$(go tool cover -func="$coverage_file" | awk '/^total:/ { gsub("%", "", $3); print $3 }')"
awk -v actual="$total" -v minimum="$minimum_total" 'BEGIN { exit !(actual + 0 >= minimum + 0) }' || {
  echo "repository coverage ${total}% is below required ${minimum_total}%" >&2
  exit 1
}
echo "repository coverage: ${total}% (minimum ${minimum_total}%)"

[[ -n "$base_sha" ]] || exit 0
git cat-file -e "${base_sha}^{commit}" 2>/dev/null || {
  echo "diff coverage base is unavailable: ${base_sha}" >&2
  exit 2
}
git diff --unified=0 "$base_sha"...HEAD -- '*.go' >"$changed_file"
awk '
  /^\+\+\+ b\// { file=substr($0, 7); next }
  /^@@ / {
    split($0, pieces, "+"); split(pieces[2], range, " "); split(range[1], coords, ",")
    line=coords[1]; next
  }
  /^\+/ && !/^\+\+\+/ { if (file != "") print file ":" line; line++; next }
  /^ / { line++ }
' "$changed_file" >"$lines_file"

[[ -s "$lines_file" ]] || exit 0
awk '
  NR == FNR { wanted[$0]=1; next }
  NR == 1 { next }
  {
    split($1, pathAndRange, ":"); path=pathAndRange[1]
    sub("^github.com/liuzengh/trpc-agent-service/", "", path)
    split(pathAndRange[2], span, ","); split(span[1], start, "."); split(span[2], end, ".")
    for (key in wanted) {
      split(key, keyParts, ":")
      if (keyParts[1] == path && keyParts[2] + 0 >= start[1] && keyParts[2] + 0 <= end[1]) {
        executable[key]=1
        if ($3 + 0 > 0) covered[key]=1
      }
    }
  }
  END {
    for (key in executable) { total++; if (covered[key]) hit++ }
    if (total == 0) exit 0
    pct=(100 * hit / total)
    printf("changed executable-line coverage: %.1f%% (%d/%d)\n", pct, hit, total)
    exit !(pct >= 80)
  }
' "$lines_file" "$coverage_file" || {
  echo "changed executable-line coverage is below 80%" >&2
  exit 1
}
