#!/usr/bin/env bash
# Keep the three stateful platform seams that carry tenant identity, session
# semantics and provider delivery above a useful unit-test floor. This is a
# package gate, not a substitute for the repository/diff coverage ratchet.
set -euo pipefail

minimum="${TRPC_CORE_PACKAGE_MIN_COVERAGE:-80.0}"
packages=(
  ./trpcservice/tenant
  ./trpcservice/storage/session
  ./trpcservice/channels/delivery
)

for package in "${packages[@]}"; do
  output="$(go test -count=1 -cover "${package}")"
  printf '%s\n' "${output}"
  coverage="$(awk '/coverage: [0-9.]+% of statements/ { for (i = 1; i <= NF; i++) if ($i == "coverage:") { value = $(i + 1); sub("%", "", value); print value } }' <<<"${output}" | tail -n 1)"
  [[ -n "${coverage}" ]] || { echo "coverage output missing for ${package}" >&2; exit 1; }
  awk -v actual="${coverage}" -v required="${minimum}" 'BEGIN { exit !(actual + 0 >= required + 0) }' || {
    echo "core package ${package} coverage ${coverage}% is below required ${minimum}%" >&2
    exit 1
  }
  echo "core package coverage: ${package} ${coverage}% (minimum ${minimum}%)"
done
