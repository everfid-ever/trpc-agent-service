#!/usr/bin/env bash
# Runs the repository-verifiable IM callback boundary without claiming that it
# substitutes for a vendor-account or public-callback acceptance record.
set -euo pipefail

go test -count=1 ./trpcservice/integration -run '^TestIMFeishuAndWeComDurableHTTPIngressContract$'
go test -count=1 \
  ./trpcservice/channels/feishu \
  ./trpcservice/channels/feishu/protocol \
  ./trpcservice/channels/wecom \
  ./trpcservice/channels/wecom/protocol \
  ./trpcservice/channels/ingress \
  ./trpcservice/channels/delivery
