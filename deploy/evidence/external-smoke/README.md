# External smoke evidence

External smoke is opt-in and may contact providers or send visible IM messages.
No CI job runs it. Start every release record with a truthful preflight:

```bash
cp deploy/smoke/external-smoke.env.example /secure/change/external-smoke.env
chmod 600 /secure/change/external-smoke.env
TRPC_EXTERNAL_SMOKE_CONFIG_FILE=/secure/change/external-smoke.env \
  bash scripts/external_smoke_preflight.sh --evidence-dir /secure/change/external-smoke-evidence
```

With `TRPC_EXTERNAL_SMOKE_APPROVED=0`, the result is `SKIPPED`, not `PASSED`.
Set it to `1` only after owners authorize all resources. Missing references then
produce `BLOCKED` with a nonzero exit; a `READY` result only means the operator
may run the target smoke, not that it has passed.

Use the owner-only templates in `deploy/smoke/` for DeepSeek, S3/ClamAV/DLP and
IM. Do not commit their copies. Never attach tokens, secret values, raw provider
responses, customer IDs, or full message payloads to evidence. Record only the
revision, time window, selected target, sanitized result, a stable external
request/message identifier where approved, and a link to the access-controlled
raw log.

Use [release-record.template.md](./release-record.template.md) as the
access-controlled change-record format. `PASSED` may only be entered after an
operator actually runs the approved target command; preflight output alone is
not evidence of a passed smoke.

OTEL and Kubernetes need environment-specific query/dry-run and drain plans;
put their reviewed ticket or runbook references in the preflight config. They
remain `READY` until an operator appends real evidence to the release record.
