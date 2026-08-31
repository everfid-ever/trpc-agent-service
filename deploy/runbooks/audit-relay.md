# Audit Relay incident runbook

1. Check `/readyz` and `/metrics` on every `audit-relay` instance. A missing or stale snapshot means the PostgreSQL query path is not healthy even if an older gauge still exists.
2. Inspect source PostgreSQL connectivity, independent compliance PostgreSQL connectivity/schema readiness, Relay logs and the fixed-state `trpc_audit_outbox_backlog` gauges. Do not delete, rewrite, or manually mark Outbox rows as published.
3. For `dead_letter > 0`, preserve the rows and diagnose resolver or sink incompatibility. For pending/retry lag, restore the dependency or add Relay capacity; leases and idempotent export provide recovery.
4. Confirm recovery only after backlog age/count fall below the configured thresholds, dead-letter is zero, export errors stop increasing, and immutable `compliance.audit_event` rows are queryable in the compliance database.
5. For `ArtifactQuarantined`, query `compliance.quarantine_alert` by the incident window, then follow `docs/runbook/artifact-lifecycle.md`. Do not acknowledge an alert by editing or deleting the immutable row.
6. Record the incident window and the oldest affected Outbox timestamp. Telemetry loss never substitutes for Audit Outbox or compliance-store verification.
