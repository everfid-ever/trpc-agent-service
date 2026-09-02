# Audit retention purge runbook

Destruction of compliance facts is one-way and irreversible. This runbook covers
policy configuration, legal hold, batch approval and quarantine. The `audit-purge`
reconciler plans and executes approved batches; approval runs only through the
operator CLI, never over HTTP.

## Safety defaults

- `TRPC_AUDIT_PURGE_DRY_RUN=true` and `TRPC_AUDIT_PURGE_REQUIRE_APPROVAL=true`
  are the defaults. Do not run production with both disabled without an explicit
  change request.
- The compliance store refuses `DELETE` unless the session is a `compliance_purger`
  member and the transaction set `compliance.purge_authorized` inside the guarded
  SECURITY DEFINER function. Setting the GUC from a client grants nothing.

## Grant the purger role (once per environment)

The migration creates `compliance_purger` (`NOLOGIN`). The purge service account
must be made a member out of band:

```sql
GRANT compliance_purger TO <audit_purge_db_user>;
```

The `audit-purge` role's `TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN` must authenticate as
that user. A superuser is implicitly a member and can drive dev/test rehearsals.

## Configure retention policy and floor

Floors are seeded at `default=180d`, `security=365d`, `billing=10y` and may only
increase. A tenant override is append-only and may never go below the floor:

```sql
INSERT INTO compliance.audit_retention_policy(tenant_id, version, retention_seconds, actor, reason)
VALUES ('<tenant>', 1, 86400, '<operator>', 'shorter business retention'); -- still clamped by GREATEST(floor, policy)
```

Effective retention = `GREATEST(tenant policy, floor(class))` where `class` comes
from the longest-prefix `action` rule. Unknown actions fall back to `default`.

## Place / release a legal hold

```sql
INSERT INTO compliance.audit_legal_hold(tenant_id, hold_id, event, scope_start, scope_end, actor, reason)
VALUES ('<tenant>', 'lit-2026-09-02', 'placed', '-infinity', 'infinity', '<counsel>', 'case hold');
-- release (append-only):
INSERT INTO compliance.audit_legal_hold(tenant_id, hold_id, event, actor, reason)
VALUES ('<tenant>', 'lit-2026-09-02', 'released', '<counsel>', 'hold lifted');
```

Active holds block purge of events whose `occurred_at` falls inside the range.

## Approve a batch

```sh
TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN=... go run ./cmd/audit-purge list <tenant>
TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN=... go run ./cmd/audit-purge approve -approver <sub> -reason <why> <tenant> <batch>
```

The reconciler only executes `approved` batches. Each batch is scoped to one
`(tenant, class)` and one cutoff; the candidate digest is recomputed at execution
time and any divergence fails closed.

## Inspect a destruction certificate

```sh
TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN=... go run ./cmd/audit-purge certificate <tenant> <batch>
```

The certificate binds `policy_version`, `floor_version`, tenant, window, candidate
digest and alert count. It is immutable and never deleted.

## Quarantine

```sh
TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN=... go run ./cmd/audit-purge quarantine -owner <sub> <tenant> <batch>
```

Quarantined batches are terminal. Diagnose `last_error_class` (`divergence`,
`unresolved_quarantine`, `preview_expired`); for `unresolved_quarantine`, record a
`compliance.audit_quarantine_resolution` row and re-plan.

## Incident response

- Purge backlog (oldest retained age beyond policy) growing: verify the
  `audit-purge` replica is Ready, the purger role is granted, and dry-run is off
  when destruction is intended.
- A batch repeatedly failing with `divergence`: inspect what changed in the
  candidate window; do not edit the batch row.
- `audit_query_record` rows are never destroyed by the event purge path.

Rollback note: `000002_audit_retention.down` refuses to run once any batch is
`completed`; destruction is not reversible.
