# Business audit retention purge runbook

This role destroys expired source-library `audit_event` rows and already
published `outbox(kind='audit')` rows. It is independent of compliance-fact
retention: the source row is eligible only before the relay watermark, so every
non-`published` Outbox state—including `dead_letter`—blocks deletion.

## Safety defaults

- `TRPC_BUSINESS_AUDIT_PURGE_DRY_RUN=true` is the default. Keep it enabled
  until a reviewed destruction change is approved.
- `TRPC_BUSINESS_AUDIT_RETENTION` defaults to `4320h` (180 days) and is never
  allowed below 24 hours by the role configuration.
- Each batch rechecks its immutable candidate count/digest and relay watermark
  immediately before deletion. Drift leaves the batch failed; it does not
  delete a partial candidate set.
- A completed batch creates an immutable destruction certificate. Schema
  rollback cannot restore destroyed source facts; do not use a migration down
  as an operational recovery mechanism.

## Provision the dedicated database principal

`000035_business_audit_retention` creates `audit_retention_purger` as a
`NOLOGIN` role. Grant it only to the database user carried by the dedicated
business-purge workload secret:

```sql
GRANT audit_retention_purger TO <business_audit_purge_db_user>;
```

The workload's `TRPC_BUSINESS_AUDIT_PURGE_POSTGRES_DSN` must authenticate as
that user. Do not reuse the gateway, worker, or audit-relay database user.
The role has function-only access; normal sessions cannot bypass the immutable
`audit_event` trigger by setting `audit.purge_authorized` themselves.

## Enable a controlled run

The Helm role is `business-audit-purge`. Its existing environment secret must
provide at least:

```text
TRPC_BUSINESS_AUDIT_PURGE_POSTGRES_DSN=...
TRPC_BUSINESS_AUDIT_PURGE_OWNER=business-audit-purge-<environment>
```

Keep dry-run on initially, observe `/metrics` and the batch tables, then make
the reviewed values explicit:

```text
TRPC_BUSINESS_AUDIT_PURGE_DRY_RUN=false
TRPC_BUSINESS_AUDIT_RETENTION=4320h
TRPC_BUSINESS_AUDIT_PURGE_MAX_BATCH_SIZE=1000
```

`dead_letter` audit Outbox rows require recovery or an explicit incident
decision before source cleanup can proceed; they must never be deleted by this
role.
