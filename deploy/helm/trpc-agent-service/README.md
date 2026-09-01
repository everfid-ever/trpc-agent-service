# trpc-agent-service Helm chart

This chart deploys each implemented production role as an independent
Deployment and ServiceAccount. It intentionally does not install PostgreSQL,
Redis, object storage, scanners, model providers, ingress controllers, or
SecretProviderClass objects. Those are environment authorities and must be
provisioned separately.

## Safety defaults

- Every role runs as non-root with a read-only root filesystem, all Linux
  capabilities dropped, and no service-account token mounted by default.
- No Kubernetes `Secret` or secret-bearing `ConfigMap` is generated. Each role
  references an existing, role-scoped environment Secret for DSNs and other
  bootstrap locators. Runtime model, channel, payload, and DLP material is
  mounted read-only through Secrets Store CSI when enabled.
- NetworkPolicy starts from default-deny. DNS is the only default egress;
  database, Redis, object store, Collector, scanner, provider, ingress, and
  monitoring rules must be explicitly supplied per role.
- Gateway and Worker have PDB and HPA enabled. All roles use anti-affinity,
  topology spread, startup/liveness/readiness probes, zero-unavailable rolling
  updates, and a shell-free preStop hook.
- A preStop hook sends SIGTERM to PID 1 so the application enters the same
  lifecycle drain used for normal process termination. Worker grace is longer
  than its default drain, bundle-close, and HTTP shutdown budgets.

The defaults render references to environment Secrets such as
`trpc-agent-worker-env`; they do not create those Secrets. A missing Secret or
required variable makes the role fail closed instead of silently falling back
to an in-memory or mock backend.

## Controlled schema migration

Set `migration.enabled=true` only in a reviewed release values file. The chart
then runs the target image as a blocking `pre-install,pre-upgrade` hook before
changing application Deployments. `migration.expectedCurrent` and
`migration.target` are exact six-digit versions; `latest` is deliberately not
accepted. The Job takes a PostgreSQL session advisory lock, verifies the full
applied prefix and immutable checksums, applies forward transactions, and
verifies the target. A retry at the target succeeds without executing DDL
again; a source mismatch, gap, unknown row, checksum drift, or downgrade blocks
the release.

Only `TRPC_POSTGRES_DSN` belongs in the migration role's existing environment
Secret. Hook values override transition/timeout variables. Migration
ServiceAccount and NetworkPolicy prerequisites are retained until the next
hook run because the Job depends on them; after uninstall, remove those two
hook resources through the environment's reviewed cleanup procedure.

Production rollback never runs migration down SQL. Roll back the image and
immutable configuration while retaining the expand schema; N-1 readiness
checks its known checksums and tolerates later expand rows during the bounded
observation window. Contract cleanup is a separate, explicitly reviewed
release after rollback support ends.

## Render and install

Copy `values.production.example.yaml`, replace selectors, CIDRs, image digest,
existing Secret names, and SecretProviderClass names, then render before
installing:

```bash
helm lint deploy/helm/trpc-agent-service \
  --values /secure/change/production-values.yaml
helm template trpc-agent deploy/helm/trpc-agent-service \
  --namespace trpc-agent \
  --values /secure/change/production-values.yaml > rendered.yaml
kubectl apply --server-side --dry-run=server -f rendered.yaml
helm upgrade --install trpc-agent deploy/helm/trpc-agent-service \
  --namespace trpc-agent --create-namespace \
  --values /secure/change/production-values.yaml
```

Do not put credential values in a Helm values file. `existingEnvSecret` may
contain bootstrap environment entries such as `TRPC_POSTGRES_DSN` and
`TRPC_REDIS_PASSWORD`; the remaining scoped secrets belong in the CSI mount
whose path is supplied as `TRPC_SECRET_ROOT` by the chart.
Set `serviceAccountAnnotations` independently under each role; reusing one
cloud workload identity across Gateway, Worker, Channel, and Relay defeats the
chart's least-privilege boundary.

## NetworkPolicy values

`roles.<role>.networkPolicy.ingress` and `egress` are lists of Kubernetes
NetworkPolicy rules, not host names. Standard NetworkPolicy cannot safely
express Feishu, WeCom, model-provider, or other DNS allowlists. Use stable
provider CIDRs, an egress gateway selected by pod/namespace labels, or a
CNI-specific FQDN policy. Leaving a rule empty denies that traffic and keeps
the corresponding readiness check or operation fail closed.

The example file demonstrates selector-based PostgreSQL, Redis, object-store,
scanner, Collector, ingress-controller, and monitoring access. Its labels are
placeholders; they are not production authorization.

## Scaling boundary

Worker and Audit Relay HPAs use CPU plus external queue/Outbox metrics. The
metrics adapter must publish `trpc_broker_backlog_total` and
`trpc_audit_outbox_active_backlog` with an `AverageValue` query, and the two fresh-only lag
gauges with a `Value` query. Because every replica observes the same global
authority, adapter queries must use `max`, never `sum`, across scrape targets.
Autoscaling gauges disappear when their source snapshot is stale; a missing
custom metric therefore blocks unsafe scale-down while CPU may still scale up.

Gateway callback QPS/latency, Worker active sessions/model concurrency, and
Channel callback/delivery metrics are still release gates. A rendered HPA is
not evidence that the external-metrics API or adapter query is working.
