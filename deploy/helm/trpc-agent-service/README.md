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

The built-in HPA uses CPU as a safe bootstrap signal because it requires no
custom-metrics API. Production promotion must replace or extend it with the
frozen role SLI: Gateway callback QPS/latency, Worker queue oldest age/active
sessions/model concurrency, Relay Outbox lag, and Channel callback or delivery
backlog. CPU-only autoscaling is not release evidence for those capacity SLOs.
