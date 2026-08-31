# Kubernetes rollout and rollback

This runbook covers the production Helm chart in
`deploy/helm/trpc-agent-service`. It does not provision shared databases,
Redis, object storage, scanners, provider credentials, or ingress.

## Preconditions

1. Use an immutable image digest and retain the previous N-1 digest.
2. Apply compatible expand migrations through the controlled migration
   procedure before enabling a producer that needs the new schema. Normal
   application Pods must never migrate schemas on startup.
3. Create one least-privilege environment Secret and, where required, one
   SecretProviderClass/workload identity per role. Never render credential
   values into Helm output.
4. Review every NetworkPolicy rule against the role dependency matrix. Empty
   dependency/provider rules intentionally leave the role unready or the
   operation fail closed.
5. Confirm the cluster has Metrics Server (or a compatible resource metrics
   API), a NetworkPolicy-enforcing CNI, and Secrets Store CSI before enabling
   their corresponding chart features.

## Preflight

```bash
helm lint deploy/helm/trpc-agent-service \
  --strict --values /secure/change/production-values.yaml
helm template trpc-agent deploy/helm/trpc-agent-service \
  --namespace trpc-agent \
  --values /secure/change/production-values.yaml > rendered.yaml
kubeconform -strict -summary rendered.yaml
kubectl apply --server-side --dry-run=server -f rendered.yaml
```

Inspect only object names, images, ServiceAccounts, probes, resource requests,
PDB/HPA bounds, and policy selectors. Do not print Secret objects or decoded
environment values into a ticket or CI log.

## Canary and promotion

1. Record the current Helm revision, image digest, active ConfigSnapshot
   versions, schema checksum, Audit backlog, and operational SLO snapshot.
2. Upgrade consumers before producers whenever an Envelope or stored contract
   changes. Select a canary tenant/config through immutable control-plane
   snapshots; do not create a second ad-hoc deployment state machine.
3. Install or upgrade with `--atomic --wait` and a timeout longer than the
   Worker termination grace period.
4. Verify every enabled Deployment has available replicas, readiness is green,
   PDB expected disruptions are nonzero, HPA can read metrics, Audit oldest age
   is within threshold, and no stale-fence/cross-tenant/secret-canary alert is
   firing.
5. Promote by publishing a new immutable active configuration. In-flight
   requests remain pinned to their original versions.

## Drain evidence

Delete one non-canary Worker Pod and observe, without deleting Broker pending
entries or changing fences:

- `/readyz` becomes unavailable before termination completes;
- the replacement becomes ready on another node when capacity permits;
- owned work finishes inside the drain window or is reclaimed after lease
  expiry;
- the old fence cannot commit after takeover;
- a replay reads the durable result and does not repeat a confirmed Tool side
  effect.

This is cluster acceptance evidence and is intentionally separate from Helm
schema rendering.

## Rollback

Rollback is allowed only while the database remains N-1 compatible. Use the
recorded Helm revision/image digest, then publish a higher immutable
ConfigSnapshot that points new requests at the previous compatible versions.
Do not mutate old snapshots or delete Outbox/Broker/Audit rows.

Stop promotion and rollback when any of these occur: readiness does not
stabilize, Audit lag or dead-letter rises, stale-fence writes are attempted,
secret canary fires, cross-tenant checks fail, or the new version requires a
contracted schema. A contract migration is a later controlled change after the
rollback window closes; it is never part of an emergency rollback.

## Known boundary

The chart's initial HPA uses CPU resource metrics. Queue/Outbox/backlog custom
metrics, migration Jobs, capacity load generation, and real-provider smoke are
separate release gates and must not be inferred from a successful Helm render.
