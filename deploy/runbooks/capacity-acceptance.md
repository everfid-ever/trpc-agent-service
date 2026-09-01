# Capacity acceptance

1. Freeze the image digest, ConfigVersion, migration revision, Worker shard
   topology, HPA bounds, external-metrics adapter queries, database pool limits,
   and explicit criteria before generating load.
2. Confirm the adapter uses `max` across replica scrape targets for global
   queue/Outbox gauges. Confirm `trpc_broker_autoscaling_snapshot_ready` and
   `trpc_audit_autoscaling_snapshot_ready` remain `1`; missing external metrics
   must block scale-down.
3. Run a stepped load long enough to meet `minimum_duration_seconds`. Preserve
   raw request results, HPA events, replica counts, Prometheus samples, traces,
   audit references, PostgreSQL/Redis saturation, and the post-load drain.
4. Produce a schema-version `1` JSON report for the observed run. Do not insert
   fixture, WebUI, or estimated values into a real acceptance report.
5. Evaluate it with:

   ```sh
   go run ./cmd/capacity-evaluate ./capacity-report.json
   ```

The command exits nonzero for incomplete accounting, insufficient metric
coverage, missed throughput/latency/lag criteria, missing Worker scale-up,
remaining Broker backlog, Audit dead-letter, or incomplete evidence references.
Passing the evaluator proves only that the supplied measurements satisfy the
frozen criteria; the raw evidence remains part of the release record.
