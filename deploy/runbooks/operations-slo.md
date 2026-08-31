# Operations telemetry and SLO runbook

1. Start with the `trpc:operation:requests:rate5m`, `trpc:operation:errors:rate5m` and `trpc:operation:error_ratio5m` recording rules. Filter only by the fixed `operation`, `component` and `outcome` labels; never add tenant, user, session or request labels.
2. For a high error ratio, check the corresponding role `/readyz`, logs and durable facts (Inbox/Outbox, preprocess job, execution status or Delivery Ledger). An exporter or Collector outage is telemetry degradation, not proof that a business fact was lost.
3. For high latency, inspect the p95 histogram by operation and compare queue/lease age and backend health. Restore capacity or dependency health before changing the SLO threshold.
4. Confirm recovery after the five-minute error ratio and p95 latency return below threshold for at least one evaluation window, then attach the durable request/audit evidence to the incident.
