# Release-readiness evidence

This directory contains only the evidence format, never production credentials,
rendered production values, provider responses, or customer data.

Run the reproducible chart gate against the checked-in, non-secret production
example:

```bash
bash scripts/helm_admission.sh --output-dir /secure/change/trpc-helm-evidence
```

The output directory contains `helm-lint.txt`, `rendered.yaml`,
`kubeconform.txt`, `role-lifecycle.txt`, `tool-versions.txt`, and `manifest.txt`.
Attach those files to the reviewed release record only after checking that the
input was the checked-in example or an approved, locator-only values file.

The rendered contract proves chart structure: all production roles have
`/readyz`, `/livez`, and the shell-free `prestop` hook. It does not prove
cluster scheduling, workload identity, external metrics, live dependency
readiness, or actual drain/reclaim behavior. Record those cluster observations
separately in the rollout runbook before promotion.
