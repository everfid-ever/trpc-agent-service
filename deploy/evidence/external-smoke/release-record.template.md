# External smoke release record

> Copy this file into the access-controlled change record. Do not commit the
> completed copy and do not include credentials, raw provider bodies, customer
> content, user IDs, or bearer tokens.

## Context

- Revision:
- Release image digest:
- Time window (UTC):
- Operator:
- Approval ticket(s):
- Preflight status artifact:

## Results

| Target | Status (`SKIPPED` / `BLOCKED` / `PASSED` / `FAILED`) | Authorized test scope | Sanitized result / stable external ID | Protected raw-evidence link |
|---|---|---|---|---|
| DeepSeek Gateway/Worker |  |  |  |  |
| S3 / ClamAV / DLP |  |  |  |  |
| Feishu |  |  |  |  |
| WeCom |  |  |  |  |
| OTEL query / trace correlation |  |  |  |  |
| Kubernetes server dry-run / drain |  |  |  |  |

`SKIPPED` requires the unavailability or deferral reason. `BLOCKED` requires
the missing permission, resource, or owner. Only a command actually run in the
approved environment may be `PASSED`; link its sanitized command outcome and
the access-controlled raw evidence. A `READY` preflight is not a test result.

## Operator attestation

- [ ] No secret value, credential-file content, full request/response body, or customer identifier appears in this record.
- [ ] Every visible-provider message used an authorized test account/message.
- [ ] Kubernetes evidence includes the exact context/namespace and confirms no server-side mutation was performed by dry-run.
- [ ] OTEL evidence identifies the expected trace/metric correlation without exporting payload content.
