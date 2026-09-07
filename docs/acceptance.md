# Acceptance and release evidence

| ID | Automated evidence | Release evidence still required |
|---|---|---|
| AC-01 | Compose validation, demo script, CI integration workflow | Fresh-clone log and recording |
| AC-02 | correlated five-domain smoke/steady/peak/chaos scenarios; malformed, duplicate, late, delivery, routing, latency, and reconciliation reports | Reviewed sustained-run evidence |
| AC-03 | idempotent MERGE/checkpoint design | scheduled driver-kill report |
| AC-04 | deterministic transformation and duplicate tests | repeated snapshot logical diff |
| AC-05 | candidate branch quality gate | failed-gate reference snapshot evidence |
| AC-06 | bounded backfill CLI and rollback runbook | reviewed publish/rollback exercise |
| AC-07 | ignore policy, redaction, Bandit, pip-audit | image/secret scanner reports |
| AC-08 | compute benchmark plus end-to-end stress harness and per-run evidence | signed benchmark results on release hardware |
| AC-09 | Iceberg properties and quality metadata | catalog metadata export |
| AC-10 | ADRs, deployment code, runbooks | release checklist sign-off |

A release is not production-approved merely because unit tests pass. The rightmost evidence must be
attached to the immutable image digest and reviewed by Platform, Security, Analytics Engineering,
and SRE for the initial production promotion.
