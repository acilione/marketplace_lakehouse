# Analytics operations

This guide applies to purchase analytics, not the archived reconciliation prototype. Commands
run from the repository root on Linux/WSL. Never reset volumes to resolve a processing error.

## Complete acceptance walkthrough

1. Run `make check` for formatting, strict types, Python/TypeScript tests, production builds,
   dependency/security checks and Compose validation.
2. Run `make analytics-import-retail` once, then inspect `data/retail/import-report.json`.
   Check the checksum, excluded cancellations, non-product rows, conflicting invoices and line counts.
3. Run `make analytics-real`. Open `http://localhost:24273`; GBP is selected automatically when no
   currency was requested. Confirm the attribution and historical 2011-12-09 observation date.
4. In Customers, open a customer, inspect daily amounts, close detail, paginate and change dates.
   Recency/segment remain publication-time measures. In Products, inspect daily purchase history.
5. In Retention, verify M0 is 100%, later observed zero periods remain zero, and December 2011 is
   incomplete. Future months must be absent, not zero. Hover cells for numerator/denominator.
6. In Pipeline, inspect the publication and table snapshot IDs. Refresh should not substitute fixtures
   when services are unavailable. Platform administration must require sign-in.
7. Re-run `make analytics-real`; identical input must not increase customer/product totals.
8. Stop the real stack with `make analytics-stop ANALYTICS_PROJECT=marketplace-lakehouse-retail`
   before running a second stack on a constrained host.
9. Run `make analytics-demo ANALYTICS_ORDERS=100 ANALYTICS_CUSTOMERS=20`, then `make analytics-verify`.
   This exercises duplicates, owner changes, partial returns, cancellation, domain quarantine,
   failure before pointer commit, retry, advisory-lock exclusion and full-recompute equivalence.
   Verification only accepts a generated publication; it never changes the real-data walkthrough.
10. With Playwright/Chromium installed, run
    `python architecture-site/tests/analytics_smoke.py --url http://localhost:24173`.
    For the real walkthrough use `--url http://localhost:24273 --retail`.

The GitHub Actions analytics job automates the small generated pipeline and recovery suite. It
does not download UCI on every pull request. An actual distributed cluster and Airflow deployment
are not covered by that test. Unit tests cover no-input daily aging and cohort definitions.

## Invalid purchase repair

Structural JSON/envelope failures enter the ingestion quarantine. Domain-invalid snapshots and
equal-version content conflicts enter `lakehouse.bronze.purchase_rejections`. Use an authenticated
Platform query or a separately authorized Trino client:

```sql
SELECT order_key, version, reason, source_snapshot, content_hash, known_at
FROM lakehouse.bronze.purchase_rejections
ORDER BY known_at DESC
LIMIT 50;
```

These records can contain pseudonymous customer evidence; do not expose the table anonymously.
Check the raw event at the recorded snapshot and compare canonical state. Fix the source and
publish a **higher-version complete order snapshot**, containing all current lines and linked
returns. Use the JSONL import command in the root README, then `make analytics-refresh` with an
appropriate observation date. For a historical workspace never accidentally advance the cutoff
to today's date; profile aging is intentional and cannot move backwards in the same namespace.

An ambiguous order is withheld for that processed range; unrelated healthy orders continue. A
corrected higher version can subsequently restore that order's contributions. Rejection evidence
is retained, not deleted when repaired. The current `rejected_snapshots` metric is the last range's
count, **not an unresolved-case count**. The pipeline does not provide an automated repair editor.

## Failed jobs and unavailable services

1. Inspect the failing job log, exit status and current publication before retrying.
2. Verify Kafka, object storage, catalog and PostgreSQL health. Do not recreate checkpoints.
3. Restart the bounded ingestion/publication command with the same source and cutoff. Iceberg
   sink checkpoints handle streaming replay; canonical versions handle duplicate business delivery.
4. A partial table-write failure leaves the public pointer unchanged. Retry restores the working
   tables to the last published baseline and rebuilds affected outputs.
5. If advisory-lock ownership is uncertain after a network partition, stop/exclude the original
   writer before starting another. The lock is not a fencing token enforced by Iceberg commits.
6. Source overwrite/delete or missing snapshot ancestry requires an isolated full rebuild and
   equivalence check. Do not skip missing ranges or force the source cursor forward.

## Freshness and rejected-data monitoring

The durable exporter exposes `marketplace_pipeline_publication_seconds`, last-success/failure
timestamps, duration and `marketplace_pipeline_rejected_snapshots`. Prometheus rules alert on a
successful-refresh age above 30 minutes and on rejected purchase snapshots. Start the optional monitoring
profile to collect them; `analytics-up` alone does not run Prometheus/Grafana.

The freshness alert uses last successful processing, not source event recency: the UCI dataset is
historical by design. A bounded demonstration will naturally become stale after it stops. Enable
freshness paging only with an active scheduler and an agreed service objective. A no-input,
same-cutoff refresh preserves the existing publication instead of rewriting tables. Rule evaluation
tests do not prove alert routing or on-call response; exercise those in the deployment environment.

## Retention and compaction

```bash
make analytics-retention-plan
make analytics-retention-apply
# Add ANALYTICS_PROJECT=marketplace-lakehouse-retail for the public dataset stack.
```

Both commands hold the writer advisory lock. The plan retains the current head plus publications
from the last seven days and at least the latest ten. Every publication now has a unique owned tag,
including no-input reruns, so old pagination sessions keep their referenced snapshot set. Apply
drops only obsolete owned output tags, expires eligible output snapshots, and then removes old
PostgreSQL documents. A failure is safe to retry. Pagination beyond retention returns an expired
publication response; return to the first page.

Raw source snapshots and `analytics_input_*` tags are deliberately retained. The reader requires
append-only ancestry; **raw compaction and source-history expiration are not enabled**. Long-running
production deployment requires a tested cursor migration or another range ledger before that
history can be bounded. Arbitrary orphan deletion is not included.

`make analytics-compaction-plan` previews output-only compaction. `make analytics-compact` holds
the writer lock, restores the published baseline after any failed attempt, rewrites output data
files and atomically publishes a new tagged snapshot set. Old readers remain pinned to old tags.
The source cursor and raw files are unchanged. Do not run uncoordinated manual maintenance against
working tables. Verify unchanged analytics and run the equivalence suite on the generated stack
after compaction; source-table compaction remains unsupported.

## Backup and restore procedure

This is an operator procedure, not a claim that a restore drill has been performed:

1. Stop ingestion/publication and exclude all writers; record the current publication document.
2. Take a consistent PostgreSQL backup of the Iceberg catalog database and `analytics_control`
   schema, then preserve every object referenced by retained snapshots and streaming checkpoints.
3. Store encrypted backups separately from the application host; do not commit dumps or credentials.
4. Restore to an isolated Compose project/environment, never over the active volumes.
5. Verify the catalog resolves every snapshot in the publication, compare row counts and per-currency
   totals, replay a bounded input, then test API pagination and read permissions.
6. Record measured recovery duration, recovered source position and any data-loss window before
   assigning an RTO/RPO. Promote only after reconciliation with the source evidence.

## Local credentials and exposure

Platform: `operator` / `DEV_DASHBOARD_OPERATOR_PASSWORD`, or `viewer` /
`DEV_DASHBOARD_VIEWER_PASSWORD`. MinIO: `DEV_MINIO_ROOT_USER` / `DEV_MINIO_ROOT_PASSWORD`.
PostgreSQL: `DEV_POSTGRES_USER` / `DEV_POSTGRES_PASSWORD`. Grafana: `DEV_GRAFANA_ADMIN` /
`DEV_GRAFANA_PASSWORD`. Values are in the clone's `.env`, initially copied from `.env.example`.
The optional local Prometheus and Trino services do not implement password login. Do not invent
credentials for them or expose them publicly; production needs network access controls and an
authenticated reverse proxy or supported identity integration. Local Docker service control is
not a production administration architecture.
