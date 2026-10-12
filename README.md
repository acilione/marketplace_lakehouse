# Marketplace Lakehouse

Customer profiling and purchase/product analytics for e-commerce, built with Kafka, Spark,
Iceberg, Trino and React/TypeScript. The platform answers who buys again, which products are purchased,
and how retention changes across customer cohorts. Payment reconciliation, fraud investigations,
campaign execution and online recommendation serving are outside its scope.

## Start the analytics platform

From the repository root on Linux or WSL, with Docker and Compose 2.24.4+:

```bash
[ -f .env ] || cp .env.example .env
make analytics-demo
```

Open **http://localhost:24173**. The command publishes generated purchase snapshots through Kafka,
Spark Bronze ingestion, canonical Silver facts, Gold customer/product marts and Trino. The
dashboard reads published table snapshots, never canned results. Generated inputs are labelled
explicitly; no real merchant connector is installed. The page opens without an initial login.

Customers, Products, Retention, Segments and Pipeline are the analytical views. Platform retains authenticated
SQL, catalog, Kafka and service controls. Sign in there as `operator` with
`DEV_DASHBOARD_OPERATOR_PASSWORD` from `.env` (example: `change-me-local-operator-password`),
or `viewer` with `DEV_DASHBOARD_VIEWER_PASSWORD` (example: `change-me-local-viewer-password`).
After signing in, analytical data reloads for the new session. Source-data publications require authentication;
only explicitly published generated demonstrations and the allowlisted public UCI dataset are eligible for guest access.

```bash
make analytics-demo ANALYTICS_ORDERS=10000 ANALYTICS_CUSTOMERS=1000 ANALYTICS_SEED=43
make analytics-refresh  # process new sources and age profiles to the current UTC date
# Deterministic generated example observation date:
make analytics-refresh ANALYTICS_ARGS="--as-of 2026-10-12 --public-demo"
make analytics-stop     # stop the isolated project, preserving volumes
```

Default generation uses 1,000 orders, 100 possible repeat customers, seed 42 and a fixed
2026-10-12 observation date. Reusing identical generation arguments replays the same records
without adding purchases. Seed, customer population and observation timestamp identify a source;
changing them creates a separate source whose history is retained. Corrections to an existing
order must increase its version. Changed content at the same version is quarantined; it never overwrites a valid published purchase.

This reuses the isolated `marketplace-lakehouse-showcase` Compose project without resetting its
volumes or touching sibling projects. Trino is at http://localhost:24186; Kafka, PostgreSQL and
object storage stay private. First builds download large images and take several minutes.


## Real transaction walkthrough

Use Python 3.12 / Java 21 for local tests; application containers carry their own runtimes.
Allow approximately 12–16 GB of available memory for the full local stack and substantial image
disk space. Avoid running multiple stacks concurrently on constrained hosts.

```bash
make bootstrap
make analytics-import-retail
make analytics-real
# Open http://localhost:24273 and select GBP.
make analytics-stop ANALYTICS_PROJECT=marketplace-lakehouse-retail
```

The importer downloads [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii),
Chen, D. (2012), DOI [10.24432/C5CG6D](https://doi.org/10.24432/C5CG6D), **CC BY 4.0**.
It describes a UK retailer with many wholesale customers, not a representative multi-seller marketplace.
Its historical observation date is **2011-12-09**. A separate Compose project and volumes prevent
mixing this history with generated purchases. Replaying the same import does not increase totals.

[`retail_import.py`](src/marketplace_data/retail_import.py) streams XLSX worksheet XML, groups
orders in disk-backed SQLite and emits validated full-state JSONL. It accepts positive-price,
positive-quantity product rows with five-digit stock codes and an optional uppercase suffix.
Stable worksheet/row IDs distinguish repeated SKU lines. Customer identifiers are deterministically
pseudonymized, not claimed anonymous. No categories or sellers are invented.

Cancellation invoices are unlinked and excluded, alongside non-product charges and invalid rows.
Conflicting invoice headers exclude the accepted invoice as a whole. Quantities × source unit prices
are rounded half-up to GBP pennies. Europe/London is the assumed source timezone, converted to UTC.
Tax/discount composition cannot be established from the historical workbook alone. Thus the UCI
walkthrough shows **accepted positive product purchase value, not return-adjusted net revenue**.
The general contract supports linked partial returns, but this adapter does not fabricate them.

Ignored `data/retail/import-report.json` contains attribution, exclusion counts and workbook SHA-256.
The verified import produced **39,427 orders and 1,026,638 product lines**, excluding 19,494 cancellation
rows. These are import counts, not capacity measurements. The importer refuses to overwrite existing
results; choose new output paths for a new import. The UI also displays attribution and these limitations.


## Architecture and implementation

```text
Full purchase snapshots / validated JSONL / generated load
                         |
                  Kafka orders topic
                         |
       Spark native Iceberg streaming sink
                         |
          bronze.purchase_events (append)
                         |
     version validation + affected order keys
                         |
 silver.purchase_orders + silver.purchase_items
                         |
 gold.customer_daily -> gold.customer_profiles
                         + gold.product_daily + gold.customer_cohorts
                         |
 PostgreSQL atomic publication + processed cursor
                         |
 Trino snapshot-pinned SQL -> read API -> React
```

| Boundary | Implementation |
| --- | --- |
| Source contract | [analytics_contracts.py](src/marketplace_data/analytics_contracts.py): Pydantic full-state snapshots with stable source/order/line IDs, monotonic versions, decimal amounts, multiple items and partial returns. No storage dependency. |
| Source adapter | [purchase_source.py](src/marketplace_data/purchase_source.py): validated JSONL or lazy repeat-customer load generation; Kafka acknowledgements and bounded producer backpressure. No full dataset list in driver memory. |
| Raw ingestion | [purchase_ingest.py](src/marketplace_data/jobs/purchase_ingest.py): JSON envelope validation, structural quarantine, dedicated checkpoint and native Iceberg streaming commits. No event-time watermark discards late corrections. Transport duplicates may remain in Bronze. |
| Metric definitions | [transforms/analytics.py](src/marketplace_data/transforms/analytics.py): set-based Spark expressions with explicit inputs/as-of date, no hidden reads or clock. Frequency counts distinct orders, not joined lines. |
| Incremental job | [customer_analytics.py](src/marketplace_data/jobs/customer_analytics.py): append-snapshot range validation, guarded order updates and replacement of affected customer/product groups, including emptied groups. Daily aging also updates inactive customers. |
| Storage adapter | [iceberg.py](src/marketplace_data/iceberg.py): version-guarded MERGE and atomic scoped replacement with deletion tombstones. The Spark 4.1/Iceberg 1.11 cached-RDD compatibility workaround remains isolated here; production optimization requires a verified replacement. |
| Publication | [publication.py](src/marketplace_data/publication.py): dedicated PostgreSQL control schema, advisory writer lock and transactional pointer to immutable documents containing source bounds and output snapshot IDs. |
| Scheduling | The `customer_purchase_analytics` Airflow DAG runs bounded ingestion followed by publication every 15 minutes with one active run. UTC day changes trigger profile aging automatically. The local `make analytics-demo` command is a bounded demonstration, not a background scheduler. Airflow deployment remains optional and requires its Spark connection and runtime setup. |
| Read API | [server/analytics.ts](architecture-site/server/analytics.ts): pinned Trino snapshots, validated date/currency filters, publication-bound 50-row keyset pagination, daily drill-downs and a bounded 60-second aggregate cache. Exact snapshot IDs remain strings. |
| Query isolation | [resource-groups.json](infrastructure/trino/resource-groups.json): interactive and exploratory concurrency/queue budgets. API requests also set server-side execution and physical-scan limits. A response row limit alone would not bound work. |
| Frontend | [src/analytics](architecture-site/src/analytics): React 19, strict TypeScript, separate request hook/model/table/workspace; compact metrics, date/currency selection, pagination, customer/product history, cohort retention, segments and lineage. Abortable requests, explicit empty/error states and no reconciliation dependency in the main bundle. |

### Data and metric definitions

The `order.snapshot` payload is **complete state**, not a partial patch: source, order_id, version,
optional pseudonymous customer_id, currency, country, purchased_at, status, items and returns.
Arrays are JSON strings inside the existing envelope's `MAP<string,string>` payload. Stable
line IDs distinguish two lines with the same SKU. Omitting a previous line removes it; an empty
item list removes all contributions.

CONFIRMED and REFUNDED orders qualify; CANCELLED orders do not. Return records reference original
lines and have stable return IDs, quantity, amount and timestamp. Returns cannot exceed purchased
quantity or amount. A returned purchase still counts as a historical order. Current v1 metrics
restate returns at the original purchase date: these are purchase analytics, not a cash-flow ledger
or knowledge-time training dataset. Net purchases exclude tax and shipping, subtract returns from
discounted line amounts, and are neither marketplace revenue nor predicted lifetime value.

Daily customer grain is customer / UTC purchase date / currency. Profiles contain inclusive
30/90/365-day windows, first/last purchase, lifetime frequency and recency. Currencies remain
separate; no implicit FX conversion exists. Unknown customer IDs remain null: their purchases
appear in product analytics, not customer profiles. Top-level counts describe identified customers.

Policy `purchase-v1` assigns segments in precedence order: Inactive after 90 days without purchase;
Repeat with at least three purchases in 90 days; New with one lifetime purchase; Returning otherwise.
The summary's repeat-buyer count uses at least two orders, intentionally distinct from the stricter
Repeat segment. Frontend code displays stored classifications instead of reimplementing rules.

### Corrections, retries and publication

Initial runs read a bounded source snapshot; subsequent runs read the processed append range.
The reader checks ancestry and rejects source overwrite/delete operations or expired history:
it must never silently skip corrections. Raw-table compaction therefore needs a tested
range-aware recovery process before being enabled.

Older versions cannot regress canonical state. Invalid business records and equal-version conflicts
are written to `bronze.purchase_rejections` with deterministic identities, reasons and source snapshot.
The affected order is withheld for that input range; healthy orders continue and published state remains
unchanged for conflicting orders. Publication records the rejected count and advances source progress
only after durable quarantine succeeds. Repair by submitting a corrected, higher-version full snapshot,
not by editing Bronze. Structural envelope failures use the existing ingestion quarantine.

Affected keys include old and new customer/product owners. Totals are recomputed from canonical
facts, never blindly added to existing totals. Removed lines, cancellations and empty groups are
deleted in the same scoped MERGE. Daily aging also processes profiles with no new source activity.

Iceberg commits are individually atomic, **not a multi-table transaction**. The PostgreSQL pointer
makes a complete snapshot set visible to the API. Retried jobs restore the last published working
baseline before reprocessing, preventing abandoned intermediate versions from contaminating results.
Only the coordinated writer may update these analytics tables; direct writes/concurrent maintenance
that bypass its lock are unsupported.

Publication tags protect output snapshots from ordinary expiration. `make analytics-retention-plan`
previews cleanup; `make analytics-retention-apply` keeps at least seven days and ten publications,
including the head, while releasing obsolete owned output tags and expiring eligible output snapshots.
Raw history is deliberately untouched. Back up PostgreSQL control metadata, catalog and object storage
together. Advisory locking is not distributed fencing: exclude a disconnected writer before recovery.

### Import your purchase data

Provide one complete snapshot per line in a JSONL file, for example:

```json
{"source":"shop","order_id":"o-1","version":1,"customer_id":"token-123","currency":"EUR","country":"IT","purchased_at":"2026-10-01T12:00:00Z","status":"CONFIRMED","items":[{"line_id":"1","product_id":"sku-8","quantity":2,"amount":"39.90"}],"returns":[]}
```

After `make analytics-up`, publish a read-only mounted file:

```bash
docker compose -p marketplace-lakehouse-showcase -f docker-compose.yml -f docker-compose.showcase.yml run --rm --no-deps \
  -v "$PWD/data/purchases.jsonl:/input/purchases.jsonl:ro" spark-master \
  python3 -m marketplace_data.purchase_source --input /input/purchases.jsonl
make analytics-refresh
```

Use pseudonymous IDs and stable, increasing versions. This is a canonical JSONL adapter, not
an installed merchant CDC connector. The public-data adapter below excludes unlinked cancellations
instead of inventing return-to-order relationships.

## Checks and production boundaries

```bash
make bootstrap
make lint type
make python-test
make architecture-check
make compose-validate
make analytics-demo ANALYTICS_ORDERS=1000
make analytics-verify  # isolated generated-source correction/recovery checks
# With Playwright and Chromium installed:
python architecture-site/tests/analytics_smoke.py --url http://localhost:24173
```

Check both currencies and all analytical tabs. Replay identical input and confirm unchanged
totals. Send a higher-version partial return, removed line and reassignment and check both old/new
groups. Refresh on the next UTC business date without new events to check aging. Invalid updates
must be quarantined without changing the affected order; healthy orders should continue. The new analytics browser smoke test checks desktop/mobile,
currency selection, tabs and unauthenticated control protection.

[ADR-008](docs/adr/ADR-008-customer-analytics-architecture.md) records the papers, workload estimates
and capacity acceptance criteria. The reference assumes 1 million orders/day, 3 million lines/day
and 10 million roughly 1 KB envelopes/day: 116 events/s average, a hypothetical 10x burst, and
2.19 billion lines in two years. These are planning assumptions, **not measured capacity**.
Affected-key processing is not proof of pruning: measure scans, shuffle, spill and skew as history grows.

Remaining production gates include real CDC/outbox connectors, customer deletion/replay suppression,
tenant identity/access controls, restore drills, distributed writer fencing and multi-node
scale/soak tests. Profile aging, key layouts, compatibility materialization and Trino budgets need
measured tuning. No laptop test validates billion-row performance or the proposed 20-client SLO.

Validation performed locally: the complete 1,000-order Kafka-to-dashboard command, the
Iceberg/PostgreSQL failed-publication/correction/replay/equivalence check, desktop/mobile browser
checks including pagination, Python tests with the existing 80% coverage gate, frontend/API tests
and production builds, lint/types/Compose checks, and Python/npm dependency audits. This evidence
does not include deployment of the optional Airflow DAG or a distributed performance benchmark.


## Analytical views

- **Customers:** rolling purchase profiles and descriptive segments; select an ID for daily gross,
  returned and net amounts. Unknown customers are excluded, not merged into a shared identity.
- **Products:** purchase counts, units and returns by product; select an ID for daily purchase history.
  Purchase-date filters support at most 366 days. Currency groups never mix.
- **Retention:** monthly first-observed-purchase cohorts. The denominator is the original cohort size;
  active customers placed a qualifying purchase that month. Observed empty months are zero, future
  months are absent, and incomplete months carry `*`. The UI displays the latest 24 cohorts and
  months 0–12. First observed purchase is not necessarily account acquisition.
- **Segments:** classifications at publication time, independently of purchase-table date filters.
- **Pipeline:** publication timestamp, policy, table snapshots and rejected count for the latest range.
- **Platform:** authenticated SQL, Kafka inspection and service controls.

`gold.customer_cohorts` has grain cohort month / activity month / currency. It is rebuilt from customer
daily history using the same publication cutoff; this remains a measured optimization target at scale.
Customer/product date filters do not change cohort acquisition definitions or profile recency.
Pagination stays on one publication; Refresh returns to the latest first page.

## Evaluation and operations

```bash
make check
make analytics-verify
make analytics-benchmark BENCHMARK_ARGS="--orders 1000 --delta 100 --clients 4 --requests 20 --verify"
make analytics-retention-plan       # dry-run
make analytics-retention-apply      # reviewed output-retention plan only
```

GitHub Actions runs Kafka-to-dashboard ingestion and Iceberg/PostgreSQL correction, quarantine,
failed-publication and replay-equivalence checks in addition to unit/build/security checks.
The benchmark uses a dedicated project, records initial/fixed-delta/no-input processing durations
and mixed-view HTTP p50/p95 with failure counts. Results are ignored runtime artifacts, not invented
performance claims. The first HTTP request is not guaranteed cold-cache.

See [operations and repair](docs/analytics-operations.md), [benchmark protocol](benchmarks/README.md)
and [architecture decisions](docs/adr/ADR-008-customer-analytics-architecture.md).
Production still requires fencing, source-cursor maintenance, privacy deletion, identity isolation,
backup/restore exercises and scale/soak evidence.

The [archived platform reference](docs/legacy-platform.md) preserves original multi-topic jobs and
deployment examples; it is not the current analytical entry point. The retired reconciliation UI
and case API have been removed. Code is Apache-2.0; the real dataset is separately attributed CC BY 4.0.
