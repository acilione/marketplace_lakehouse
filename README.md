# Marketplace Lakehouse

An e-commerce data platform that turns purchase events into customer profiles, product analytics
and retention cohorts. Kafka carries purchase snapshots, Spark builds Iceberg tables, and Trino
serves a React dashboard from a consistent published version of the data.

[Scope](#scope) · [Quick run](#quick-run) · [Stack](#stack) ·
[Architecture](#architecture-overview) · [Data model](#data-and-metric-definitions) ·
[Tests](#tests-and-validation) · [Operations](#evaluation-and-operations)

## Scope

The platform provides a reusable analytical foundation for marketplace and retail purchase data:

- **Customer analytics:** purchase frequency, recency, rolling purchase amounts and descriptive segments.
- **Product analytics:** purchased units, order counts, linked returns and daily purchase history.
- **Retention analysis:** monthly first-purchase cohorts and repeat purchasing over time.
- **Reliable processing:** versioned corrections, duplicate-safe results, invalid-record quarantine,
  recoverable publication and traceable table snapshots.
- **Operational visibility:** a single dashboard for analytics, SQL queries, Kafka inspection and
  authenticated local service controls.

It is not a storefront, payment reconciliation system, fraud engine, campaign platform or online
recommendation service. Customer profiles describe observed purchasing behavior; they are not
predictive models. A real merchant CDC/outbox connector is not installed.

The repository includes a runnable local deployment, a public historical retail-data adapter,
generated integration/load inputs and automated checks. It is production-oriented, but distributed
capacity, privacy workflows and deployment hardening remain explicit [production gates](#production-boundaries).

## Quick run

Run commands from the repository root in **Linux or WSL**, with GNU Make, Docker and
**Docker Compose 2.24.4+**. Allocate approximately **12–16 GB of available memory** and sufficient
disk space for container images and data. First builds can take several minutes. Run one stack
at a time on constrained machines. Containers provide the application runtimes; Python 3.12
with pip is recommended for the importer/development tools, and Java 21 for local Spark tests.

```bash
git clone https://github.com/acilione/marketplace_lakehouse.git
cd marketplace_lakehouse
[ -f .env ] || cp .env.example .env
```

### Generated end-to-end run

```bash
make analytics-demo
```

Open **http://localhost:24173**. The command publishes generated purchase snapshots through Kafka,
Spark Bronze ingestion, canonical Silver facts, Gold customer/product marts and Trino. The
dashboard reads published table snapshots, never canned results. Generated inputs are labelled
explicitly. The page opens without an initial login. This is a bounded pipeline run, not a continuously
running event producer or scheduler.

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


### Real transaction run

```bash
make bootstrap
make analytics-import-retail  # once: download and convert the public dataset
make analytics-real
```

Open **http://localhost:24273**; GBP is selected automatically. This sends real historical purchase
records through the same pipeline. The importer refuses to overwrite an existing import; on subsequent
runs, use `make analytics-real` directly. Replaying identical input does not increase analytical totals.

| Run | Dashboard | Trino | Compose project |
| --- | --- | --- | --- |
| Generated | http://localhost:24173 | http://localhost:24186 | `marketplace-lakehouse-showcase` |
| Public retail data | http://localhost:24273 | http://localhost:24286 | `marketplace-lakehouse-retail` |

The projects use separate volumes. Stop either without deleting its data:

```bash
make analytics-stop
make analytics-stop ANALYTICS_PROJECT=marketplace-lakehouse-retail
```

### Dashboard access

Customers, Products, Retention, Segments and Pipeline are available without login only for explicitly
published generated demonstrations and the allowlisted public dataset. Publications from your own
source data require authentication.

The Platform tab provides authenticated SQL, catalog, Kafka and service controls. Local example accounts:

| Account | Password setting in `.env` | Example password |
| --- | --- | --- |
| `operator` | `DEV_DASHBOARD_OPERATOR_PASSWORD` | `change-me-local-operator-password` |
| `viewer` | `DEV_DASHBOARD_VIEWER_PASSWORD` | `change-me-local-viewer-password` |

Permissions depend on the account role. Session changes reload analytical data and clear stale private
results. Example credentials are for local development only; do not expose this stack publicly with them.
See [operations](docs/analytics-operations.md) for service credentials and administration.

## Stack

| Layer | Technologies | Responsibility |
| --- | --- | --- |
| Contracts and sources | Python, Pydantic, Confluent Kafka client | Validate full purchase snapshots; publish JSONL imports or generated inputs. |
| Event transport | Apache Kafka | Buffer purchase events and provide replayable ingestion offsets. |
| Processing | Apache Spark / PySpark | Ingest Bronze events; validate and update Silver facts; compute Gold analytics. |
| Analytical storage | Apache Iceberg, Parquet, MinIO, Iceberg REST catalog | Store versioned tables and checkpoints on S3-compatible object storage. |
| Metadata and publication | PostgreSQL | Persist catalog metadata and the analytics control schema, writer lock and publication pointer. |
| SQL serving | Trino | Query published Iceberg snapshots with interactive/exploratory resource budgets. |
| API and dashboard | Node.js native HTTP, React, TypeScript, Vite, Nginx | Serve filtered analytical queries and a compact browser interface. |
| Local infrastructure | Docker Compose, GNU Make | Build and run isolated, repeatable local environments. |
| Optional scheduling and monitoring | Apache Airflow, Prometheus, Grafana | Schedule refreshes and inspect operational metrics; not started by the analytics quick run. |
| Quality | pytest, Hypothesis, Vitest, Playwright, Ruff, mypy, ESLint, GitHub Actions | Validate contracts, transformations, APIs, UI behavior, recovery and builds. |

Pinned versions live in [pyproject.toml](pyproject.toml), [runtime.lock](requirements/runtime.lock),
[package.json](architecture-site/package.json), [package-lock.json](architecture-site/package-lock.json)
and the [Compose configuration](docker-compose.yml). Spark and Iceberg versions are selected as a
compatible pair; see the [dependency policy](docs/version-policy.md).

## Architecture overview

```text
Public retail adapter / purchase JSONL / generated load
                         |
                 Kafka purchase events
                         |
               Spark bounded ingestion
                         |
              Bronze: raw purchase events
                         |
       Spark validation + versioned corrections
                         |
           Silver: canonical orders and items
                         |
             Spark analytical transformations
                         |
       Gold: daily purchases, profiles and cohorts
                         |
           Trino -> Node.js API -> React dashboard

Storage: Iceberg tables / Parquet on MinIO
Control: PostgreSQL publication pointer -> immutable table snapshot IDs
```

1. **Ingest:** a source emits complete, versioned order snapshots. Spark persists accepted envelopes
   in append-only Bronze tables using an Iceberg streaming sink and a dedicated checkpoint.
2. **Transform:** the analytical job processes a bounded input range, quarantines invalid updates and
   replaces affected canonical orders and aggregate groups. Corrections are recomputed, not added twice.
3. **Publish:** after output commits succeed, PostgreSQL atomically advances a document naming the
   complete output snapshot set and processed source cursor. Partial failures leave the prior publication visible.
4. **Serve:** the API reads one publication and pins its Trino queries to those snapshots. React displays
   stored metrics, rather than recalculating business rules in the browser.

PostgreSQL stores the control metadata, not the analytical purchase history. Iceberg commits are atomic
per table; the publication pointer supplies the coordinated read boundary across tables. Kafka transports
events, Spark handles analytical computation, and Trino handles interactive reads independently.

The local commands drain currently available input and exit. Continuous scheduling is a separate concern:
the optional Airflow DAG defines a 15-minute refresh cadence and requires its own configured deployment.

### Code organization

| Directory | Responsibility |
| --- | --- |
| `src/marketplace_data/` | Contracts, source adapters, pure transformations, processing jobs and storage/publication adapters. |
| `apps/analytics/` | Thin Spark entry points for ingestion and publication. |
| `architecture-site/src/analytics/` | React analytics views, typed models and request lifecycle. |
| `architecture-site/server/` | Authentication, analytics API, Trino access, Kafka inspection and service-control integration. |
| `scripts/` | Recovery verification, retention/compaction and reproducible benchmarks. |
| `docker/`, `infrastructure/`, `orchestration/`, `observability/` | Runtime images, service configuration, optional scheduling and monitoring. |
| `tests/`, `architecture-site/tests/`, `docs/` | Tests, browser checks, architecture decisions and operational guidance. |

## Real-data provenance

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


## Implementation details

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
an installed merchant CDC connector. The public-data adapter excludes unlinked cancellations
instead of inventing return-to-order relationships.

## Tests and validation

```bash
make bootstrap
make check  # lint, types, Python/TypeScript tests, builds, Compose and security checks

# Integration checks: use the generated stack, not imported merchant data.
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

Local validation includes 97 Python tests and 33 frontend/API tests, desktop/mobile browser checks,
the complete generated and public-data pipelines, failed-publication recovery, replay equivalence
and post-compaction reference comparison. These are recorded development results, not a claim that
every deployment or workload has been tested. The configured coverage gate is 80%; job orchestration
and storage integration also require the separate end-to-end checks.

GitHub Actions is configured to run quality/security checks, image builds and a small generated-data
integration job with quarantine, recovery and maintenance verification. It does not download the
public dataset or deploy the website. Consult the actual workflow run for its current status.

## Production boundaries

[ADR-008](docs/adr/ADR-008-customer-analytics-architecture.md) records the papers, workload estimates
and capacity acceptance criteria. The reference assumes 1 million orders/day, 3 million lines/day
and 10 million roughly 1 KB envelopes/day: 116 events/s average, a hypothetical 10x burst, and
2.19 billion lines in two years. These are planning assumptions, **not measured capacity**.
Affected-key processing is not proof of pruning: measure scans, shuffle, spill and skew as history grows.

Remaining production gates include real CDC/outbox connectors, customer deletion/replay suppression,
tenant identity/access controls, restore drills, distributed writer fencing and multi-node
scale/soak tests. Profile aging, key layouts, compatibility materialization and Trino budgets need
measured tuning. No laptop test validates billion-row performance or the proposed 20-client SLO.

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
make analytics-benchmark BENCHMARK_ARGS="--orders 1000 --delta 100 --clients 4 --requests 20 --verify"
make analytics-retention-plan       # dry-run
make analytics-retention-apply      # reviewed output-retention plan only
make analytics-compaction-plan      # dry-run
make analytics-compact              # coordinated output compaction and republication
```

Maintenance commands require a running stack and default to the generated project. To target the
real-data project, append `ANALYTICS_PROJECT=marketplace-lakehouse-retail`. Review dry-run output
before applying changes; raw-table history is deliberately excluded from this maintenance workflow.

The benchmark uses a dedicated project, records initial/fixed-delta/no-input processing durations
and mixed-view HTTP p50/p95 with failure counts. Results are ignored runtime artifacts, not invented
performance claims. The first HTTP request is not guaranteed cold-cache.

See [operations and repair](docs/analytics-operations.md), [benchmark protocol](benchmarks/README.md)
and [architecture decisions](docs/adr/ADR-008-customer-analytics-architecture.md).
The [archived platform reference](docs/legacy-platform.md) preserves original multi-topic jobs and
deployment examples; it is not the current analytical entry point. The retired reconciliation UI
and case API have been removed. Code is Apache-2.0; the real dataset is separately attributed CC BY 4.0.
