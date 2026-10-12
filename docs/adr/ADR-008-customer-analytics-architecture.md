# ADR-008 Marketplace customer analytics architecture

Status: Core purchase analytics implemented; production capacity validation pending

Date: 2026-10-12

## Scope and decision

Build a marketplace customer profiling and purchase analytics lakehouse. Keep Spark for distributed
transformation, Iceberg for analytical storage, and Trino for SQL access. Start with purchase-based
profiles, category preferences, repeat purchasing, cohorts and descriptive RFM segmentation.
Payment reconciliation, investigation workflows, fraud scoring, campaign execution and online
recommendation serving are outside the platform's scope.

Use incremental batch processing for customer analytics and buffered micro-batch ingestion where
Kafka is needed. Optimize for trustworthy published results within tens of minutes, not millisecond
profile updates. The workload below is a planning model, not a measurement of this repository or an
assertion about a particular merchant. Production sizing is conditional on the acceptance tests.

For this new scope, the proposed ingestion cadence supersedes the fixed 30-second choice in
[ADR-002](ADR-002-micro-batch.md). The production example now uses two-minute triggers. Retain the storage,
publication and orchestration principles in ADR-001, ADR-005 and ADR-006, subject to the corrections
below. The new analytics application replaces the control-tower entry point; remaining gates are
recorded in the README and the implementation status below.

## Research and practical implications

The following are the source conclusions relevant to this design. Their application to this
repository is an engineering decision, not a benchmark result from the papers.

| Research | Relevant conclusion | Decision for this platform |
| --- | --- | --- |
| Parnas, *On the Criteria To Be Used in Decomposing Systems into Modules*, CACM, 1972 ([paper](https://www.cs.lafayette.edu/~gexia/cs301/resources/parnas.html), [DOI](https://doi.org/10.1145/361598.361623)) | Hide decisions that can change behind module boundaries; a sequence of processing steps is not by itself a modular architecture. The paper also discusses abstraction overhead. | Isolate source formats, metric definitions, storage operations and presentation. Keep transformations set-based rather than wrapping every input row in domain objects. |
| Saltzer, Reed and Clark, *End-to-End Arguments in System Design*, TOCS, 1984 ([paper](https://web.mit.edu/saltzer/www/publications/endtoend/endtoend.pdf)) | Lower-level delivery and recovery mechanisms cannot alone establish application-level correctness. | Kafka acknowledgements and Spark checkpoints do not prove correct customer totals. Verify source coverage, business keys, replay equivalence and published aggregates. |
| Akidau et al., *The Dataflow Model*, VLDB, 2015 ([paper](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)) | Event time, processing time, refinement and the correctness/latency/cost trade-off need explicit semantics. Watermarks are not universal proof that all data arrived. | Preserve late corrections; define an as-of cutoff and correction policy. Share transformation definitions between incremental runs and backfills. Do not require sub-second processing without a consumer need. |
| Armbrust et al., *Lakehouse*, CIDR, 2021 ([paper](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf)) | Open data formats plus transactional metadata and optimized data access can support analytical and data-science workloads without obligatory lake-to-warehouse duplication. | Retain Iceberg/Parquet and separate processing from querying. The paper's engine-specific performance results are not evidence of this project's throughput. |
| Fader, Hardie and Lee, *RFM and CLV: Using Iso-Value Curves for Customer Base Analysis*, JMR, 2005 ([author manuscript](https://www.brucehardie.com/papers/rfm_clv_2005-02-16.pdf)) | Historical purchasing summaries and forward-looking value are different; predictive models require assumptions and validation on subsequent behaviour. | Deliver descriptive profiles first. Keep customer tenure and observation windows. Do not label past spend as predicted lifetime value or inactivity as confirmed churn. Any later predictive model needs temporal holdouts. |

These foundational papers guide semantics and boundaries. Current engine documentation governs
implementation details. No old comparative performance result is used to select cluster sizes.

## Real data and workload assumptions

Two real-world references bound different concerns:

- [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) contains 1,067,371
  records from a UK online retailer between December 2009 and December 2011, with invoice, product,
  quantity, price, customer and country fields. It includes missing values and cancellations and is
  licensed CC BY 4.0. It is useful for realistic analytical semantics, not distributed-scale proof.
  It is a retailer dataset with many wholesale customers, not a representative multi-seller marketplace.
- [Mercado Libre's company description](https://investor.mercadolibre.com/about-meli) reports a
  logistics network handling over 1.8 billion items per year. That establishes a real large-scale
  reference, not an order count, Kafka message rate, peak multiplier or customer distribution.

Use an attributed real transaction dataset for the analytical walkthrough. Keep a separate,
explicitly generated load dataset for performance tests; multiplying identifiers cannot create
genuine customer behaviour or validate a predictive model. Do not invent missing categories,
seller identities or browsing histories and present them as observed source attributes.

### Purchase workload

Assume three order lines per order and ten source envelopes per order on average, including
order/line creation, subsequent changes and an amortized allowance for returns and dimension
changes. Assume 1,000 serialized bytes per envelope before compression. These are adjustable
estimates; measure distributions, not just averages, on the first real connector.

| Scenario | Orders/day | New order lines/day | Envelopes/day | Average envelopes/s | Short burst at 10x average | Raw GB/day |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Smaller deployment | 100,000 | 300,000 | 1,000,000 | 12 | 116 | 1 |
| Reference deployment | 1,000,000 | 3,000,000 | 10,000,000 | 116 | 1,157 | 10 |
| Growth test | 5,000,000 | 15,000,000 | 50,000,000 | 579 | 5,787 | 50 |

Rates are rounded; a day is 86,400 seconds. GB/TB use decimal units. A 10x burst is a short-period
stress assumption, not an all-day multiplier or a measured industry norm. Add sustained high-volume
days separately. Reference dimensions are 10 million customer keys and 1 million product/listing
keys; their cardinalities and active/inactive mix are independent test parameters.

The reference produces **2.19 billion new order lines in two years**. The primary challenge is
historical scans, correction joins, skew and concurrent queries, not an extraordinary average Kafka
ingress rate. At 15-minute intervals a routine batch receives about 104,167 envelopes on average,
but an unbounded join can still read years of history.

### Storage and optional behaviour events

- Purchase envelopes: 10 GB/day is 3.65 TB/year before compression. A provisional 3:1 storage
  compression ratio would reduce one year's raw payload to about 1.22 TB; measure this ratio.
- Kafka at three days retention and replication factor three: approximately 90 GB of uncompressed
  payload for the reference. At a maximum 60% disk occupancy, reserve at least 150 GB cluster-wide
  for that payload alone. Add indexes, record overhead, bursts, rebalance space and growth. This is
  a storage calculation, not a recommendation to provision three tiny production brokers.
- At an assumed 250 compressed bytes per current order line, line facts alone add about 274 GB/year.
  Bronze, order facts, dimensions, profile history, return facts, old snapshots and backups are extra.
- At 400 compressed bytes per customer, a full 10-million-customer profile snapshot is roughly 4 GB.
  Keeping one per day adds 1.46 TB/year. Do not persist full customer snapshots every 15 minutes:
  that would add roughly 140 TB/year under the same assumptions.
- An optional 100 million view/cart/search events per day at 600 bytes adds **60 GB/day**. Combined
  ingress becomes about 1,273 messages/s average and 12,731/s at the assumed 10x burst. This event
  count is independent of purchases; it must eventually be estimated from sessions and events/session.

Keep clickstream out of the first implementation. Purchase-only profiling does not justify its
collection, privacy exposure, storage and session-processing cost. Revisit it only for explicit
funnel or browsing-affinity questions. If added, isolate its topics, jobs and resource budgets.

## Data model and metric contracts

The grain of a record is part of its contract, not a frontend interpretation.

| Dataset | Grain and ownership |
| --- | --- |
| Raw source events | One received envelope, source position, ingestion batch, schema version, event time and ingestion time. Preserve source evidence under a defined retention/deletion policy. |
| Orders | One current order per source/marketplace and order ID, plus versioned changes. Owns order-level status and totals. |
| Order items | One current line per source, order ID and stable line ID. Owns listing/product, seller, quantity, allocated discounts and line amount. SKU is not a unique line identifier. |
| Returns | One return line linked to the original order line, with quantity, amount, effective time and source version. Partial returns must be representable. |
| Customer identity and attributes | Stable pseudonymous source-qualified identity; explicit, auditable account links; type-2 history only for attributes needed in historical analysis. Unknown customers must not all become one person. |
| Products and listings | Product and seller-specific listing identifiers, with time-valid category/brand attributes where available. Decide explicitly whether a query uses purchase-time or current categorization. |
| Customer order facts | One qualifying purchase per customer/order, used for exact order frequency and average order value without line-join double counting. |
| Customer daily metrics | One customer/business-date/currency, with exact purchases, gross/net purchase amounts and return measures. |
| Current customer profiles | One customer/currency/policy version, with last purchase, tenure, rolling 30/90/365-day measures and descriptive segment. |
| Product and cohort marts | Explicit product/day/currency or cohort/activity-period grains, including denominator and observation cutoff. |

Define a qualifying purchase using a versioned business rule, initially confirmed, non-cancelled
orders. Retain original purchase frequency and track returns separately; do not silently turn a
returned purchase into an order that never existed. Publish gross merchandise amount, returns and
net purchase amount as separate measures. None is necessarily marketplace revenue or margin.

For this platform, R is days since the last qualifying purchase, F is distinct qualifying orders
in a declared window, and M is net purchase amount in that window and currency. Specify discount,
tax and shipping treatment. Keep currency groups separate; a later reporting-currency measure
must reference a versioned FX source and rate date. Assign profile segments with documented,
versioned thresholds, optionally per comparable market/cohort. Inactive is a descriptive label.

Store both event-time and knowledge-time metadata. Corrected business reports may restate history;
future model training must not use information that was unavailable at its prediction cutoff.
Recency and rolling-window membership change even when no events arrive: daily aging/expiration
is required for inactive customers, not only updates for customers in the latest ingestion batch.

## Architecture and code boundaries

```text
Source database changes / transactional outbox / attributed historical import
                 |
       Versioned ingestion adapters
                 |
   Kafka where needed --> Raw Iceberg events
                                 |
                    Canonical purchase facts
                                 |
            Customer profiles and analytical marts
                                 |
                 Trino --> Read API --> Website / BI

Airflow schedules and retries jobs; Spark executes set-based transformations.
The publication record binds input positions, metric versions and output snapshots.
```

Use a modular codebase, not a microservice per table or metric:

- `contracts`: versioned source schemas, stable keys, timestamp and deletion semantics.
- `domain`: metric definitions, policy versions and small reference calculations; no network I/O.
- `transforms`: Spark DataFrame/SQL expressions with explicit inputs and outputs; no hidden reads,
  writes, clock access or scheduler dependencies. Spark types are appropriate here: do not disguise
  distributed data as Python collections to make the design look framework-independent.
- `jobs`: select input bounds, call transforms, validate candidates and publish outputs.
- `adapters`: Kafka/source ingestion, Iceberg/catalog I/O and Trino queries. Compatibility
  workarounds stay inside adapters and have focused regression tests.
- `orchestration`: schedules, dependencies, retries and writer coordination only.
- `serving`: bounded read queries, authorization, pagination and API contracts.
- `frontend`: customer/product exploration and freshness indicators; no duplicate segmentation
  algorithms or direct database credentials.

Source imports and live changes use the same canonical contracts. Use transactional outbox or
database CDC where needed to avoid an application committing an order but failing its separate
event publication. Do not build speculative cross-device identity resolution in the first version.

## Processing and publication decisions

1. **Ingestion:** begin with 2-minute micro-batches for Kafka and bounded file imports for history.
   Tune upward when volumes are small. Iceberg documents the metadata and small-file cost of
   frequent streaming commits and recommends at least a minute between commits
   ([streaming maintenance](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)).
2. **Canonical updates:** process new source ranges every five minutes. Apply monotonic entity
   versions; an older late update must never overwrite newer state. Quarantine conflicting equal
   versions and distinguish source replay from a genuinely new order line.
3. **Customer metrics:** update every 15 minutes from affected order/customer keys. Store canonical
   contributions or recompute bounded affected customer-day groups from canonical facts. Never
   blindly add a retried batch to previous totals. Removed items and emptied groups require explicit
   deletion/zeroing; overwriting only non-empty partitions leaves stale results.
4. **History and aging:** daily cohorts and segment history, plus correction runs for older changes.
   Include customers whose window boundaries expire even if they have no new activity. Large
   backfills use the same definitions with explicit bounds and separate resource limits.
5. **Publication:** validate candidates before exposing them. Record an atomic publication pointer
   containing each output table's snapshot ID, source bounds, as-of time and metric version. Serve
   one such publication per request; independent table commits are not a multi-table transaction.

Keep processed-range and publication records in a dedicated PostgreSQL control schema, separate
from Airflow's internal metadata tables. Commit the publication pointer and completed-range record
together after table snapshots exist and pass checks. A failed pointer update can leave unpublished
candidate snapshots, which a retry can identify and reuse or replace. It must not expose a partial
publication or advance a range without its outputs. PostgreSQL does not make the preceding Iceberg
writes a distributed transaction; candidate isolation and idempotent recovery remain necessary.

Raw events should be append-oriented. Deduplicate source/business changes when constructing
canonical state, rather than performing a global event-ID MERGE across all historical raw files
on every micro-batch. Raw transport duplicates can remain identifiable; certified business facts
must not double-count them. Physical ingestion replay must be tested against the selected sink.

Do not mistake Iceberg's append-snapshot incremental read for a general change feed. It does not
cover arbitrary overwrite/delete/replace operations. Maintain an explicit source change log and
processed-range ledger, or use a compatible, tested changelog mechanism. Snapshot-based reads
from mutable tables must not silently skip updates
([Iceberg incremental reads](https://iceberg.apache.org/docs/latest/spark-queries/)).

## Performance and resource isolation

Partition raw events by ingestion date initially, so late arrivals remain incrementally discoverable.
Partition purchase facts by purchase date at the reference scale; pilot tables may use months.
Use sorting and measured file statistics for customer/product access. Do not create one physical
partition per customer or product. Profile-table bucketing is a benchmark option, not a universal
constant; avoid splitting a small dimension into hundreds of tiny files.

Target roughly 256–512 MiB data files on large tables after compaction, allowing smaller fresh
files until maintenance. This is a starting range, not a correctness requirement. Schedule file
and manifest maintenance, and pin snapshots needed by active readers, recovery and backfills.
Old-snapshot cleanup must respect those references and the deletion policy.

Keep transformations in Spark SQL/DataFrames. Use AQE, statistics, partition pruning and measured
broadcast limits; do not broadcast a million-row dimension solely because it is called a dimension.
Inspect shuffled bytes, spill and worst-task duration. Hot sellers/products, very large buyers and
the unknown-customer key need explicit skew tests
([Spark performance guidance](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

Trino reads prepared marts for normal dashboard interactions. Keep detailed exploratory SQL
separate with concurrency, memory, execution-time and scan budgets. A 500-row response limit does
not prevent a query scanning billions of rows. Use resource groups to stop analyst queries from
exhausting interactive capacity ([Trino resource groups](https://trino.io/docs/current/admin/resource-groups.html)).
Cache only bounded aggregate responses, keyed by publication and filters. Do not load all customers
into React or use a Spark job as the implementation of every browser request.

Production Kafka starts from a fault-tolerant deployment with replicated logs, acknowledgements
and minimum in-sync replicas configured for the intended broker-loss tolerance. Partition counts
come from measured per-partition throughput and skew: a first bound is
`ceil(peak_rate / (target_utilization * measured_partition_rate))`, followed by recovery tests.
Benchmark 12 partitions for the main purchase stream as a starting experiment, not a capacity claim.

Benchmark Spark at 16, 32 and 64 executor vCPUs with initially 4 GiB memory per executor core,
plus driver/overhead and shuffle storage accounted separately. Measure before selecting production
nodes. Keep Trino resources isolated; cap backfill and compaction concurrency. Local Docker remains
a functional environment, not a substitute for multi-node failure or capacity testing.

## Reliability and privacy

- Preserve stable source identities, source offsets/versions and event IDs. Treat retries as normal.
- Validate schema, numeric precision, business keys and required timestamps. Quarantine malformed
  records with reasons and provide a replay path. Do not manufacture valid-looking replacements.
- Advance processed ranges only with durable evidence of output completion; support crashes before
  and after each commit. Coordinate overlapping writers and retry optimistic conflicts safely.
- Separate bounded streaming deduplication from long-horizon corrections. A late legitimate update
  outside a watermark must still reach the canonical facts and affected customer metrics.
- Track source-position coverage and quarantines, not just counts. Validate distinct order counts,
  line quantities, totals per currency, return bounds and customer identity coverage. Compare
  incremental results to a reference full recomputation on bounded datasets.
- Monitor last successful publication, oldest unprocessed event, backlog, quarantine rates, duplicate
  conflicts, files per partition, compaction debt and query percentiles. Keep last-good data visible
  with its age when updates fail; never silently substitute fixtures.
- Back up and restore the catalog, orchestration/processed-range state and security configuration.
  Object storage alone does not restore a functioning lakehouse. Test checkpoint and catalog loss.
- Keep direct identifiers out of analytical facts and restrict identity mappings. Do not expose real
  customer profiles through the current anonymous showcase endpoint. Separate demonstration data
  from authenticated customer access. Include deletes in snapshots, derived marts, caches and replay
  rules; retain a suppression mechanism so old-source replay cannot resurrect deleted profiles.

## Performance targets and acceptance evidence

These are proposed service targets, not achieved measurements. Freshness is measured from durable
source availability to certified publication; event-time lateness is reported separately.

| Workload | Proposed target at reference scale |
| --- | --- |
| Valid source data available in canonical facts | p95 within 10 minutes |
| Current customer purchase metrics | p95 within 30 minutes |
| Daily cohort/segment publication | Ready by 06:00 UTC for the defined prior UTC day |
| Standard filtered dashboard queries | p95 under 3 seconds, 20 concurrent clients, defined query mix |
| Bounded customer detail query | p95 under 2 seconds, pagination required |
| Failure recovery | Resume ingestion within 15 minutes after dependency recovery; drain a one-hour peak backlog within a further hour while accepting reference traffic |

The correctness acceptance criterion is no lost in-scope source changes and no duplicate canonical
facts within the tested failure model. This is not a universal zero-data-loss promise for region
destruction, expired source retention or unavailable upstream history. Disaster RPO/RTO require
an explicit deployment/backup design and a restore drill.

Benchmark levels:

1. Real-data functional run: attributed UCI data; cancellations, missing identity and repeat purchases
   handled according to explicit contracts. Stable source row IDs; do not deduplicate legitimate
   repeated invoice/SKU rows by assuming that pair is unique.
2. Incremental equivalence: repeated deliveries, out-of-order versions, partial returns, item removal,
   empty aggregates, midnight aging and schema changes must match a bounded full recomputation.
3. Growing history: 1 million, 10 million and 100 million line facts initially; later 1 billion or more
   on provisioned resources. Hold incoming batch size fixed while history grows. Record scanned
   bytes to expose accidental full scans. Tests below the intended history size cannot validate it.
4. Mixed load: replay reference traffic plus 10x bursts for 30 minutes, while 20 clients issue the
   published dashboard query mix. Run a separate 24-hour soak and sustained growth-scenario test.
5. Recovery: kill executors and the driver, interrupt a commit, replay offsets, restart a broker,
   make the catalog temporarily unavailable, and interrupt publication between tables. Verify
   old-or-new publication consistency and exact customer metrics after recovery.
6. Maintenance: run compaction and a bounded backfill with foreground ingestion/queries; compare
   throughput, latency, conflict rate and cost before/after. Test deletes against replay and caches.

Record dataset/version, event-size percentiles, source rate, committed throughput, hardware, image
digests, executor resources, shuffle/spill, files read, bytes scanned, latency distributions,
publication freshness, recovery duration and cost per million events/TB processed. For short
comparisons run a warm-up and at least five measured repetitions. Report cold and warm queries
separately. Preserve the existing benchmark evidence policy: no extrapolated production claim
from a successful laptop demonstration.

## Original repository gaps and implementation order

The following assessment motivated the implementation. It describes the original paths, not
the new customer analytics pipeline. Legacy jobs remain for compatibility.

| Evidence in current code | Required change |
| --- | --- |
| [Order lines](../../src/marketplace_data/transforms/orders.py) extract a single SKU/quantity from order-created events. | Introduce stable multi-line purchase contracts and mutable line state, including removal and returns. |
| [Silver customers](../../src/marketplace_data/jobs/silver_customers.py) reads the full Bronze source and replaces customer history. | Rebuild only affected customer histories with deterministic late-change boundaries; retain full rebuild as recovery/reference mode. |
| [Silver orders](../../src/marketplace_data/jobs/silver_orders.py) defaults to a full read and optionally uses snapshot-range reads. | Make processed ranges first-class and compatible with actual source-table operations. |
| [Repository MERGE](../../src/marketplace_data/iceberg.py) updates matching rows unconditionally and materializes via a cached Python RDD for a compatibility workaround. | Prevent stale-version regression; benchmark and isolate the workaround, then replace it only with a verified compatible engine path. Do not remove it blindly. |
| [Gold KPIs](../../src/marketplace_data/jobs/gold_kpis.py) scans all current orders. Its [transform](../../src/marketplace_data/transforms/gold.py) does not group money by currency. | Build affected-key customer/order/day marts, currency-safe measures and exact order-level denominators. |
| [Production example](../../config/production.example.yaml) fixes 1,200 shuffle partitions and 30-second triggers. | Derive settings from benchmark evidence and freshness requirements, not the production label. |
| [Generator](../../src/marketplace_data/generator.py) materializes the generated sequence as a list; showcase is intentionally bounded. | Use lazy/bounded or distributed load generation with repeat-customer and skew controls; never generate the scale dataset in driver memory. |
| [Query adapter](../../architecture-site/server/trino.ts) bounds rows and time, while the current UI focuses on cases. | Add analytical read models, query resource controls and a customer/product interface; remove case-management dependencies from the main experience. |

Implementation order is contracts and metric reference tests, real-data import, canonical purchase
facts, incremental customer marts, consistent publication, analytical API/UI, then scale and recovery
validation. Do not add a second streaming engine, feature store, online recommendation service or
extra analytical database before a measured requirement justifies it.

## Implementation status

The new `order.snapshot` contract, append-oriented `bronze.purchase_events`, versioned canonical
purchase orders/items, affected-key customer/product marts, daily profile aging, PostgreSQL
publication ledger, pinned analytical API and customer/product React application are implemented.
The JSONL adapter accepts canonical source snapshots; generated load uses a bounded iterator.
Scoped replacement deletes emptied groups, and retries restore the published working baseline
before rebuilding. Dedicated integration checks inject a failed publication and compare incremental
outputs against complete reference recomputation. See the README for commands and latest evidence.

The attributed UCI import now streams XLSX into audited JSONL; unlinked cancellations are explicitly
excluded rather than mapped to invented orders. The React application provides customer/product
history, date filters and monthly retention cohorts. Publication-bound pagination, bounded aggregate
caching, durable business quarantine, output retention planning/application, freshness/rejection
alerts and a dedicated end-to-end benchmark harness are implemented. CI executes the generated
Kafka-to-dashboard flow and failure/replay checks. The retired case UI/API have been removed.

This is not completion of all production gates above. Deployed CDC, identity/deletion suppression,
distributed fencing, source-cursor maintenance, restore drills, multi-node recovery and scale/soak
validation remain open. The compatibility RDD materialization remains in the storage adapter pending a verified
engine replacement. The proposed hardware and service targets are still unmeasured planning inputs.
