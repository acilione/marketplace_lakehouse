# Architecture and processing semantics

## Local operations control room

The `dashboard` Compose profile adds a TypeScript control API behind the architecture site's Nginx
origin. Signed, expiring HttpOnly sessions authenticate viewers and operators. Only operators can
request service changes; requests and outcomes are logged. Neither the browser nor the API has
Docker-socket access. An isolated controller on a private network maps a fixed catalog to exact Compose container names; arbitrary container IDs,
images, commands, and Docker API paths are not accepted. Only Trino, Prometheus, and Grafana are
lifecycle-controllable. Core stateful services remain under `make up` / `make down` ownership.

The query adapter speaks Trino's HTTP statement protocol and exposes only one statement beginning
with `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, or `EXPLAIN`. Mutation and session keywords, multiple
statements, bodies above 16 KiB, execution over 30 seconds, and results above 500 rows are rejected
or truncated. Pagination exhaustion fails explicitly and abandoned queries are cancelled. Trino
also enforces read-only access independently of the API. The catalog browser reads Iceberg REST metadata directly, while dashboard metric
cards use fixed aggregate queries against certified tables.

The Kafka adapter exposes metadata and a bounded event tail for `marketplace.*` topics only. A
dedicated long-lived consumer starts near each partition's high watermark, keeps at most 100 records
per topic in process memory, and has auto-commit disabled. Dashboard observation therefore cannot
move application consumer offsets, publish records, create topics, or alter broker configuration.

The synthetic domain simulator produces correlated customer, inventory, order, payment, and
shipment lifecycles through the same versioned envelope used by ingestion. Fixed seeds make a run
reproducible, while named load profiles control volume, pacing, malformed messages, duplicates,
lateness, and failed business outcomes. The stress runner gives every ingestion run a unique
checkpoint lineage and captured starting offsets. Ingestion runs concurrently with the producer,
then drains both streams. Run-specific identifiers isolate repeated seeds and prevent historical
rows from satisfying reconciliation. The runner executes all conformance paths and records producer, routing, latency, dataset,
and phase-duration evidence under the ignored `benchmark-results/` directory.

This profile is a local operator convenience, not a production control plane. Mounting
`/var/run/docker.sock` confers host-equivalent authority to the isolated controller container. Production service
lifecycle belongs to Kubernetes RBAC/GitOps and production SQL authorization belongs to the query
engine and identity provider.

The platform separates transport, immutable evidence, conformance, and consumer publication.
Each domain topic has its own retention and can be separated into an independent streaming query in
production. The local query shares one checkpoint lineage while accepted and rejected records use
independent sink checkpoints so a poison record cannot halt healthy data.

Bronze retains event and Kafka metadata. Structural failures retain the raw JSON or base64 Avro
bytes plus topic, partition, offset, parser error, first-seen time, and remediation state. Valid
business-rule violations remain in Bronze with quality flags. Silver excludes those flags until a
bounded correction reconciles them.

Incremental Silver input is bounded by Iceberg snapshot IDs. Ordering is `occurred_at`, producer
sequence, Kafka partition, and Kafka offset. The final fields make ties total and deterministic.
Customer history uses half-open `[valid_from, valid_to)` intervals, collapses unchanged attributes,
and allows exactly one current row per customer.

Gold is written through an explicit branch-qualified Iceberg identifier. Quality runs before the
candidate write; promotion fast-forwards `main`. Consumers therefore see the old complete version
or the new complete version, never a partial overwrite. Backfills use the same branch isolation and
never mutate `main` unless publication is explicitly requested.

Durable checkpoints live in a separate bucket from table data. Their path is a contract among query
name, environment, code lineage, schema, and partitioning. Deleting or reusing a checkpoint outside
that identity requires an incident and replay plan.

## Trust boundaries

- Producers write Kafka only; they cannot access the catalog or object store.
- Ingestion reads assigned topics and writes Bronze/quarantine prefixes only.
- Conformance reads Bronze and owns named Silver tables.
- Publication owns candidate branches and fast-forward privileges for named Gold tables.
- Maintenance has snapshot/orphan permissions but no Kafka or consumer permissions.
- Trino and BI identities are read-only on published Silver/Gold references.
