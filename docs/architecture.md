# Architecture and processing semantics

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
