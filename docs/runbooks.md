# Operational runbooks

All timestamps and ranges are UTC. Preserve logs, progress JSON, source offsets, snapshot IDs,
deployment SHA, and configuration hash before restarting or mutating anything.

## Streaming job stalled

1. Confirm Kafka, catalog, object store, DNS, and certificate health; capture Kafka lag and the last
   five streaming progress records.
2. Compare input rate, processed rate, batch duration, watermark, state rows/bytes, executor loss,
   shuffle spill, and catalog commit latency.
3. If a dependency is unavailable, allow bounded retries; page that owner when its threshold burns.
4. If capacity is the limit, add executors within quota or lower bounded input per trigger. Do not
   change partitioning or checkpoint identity during an incident.
5. Restart only after recording the query ID and latest committed batch. Verify offset continuity,
   unique event IDs, and freshness recovery before resolving.

## Checkpoint unusable

1. Declare an incident; make the checkpoint prefix read-only and preserve a versioned copy.
2. Identify the last Iceberg snapshot and complete Kafka offsets from audit plus table metadata.
3. Create a new query name and checkpoint prefix. Set explicit starting offsets; never guess.
4. Replay into an isolated candidate branch or table and reconcile positions, counts, and sums.
5. Hand over only after duplicate, missing-position, and late-data checks pass. Retain old evidence.

## Data quality gate failed

1. Confirm the consumer reference did not move and tag its current snapshot.
2. Record failing rules, sample only tokenized/non-sensitive keys, and compare source controls.
3. Identify producer, schema, code, or dependency change. Freeze only affected publication.
4. Correct via a new event or bounded remediation batch; do not edit Bronze evidence in place.
5. Rerun the candidate, review its diff, publish, and close the audit record with owner approval.

## Late-data correction

Declare exact dates/keys and source snapshots; create a unique run ID and candidate branch; execute
the same conformance code; reconcile affected Silver and Gold partitions; obtain approval for metric
changes; fast-forward the published reference; preserve the rollback tag and notify consumers.

## Bad release rollback

Stop new schedules, preserve the failing candidate and logs, restore the previously signed image and
configuration, and move the consumer reference to its tagged snapshot only if publication occurred.
Reconcile and validate SLO recovery. Do not expire evidence until the incident review closes.

## Iceberg maintenance

Run `table_maintenance --dry-run` first. Confirm no long-running writers, branch retention, rollback
tags, current file count, and catalog backup. Compact, rewrite manifests, then expire snapshots within
policy. Orphan cleanup requires separate approval and a longer safe interval.

## Credential rotation

Create the new secret in the external manager, update workload identity grants, roll one canary job,
confirm access and redaction, roll remaining jobs, then revoke the old grant and verify rejection.

