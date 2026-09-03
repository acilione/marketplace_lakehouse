# Operations guide

Every run must record the code SHA, configuration hash, input snapshot/offset evidence, output
snapshot, counts, business sums, quality outcome, and UTC timing in `ops.pipeline_runs`. Streaming
progress additionally exposes input/processed rates, batch duration, watermark, state size, and
checkpoint age.

## SLOs

| Indicator | Objective |
|---|---:|
| Bronze freshness | p95 ≤ 3 minutes; p99 ≤ 8 minutes over 30 days |
| Silver freshness | p95 ≤ 15 minutes |
| Certified daily close | 06:00 UTC |
| Critical pipeline success | ≥ 99.5% monthly |
| Streaming RPO | ≤ 5 minutes |
| Single-job RTO | ≤ 30 minutes |
| Source position reconciliation | 100% per certified batch |

Pages are reserved for freshness, correctness, durability, or recovery risk. Each page links a
dataset owner, dashboard, runbook, deployment SHA, and silence policy. Multi-window SLO burn-rate
alerts should replace fixed thresholds in the production monitoring backend.

## Safe maintenance

Compaction targets 512 MB files and rewrites manifests after data files. Snapshot expiration retains
at least five snapshots and seven days in the supplied command. Orphan deletion is intentionally not
part of the standard maintenance job: use a restricted identity, dry-run report, and an interval
greater than the longest possible Spark run plus object-store consistency margin.

## Backup and recovery

- Back up catalog PostgreSQL separately from versioned object storage.
- Preserve Kafka beyond the maximum approved replay window.
- Version the warehouse bucket and replicate critical audit prefixes.
- Quarterly, restore the catalog into an isolated environment, validate referenced objects, start a
  query from its durable checkpoint, and reconcile all source positions.
- Never prove recovery by deleting production state.

