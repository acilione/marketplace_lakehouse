# Benchmark protocol

## Purchase analytics end-to-end benchmark

```bash
make analytics-benchmark BENCHMARK_ARGS="--orders 1000 --delta 100 --customers 100 --clients 4 --requests 20 --verify"
```

Each run creates a dedicated `marketplace-lakehouse-benchmark-<label>` Compose project on ports
24373/24386. Stop another benchmark before starting the next; never run load against a merchant
dataset. The harness never deletes volumes. Stop it with
`make analytics-stop ANALYTICS_PROJECT=marketplace-lakehouse-benchmark-<label>`.

The phases are startup, initial source delivery, initial processing, a fixed new-order delta,
incremental processing, no-input processing and mixed-view HTTP requests. `--verify` adds the actual
Iceberg/PostgreSQL fault/replay/full-recompute suite. Driver/startup time is included in processing
duration, not hidden. A generation rate cap remains part of source-delivery time.

Reports in ignored `benchmark-results/<label>/report.json` include parameters, revision/diff hash,
host resources, durations, first-request timing, successful p50/p95, explicit failure counts and
per-request samples. `--evidence` collects Spark REST stage input/shuffle/spill metrics when the
driver UI is available and metadata file/record/byte counts. Stage totals include repeated reads
and attempts, not unique source size; unavailable metrics are reported, never replaced with zero.

Compare history sizes of 1,000, 100,000 and 1,000,000 orders with the same customer cardinality and
fixed delta first. Increase to larger volumes only after checking memory/disk budgets. For an
independent cardinality test, keep history fixed and increase customers. Inspect incremental scan
growth, not only rows/second. Run the existing skew/compaction scenarios separately. Do not claim
20-client latency or distributed capacity unless that exact workload has been measured.

The first HTTP sample is not a controlled cold-cache test. Warm runs include the API's bounded
aggregate cache, keyed by snapshot set, currency and date window. Record the effect explicitly.
Output retains individual samples so errors cannot disappear inside an average.

For a before/after no-input comparison on a retained benchmark only:

```bash
make analytics-benchmark BENCHMARK_ARGS="--existing-project marketplace-lakehouse-benchmark-<label> --label after-change --clients 4 --requests 24"
```

This does not load new orders. It records the publication before/after refresh, the source-tree hash
(including non-ignored untracked source), container image IDs and the image used by transient jobs.
Compare identical canonical data and observation dates; do not change the cluster while measuring.

## Controlled compute experiments and larger acceptance gates

### Local acceptance observations — 2026-10-12

These are single-run development observations, not an SLA or a controlled distributed benchmark.
The host exposed 20 logical CPUs and about 15.5 GiB RAM to WSL; unrelated services shared the host.
Spark used one two-core executor with 1 GiB heap and 16 shuffle partitions. Processing durations
include separate ingestion/publication driver startup. Dataset: 1,000 generated orders followed by
100 additional orders, 100 possible customer identities, two currencies and 2,115 qualifying lines.

| Measurement | Observed |
| --- | ---: |
| Initial processing, 1,000 orders | 106.59 s |
| Incremental processing, 100 new orders | 98.03 s |
| No-input refresh before short-circuit | 81.64 s |
| No-input refresh after short-circuit | 46.98 s |
| Publication before/after optimized no-input refresh | unchanged: 9 |
| No-input Spark data input bytes / shuffle bytes after short-circuit | 0 / 0 |
| Four clients, 24 mixed-view requests, initial p95 | 0.553 s; zero failures |
| Twenty clients, 120 mixed-view requests after compaction, p95 | 0.981 s; zero failures |
| Cohort output compaction | 16 files / 42,363 bytes → 1 file / 3,601 bytes |
| Recovery, quarantine, replay and full-reference checks | passed |
| Post-compaction full-reference comparison | passed |

The no-input comparison preserved canonical data; the intervening recovery test added eight raw
verification envelopes whose final order was cancelled. This is not a clean-room five-repetition
comparison. A subsequent no-input refresh after compaction took 34.15 s; do not attribute that whole
difference to compaction because caches and host load differed. No spill was recorded in these small
runs. Short warm API tests on this dataset do not establish latency at a million customers.

Raw phase logs and samples are retained locally under `benchmark-results/acceptance-20261012`,
`benchmark-results/optimized-20261012` and `benchmark-results/concurrency20-20261012`. They are ignored
by Git. Reproduce the commands above and attach your own complete reports to release evidence.
The later harness also records a full source-tree hash and job-image identifier. Earlier logs are
retained as collected, without retroactively inventing missing metadata.

Run every scenario at least five times after one warm-up and preserve individual results. Record
input rows and compressed bytes, file count, executor count/cores/memory, Spark configuration,
shuffle read/write, spill, task distribution, physical plans, runtime, exact hardware, and image
digest. Generated JSON belongs under ignored `benchmark-results/`; approved evidence is attached to
a release rather than quietly committed without review.

Required comparisons are AQE/broadcast strategy, skew, small files before/after compaction,
incremental versus full recompute, a 2,000 event/s burst, and driver recovery. Local observations may
not be extrapolated to production capacity.

The Spark compute scenario can be run after `make bootstrap`:

```bash
.venv/bin/python benchmarks/run.py \
  --rows 1000000 \
  --skew-percent 80 \
  --output benchmark-results/join-1m.json
```

Kafka and full-pipeline evidence use the correlated domain simulator instead:

```bash
make simulate SIMULATION_PROFILE=peak
make stress STRESS_PROFILE=steady
```

`make simulate` measures producer delivery. `make stress` additionally measures ingestion,
conformance, publication, and dataset evidence; it stores each run in its own ignored directory.
