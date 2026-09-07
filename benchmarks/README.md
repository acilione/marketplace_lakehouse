# Benchmark protocol

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
