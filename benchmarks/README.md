# Benchmark protocol

Run every scenario at least five times after one warm-up and preserve individual results. Record
input rows and compressed bytes, file count, executor count/cores/memory, Spark configuration,
shuffle read/write, spill, task distribution, physical plans, runtime, exact hardware, and image
digest. Generated JSON belongs under ignored `benchmark-results/`; approved evidence is attached to
a release rather than quietly committed without review.

Required comparisons are AQE/broadcast strategy, skew, small files before/after compaction,
incremental versus full recompute, a 2,000 event/s burst, and driver recovery. Local observations may
not be extrapolated to production capacity.

