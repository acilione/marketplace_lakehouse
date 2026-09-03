# ADR-001: Apache Iceberg tables

Status: Accepted

Use Iceberg v2 tables instead of unmanaged Parquet directories. Atomic snapshots, optimistic
commits, schema/partition evolution, row-level MERGE, branches, and time travel enable the required
correctness and recovery controls. The cost is an independently backed-up catalog plus compaction,
manifest, snapshot, and orphan-file operations.

