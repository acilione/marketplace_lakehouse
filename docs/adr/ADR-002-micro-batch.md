# ADR-002: Micro-batch Structured Streaming

Status: Accepted

Use 30-second Structured Streaming micro-batches. The target latency is minutes, so this reuses
DataFrame and Iceberg semantics without a separate continuous-processing engine. Watermarks bound
deduplication state; data outside that horizon enters bounded correction rather than silently
changing current state.

