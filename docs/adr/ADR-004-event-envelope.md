# ADR-004: Registry-managed event envelope

Status: Accepted

All events separate stable transport metadata from a versioned payload. Backward-transitive changes
may add optional/defaulted fields; breaking changes require a new major version and migration window.
The local profile supports JSON for inspection and Confluent-framed Avro for wire compatibility.

