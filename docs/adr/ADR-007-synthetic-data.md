# ADR-007: Synthetic-only demonstration data

Status: Accepted

All environments covered by this repository use deterministic generated events. Production personal
data must never be copied into a demonstration environment. This eliminates privacy transfer risk and
makes duplicates, poison messages, lateness, skew, and recovery scenarios reproducible.
