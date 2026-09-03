"""Low-cardinality application and data-quality metrics."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

ROWS = Counter(
    "marketplace_pipeline_rows_total",
    "Rows handled by a pipeline",
    ("pipeline", "outcome"),
)
RUNS = Counter(
    "marketplace_pipeline_runs_total",
    "Pipeline terminal outcomes",
    ("pipeline", "status"),
)
DURATION = Histogram(
    "marketplace_pipeline_duration_seconds",
    "Pipeline wall-clock duration",
    ("pipeline",),
)
FRESHNESS = Gauge(
    "marketplace_dataset_freshness_seconds",
    "Age of the newest committed eligible event",
    ("dataset",),
)
QUALITY_FAILURES = Counter(
    "marketplace_quality_failures_total",
    "Quality failures by stable check name",
    ("dataset", "check"),
)


def serve_metrics(port: int) -> None:
    start_http_server(port)
