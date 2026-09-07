"""Durable last-run metrics shared by short-lived local jobs and an exporter."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily

from marketplace_data.util import validate_identifier


def record(pipeline: str, **values: Any) -> None:
    directory = os.getenv("MLH_METRICS_DIRECTORY")
    if not directory:
        return
    # Each pipeline has a single writer; replacement makes exporter reads atomic.
    destination = Path(directory) / f"{validate_identifier(pipeline)}.json"
    temporary = destination.with_suffix(f".{os.getpid()}.tmp")
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        previous = json.loads(destination.read_text()) if destination.exists() else {}
        temporary.write_text(json.dumps({**previous, **values}), encoding="utf-8")
        temporary.replace(destination)
    except (OSError, ValueError):
        logging.getLogger(__name__).exception("Cannot persist pipeline telemetry")


class PipelineCollector:
    def collect(self):  # type: ignore[no-untyped-def]
        directory = Path(os.environ["MLH_METRICS_DIRECTORY"])
        fields = {
            "last_success": "Last successful completion timestamp",
            "last_failure": "Last failed completion timestamp",
            "last_quality_failure": "Last quality gate failure timestamp",
            "started": "Last start timestamp",
            "heartbeat": "Most recent ingestion batch timestamp",
            "duration": "Last run duration in seconds",
            "last_event": "Newest ingested source event timestamp",
            "source_to_bronze": "Newest batch source-to-Bronze lag in seconds",
        }
        families = {
            key: GaugeMetricFamily(f"marketplace_pipeline_{key}_seconds", text, labels=["pipeline"])
            for key, text in fields.items()
        }
        for path in directory.glob("*.json"):
            try:
                values = json.loads(path.read_text())
                for key, family in families.items():
                    family.add_metric([path.stem], float(values.get(key, 0)))
            except (OSError, ValueError, TypeError):
                logging.getLogger(__name__).exception("Cannot read %s", path.name)
        yield from families.values()


def main() -> None:
    REGISTRY.register(PipelineCollector())
    start_http_server(8000)
    while True:
        time.sleep(30)


if __name__ == "__main__":
    main()
