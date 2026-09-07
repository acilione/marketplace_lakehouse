"""Assemble and validate machine-readable evidence from a local stress run."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from marketplace_data.simulator import TOPICS

REQUIRED_DATASETS = {
    "silver.orders",
    "silver.payments",
    "silver.shipments",
    "silver.inventory_daily",
    "silver.customers_scd2",
    "gold.daily_marketplace_kpis",
}


def load_json(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected one JSON object in {path}")
    return value


def load_json_lines(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"expected JSON objects in {path}")
        rows.append(value)
    return rows


def parse_phases(values: list[str]) -> dict[str, float]:
    phases: dict[str, float] = {}
    for value in values:
        name, separator, raw_seconds = value.partition("=")
        if not separator or not name or float(raw_seconds) < 0:
            raise ValueError(f"invalid phase measurement: {value}")
        phases[name] = float(raw_seconds)
    return phases


def build_stress_report(
    *,
    run_id: str,
    profile: str,
    producer: dict[str, Any],
    bronze_rows: list[dict[str, Any]],
    ingestion_rows: list[dict[str, Any]],
    dataset_rows: list[dict[str, Any]],
    phases: dict[str, float],
) -> dict[str, Any]:
    delivered = {
        str(name): int(count)
        for name, count in dict(producer.get("delivered_by_topic", {})).items()
    }
    bronze = {str(row["source_topic"]): int(row["events"]) for row in bronze_rows}
    datasets = {str(row["dataset"]): int(row["row_count"]) for row in dataset_rows}
    ingestion = ingestion_rows[0] if ingestion_rows else {}
    accepted = int(ingestion.get("accepted_events", 0))
    quarantined = int(ingestion.get("quarantined_events", 0))
    total_routed = accepted + quarantined
    expected = int(producer.get("expected_accepted_unique", -1))
    late = int(producer.get("expected_late_unique", 0))

    checks = {
        "producer_has_no_delivery_failures": int(producer.get("failed_messages", -1)) == 0,
        "producer_reached_every_topic": all(
            delivered.get(topic, 0) > 0 for topic in TOPICS.values()
        ),
        "bronze_observed_every_topic": all(bronze.get(topic, 0) > 0 for topic in TOPICS.values()),
        "bronze_routed_records": total_routed > 0,
        "accepted_records_reconcile": expected >= 0 and expected - late <= accepted <= expected,
        "quarantine_records_reconcile": quarantined == int(producer.get("malformed_messages", -1)),
        "topic_totals_reconcile": sum(bronze.values()) == accepted,
        "dataset_rows_reconcile": len(dataset_rows) == len(REQUIRED_DATASETS)
        and all(int(row["row_count"]) == int(row.get("expected_rows", -1)) for row in dataset_rows),
        "required_datasets_are_nonempty": all(
            datasets.get(dataset, 0) > 0 for dataset in REQUIRED_DATASETS
        ),
    }
    return {
        "run_id": run_id,
        "profile": profile,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "status": "PASSED" if all(checks.values()) else "FAILED",
        "checks": checks,
        "producer": producer,
        "ingestion": {
            "accepted_events": accepted,
            "expected_accepted_unique": expected,
            "late_records_not_in_bronze": expected - accepted,
            "quarantined_events": quarantined,
            "quarantine_rate": round(quarantined / total_routed, 6) if total_routed else None,
            "source_to_bronze_latency_ms_p50": ingestion.get("latency_ms_p50"),
            "source_to_bronze_latency_ms_p95": ingestion.get("latency_ms_p95"),
            "accepted_by_topic": bronze,
        },
        "dataset_rows": datasets,
        "phase_seconds": phases,
        "total_pipeline_seconds": round(sum(phases.values()), 3),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--producer-report", required=True)
    parser.add_argument("--bronze-report", required=True)
    parser.add_argument("--ingestion-report", required=True)
    parser.add_argument("--dataset-report", required=True)
    parser.add_argument("--phase", action="append", default=[])
    parser.add_argument("--producer-seconds", type=float, default=0)
    parser.add_argument("--output", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    report = build_stress_report(
        run_id=args.run_id,
        profile=args.profile,
        producer=load_json(args.producer_report),
        bronze_rows=load_json_lines(args.bronze_report),
        ingestion_rows=load_json_lines(args.ingestion_report),
        dataset_rows=load_json_lines(args.dataset_report),
        phases=parse_phases(args.phase),
    )
    output = Path(args.output)
    report["producer_wall_seconds"] = args.producer_seconds
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(f"{json.dumps(report, indent=2, sort_keys=True)}\n", encoding="utf-8")
    if report["status"] != "PASSED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
