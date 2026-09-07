from __future__ import annotations

import json
from pathlib import Path

import pytest

from marketplace_data.simulator import TOPICS
from marketplace_data.stress_report import REQUIRED_DATASETS, build_stress_report, parse_phases


def test_stress_report_passes_complete_evidence() -> None:
    producer = {
        "failed_messages": 0,
        "expected_accepted_unique": 45,
        "malformed_messages": 5,
        "delivered_by_topic": dict.fromkeys(TOPICS.values(), 10),
    }
    bronze = [{"source_topic": topic, "events": 9} for topic in TOPICS.values()]
    ingestion = [
        {
            "accepted_events": 45,
            "quarantined_events": 5,
            "latency_ms_p50": 100,
            "latency_ms_p95": 250,
        }
    ]
    datasets = [
        {"dataset": dataset, "row_count": 1, "expected_rows": 1} for dataset in REQUIRED_DATASETS
    ]

    report = build_stress_report(
        run_id="stress_test",
        profile="smoke",
        producer=producer,
        bronze_rows=bronze,
        ingestion_rows=ingestion,
        dataset_rows=datasets,
        phases={"producer": 2.5, "bronze": 4.5},
    )

    assert report["status"] == "PASSED"
    assert report["ingestion"]["quarantine_rate"] == 0.1
    assert report["total_pipeline_seconds"] == 7.0


def test_stress_report_fails_when_a_topic_is_missing() -> None:
    producer = {
        "failed_messages": 0,
        "delivered_by_topic": dict.fromkeys(TOPICS.values(), 1),
    }
    bronze = [{"source_topic": topic, "events": 1} for topic in list(TOPICS.values())[1:]]
    datasets = [{"dataset": dataset, "row_count": 1} for dataset in REQUIRED_DATASETS]

    report = build_stress_report(
        run_id="stress_test",
        profile="smoke",
        producer=producer,
        bronze_rows=bronze,
        ingestion_rows=[{"accepted_events": 4, "quarantined_events": 0}],
        dataset_rows=datasets,
        phases={},
    )

    assert report["status"] == "FAILED"
    assert not report["checks"]["bronze_observed_every_topic"]


def test_phase_parser_rejects_invalid_measurements() -> None:
    assert parse_phases(["producer=1.25", "bronze=3"]) == {"producer": 1.25, "bronze": 3.0}
    with pytest.raises(ValueError):
        parse_phases(["producer=-1"])


def test_cli_writes_failure_evidence_and_exits_nonzero(tmp_path: Path) -> None:
    from marketplace_data.stress_report import main

    producer = tmp_path / "producer.json"
    producer.write_text(
        json.dumps({"failed_messages": 0, "expected_accepted_unique": 50, "malformed_messages": 0})
    )
    empty = tmp_path / "empty.jsonl"
    empty.write_text("")
    output = tmp_path / "report.json"
    with pytest.raises(SystemExit) as caught:
        main(
            [
                "--run-id",
                "test",
                "--profile",
                "smoke",
                "--producer-report",
                str(producer),
                "--bronze-report",
                str(empty),
                "--ingestion-report",
                str(empty),
                "--dataset-report",
                str(empty),
                "--output",
                str(output),
            ]
        )
    assert caught.value.code == 1
    assert json.loads(output.read_text())["status"] == "FAILED"


def test_historical_data_cannot_satisfy_run_reconciliation() -> None:
    report = build_stress_report(
        run_id="new",
        profile="smoke",
        producer={
            "failed_messages": 0,
            "expected_accepted_unique": 50,
            "malformed_messages": 0,
            "delivered_by_topic": dict.fromkeys(TOPICS.values(), 10),
        },
        bronze_rows=[{"source_topic": topic, "events": 400} for topic in TOPICS.values()],
        ingestion_rows=[{"accepted_events": 2000, "quarantined_events": 0}],
        dataset_rows=[
            {"dataset": name, "row_count": 1, "expected_rows": 1} for name in REQUIRED_DATASETS
        ],
        phases={},
    )
    assert report["status"] == "FAILED"
    assert not report["checks"]["accepted_records_reconcile"]
