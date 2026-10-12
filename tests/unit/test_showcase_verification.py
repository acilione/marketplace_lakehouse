"""Verification rejects a successful-looking run when stored evidence disagrees."""

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from marketplace_data.showcase import generate_showcase

SPEC = importlib.util.spec_from_file_location(
    "showcase_runner", Path(__file__).resolve().parents[2] / "scripts/showcase.py"
)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def setup_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], Any]:
    run_id = "a" * 32
    events = generate_showcase(
        {
            "orders": 8,
            "seed": 7,
            "events_per_second": 20,
            "run_id": run_id,
            "started_at": "2026-10-12T10:00:00+00:00",
        }
    )
    directory = tmp_path / run_id
    directory.mkdir()
    ledger = [
        {"event": event.model_dump(mode="json"), "topic": "topic", "partition": 0, "offset": str(i)}
        for i, event in enumerate(events)
    ]
    (directory / "ledger.json").write_text(json.dumps(ledger))
    monkeypatch.setattr(runner, "WORK", tmp_path)
    unique = {item["event"]["event_id"]: item for item in ledger}
    bronze = [
        {
            "event_id": event_id,
            "event_type": item["event"]["event_type"],
            "payload": item["event"]["payload"],
            "source_topic": "topic",
            "source_partition": 0,
            "source_offset": item["offset"],
            "quality_flags": [],
            "occurred_at": item["event"]["occurred_at"],
            "produced_at": item["event"]["produced_at"],
            "trace_id": item["event"]["trace_id"],
            "producer_sequence": item["event"]["producer_sequence"],
        }
        for event_id, item in unique.items()
    ]
    orders, latest = [], {}
    for event in events:
        if event.event_type == "order.confirmed":
            if event == events[-1] and orders:
                continue
            orders.append(
                {
                    "order_id": event.payload["order_id"],
                    "last_event_id": event.event_id,
                    "status": "CONFIRMED",
                    "amount": event.payload["gross_amount"],
                    "seller_id": event.payload["seller_id"],
                    "market": event.payload["market"],
                    "currency": event.payload["currency"],
                }
            )
        else:
            latest[event.payload["payment_id"]] = event
    payments = [
        {
            "payment_id": key,
            "order_id": event.payload["order_id"],
            "last_event_id": event.event_id,
            "status": event.event_type.split(".")[1].upper(),
            "currency": event.payload["currency"],
            "amount": event.payload["amount"],
        }
        for key, event in latest.items()
    ]

    def query(sql: str) -> list[dict[str, Any]]:
        if "$refs" in sql:
            return [{"id": "123"}]
        if "WITH expected" in sql:
            return [{"differences": 0}]
        if "count(*) AS count" in sql:
            return [{"count": 5}]
        if "bronze.marketplace_events" in sql:
            return bronze
        if "silver.orders" in sql:
            return orders
        if "silver.payments" in sql:
            return payments
        raise AssertionError(sql)

    monkeypatch.setattr(runner, "query", query)
    return {"runId": run_id, "phase": "initial"}, (bronze, orders, payments)


def test_verified_counts_include_acknowledged_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, _ = setup_run(tmp_path, monkeypatch)
    publication = runner.verify(report)
    assert publication["verified"]
    assert publication["counts"] == {
        "acknowledged": 24,
        "bronze": 23,
        "orders": 8,
        "payments": 8,
        "gold": 5,
    }


@pytest.mark.parametrize(
    "corruption", ["missing", "coordinate", "quality", "order", "payment", "timestamp", "sequence"]
)
def test_verification_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    report, (bronze, orders, payments) = setup_run(tmp_path, monkeypatch)
    if corruption == "missing":
        bronze.pop()
    elif corruption == "coordinate":
        bronze[0]["source_offset"] = "9999"
    elif corruption == "quality":
        bronze[0]["quality_flags"] = ["NEGATIVE_AMOUNT"]
    elif corruption == "order":
        orders[0]["amount"] = "0.00"
    elif corruption == "timestamp":
        bronze[0]["occurred_at"] = "2026-01-01T00:00:00Z"
    elif corruption == "sequence":
        bronze[0]["producer_sequence"] = 999
    else:
        payments[0]["amount"] = "0.00"
    with pytest.raises(RuntimeError):
        runner.verify(report)
