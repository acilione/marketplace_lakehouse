from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastavro import validate
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from marketplace_data.contracts import CONTRACTS_ROOT, EventEnvelope, load_avro_schema


def envelope(**overrides):  # type: ignore[no-untyped-def]
    values = {
        "event_id": "01JABCDEFGHIJKLMNOPQRSTUVWXY",
        "event_type": "order.created",
        "event_version": 1,
        "occurred_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "produced_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "producer": "test",
        "trace_id": "trace-12345678",
        "partition_key": "order-1",
        "payload": {"order_id": "order-1", "gross_amount": "10.20"},
    }
    values.update(overrides)
    return EventEnvelope(**values)


def test_event_contract_round_trip() -> None:
    event = envelope()
    assert EventEnvelope.model_validate_json(event.as_json()) == event


def test_floating_point_money_is_rejected() -> None:
    with pytest.raises(ValidationError, match="decimal string"):
        envelope(payload={"order_id": "order-1", "gross_amount": 10.2})


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        envelope(occurred_at=datetime(2026, 1, 1))  # noqa: DTZ001 - intentional invalid input


def test_unknown_domain_and_contract_path_are_rejected() -> None:
    with pytest.raises(ValidationError, match="unsupported event domain"):
        envelope(event_type="unknown.created")
    with pytest.raises(ValueError, match="unknown contract"):
        load_avro_schema("../README.md")


def test_avro_contract_is_valid() -> None:
    schema = load_avro_schema()
    event = envelope()
    record = event.model_dump()
    record["occurred_at"] = event.occurred_at
    record["produced_at"] = event.produced_at
    assert validate(record, schema)


def test_json_payload_contract_accepts_fixed_precision_money() -> None:
    schema = json.loads((CONTRACTS_ROOT / "order-event-v1.schema.json").read_text())
    payload = {
        "order_id": "order-1",
        "seller_id": "seller-1",
        "customer_token": "token-1",
        "currency": "EUR",
        "gross_amount": "123.45",
        "market": "IT",
    }
    assert not list(Draft202012Validator(schema).iter_errors(payload))
