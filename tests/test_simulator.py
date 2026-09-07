from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from marketplace_data.simulator import (
    TOPICS,
    SimulationScenario,
    generate_records,
    publish,
    scenario_from_profile,
)


def scenario(**overrides: object) -> SimulationScenario:
    baseline = scenario_from_profile(
        "smoke",
        seed=17,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        orders=2,
        events_per_second=10_000,
        duplicate_rate=0,
        malformed_rate=0,
        late_rate=0,
    )
    values = {
        "payment_failure_rate": 0,
        "cancellation_rate": 0,
        "shipment_exception_rate": 0,
        **overrides,
    }
    return replace(baseline, **values)


def decoded_records(simulation: SimulationScenario) -> list[dict[str, object]]:
    return [json.loads(record.value) for record in generate_records(simulation)]


def test_simulator_populates_every_domain_with_correlated_events() -> None:
    records = list(generate_records(scenario(orders=1)))
    assert {record.topic for record in records} == set(TOPICS.values())

    decoded = [json.loads(record.value) for record in records]
    created = next(event for event in decoded if event["event_type"] == "order.created")
    order_id = created["payload"]["order_id"]
    related = [
        event for event in decoded if event["event_type"].startswith(("payment.", "shipment."))
    ]
    assert related
    assert all(event["payload"]["order_id"] == order_id for event in related)


def test_simulator_is_reproducible_for_a_fixed_scenario() -> None:
    simulation = scenario(orders=3)
    first = [(record.topic, record.key, record.value) for record in generate_records(simulation)]
    second = [(record.topic, record.key, record.value) for record in generate_records(simulation)]
    assert first == second


def test_fault_injection_marks_malformed_late_duplicates() -> None:
    records = list(
        generate_records(scenario(orders=1, duplicate_rate=1, malformed_rate=1, late_rate=1))
    )
    assert len(records) % 2 == 0
    assert all(record.malformed and record.late for record in records)
    assert sum(record.duplicate for record in records) == len(records) // 2
    for record in records:
        assert b'"simulation_id"' in record.value
        with pytest.raises(json.JSONDecodeError):
            json.loads(record.value)


def test_distinct_runs_with_same_seed_do_not_reuse_entity_or_event_ids() -> None:
    first = decoded_records(scenario(simulation_id="run1"))
    second = decoded_records(scenario(simulation_id="run2"))
    assert {item["event_id"] for item in first}.isdisjoint(item["event_id"] for item in second)
    assert {item["partition_key"] for item in first}.isdisjoint(
        item["partition_key"] for item in second
    )


def test_scenario_rejects_invalid_outcome_rates() -> None:
    with pytest.raises(ValueError, match="cannot sum above one"):
        scenario(payment_failure_rate=0.6, cancellation_rate=0.5)


def test_publish_reports_delivery_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeMessage:
        def __init__(self, topic: str) -> None:
            self._topic = topic

        def topic(self) -> str:
            return self._topic

    class FakeProducer:
        def __init__(self, settings: dict[str, object]) -> None:
            assert settings["enable.idempotence"] is True

        def produce(self, topic: str, **kwargs: object) -> None:
            callback = kwargs["on_delivery"]
            assert callable(callback)
            callback(None, FakeMessage(topic))

        def poll(self, timeout: float) -> None:
            assert timeout >= 0

        def flush(self, timeout: int) -> int:
            assert timeout == 30
            return 0

    monkeypatch.setattr("marketplace_data.simulator.Producer", FakeProducer)
    monkeypatch.setattr("marketplace_data.simulator.time.sleep", lambda _: None)
    report = publish("kafka:9092", scenario(orders=1), progress_every=0)

    assert report.failed_messages == 0
    assert report.delivered_messages == report.attempted_messages
    assert set(report.delivered_by_topic) == set(TOPICS.values())
    assert report.delivery_latency_ms_p95 is not None
