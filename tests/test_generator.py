from __future__ import annotations

import pytest

from marketplace_data.contracts import EventEnvelope
from marketplace_data.generator import Scenario, generate_events, main, publish


def test_generator_is_reproducible_and_reuses_duplicate_id() -> None:
    scenario = Scenario(count=5, seed=7, duplicate_rate=1, malformed_rate=0, late_rate=0)
    first = generate_events(scenario)
    second = generate_events(scenario)

    assert [item.event_id for item in first if isinstance(item, EventEnvelope)] == [
        item.event_id for item in second if isinstance(item, EventEnvelope)
    ]
    assert first[0] == first[1]


@pytest.mark.parametrize("rate", [-0.1, 1.1])
def test_generator_rejects_invalid_fault_rates(rate: float) -> None:
    with pytest.raises(ValueError, match="fault rates"):
        Scenario(duplicate_rate=rate)


def test_stdout_mode_emits_requested_event(capsys: pytest.CaptureFixture[str]) -> None:
    main(["--count", "1", "--stdout"])
    assert len(capsys.readouterr().out.splitlines()) == 1


def test_publish_uses_idempotent_producer(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, bytes, bytes]] = []

    class FakeProducer:
        def __init__(self, settings):  # type: ignore[no-untyped-def]
            assert settings["enable.idempotence"] is True

        def produce(self, topic: str, *, key: bytes, value: bytes) -> None:
            calls.append((topic, key, value))

        def poll(self, timeout: int) -> None:
            assert timeout == 0

        def flush(self, timeout: int) -> None:
            assert timeout == 30

    monkeypatch.setattr("marketplace_data.generator.Producer", FakeProducer)
    monkeypatch.setattr("marketplace_data.generator.time.sleep", lambda _: None)
    publish("kafka:9092", Scenario(count=2, events_per_second=1000), "orders")
    assert len(calls) == 2
