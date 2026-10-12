"""The showcase feeds real pipeline inputs; fixtures here test only its producer."""

from collections import Counter
from typing import Any
from unittest.mock import MagicMock

import pytest

from marketplace_data.showcase import generate_showcase, publish, validate_config


def config() -> dict[str, Any]:
    return {
        "orders": 32,
        "seed": 7,
        "events_per_second": 1000,
        "run_id": "a" * 32,
        "started_at": "2026-10-12T10:00:00+00:00",
    }


def test_showcase_is_reproducible_and_covers_each_discrepancy() -> None:
    events = generate_showcase(config())
    assert events == generate_showcase(config())
    assert len(events) == 93
    assert len({event.event_id for event in events}) == 92
    assert events[0].as_json() == events[-1].as_json()
    counts = Counter(event.event_type for event in events[:-1])
    assert counts == {"order.confirmed": 32, "payment.authorized": 32, "payment.captured": 28}
    captures = [event for event in events if event.event_type == "payment.captured"]
    assert sum(event.payload["currency"] == "USD" for event in captures) == 4
    assert all(event.produced_at >= event.occurred_at for event in events)


def test_delayed_capture_has_stable_identity_and_targets_one_missing_payment() -> None:
    initial = generate_showcase(config())
    advance = generate_showcase(config(), "advance")
    assert len(advance) == 1
    assert advance[0].payload["order_id"] == initial[0].payload["order_id"]
    assert advance[0].event_id not in {event.event_id for event in initial}
    assert generate_showcase(config(), "advance") == advance


@pytest.mark.parametrize(
    "field,value",
    [
        ("orders", 0),
        ("orders", 101),
        ("orders", True),
        ("seed", -1),
        ("events_per_second", 0),
        ("events_per_second", "20"),
    ],
)
def test_config_bounds(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        validate_config({**config(), field: value})


def test_invalid_identity_or_phase_is_rejected() -> None:
    with pytest.raises(ValueError):
        generate_showcase({**config(), "run_id": "../escape"})
    with pytest.raises(ValueError):
        generate_showcase(config(), "invalid")


def test_publish_requires_every_kafka_acknowledgement(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = MagicMock()
    producer.flush.return_value = 0
    monkeypatch.setattr("marketplace_data.showcase.Producer", lambda _: producer)
    monkeypatch.setattr("marketplace_data.showcase.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="incomplete"):
        publish(config(), "initial", "kafka:9092")

    def acknowledge(topic: str, **kwargs: Any) -> None:
        message = MagicMock()
        message.topic.return_value = topic
        message.partition.return_value = 3
        message.offset.return_value = 9007199254740993
        kwargs["on_delivery"](None, message)

    producer.produce.side_effect = acknowledge
    receipts = publish(config(), "initial", "kafka:9092")["deliveries"]
    assert len(receipts) == 93
    assert receipts[0]["offset"] == "9007199254740993"
    assert receipts[0]["partition"] == 3
    assert len(receipts[0]["sha256"]) == 64
