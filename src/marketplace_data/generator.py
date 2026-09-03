"""Deterministic synthetic marketplace event generator and Kafka producer."""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Producer

from marketplace_data.contracts import EventEnvelope


@dataclass(frozen=True)
class Scenario:
    count: int = 1_000
    seed: int = 42
    duplicate_rate: float = 0.005
    malformed_rate: float = 0.002
    late_rate: float = 0.01
    events_per_second: int = 500
    start_time: datetime = field(default_factory=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc))

    def __post_init__(self) -> None:
        if self.count < 1 or self.events_per_second < 1:
            raise ValueError("count and events_per_second must be positive")
        for value in (self.duplicate_rate, self.malformed_rate, self.late_rate):
            if not 0 <= value <= 1:
                raise ValueError("fault rates must be between zero and one")


def generate_events(scenario: Scenario) -> list[EventEnvelope | bytes]:
    # Determinism is the security property for synthetic replay; this is not used for credentials.
    rng = random.Random(scenario.seed)  # noqa: S311  # nosec B311
    now = scenario.start_time
    events: list[EventEnvelope | bytes] = []
    valid: list[EventEnvelope] = []
    for index in range(scenario.count):
        if rng.random() < scenario.malformed_rate:
            events.append(b'{"event_id":"truncated"')
            continue
        order_id = f"order-{index:010d}"
        occurred_at = now - timedelta(minutes=30 if rng.random() < scenario.late_rate else 0)
        amount = Decimal(rng.randrange(100, 100_000)) / Decimal(100)
        event = EventEnvelope(
            event_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{scenario.seed}:{index}")),
            event_type="order.created",
            event_version=1,
            occurred_at=occurred_at,
            produced_at=now,
            producer="synthetic-generator",
            trace_id=uuid.uuid5(uuid.NAMESPACE_OID, f"trace:{scenario.seed}:{index}").hex,
            partition_key=order_id,
            producer_sequence=index,
            payload={
                "order_id": order_id,
                "seller_id": f"seller-{rng.randrange(100):04d}",
                "customer_token": f"tok-{rng.randrange(10_000):08d}",
                "currency": "EUR",
                "gross_amount": str(amount),
                "market": rng.choice(["IT", "DE", "FR", "ES"]),
                "created_at": occurred_at.isoformat(),
                "sku": f"sku-{rng.randrange(1_000):06d}",
                "location_id": f"loc-{rng.randrange(20):03d}",
                "quantity": str(rng.randrange(1, 5)),
            },
        )
        valid.append(event)
        events.append(event)
        if rng.random() < scenario.duplicate_rate:
            events.append(event)
    return events


def publish(bootstrap_servers: str, scenario: Scenario, topic: str) -> None:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "client.id": "marketplace-synthetic-generator",
        }
    )
    interval = 1 / scenario.events_per_second
    for item in generate_events(scenario):
        started = time.monotonic()
        if isinstance(item, bytes):
            value, key = item, b"malformed"
        else:
            value, key = item.as_json(), item.partition_key.encode()
        producer.produce(topic, key=key, value=value)
        producer.poll(0)
        remaining = interval - (time.monotonic() - started)
        if remaining > 0:
            time.sleep(remaining)
    producer.flush(30)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--topic", default="marketplace.orders.v1")
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duplicate-rate", type=float, default=0.005)
    parser.add_argument("--malformed-rate", type=float, default=0.002)
    parser.add_argument("--late-rate", type=float, default=0.01)
    parser.add_argument("--events-per-second", type=int, default=500)
    parser.add_argument("--stdout", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    scenario = Scenario(
        count=args.count,
        seed=args.seed,
        duplicate_rate=args.duplicate_rate,
        malformed_rate=args.malformed_rate,
        late_rate=args.late_rate,
        events_per_second=args.events_per_second,
    )
    if args.stdout:
        for event in generate_events(scenario):
            value = (
                event.decode(errors="replace")
                if isinstance(event, bytes)
                else event.model_dump_json()
            )
            print(json.dumps({"value": value}))
        return
    publish(args.bootstrap_servers, scenario, args.topic)


if __name__ == "__main__":
    main()
