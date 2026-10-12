"""Publish validated purchase snapshots from JSONL, or an explicitly generated load."""

from __future__ import annotations

import argparse
import random
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from marketplace_data.analytics_contracts import PurchaseSnapshot
from marketplace_data.contracts import EventEnvelope


def generated_purchases(
    count: int, customers: int, seed: int, as_of: datetime
) -> Iterator[PurchaseSnapshot]:
    """Bounded driver memory; repeat buyers, multi-line orders and partial returns."""
    if count < 1 or customers < 1:
        raise ValueError("count and customers must be positive")
    # Seeded, reproducible test data; never used for credentials.
    rng = random.Random(seed)  # noqa: S311  # nosec B311
    # Changing population or observation time defines a different fixture source,
    # not conflicting version-1 content for previously published order identities.
    source_id = uuid.uuid5(
        uuid.NAMESPACE_URL, f"purchase-v1:{seed}:{customers}:{as_of.isoformat()}"
    ).hex
    for index in range(count):
        purchased = as_of - timedelta(days=rng.randrange(365))
        items = [
            {
                "line_id": str(line),
                "product_id": f"product-{rng.randrange(200):04d}",
                "quantity": 2,
                "amount": str(Decimal(rng.randrange(1000, 20000)) / 100),
            }
            for line in range(1, rng.randrange(2, 5))
        ]
        returns = []
        if index % 11 == 0:
            returns = [
                {
                    "return_id": "return-1",
                    "line_id": "1",
                    "quantity": 1,
                    "amount": str((Decimal(str(items[0]["amount"])) / 2).quantize(Decimal("0.01"))),
                    "occurred_at": purchased.isoformat(),
                }
            ]
        yield PurchaseSnapshot.model_validate(
            {
                "source": f"generated-{source_id}",
                "order_id": f"order-{index:010d}",
                "version": 1,
                "customer_id": f"customer-{rng.randrange(customers):08d}",
                "currency": "USD" if index % 5 == 0 else "EUR",
                "country": rng.choice(["IT", "DE", "FR"]),
                "purchased_at": purchased.isoformat(),
                "status": "CANCELLED" if index % 29 == 0 else "CONFIRMED",
                "items": items,
                "returns": returns,
            }
        )


def read_purchases(path: Path) -> Iterator[PurchaseSnapshot]:
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    yield PurchaseSnapshot.model_validate_json(line)
                except ValueError as error:
                    raise ValueError(f"invalid purchase at line {line_number}") from error


def envelope(purchase: PurchaseSnapshot) -> EventEnvelope:
    identity = f"{purchase.source}:{purchase.order_id}:{purchase.version}"
    return EventEnvelope(
        event_id=str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
        event_type="order.snapshot",
        event_version=1,
        occurred_at=purchase.purchased_at,
        produced_at=datetime.now(timezone.utc),
        producer=purchase.source,
        trace_id=uuid.uuid5(uuid.NAMESPACE_OID, identity).hex,
        partition_key=f"{purchase.source}:{purchase.order_id}",
        producer_sequence=purchase.version,
        payload=purchase.payload(),
    )


def publish(purchases: Iterator[PurchaseSnapshot], bootstrap: str, rate: int) -> None:
    if rate < 1:
        raise ValueError("rate must be positive")
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
        }
    )
    failures: list[str] = []

    def delivered(error: Any, message: Any) -> None:
        if error is not None and not failures:
            failures.append(str(error))

    try:
        for purchase in purchases:
            if failures:
                raise RuntimeError(f"Kafka delivery failed: {failures[0]}")
            event = envelope(purchase)
            deadline = time.monotonic() + 30
            while True:
                try:
                    producer.produce(
                        "marketplace.orders.v1",
                        key=event.partition_key.encode(),
                        value=event.as_json(),
                        on_delivery=delivered,
                    )
                    break
                except BufferError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "Kafka producer backpressure exceeded 30 seconds"
                        ) from None
                    producer.poll(0.1)
            producer.poll(0)
            time.sleep(1 / rate)
    finally:
        remaining = producer.flush(30)
    if remaining or failures:
        raise RuntimeError(
            f"Kafka publication incomplete: queued={remaining}, errors={failures[:1]}"
        )


def main(argv: list[str] | None = None) -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    mode = cli.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--input", type=Path, help="JSONL full purchase snapshots; no fixtures substituted"
    )
    mode.add_argument("--generate", type=int, help="Explicitly generated test order count")
    cli.add_argument("--customers", type=int, default=100)
    cli.add_argument("--seed", type=int, default=42)
    cli.add_argument(
        "--as-of",
        type=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
        default=datetime.now(timezone.utc),
    )
    cli.add_argument("--bootstrap-servers", default="kafka:9092")
    cli.add_argument("--rate", type=int, default=500)
    cli.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Skip deterministic generated prefix for incremental load tests",
    )
    args = cli.parse_args(argv)
    if args.as_of.tzinfo is None:
        cli.error("--as-of must include timezone")
    if args.skip < 0 or (args.skip and args.input):
        cli.error("--skip must be nonnegative and is only supported for generated input")
    purchases = (
        read_purchases(args.input)
        if args.input
        else generated_purchases(args.generate, args.customers, args.seed, args.as_of)
    )
    from itertools import islice

    publish(islice(purchases, args.skip, None), args.bootstrap_servers, args.rate)


if __name__ == "__main__":
    main()
