"""Publish reproducible order/payment histories and record Kafka delivery coordinates."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from marketplace_data.contracts import EventEnvelope


def validate_config(config: dict[str, Any]) -> None:
    for name, low, high in (
        ("orders", 8, 100),
        ("seed", 0, 2**31 - 1),
        ("events_per_second", 1, 1000),
    ):
        value = config.get(name)
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{name} must be an integer between {low} and {high}")


def generate_showcase(config: dict[str, Any], phase: str = "initial") -> list[EventEnvelope]:
    validate_config(config)
    if phase not in ("initial", "advance"):
        raise ValueError("Unknown showcase phase")
    run_id = str(config["run_id"])
    if not re.fullmatch(r"[a-f0-9]{32}", run_id):
        raise ValueError("Invalid showcase run identifier")
    base = datetime.fromisoformat(config["started_at"])
    if base.tzinfo is None:
        raise ValueError("started_at must have a timezone")
    rng = random.Random(config["seed"])  # noqa: S311  # nosec B311
    events: list[EventEnvelope] = []
    for index in range(config["orders"]):
        order_id = f"SC-{run_id}-{index:04d}"
        payment_id = f"PAY-{run_id}-{index:04d}"
        market = ["IT", "DE", "FR", "ES", "GB"][index % 5]
        currency = "GBP" if market == "GB" else "EUR"
        amount = Decimal(rng.randrange(5000, 150000)) / 100
        confirmed = base - timedelta(minutes=150 + index % 30)
        order_payload = {
            "order_id": order_id,
            "seller_id": f"seller-{index % 5:02d}",
            "customer_token": f"showcase-{run_id}-{index:04d}",
            "market": market,
            "currency": currency,
            "gross_amount": f"{amount:.2f}",
            "created_at": confirmed.isoformat(),
        }
        payment_payload = {
            "order_id": order_id,
            "payment_id": payment_id,
            "currency": currency,
            "amount": f"{amount:.2f}",
        }
        kinds = ["order.confirmed", "payment.authorized"]
        if index % 8 != 0:
            kinds.append("payment.captured")
        if phase == "advance":
            if index != 0:
                continue
            kinds = ["payment.captured"]
        for kind in kinds:
            sequence = {"order.confirmed": 1, "payment.authorized": 2, "payment.captured": 3}[kind]
            payload = dict(order_payload if kind.startswith("order.") else payment_payload)
            if kind == "payment.captured" and index % 8 == 1:
                payload["amount"] = f"{amount - 10:.2f}"
            if kind == "payment.captured" and index % 8 == 2:
                payload["currency"] = "USD"
            events.append(
                EventEnvelope(
                    event_id=str(uuid.uuid5(uuid.UUID(run_id), f"{order_id}:{kind}")),
                    event_type=kind,
                    event_version=1,
                    occurred_at=confirmed + timedelta(minutes=sequence - 1),
                    produced_at=base,
                    producer="marketplace-showcase",
                    trace_id=run_id,
                    partition_key=order_id,
                    producer_sequence=sequence,
                    payload=payload,
                )
            )
    if phase == "initial":
        # A transport retry retains its event ID and bytes, unlike a second business payment.
        events.append(events[0])
    return events


def publish(config: dict[str, Any], phase: str, brokers: str) -> dict[str, Any]:
    events = generate_showcase(config, phase)
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            "client.id": "marketplace-showcase",
            "delivery.timeout.ms": 30000,
        }
    )
    deliveries: list[dict[str, Any]] = []
    errors: list[str] = []

    def delivered(error: Any, message: Any, event: EventEnvelope, digest: str) -> None:
        if error:
            errors.append(str(error))
        else:
            deliveries.append(
                {
                    "event": event.model_dump(mode="json"),
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": str(message.offset()),
                    "sha256": digest,
                }
            )

    for event in events:
        value = event.as_json()
        digest = hashlib.sha256(value).hexdigest()
        topic = (
            "marketplace.orders.v1"
            if event.event_type.startswith("order.")
            else "marketplace.payments.v1"
        )
        producer.produce(
            topic,
            key=event.partition_key.encode(),
            value=value,
            on_delivery=partial(delivered, event=event, digest=digest),
        )
        producer.poll(0)
        time.sleep(1 / config["events_per_second"])
    remaining = producer.flush(35)
    if errors or remaining or len(deliveries) != len(events):
        raise RuntimeError(
            f"Kafka deliveries incomplete: {len(deliveries)}/{len(events)}; {errors}"
        )
    return {"deliveries": deliveries, "phase": phase}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--phase", choices=("initial", "advance"), default="initial")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    args = parser.parse_args()
    result = publish(json.loads(args.config.read_text()), args.phase, args.bootstrap_servers)
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
