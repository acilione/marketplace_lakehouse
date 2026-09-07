"""Correlated, reproducible five-domain marketplace traffic simulator."""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer

from marketplace_data.contracts import EventEnvelope

TOPICS: Mapping[str, str] = {
    "order": "marketplace.orders.v1",
    "payment": "marketplace.payments.v1",
    "inventory": "marketplace.inventory.v1",
    "shipment": "marketplace.shipments.v1",
    "customer": "marketplace.customers.cdc.v1",
}


@dataclass(frozen=True)
class SimulationProfile:
    orders: int
    events_per_second: int
    duplicate_rate: float
    malformed_rate: float
    late_rate: float
    payment_failure_rate: float
    cancellation_rate: float
    shipment_exception_rate: float


PROFILES: Mapping[str, SimulationProfile] = {
    "smoke": SimulationProfile(100, 250, 0.005, 0.002, 0.01, 0.03, 0.05, 0.02),
    "steady": SimulationProfile(5_000, 1_000, 0.005, 0.002, 0.01, 0.03, 0.05, 0.02),
    "peak": SimulationProfile(15_000, 2_000, 0.01, 0.005, 0.02, 0.04, 0.08, 0.04),
    "chaos": SimulationProfile(5_000, 1_500, 0.03, 0.02, 0.10, 0.10, 0.15, 0.10),
}


@dataclass(frozen=True)
class SimulationScenario:
    profile: str
    orders: int
    seed: int
    events_per_second: int
    duplicate_rate: float
    malformed_rate: float
    late_rate: float
    payment_failure_rate: float
    cancellation_rate: float
    shipment_exception_rate: float
    start_time: datetime
    late_by_hours: int = 30
    customer_pool_size: int = 1_000
    simulation_id: str | None = None

    def __post_init__(self) -> None:
        if self.orders < 1 or self.events_per_second < 1 or self.customer_pool_size < 1:
            raise ValueError("orders, events_per_second, and customer_pool_size must be positive")
        rates = (
            self.duplicate_rate,
            self.malformed_rate,
            self.late_rate,
            self.payment_failure_rate,
            self.cancellation_rate,
            self.shipment_exception_rate,
        )
        if any(not 0 <= value <= 1 for value in rates):
            raise ValueError("simulation rates must be between zero and one")
        if self.payment_failure_rate + self.cancellation_rate > 1:
            raise ValueError("payment failure and cancellation rates cannot sum above one")
        if self.start_time.tzinfo is None or self.start_time.utcoffset() != timedelta(0):
            raise ValueError("start_time must be timezone-aware UTC")
        if self.late_by_hours < 1:
            raise ValueError("late_by_hours must be positive")


@dataclass(frozen=True)
class SimulationRecord:
    topic: str
    key: bytes
    value: bytes
    event_type: str
    malformed: bool = False
    duplicate: bool = False
    late: bool = False


@dataclass(frozen=True)
class SimulationReport:
    simulation_id: str
    profile: str
    seed: int
    requested_orders: int
    configured_events_per_second: int
    started_at: str
    ended_at: str
    runtime_seconds: float
    logical_events: int
    attempted_messages: int
    delivered_messages: int
    failed_messages: int
    duplicate_messages: int
    malformed_messages: int
    late_messages: int
    achieved_events_per_second: float
    delivery_latency_ms_p50: float | None
    delivery_latency_ms_p95: float | None
    delivery_latency_ms_max: float | None
    queue_full_retries: int
    attempted_by_topic: dict[str, int]
    delivered_by_topic: dict[str, int]
    logical_by_event_type: dict[str, int]
    delivery_errors: dict[str, int]
    expected_accepted_unique: int
    expected_late_unique: int


def scenario_from_profile(
    profile_name: str,
    *,
    seed: int,
    start_time: datetime,
    orders: int | None = None,
    events_per_second: int | None = None,
    duplicate_rate: float | None = None,
    malformed_rate: float | None = None,
    late_rate: float | None = None,
) -> SimulationScenario:
    try:
        profile = PROFILES[profile_name]
    except KeyError as error:
        raise ValueError(f"unknown simulation profile: {profile_name}") from error
    return SimulationScenario(
        profile=profile_name,
        orders=orders if orders is not None else profile.orders,
        seed=seed,
        events_per_second=(
            events_per_second if events_per_second is not None else profile.events_per_second
        ),
        duplicate_rate=duplicate_rate if duplicate_rate is not None else profile.duplicate_rate,
        malformed_rate=malformed_rate if malformed_rate is not None else profile.malformed_rate,
        late_rate=late_rate if late_rate is not None else profile.late_rate,
        payment_failure_rate=profile.payment_failure_rate,
        cancellation_rate=profile.cancellation_rate,
        shipment_exception_rate=profile.shipment_exception_rate,
        start_time=start_time,
        customer_pool_size=min(1_000, max(10, (orders or profile.orders) // 5)),
    )


def _identifier(scenario: SimulationScenario, kind: str, value: str) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"marketplace-simulation:{scenario.simulation_id or scenario.seed}:{kind}:{value}",
        )
    )


def _event_record(
    scenario: SimulationScenario,
    rng: random.Random,
    *,
    event_type: str,
    entity_id: str,
    step: str,
    produced_at: datetime,
    payload: dict[str, str],
    sequence: int,
) -> SimulationRecord:
    domain = event_type.split(".", maxsplit=1)[0]
    late = rng.random() < scenario.late_rate
    occurred_at = produced_at - timedelta(hours=scenario.late_by_hours) if late else produced_at
    envelope = EventEnvelope(
        event_id=_identifier(scenario, "event", f"{entity_id}:{step}"),
        event_type=event_type,
        event_version=1,
        occurred_at=occurred_at,
        produced_at=produced_at,
        producer="marketplace-domain-simulator",
        trace_id=_identifier(scenario, "trace", entity_id).replace("-", ""),
        partition_key=entity_id,
        payload={
            **payload,
            "simulation_id": scenario.simulation_id
            or f"simulation-{scenario.profile}-{scenario.seed}",
        },
        producer_sequence=sequence,
    )
    return SimulationRecord(
        topic=TOPICS[domain],
        key=entity_id.encode("utf-8"),
        value=envelope.as_json(),
        event_type=event_type,
        late=late,
    )


def _workflow_records(
    scenario: SimulationScenario,
    rng: random.Random,
    index: int,
    initialized_inventory: set[tuple[str, str]],
) -> list[SimulationRecord]:
    # A workflow averages roughly nine records. Advancing business time at the configured
    # message rate keeps event time aligned with wall time even for long peak scenarios.
    base = scenario.start_time + timedelta(seconds=(index * 9) / scenario.events_per_second)
    scope = scenario.simulation_id or f"{scenario.seed:010d}"
    order_id = f"order-{scope}-{index:010d}"
    payment_id = f"payment-{scope}-{index:010d}"
    shipment_id = f"shipment-{scope}-{index:010d}"
    customer_number = index % scenario.customer_pool_size
    customer_id = f"customer-{scope}-{customer_number:08d}"
    customer_token = f"tok-{scope}-{customer_number:08d}"
    sku = f"sku-{scope}-{rng.randrange(1_000):06d}"
    location_id = f"loc-{rng.randrange(20):03d}"
    seller_id = f"seller-{rng.randrange(100):04d}"
    market = rng.choice(["IT", "DE", "FR", "ES"])
    amount = Decimal(rng.randrange(100, 100_000)) / Decimal(100)
    quantity = rng.randrange(1, 5)
    sequence_base = index * 20

    def event(
        event_type: str,
        entity_id: str,
        step: str,
        seconds: int,
        payload: dict[str, str],
    ) -> SimulationRecord:
        return _event_record(
            scenario,
            rng,
            event_type=event_type,
            entity_id=entity_id,
            step=step,
            produced_at=base + timedelta(milliseconds=seconds * 100),
            payload=payload,
            sequence=sequence_base + seconds,
        )

    records = [
        event(
            "customer.changed",
            customer_id,
            f"customer-{index}",
            0,
            {
                "customer_id": customer_id,
                "email_token": customer_token,
                "country": market,
                "customer_tier": rng.choice(["STANDARD", "PLUS", "PREMIUM"]),
                "op": "c" if index < scenario.customer_pool_size else "u",
                "source_lsn": str(sequence_base),
            },
        )
    ]
    inventory_key = (sku, location_id)
    if inventory_key not in initialized_inventory:
        initialized_inventory.add(inventory_key)
        records.append(
            event(
                "inventory.adjusted",
                f"{sku}:{location_id}",
                "initial-stock",
                0,
                {"sku": sku, "location_id": location_id, "quantity_delta": "100"},
            )
        )
    records.extend(
        [
            event(
                "order.created",
                order_id,
                "created",
                1,
                {
                    "order_id": order_id,
                    "seller_id": seller_id,
                    "customer_token": customer_token,
                    "currency": "EUR",
                    "gross_amount": str(amount),
                    "market": market,
                    "created_at": (base + timedelta(milliseconds=100)).isoformat(),
                    "sku": sku,
                    "location_id": location_id,
                    "quantity": str(quantity),
                },
            ),
            event(
                "inventory.reserved",
                f"{sku}:{location_id}",
                f"reserved-{order_id}",
                2,
                {
                    "sku": sku,
                    "location_id": location_id,
                    "quantity": str(quantity),
                    "order_id": order_id,
                },
            ),
            event(
                "payment.authorized",
                payment_id,
                "authorized",
                3,
                {
                    "payment_id": payment_id,
                    "order_id": order_id,
                    "currency": "EUR",
                    "amount": str(amount),
                },
            ),
        ]
    )

    outcome = rng.random()
    if outcome < scenario.payment_failure_rate:
        records.extend(
            [
                event(
                    "payment.failed",
                    payment_id,
                    "failed",
                    4,
                    {
                        "payment_id": payment_id,
                        "order_id": order_id,
                        "currency": "EUR",
                        "amount": str(amount),
                    },
                ),
                event(
                    "order.cancelled",
                    order_id,
                    "payment-failed",
                    5,
                    {
                        "order_id": order_id,
                        "seller_id": seller_id,
                        "customer_token": customer_token,
                        "currency": "EUR",
                        "gross_amount": str(amount),
                        "market": market,
                        "created_at": (base + timedelta(milliseconds=100)).isoformat(),
                    },
                ),
                event(
                    "inventory.released",
                    f"{sku}:{location_id}",
                    f"payment-failed-{order_id}",
                    6,
                    {
                        "sku": sku,
                        "location_id": location_id,
                        "quantity": str(quantity),
                        "order_id": order_id,
                    },
                ),
            ]
        )
    elif outcome < scenario.payment_failure_rate + scenario.cancellation_rate:
        records.extend(
            [
                event(
                    "payment.captured",
                    payment_id,
                    "captured",
                    4,
                    {
                        "payment_id": payment_id,
                        "order_id": order_id,
                        "currency": "EUR",
                        "amount": str(amount),
                    },
                ),
                event(
                    "order.confirmed",
                    order_id,
                    "confirmed",
                    5,
                    {
                        "order_id": order_id,
                        "seller_id": seller_id,
                        "customer_token": customer_token,
                        "currency": "EUR",
                        "gross_amount": str(amount),
                        "market": market,
                        "created_at": (base + timedelta(milliseconds=100)).isoformat(),
                    },
                ),
                event(
                    "order.cancelled",
                    order_id,
                    "cancelled",
                    6,
                    {
                        "order_id": order_id,
                        "seller_id": seller_id,
                        "customer_token": customer_token,
                        "currency": "EUR",
                        "gross_amount": str(amount),
                        "market": market,
                        "created_at": (base + timedelta(milliseconds=100)).isoformat(),
                    },
                ),
                event(
                    "payment.reversed",
                    payment_id,
                    "reversed",
                    7,
                    {
                        "payment_id": payment_id,
                        "order_id": order_id,
                        "currency": "EUR",
                        "amount": str(amount),
                    },
                ),
                event(
                    "inventory.released",
                    f"{sku}:{location_id}",
                    f"cancelled-{order_id}",
                    8,
                    {
                        "sku": sku,
                        "location_id": location_id,
                        "quantity": str(quantity),
                        "order_id": order_id,
                    },
                ),
            ]
        )
    else:
        shipment_terminal = (
            "shipment.exception"
            if rng.random() < scenario.shipment_exception_rate
            else "shipment.delivered"
        )
        records.extend(
            [
                event(
                    "payment.captured",
                    payment_id,
                    "captured",
                    4,
                    {
                        "payment_id": payment_id,
                        "order_id": order_id,
                        "currency": "EUR",
                        "amount": str(amount),
                    },
                ),
                event(
                    "order.confirmed",
                    order_id,
                    "confirmed",
                    5,
                    {
                        "order_id": order_id,
                        "seller_id": seller_id,
                        "customer_token": customer_token,
                        "currency": "EUR",
                        "gross_amount": str(amount),
                        "market": market,
                        "created_at": (base + timedelta(seconds=1)).isoformat(),
                    },
                ),
                event(
                    "shipment.created",
                    shipment_id,
                    "created",
                    6,
                    {
                        "shipment_id": shipment_id,
                        "order_id": order_id,
                        "promised_at": (base + timedelta(days=2)).isoformat(),
                    },
                ),
                event(
                    "shipment.dispatched",
                    shipment_id,
                    "dispatched",
                    7,
                    {
                        "shipment_id": shipment_id,
                        "order_id": order_id,
                        "promised_at": (base + timedelta(days=2)).isoformat(),
                    },
                ),
                event(
                    shipment_terminal,
                    shipment_id,
                    "terminal",
                    8,
                    {
                        "shipment_id": shipment_id,
                        "order_id": order_id,
                        "promised_at": (base + timedelta(days=2)).isoformat(),
                    },
                ),
            ]
        )
    return records


def generate_records(scenario: SimulationScenario) -> Iterator[SimulationRecord]:
    """Yield correlated records without retaining the complete scenario in memory."""
    # Deterministic synthetic replay, never credential generation.
    rng = random.Random(scenario.seed)  # noqa: S311  # nosec B311
    initialized_inventory: set[tuple[str, str]] = set()
    for index in range(scenario.orders):
        for record in _workflow_records(scenario, rng, index, initialized_inventory):
            malformed = rng.random() < scenario.malformed_rate
            emitted = (
                replace(
                    record,
                    value=(
                        json.dumps(
                            {
                                "simulation_id": scenario.simulation_id
                                or f"simulation-{scenario.profile}-{scenario.seed}",
                                "event_id": "malformed",
                            }
                        )[:-1]
                    ).encode(),
                    malformed=True,
                )
                if malformed
                else record
            )
            yield emitted
            if rng.random() < scenario.duplicate_rate:
                yield replace(emitted, duplicate=True)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * percentile)))
    return round(ordered[index], 3)


def publish(
    bootstrap_servers: str,
    scenario: SimulationScenario,
    *,
    progress_every: int = 10_000,
) -> SimulationReport:
    simulation_id = scenario.simulation_id or f"simulation-{scenario.profile}-{scenario.seed}"
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "client.id": simulation_id,
            "linger.ms": 5,
        }
    )
    attempted_by_topic: Counter[str] = Counter()
    delivered_by_topic: Counter[str] = Counter()
    logical_by_event_type: Counter[str] = Counter()
    delivery_errors: Counter[str] = Counter()
    delivery_latencies: list[float] = []
    logical_events = duplicate_messages = malformed_messages = late_messages = 0
    delivered_messages = failed_messages = queue_full_retries = 0
    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    attempted_messages = 0
    expected_accepted_unique = expected_late_unique = 0

    def delivery_callback(
        error: KafkaError | None,
        message: Message,
        enqueued_at: float,
    ) -> None:
        nonlocal delivered_messages, failed_messages
        if error is not None:
            failed_messages += 1
            delivery_errors[str(error)] += 1
            return
        delivered_messages += 1
        delivered_by_topic[message.topic() or "unknown"] += 1
        delivery_latencies.append((time.monotonic() - enqueued_at) * 1_000)

    for record in generate_records(scenario):
        if not record.duplicate:
            if not record.malformed:
                expected_accepted_unique += 1
                expected_late_unique += int(record.late)
            logical_events += 1
            logical_by_event_type[record.event_type] += 1
        else:
            duplicate_messages += 1
        malformed_messages += int(record.malformed)
        late_messages += int(record.late)
        enqueued_at = time.monotonic()

        def on_delivery(
            error: KafkaError | None,
            message: Message,
            at: float = enqueued_at,
        ) -> None:
            delivery_callback(error, message, at)

        while True:
            try:
                producer.produce(
                    record.topic,
                    key=record.key,
                    value=record.value,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                queue_full_retries += 1
                producer.poll(0.05)
        attempted_messages += 1
        attempted_by_topic[record.topic] += 1
        producer.poll(0)

        target_time = started + (attempted_messages / scenario.events_per_second)
        remaining = target_time - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        if progress_every > 0 and attempted_messages % progress_every == 0:
            elapsed = max(time.monotonic() - started, 0.001)
            sys.stderr.write(
                f"published={attempted_messages} rate={attempted_messages / elapsed:.1f} events/s\n"
            )

    undelivered = producer.flush(30)
    if undelivered:
        failed_messages += undelivered
        delivery_errors["FLUSH_TIMEOUT"] += undelivered
    runtime = max(time.monotonic() - started, 0.000_001)
    ended_at = datetime.now(timezone.utc)
    return SimulationReport(
        simulation_id=simulation_id,
        expected_accepted_unique=expected_accepted_unique,
        expected_late_unique=expected_late_unique,
        profile=scenario.profile,
        seed=scenario.seed,
        requested_orders=scenario.orders,
        configured_events_per_second=scenario.events_per_second,
        started_at=started_at.isoformat(),
        ended_at=ended_at.isoformat(),
        runtime_seconds=round(runtime, 6),
        logical_events=logical_events,
        attempted_messages=attempted_messages,
        delivered_messages=delivered_messages,
        failed_messages=failed_messages,
        duplicate_messages=duplicate_messages,
        malformed_messages=malformed_messages,
        late_messages=late_messages,
        achieved_events_per_second=round(delivered_messages / runtime, 3),
        delivery_latency_ms_p50=_percentile(delivery_latencies, 0.50),
        delivery_latency_ms_p95=_percentile(delivery_latencies, 0.95),
        delivery_latency_ms_max=(round(max(delivery_latencies), 3) if delivery_latencies else None),
        queue_full_retries=queue_full_retries,
        attempted_by_topic=dict(sorted(attempted_by_topic.items())),
        delivered_by_topic=dict(sorted(delivered_by_topic.items())),
        logical_by_event_type=dict(sorted(logical_by_event_type.items())),
        delivery_errors=dict(sorted(delivery_errors.items())),
    )


def write_report(report: SimulationReport, destination: str) -> None:
    document = json.dumps(asdict(report), indent=2, sort_keys=True)
    if destination == "-":
        sys.stdout.write(f"{document}\n")
        return
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{document}\n", encoding="utf-8")


def _utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("start time must be an ISO-8601 UTC timestamp")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="smoke")
    parser.add_argument("--orders", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--events-per-second", type=int)
    parser.add_argument("--duplicate-rate", type=float)
    parser.add_argument("--malformed-rate", type=float)
    parser.add_argument("--late-rate", type=float)
    parser.add_argument("--start-time", type=_utc_timestamp)
    parser.add_argument("--progress-every", type=int, default=10_000)
    parser.add_argument("--report", default="-")
    parser.add_argument("--simulation-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    scenario = scenario_from_profile(
        args.profile,
        seed=args.seed,
        start_time=args.start_time or datetime.now(timezone.utc),
        orders=args.orders,
        events_per_second=args.events_per_second,
        duplicate_rate=args.duplicate_rate,
        malformed_rate=args.malformed_rate,
        late_rate=args.late_rate,
    )
    scenario = replace(scenario, simulation_id=args.simulation_id)
    report = publish(args.bootstrap_servers, scenario, progress_every=args.progress_every)
    write_report(report, args.report)
    if report.failed_messages:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
