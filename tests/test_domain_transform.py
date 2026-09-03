from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from marketplace_data.schemas import BRONZE_SCHEMA
from marketplace_data.transforms.domains import conform_payments, conform_shipments, inventory_daily


def event(event_id: str, event_type: str, day: int, offset: int, payload: dict[str, str]) -> Row:
    occurred = datetime(2026, 1, day, tzinfo=timezone.utc)
    return Row(
        event_id=event_id,
        event_type=event_type,
        event_version=1,
        occurred_at=occurred,
        produced_at=occurred,
        producer="test",
        trace_id=f"trace-{event_id}",
        partition_key=next(iter(payload.values())),
        payload=payload,
        producer_sequence=offset,
        source_topic="domain",
        source_partition=0,
        source_offset=offset,
        source_timestamp=occurred,
        ingested_at=occurred,
        ingestion_run_id="run",
        quality_flags=[],
    )


def test_payment_shipment_and_inventory_conformance(spark: SparkSession) -> None:
    rows = [
        event(
            "p1",
            "payment.authorized",
            1,
            1,
            {"payment_id": "pay-1", "order_id": "o1", "currency": "EUR", "amount": "10"},
        ),
        event(
            "p2",
            "payment.captured",
            2,
            2,
            {"payment_id": "pay-1", "order_id": "o1", "currency": "EUR", "amount": "10"},
        ),
        event(
            "s1",
            "shipment.delivered",
            2,
            3,
            {"shipment_id": "ship-1", "order_id": "o1", "promised_at": "2026-01-03T00:00:00Z"},
        ),
        event(
            "i1",
            "inventory.adjusted",
            1,
            4,
            {"sku": "sku-1", "location_id": "loc-1", "quantity_delta": "10"},
        ),
        event(
            "i2",
            "inventory.reserved",
            2,
            5,
            {"sku": "sku-1", "location_id": "loc-1", "quantity": "3"},
        ),
    ]
    bronze = spark.createDataFrame(rows, BRONZE_SCHEMA)

    assert conform_payments(bronze).first().status == "CAPTURED"
    shipment = conform_shipments(bronze).first()
    assert shipment.status == "DELIVERED"
    assert shipment.delivered_at is not None
    inventory = inventory_daily(bronze).orderBy("date").collect()
    assert [row.available_units for row in inventory] == [10, 7]
    assert not inventory[-1].is_stockout
