from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from marketplace_data.schemas import BRONZE_SCHEMA
from marketplace_data.transforms.orders import (
    conform_order_lines,
    conform_order_status_history,
    conform_orders,
)


def bronze_row(event_id: str, event_type: str, minute: int, offset: int) -> Row:
    occurred = datetime(2026, 1, 1, 0, minute, tzinfo=timezone.utc)
    return Row(
        event_id=event_id,
        event_type=event_type,
        event_version=1,
        occurred_at=occurred,
        produced_at=occurred,
        producer="test",
        trace_id="trace-12345678",
        partition_key="order-1",
        payload={
            "order_id": "order-1",
            "seller_id": "seller-1",
            "customer_token": "token-1",
            "market": "IT",
            "currency": "EUR",
            "gross_amount": "12.34",
            "created_at": "2026-01-01T00:00:00Z",
            "sku": "sku-1",
            "location_id": "loc-1",
            "quantity": "2",
        },
        producer_sequence=offset,
        source_topic="marketplace.orders.v1",
        source_partition=0,
        source_offset=offset,
        source_timestamp=occurred,
        ingested_at=occurred,
        ingestion_run_id="run",
        quality_flags=[],
    )


def test_latest_state_and_history_are_deterministic(spark: SparkSession) -> None:
    frame = spark.createDataFrame(
        [
            bronze_row("event-created", "order.created", 0, 1),
            bronze_row("event-confirmed", "order.confirmed", 1, 2),
            bronze_row("event-confirmed", "order.confirmed", 1, 2),
        ],
        BRONZE_SCHEMA,
    )
    order = conform_orders(frame).first()

    assert order.order_id == "order-1"
    assert order.status == "CONFIRMED"
    assert conform_order_status_history(frame).count() == 2
    line = conform_order_lines(frame).first()
    assert line.sku == "sku-1"
    assert line.quantity == 2
