from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from marketplace_data.analytics_contracts import PurchaseSnapshot
from marketplace_data.jobs.customer_analytics import validate_purchases
from marketplace_data.schemas import BRONZE_SCHEMA
from marketplace_data.transforms.analytics import (
    customer_cohorts,
    customer_daily,
    customer_profiles,
    latest_purchases,
    product_daily,
    purchase_changes,
    purchase_items,
)


def test_cohorts_include_zero_periods_and_exclude_future(spark: SparkSession) -> None:
    purchases = [
        snapshot(returns=[], purchased_at="2026-01-01T12:00:00Z"),
        snapshot(returns=[], order_id="repeat", purchased_at="2026-03-01T12:00:00Z"),
        snapshot(
            returns=[], order_id="other", customer_id="other", purchased_at="2026-01-10T12:00:00Z"
        ),
    ]
    daily = customer_daily(
        purchase_items(latest_purchases(purchase_changes(bronze(spark, *purchases))))
    )
    cells = customer_cohorts(daily, date(2026, 3, 12)).orderBy("month_number").collect()
    assert [cell.active_customers for cell in cells] == [2, 0, 1]
    assert [cell.cohort_size for cell in cells] == [2, 2, 2]
    assert cells[2].retention == Decimal("0.5")
    assert [cell.complete for cell in cells] == [True, True, False]


def test_quarantine_excludes_ambiguous_order_but_not_healthy_orders(spark: SparkSession) -> None:
    from marketplace_data.jobs.customer_analytics import rejected_purchases

    changes = purchase_changes(
        bronze(spark, snapshot(), snapshot(customer_id="conflict"), snapshot(order_id="healthy"))
    )
    rejected = rejected_purchases(changes)
    assert {row.order_key for row in rejected.select("order_key").collect()} == {"test:order-1"}


def snapshot(**changes):  # type: ignore[no-untyped-def]
    value = {
        "source": "test",
        "order_id": "order-1",
        "version": 1,
        "customer_id": "customer-1",
        "currency": "EUR",
        "country": "IT",
        "purchased_at": "2026-01-01T12:00:00Z",
        "status": "CONFIRMED",
        "items": [
            {"line_id": "1", "product_id": "sku-1", "quantity": 2, "amount": "20.00"},
            {"line_id": "2", "product_id": "sku-1", "quantity": 1, "amount": "10.00"},
        ],
        "returns": [
            {
                "return_id": "r1",
                "line_id": "1",
                "quantity": 1,
                "amount": "10.00",
                "occurred_at": "2026-01-02T12:00:00Z",
            }
        ],
    }
    value.update(changes)
    return PurchaseSnapshot.model_validate(value)


def bronze(spark: SparkSession, *purchases: PurchaseSnapshot):  # type: ignore[no-untyped-def]
    now = datetime(2026, 1, 3, tzinfo=timezone.utc)
    records = []
    for index, purchase in enumerate(purchases):
        records.append(
            {
                "event_id": f"event-{index:010d}",
                "event_type": "order.snapshot",
                "event_version": 1,
                "occurred_at": now,
                "produced_at": now,
                "producer": "test",
                "trace_id": "test-trace",
                "partition_key": purchase.order_id,
                "payload": purchase.payload(),
                "producer_sequence": purchase.version,
                "source_topic": "marketplace.orders.v1",
                "source_partition": 0,
                "source_offset": index,
                "source_timestamp": now,
                "ingested_at": now,
                "ingestion_run_id": "test",
                "quality_flags": [],
            }
        )
    return spark.createDataFrame(records, BRONZE_SCHEMA)


def test_contract_supports_partial_returns_and_rejects_invalid_money() -> None:
    assert snapshot().returns[0].amount == Decimal("10")
    with pytest.raises(ValidationError, match="decimal string"):
        snapshot(items=[{"line_id": "1", "product_id": "sku", "quantity": 1, "amount": 1.5}])
    with pytest.raises(ValidationError, match="exceeds"):
        snapshot(items=[{"line_id": "1", "product_id": "sku", "quantity": 1, "amount": "1"}])
    with pytest.raises(ValidationError, match="unique"):
        snapshot(items=[snapshot().items[0], snapshot().items[0]])


def test_multiline_replay_currency_frequency_and_aging(spark: SparkSession) -> None:
    purchases = purchase_changes(
        bronze(spark, snapshot(), snapshot(), snapshot(order_id="usd", currency="USD"))
    )
    validate_purchases(purchases)
    items = purchase_items(latest_purchases(purchases))
    daily = customer_daily(items)
    euro = daily.where("currency = 'EUR'").first()
    assert euro.orders == 1  # two lines with the SAME SKU are still one order
    assert euro.gross_amount == Decimal("30")
    assert euro.returned_amount == Decimal("10")
    assert euro.net_amount == Decimal("20")
    assert daily.count() == 2  # currencies are never summed
    product = product_daily(items).where("currency = 'EUR'").first()
    assert product.orders == 1 and product.units == 3 and product.returned_units == 1
    profile = customer_profiles(daily, date(2026, 1, 30)).where("currency = 'EUR'").first()
    assert profile.orders_30d == 1 and profile.recency_days == 29
    aged = customer_profiles(daily, date(2026, 4, 3)).where("currency = 'EUR'").first()
    assert aged.orders_90d == 0 and aged.segment == "Inactive"


def test_versions_removal_cancellation_and_unknown_customer(spark: SparkSession) -> None:
    latest = latest_purchases(
        purchase_changes(
            bronze(
                spark,
                snapshot(version=2, items=[], returns=[]),
                snapshot(version=1),
            )
        )
    )
    assert latest.first().version == 2
    assert purchase_items(latest).count() == 0
    cancelled = purchase_items(
        latest_purchases(purchase_changes(bronze(spark, snapshot(status="CANCELLED"))))
    )
    assert cancelled.count() == 0
    unknown = purchase_items(purchase_changes(bronze(spark, snapshot(customer_id=None))))
    assert unknown.count() == 2
    assert customer_daily(unknown).count() == 0


def test_equal_version_conflict_fails_closed(spark: SparkSession) -> None:
    changes = purchase_changes(bronze(spark, snapshot(), snapshot(country="DE")))
    with pytest.raises(ValueError, match="conflicting"):
        validate_purchases(changes)


def test_direct_kafka_amount_cannot_silently_round(spark: SparkSession) -> None:
    raw = bronze(spark, snapshot()).withColumn(
        "payload",
        F.transform_values(
            "payload",
            lambda key, value: F.when(
                key == "items", F.regexp_replace(value, "20.00", "20.009")
            ).otherwise(value),
        ),
    )
    with pytest.raises(ValueError, match="invalid purchase"):
        validate_purchases(purchase_changes(raw))


def test_return_validation_does_not_sum_different_versions(spark: SparkSession) -> None:
    # Each full-state snapshot repeats cumulative return history. Versions must
    # not be summed when multiple updates arrive in one source range.
    changes = purchase_changes(
        bronze(spark, snapshot(version=1), snapshot(version=2), snapshot(version=3))
    )
    validate_purchases(changes)
    assert purchase_items(latest_purchases(changes)).agg({"net_amount": "sum"}).first()[
        0
    ] == Decimal("20")
