from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession

from marketplace_data.transforms.gold import daily_marketplace_kpis, demand_features


def test_daily_kpis_use_fixed_precision_and_business_status(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [
            (
                "o1",
                "s1",
                "IT",
                Decimal("10.00"),
                "CONFIRMED",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
            (
                "o2",
                "s1",
                "IT",
                Decimal("5.00"),
                "CANCELLED",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
        ],
        ["order_id", "seller_id", "market", "gross_amount", "status", "created_at"],
    )
    row = daily_marketplace_kpis(orders).first()

    assert row.order_count == 2
    assert row.gmv == Decimal("15.00")
    assert row.net_revenue == Decimal("10.00")
    assert row.cancelled_orders == 1
    assert row.cancellation_rate == Decimal("0.500000")


def test_seller_tier_and_demand_features(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [
            (
                "o1",
                "s1",
                "IT",
                Decimal("10.00"),
                "CONFIRMED",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        ],
        ["order_id", "seller_id", "market", "gross_amount", "status", "created_at"],
    )
    sellers = spark.createDataFrame([("s1", "PREMIUM")], ["seller_id", "seller_tier"])
    assert daily_marketplace_kpis(orders, sellers).first().seller_tier == "PREMIUM"

    order_lines = spark.createDataFrame(
        [
            ("sku-1", "loc-1", datetime(2026, 1, day, tzinfo=timezone.utc), 2)
            for day in range(1, 10)
        ],
        ["sku", "location_id", "created_at", "quantity"],
    )
    inventory = spark.createDataFrame(
        [("sku-1", "loc-1", datetime(2026, 1, 9, tzinfo=timezone.utc).date(), 3, False)],
        ["sku", "location_id", "date", "available_units", "is_stockout"],
    )
    last = demand_features(order_lines, inventory).orderBy("forecast_date", ascending=False).first()
    assert last.demand_lag_1 == 2
    assert last.available_units == 3
