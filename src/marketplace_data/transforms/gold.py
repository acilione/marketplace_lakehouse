"""Business-ready marts and feature transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_marketplace_kpis(orders: DataFrame, sellers: DataFrame | None = None) -> DataFrame:
    enriched = orders
    if sellers is None:
        enriched = enriched.withColumn("seller_tier", F.lit("UNCLASSIFIED"))
    else:
        enriched = enriched.join(F.broadcast(sellers), "seller_id", "left").fillna(
            {"seller_tier": "UNCLASSIFIED"}
        )

    grouped = enriched.groupBy(
        F.to_date("created_at").alias("metric_date"), "market", "seller_tier"
    ).agg(
        F.count("order_id").cast("long").alias("order_count"),
        F.sum("gross_amount").cast("decimal(38,2)").alias("gmv"),
        F.sum(
            F.when(~F.col("status").isin("CANCELLED", "REFUNDED"), F.col("gross_amount"))
            .otherwise(F.lit(0))
            .cast("decimal(38,2)")
        ).alias("net_revenue"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))
        .cast("long")
        .alias("cancelled_orders"),
    )
    return grouped.withColumn(
        "cancellation_rate",
        F.when(
            F.col("order_count") > 0,
            (F.col("cancelled_orders") / F.col("order_count")).cast("decimal(12,6)"),
        ),
    )


def demand_features(order_lines: DataFrame, inventory: DataFrame) -> DataFrame:
    """Build leakage-safe daily lags over completed historical demand."""
    daily = order_lines.groupBy("sku", "location_id", F.to_date("created_at").alias("date")).agg(
        F.sum("quantity").cast("long").alias("demand_units")
    )
    by_item = Window.partitionBy("sku", "location_id").orderBy("date")
    rolling_7 = by_item.rowsBetween(-7, -1)
    rolling_28 = by_item.rowsBetween(-28, -1)
    stock = inventory.select("sku", "location_id", "date", "available_units", "is_stockout")
    return (
        daily.withColumn("demand_lag_1", F.lag("demand_units", 1).over(by_item))
        .withColumn("demand_lag_7", F.lag("demand_units", 7).over(by_item))
        .withColumn("demand_avg_7", F.avg("demand_units").over(rolling_7))
        .withColumn("demand_avg_28", F.avg("demand_units").over(rolling_28))
        .join(stock, ["sku", "location_id", "date"], "left")
        .withColumnRenamed("date", "forecast_date")
    )
