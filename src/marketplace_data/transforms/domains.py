"""Conformance for payments, shipments, and inventory event families."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _latest(frame: DataFrame, key: str, occurred_at: str = "updated_at") -> DataFrame:
    ordering = Window.partitionBy(key).orderBy(
        F.col(occurred_at).desc(),
        F.col("producer_sequence").desc_nulls_last(),
        F.col("source_partition").desc(),
        F.col("source_offset").desc(),
    )
    return (
        frame.withColumn("_rank", F.row_number().over(ordering))
        .where(F.col("_rank") == 1)
        .drop("_rank", "producer_sequence")
    )


def _status_map(values: Mapping[str, str]) -> Column:
    return F.create_map(
        *[item for pair in values.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )


def conform_payments(bronze: DataFrame) -> DataFrame:
    statuses = {
        "payment.authorized": "AUTHORIZED",
        "payment.captured": "CAPTURED",
        "payment.failed": "FAILED",
        "payment.reversed": "REVERSED",
    }
    typed = bronze.where(
        F.col("event_type").isin(list(statuses)) & (F.size("quality_flags") == 0)
    ).select(
        F.coalesce(F.col("payload.payment_id"), F.col("partition_key")).alias("payment_id"),
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.amount").cast("decimal(20,2)").alias("amount"),
        _status_map(statuses)[F.col("event_type")].alias("status"),
        F.col("occurred_at").alias("updated_at"),
        F.col("event_id").alias("last_event_id"),
        "producer_sequence",
        "source_partition",
        "source_offset",
    )
    return _latest(typed, "payment_id")


def conform_shipments(bronze: DataFrame) -> DataFrame:
    statuses = {
        "shipment.created": "CREATED",
        "shipment.dispatched": "DISPATCHED",
        "shipment.delivered": "DELIVERED",
        "shipment.exception": "EXCEPTION",
    }
    typed = bronze.where(
        F.col("event_type").isin(list(statuses)) & (F.size("quality_flags") == 0)
    ).select(
        F.coalesce(F.col("payload.shipment_id"), F.col("partition_key")).alias("shipment_id"),
        F.col("payload.order_id").alias("order_id"),
        _status_map(statuses)[F.col("event_type")].alias("status"),
        F.to_timestamp(F.col("payload.promised_at")).alias("promised_at"),
        F.when(F.col("event_type") == "shipment.delivered", F.col("occurred_at")).alias(
            "delivered_at"
        ),
        F.col("occurred_at").alias("updated_at"),
        F.col("event_id").alias("last_event_id"),
        "producer_sequence",
        "source_partition",
        "source_offset",
    )
    return _latest(typed, "shipment_id")


def inventory_daily(bronze: DataFrame) -> DataFrame:
    events = bronze.where(
        F.col("event_type").isin("inventory.reserved", "inventory.released", "inventory.adjusted")
        & (F.size("quality_flags") == 0)
    ).select(
        F.col("payload.sku").alias("sku"),
        F.col("payload.location_id").alias("location_id"),
        F.to_date("occurred_at").alias("date"),
        F.when(
            F.col("event_type") == "inventory.reserved",
            -F.abs(F.col("payload.quantity").cast("long")),
        )
        .when(
            F.col("event_type") == "inventory.released",
            F.abs(F.col("payload.quantity").cast("long")),
        )
        .otherwise(F.col("payload.quantity_delta").cast("long"))
        .alias("quantity_delta"),
    )
    daily = events.groupBy("sku", "location_id", "date").agg(
        F.sum("quantity_delta").cast("long").alias("daily_delta")
    )
    running = (
        Window.partitionBy("sku", "location_id")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return daily.withColumn("available_units", F.sum("daily_delta").over(running)).withColumn(
        "is_stockout", F.col("available_units") <= 0
    )
