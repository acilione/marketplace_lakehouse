"""Order conformance and deterministic state-history construction."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

_STATUS_BY_EVENT = {
    "order.created": "CREATED",
    "order.confirmed": "CONFIRMED",
    "order.cancelled": "CANCELLED",
    "order.refunded": "REFUNDED",
}


def eligible_order_events(bronze: DataFrame) -> DataFrame:
    return bronze.where(
        F.col("event_type").isin(list(_STATUS_BY_EVENT)) & (F.size("quality_flags") == 0)
    )


def _typed_order_events(bronze: DataFrame) -> DataFrame:
    events = eligible_order_events(bronze)
    status_expression = F.create_map(
        *[item for pair in _STATUS_BY_EVENT.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    return events.select(
        F.coalesce(F.col("payload.order_id"), F.col("partition_key")).alias("order_id"),
        F.col("payload.seller_id").alias("seller_id"),
        F.col("payload.customer_token").alias("customer_token"),
        F.col("payload.market").alias("market"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.gross_amount").cast("decimal(20,2)").alias("gross_amount"),
        status_expression[F.col("event_type")].alias("status"),
        F.coalesce(F.to_timestamp(F.col("payload.created_at")), F.col("occurred_at")).alias(
            "created_at"
        ),
        F.col("occurred_at").alias("updated_at"),
        F.col("event_id").alias("last_event_id"),
        "source_partition",
        "source_offset",
        F.coalesce(F.col("producer_sequence"), F.lit(-1)).alias("producer_sequence"),
    )


def conform_orders(bronze: DataFrame) -> DataFrame:
    """Return one latest row per order using the documented total ordering."""
    typed = _typed_order_events(bronze)
    order = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(),
        F.col("producer_sequence").desc(),
        F.col("source_partition").desc(),
        F.col("source_offset").desc(),
    )
    return (
        typed.withColumn("_rank", F.row_number().over(order))
        .where(F.col("_rank") == 1)
        .drop("_rank", "producer_sequence")
    )


def conform_order_status_history(bronze: DataFrame) -> DataFrame:
    """Return immutable transitions, deduplicated by event identifier."""
    typed = _typed_order_events(bronze)
    event_order = Window.partitionBy("last_event_id").orderBy(
        F.col("updated_at").desc(),
        F.col("source_partition").desc(),
        F.col("source_offset").desc(),
    )
    return (
        typed.withColumn("_rank", F.row_number().over(event_order))
        .where(F.col("_rank") == 1)
        .select(
            "last_event_id",
            "order_id",
            "status",
            F.col("updated_at").alias("occurred_at"),
            "source_partition",
            "source_offset",
        )
        .withColumnRenamed("last_event_id", "event_id")
    )


def conform_order_lines(bronze: DataFrame) -> DataFrame:
    """Extract the local profile's single line item from immutable order-created events."""
    created = eligible_order_events(bronze).where(F.col("event_type") == "order.created")
    event_order = Window.partitionBy("event_id").orderBy(
        F.col("source_partition").desc(), F.col("source_offset").desc()
    )
    return (
        created.where(F.col("payload.sku").isNotNull())
        .select(
            "event_id",
            F.col("payload.order_id").alias("order_id"),
            F.col("payload.sku").alias("sku"),
            F.col("payload.location_id").alias("location_id"),
            F.col("payload.quantity").cast("long").alias("quantity"),
            F.col("occurred_at").alias("created_at"),
            "source_partition",
            "source_offset",
        )
        .withColumn("_rank", F.row_number().over(event_order))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )
