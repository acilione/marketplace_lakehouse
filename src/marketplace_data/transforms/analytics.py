"""Set-based purchase and profile definitions. No I/O or implicit clock access."""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

POLICY_VERSION = "purchase-v1"
ITEM_SCHEMA = (
    "array<struct<line_id:string,product_id:string,category:string,"
    "quantity:bigint,amount:decimal(20,2)>>"
)
RETURN_SCHEMA = (
    "array<struct<return_id:string,line_id:string,quantity:bigint,"
    "amount:decimal(20,2),occurred_at:timestamp>>"
)


def purchase_changes(bronze: DataFrame) -> DataFrame:
    """Source-qualified full order snapshots; event-time never outranks version."""

    def invalid_money(value: Column) -> Column:
        # DECIMAL parsing rounds excess scale; validate the original text first.
        return F.exists(
            F.from_json(value, "array<struct<amount:string>>"),
            lambda item: item.amount.isNull() | ~item.amount.rlike(r"^[0-9]{1,18}(\.[0-9]{1,2})?$"),
        )

    rows = bronze.where(F.col("event_type") == "order.snapshot").select(
        F.concat_ws(":", "payload.source", "payload.order_id").alias("order_key"),
        F.when(
            F.length(F.trim(F.col("payload.customer_id"))) > 0,
            F.concat_ws(":", "payload.source", "payload.customer_id"),
        ).alias("customer_key"),
        F.col("payload.source").alias("source"),
        F.col("payload.version").try_cast("long").alias("version"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.country").alias("country"),
        F.col("payload.purchased_at").try_cast("timestamp").alias("purchased_at"),
        F.col("payload.status").alias("status"),
        F.from_json("payload.items", ITEM_SCHEMA).alias("items"),
        F.from_json(F.coalesce(F.col("payload.returns"), F.lit("[]")), RETURN_SCHEMA).alias(
            "returns"
        ),
        F.sha2(F.to_json(F.sort_array(F.map_entries("payload"))), 256).alias("content_hash"),
        F.col("ingested_at").alias("known_at"),
        F.col("event_id").alias("last_event_id"),
        (
            invalid_money(F.col("payload.items"))
            | invalid_money(F.coalesce(F.col("payload.returns"), F.lit("[]")))
        ).alias("invalid_money"),
    )
    return rows


def latest_purchases(changes: DataFrame) -> DataFrame:
    ordering = Window.partitionBy("order_key").orderBy(
        F.col("version").desc(), F.col("last_event_id").desc(), F.col("known_at").asc()
    )
    return (
        changes.withColumn("_rank", F.row_number().over(ordering)).where("_rank = 1").drop("_rank")
    )


def purchase_items(orders: DataFrame) -> DataFrame:
    """Return at purchase-date attribution; returns never remove order frequency."""
    refunds = (
        orders.select("order_key", "version", F.explode("returns").alias("r"))
        .groupBy("order_key", "version", F.col("r.line_id").alias("line_id"))
        .agg(
            F.sum("r.quantity").alias("returned_quantity"),
            F.sum("r.amount").cast("decimal(20,2)").alias("returned_amount"),
        )
    )
    lines = (
        orders.where("status IN ('CONFIRMED', 'REFUNDED')")
        .select(
            "order_key",
            "customer_key",
            "source",
            "currency",
            "country",
            "purchased_at",
            "version",
            "known_at",
            F.explode("items").alias("item"),
        )
        .select(
            "order_key",
            "customer_key",
            "currency",
            "country",
            "purchased_at",
            "version",
            "known_at",
            F.col("item.line_id").alias("line_id"),
            F.concat_ws(":", "source", "item.product_id").alias("product_key"),
            F.col("item.category").alias("category"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.amount").alias("gross_amount"),
        )
    )
    return (
        lines.join(refunds, ["order_key", "version", "line_id"], "left")
        .fillna({"returned_quantity": 0, "returned_amount": 0})
        .withColumn(
            "net_amount", (F.col("gross_amount") - F.col("returned_amount")).cast("decimal(20,2)")
        )
    )


def customer_daily(items: DataFrame) -> DataFrame:
    return (
        items.where(F.col("customer_key").isNotNull())
        .groupBy("customer_key", "currency", F.to_date("purchased_at").alias("metric_date"))
        .agg(
            F.countDistinct("order_key").alias("orders"),
            F.sum("quantity").alias("units"),
            *[
                F.sum(c).cast("decimal(38,2)").alias(c)
                for c in ("gross_amount", "returned_amount", "net_amount")
            ],
        )
    )


def customer_profiles(daily: DataFrame, as_of: date) -> DataFrame:
    """Inclusive UTC date windows. Daily aging also updates inactive customers."""
    eligible = daily.where(F.col("metric_date") <= F.lit(as_of))
    result = (
        eligible.groupBy("customer_key", "currency")
        .agg(
            F.min("metric_date").alias("first_purchase"),
            F.max("metric_date").alias("last_purchase"),
            F.sum("orders").alias("lifetime_orders"),
            *[
                F.sum(
                    F.when(
                        F.col("metric_date") >= F.date_sub(F.lit(as_of), days - 1), F.col(metric)
                    ).otherwise(0)
                ).alias(f"{metric}_{days}d")
                for days in (30, 90, 365)
                for metric in ("orders", "net_amount")
            ],
        )
        .withColumn("recency_days", F.datediff(F.lit(as_of), "last_purchase"))
    )
    return (
        result.withColumn(
            "segment",
            F.when(F.col("recency_days") > 90, "Inactive")
            .when(F.col("orders_90d") >= 3, "Repeat")
            .when(F.col("lifetime_orders") == 1, "New")
            .otherwise("Returning"),
        )
        .withColumn("as_of_date", F.lit(as_of))
        .withColumn("policy_version", F.lit(POLICY_VERSION))
    )


def product_daily(items: DataFrame) -> DataFrame:
    return items.groupBy(
        "product_key", "currency", F.to_date("purchased_at").alias("metric_date")
    ).agg(
        F.countDistinct("order_key").alias("orders"),
        F.sum("quantity").alias("units"),
        F.sum("returned_quantity").alias("returned_units"),
        *[
            F.sum(c).cast("decimal(38,2)").alias(c)
            for c in ("gross_amount", "returned_amount", "net_amount")
        ],
    )


def customer_cohorts(daily: DataFrame, as_of: date) -> DataFrame:
    """Monthly acquisition cohorts; denominator includes every identified buyer.

    Emit zeros for observed periods, not future periods. The current calendar
    month is explicitly incomplete. First purchase means first in source history.
    """
    eligible = daily.where(F.col("metric_date") <= F.lit(as_of))
    first = eligible.groupBy("customer_key", "currency").agg(
        F.trunc(F.min("metric_date"), "month").alias("cohort_month")
    )
    sizes = first.groupBy("cohort_month", "currency").agg(F.count("*").alias("cohort_size"))
    activity = (
        eligible.join(first, ["customer_key", "currency"])
        .withColumn("activity_month", F.trunc("metric_date", "month"))
        .groupBy("cohort_month", "currency", "activity_month")
        .agg(F.countDistinct("customer_key").alias("active_customers"))
    )
    grid = sizes.withColumn(
        "activity_month",
        F.explode(
            F.sequence("cohort_month", F.trunc(F.lit(as_of), "month"), F.expr("INTERVAL 1 MONTH"))
        ),
    )
    return (
        grid.join(activity, ["cohort_month", "currency", "activity_month"], "left")
        .fillna({"active_customers": 0})
        .withColumn("month_number", F.months_between("activity_month", "cohort_month").cast("int"))
        .withColumn(
            "retention", (F.col("active_customers") / F.col("cohort_size")).cast("decimal(8,6)")
        )
        .withColumn("complete", F.last_day("activity_month") <= F.lit(as_of))
    )
