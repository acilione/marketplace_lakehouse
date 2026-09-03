"""SCD Type 2 construction with half-open, non-overlapping intervals."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_customer_scd2(changes: DataFrame) -> DataFrame:
    """Build deterministic customer history from source-faithful CDC events."""
    typed = changes.where(
        (F.col("event_type") == "customer.changed") & (F.size("quality_flags") == 0)
    ).select(
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.email_token").alias("email_token"),
        F.col("payload.country").alias("country"),
        F.col("payload.customer_tier").alias("customer_tier"),
        (F.col("payload.op") == "d").alias("is_deleted"),
        F.col("occurred_at").alias("valid_from"),
        F.col("event_id").alias("source_event_id"),
        F.col("payload.source_lsn").cast("long").alias("source_lsn"),
        "source_partition",
        "source_offset",
    )
    return build_scd2(
        typed,
        key_columns=["customer_id"],
        attribute_columns=["email_token", "country", "customer_tier", "is_deleted"],
        effective_column="valid_from",
        tie_breakers=["source_lsn", "source_partition", "source_offset"],
    ).drop("source_partition", "source_offset")


def build_scd2(
    changes: DataFrame,
    *,
    key_columns: Sequence[str],
    attribute_columns: Sequence[str],
    effective_column: str,
    tie_breakers: Sequence[str],
) -> DataFrame:
    """Collapse duplicate/unchanged events and calculate ``[from, to)`` validity."""
    attribute_hash = F.sha2(F.to_json(F.struct(*[F.col(name) for name in attribute_columns])), 256)
    ascending = [F.col(name).asc_nulls_last() for name in tie_breakers]
    same_instant = Window.partitionBy(*key_columns, effective_column).orderBy(
        *[column.desc_nulls_last() for column in map(F.col, tie_breakers)]
    )
    ordered = Window.partitionBy(*key_columns).orderBy(F.col(effective_column), *ascending)

    deduplicated = (
        changes.withColumn("attribute_hash", attribute_hash)
        .withColumn("_instant_rank", F.row_number().over(same_instant))
        .where(F.col("_instant_rank") == 1)
        .drop("_instant_rank")
    )
    changed_only = (
        deduplicated.withColumn("_previous_hash", F.lag("attribute_hash").over(ordered))
        .where(
            F.col("_previous_hash").isNull() | (F.col("attribute_hash") != F.col("_previous_hash"))
        )
        .drop("_previous_hash")
    )
    final_order = Window.partitionBy(*key_columns).orderBy(F.col(effective_column), *ascending)
    return changed_only.withColumn(
        "valid_to", F.lead(effective_column).over(final_order)
    ).withColumn("is_current", F.col("valid_to").isNull())
