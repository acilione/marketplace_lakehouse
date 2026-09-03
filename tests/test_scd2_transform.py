from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from marketplace_data.transforms.scd2 import build_scd2


def test_scd2_collapses_unchanged_values_and_closes_intervals(spark: SparkSession) -> None:
    rows = [
        ("c1", "IT", datetime(2026, 1, 1, tzinfo=timezone.utc), 1),
        ("c1", "IT", datetime(2026, 1, 2, tzinfo=timezone.utc), 2),
        ("c1", "DE", datetime(2026, 1, 3, tzinfo=timezone.utc), 3),
    ]
    frame = spark.createDataFrame(rows, ["customer_id", "country", "valid_from", "lsn"])
    result = build_scd2(
        frame,
        key_columns=["customer_id"],
        attribute_columns=["country"],
        effective_column="valid_from",
        tie_breakers=["lsn"],
    ).orderBy("valid_from")
    history = result.collect()

    assert len(history) == 2
    assert history[0].valid_to == history[1].valid_from
    assert not history[0].is_current
    assert history[1].is_current
