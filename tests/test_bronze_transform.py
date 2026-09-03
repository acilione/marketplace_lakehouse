from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from marketplace_data.transforms.bronze import decode_json, validate_and_route


def kafka_schema() -> StructType:
    return StructType(
        [
            StructField("key", BinaryType()),
            StructField("value", BinaryType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
        ]
    )


def test_valid_and_malformed_records_are_accounted_for(spark: SparkSession) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    valid = (
        b'{"event_id":"01JABCDEFGHIJKLMNOPQRSTUVWXY","event_type":"order.created",'
        b'"event_version":1,"occurred_at":"2026-01-01T00:00:00Z",'
        b'"produced_at":"2026-01-01T00:00:01Z","producer":"test",'
        b'"trace_id":"trace-12345678","partition_key":"order-1",'
        b'"payload":{"currency":"EUR","gross_amount":"10.00"}}'
    )
    rows = [
        Row(key=b"1", value=valid, topic="orders", partition=0, offset=1, timestamp=now),
        Row(key=b"2", value=b"{bad", topic="orders", partition=0, offset=2, timestamp=now),
    ]
    routed = validate_and_route(decode_json(spark.createDataFrame(rows, kafka_schema())), "run-1")

    assert routed.accepted.count() == 1
    error = routed.quarantined.select("error_code", "source_offset").first()
    assert error.error_code == "DESERIALIZATION_FAILED"
    assert error.source_offset == 2
