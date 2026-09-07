from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
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


@pytest.mark.parametrize(
    "field,value",
    [
        ("gross_amount", "not-a-number"),
        ("amount", "1e999"),
        ("quantity", "several"),
        ("promised_at", "tomorrow-ish"),
    ],
)
def test_invalid_domain_values_are_flagged_without_failing_batch(
    spark: SparkSession, field: str, value: str
) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    event = {
        "event_id": "test",
        "event_type": "order.created",
        "occurred_at": now.isoformat(),
        "produced_at": now.isoformat(),
        "partition_key": "o1",
        "payload": {field: value},
    }
    frame = spark.createDataFrame(
        [(b"1", json.dumps(event).encode(), "orders", 0, 1, now)], kafka_schema()
    )
    flags = validate_and_route(decode_json(frame), "test").accepted.select("quality_flags").first()
    assert flags is not None and f"INVALID_{field.upper()}" in flags.quality_flags
