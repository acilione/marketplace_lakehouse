"""Explicit Spark schemas. Production ingestion never infers schemas."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_version", IntegerType(), False),
        StructField("occurred_at", TimestampType(), False),
        StructField("produced_at", TimestampType(), False),
        StructField("producer", StringType(), False),
        StructField("trace_id", StringType(), False),
        StructField("partition_key", StringType(), False),
        StructField("payload", MapType(StringType(), StringType(), True), False),
        StructField("producer_sequence", LongType(), True),
    ]
)

BRONZE_SCHEMA = StructType(
    [
        *EVENT_ENVELOPE_SCHEMA.fields,
        StructField("source_topic", StringType(), False),
        StructField("source_partition", IntegerType(), False),
        StructField("source_offset", LongType(), False),
        StructField("source_timestamp", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("quality_flags", ArrayType(StringType()), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("source_topic", StringType(), False),
        StructField("source_partition", IntegerType(), False),
        StructField("source_offset", LongType(), False),
        StructField("source_timestamp", TimestampType(), True),
        StructField("raw_value", StringType(), True),
        StructField("error_code", StringType(), False),
        StructField("error_details", ArrayType(StringType()), False),
        StructField("first_seen_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("remediation_status", StringType(), False),
        StructField("remediation_batch_id", StringType(), True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("customer_token", StringType(), False),
        StructField("market", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("gross_amount", DecimalType(20, 2), False),
        StructField("status", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("last_event_id", StringType(), False),
        StructField("source_partition", IntegerType(), False),
        StructField("source_offset", LongType(), False),
    ]
)

CUSTOMERS_SCD2_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("email_token", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_tier", StringType(), True),
        StructField("is_deleted", BooleanType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("source_event_id", StringType(), False),
        StructField("source_lsn", LongType(), True),
        StructField("attribute_hash", StringType(), False),
    ]
)

DAILY_KPI_SCHEMA = StructType(
    [
        StructField("metric_date", DateType(), False),
        StructField("market", StringType(), False),
        StructField("seller_tier", StringType(), False),
        StructField("order_count", LongType(), False),
        StructField("gmv", DecimalType(38, 2), False),
        StructField("net_revenue", DecimalType(38, 2), False),
        StructField("cancelled_orders", LongType(), False),
        StructField("cancellation_rate", DecimalType(12, 6), True),
    ]
)

PIPELINE_RUN_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("environment", StringType(), False),
        StructField("code_sha", StringType(), False),
        StructField("config_hash", StringType(), False),
        StructField("input_evidence", StringType(), False),
        StructField("output_snapshot_id", LongType(), True),
        StructField("rows_read", LongType(), False),
        StructField("rows_written", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("business_sum_checks", MapType(StringType(), StringType()), False),
        StructField("quality_status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), False),
    ]
)
