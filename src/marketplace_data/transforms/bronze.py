"""Kafka decoding, structural validation, and quarantine routing."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from marketplace_data.schemas import EVENT_ENVELOPE_SCHEMA


@dataclass(frozen=True)
class RoutedEvents:
    accepted: DataFrame
    quarantined: DataFrame


def _error_array() -> Column:
    candidates = [
        F.when(F.col("decode_failed"), F.lit("DESERIALIZATION_FAILED")),
        F.when(F.col("decoded.event_id").isNull(), F.lit("EVENT_ID_MISSING")),
        F.when(F.col("decoded.event_type").isNull(), F.lit("EVENT_TYPE_MISSING")),
        F.when(F.col("decoded.occurred_at").isNull(), F.lit("OCCURRED_AT_INVALID")),
        F.when(F.col("decoded.partition_key").isNull(), F.lit("PARTITION_KEY_MISSING")),
        F.when(F.col("decoded.payload").isNull(), F.lit("PAYLOAD_MISSING")),
    ]
    return F.filter(F.array(*candidates), lambda item: item.isNotNull())


def decode_json(records: DataFrame) -> DataFrame:
    """Decode Kafka values and retain immutable source-position evidence."""
    raw_value = F.col("value").cast("string")
    decoded = F.from_json(raw_value, EVENT_ENVELOPE_SCHEMA)
    return records.select(
        decoded.alias("decoded"),
        F.get_json_object(raw_value, "$").isNull().alias("decode_failed"),
        raw_value.alias("raw_value"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("timestamp").alias("source_timestamp"),
    )


def decode_avro(records: DataFrame, avro_schema_json: str) -> DataFrame:
    """Decode Confluent-wire-format Avro for one registry-pinned schema version."""
    from pyspark.sql.avro.functions import from_avro

    # Confluent framing is magic byte + big-endian 4-byte schema id.
    payload = F.substring(F.col("value"), 6, 2_147_483_647)
    return records.select(
        from_avro(payload, avro_schema_json, {"mode": "PERMISSIVE"}).alias("decoded"),
        F.lit(False).alias("decode_failed"),
        F.base64(F.col("value")).alias("raw_value"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("timestamp").alias("source_timestamp"),
    )


def validate_and_route(decoded: DataFrame, run_id: str) -> RoutedEvents:
    """Split structurally valid records from evidence-rich quarantine records."""
    assessed = decoded.withColumn("validation_errors", _error_array()).withColumn(
        "observed_at", F.current_timestamp()
    )
    invalid = F.size(F.col("validation_errors")) > 0

    quarantined = assessed.where(invalid).select(
        "source_topic",
        "source_partition",
        "source_offset",
        "source_timestamp",
        "raw_value",
        F.element_at("validation_errors", 1).alias("error_code"),
        F.col("validation_errors").alias("error_details"),
        F.col("observed_at").alias("first_seen_at"),
        F.lit(run_id).alias("ingestion_run_id"),
        F.lit("PENDING").alias("remediation_status"),
        F.lit(None).cast("string").alias("remediation_batch_id"),
    )

    accepted = assessed.where(~invalid).select(
        "decoded.*",
        "source_topic",
        "source_partition",
        "source_offset",
        "source_timestamp",
        F.col("observed_at").alias("ingested_at"),
        F.lit(run_id).alias("ingestion_run_id"),
        business_quality_flags().alias("quality_flags"),
    )
    return RoutedEvents(accepted=accepted, quarantined=quarantined)


def business_quality_flags() -> Column:
    """Non-structural issues remain in Bronze and are excluded from conformance."""
    flags = [
        F.when(
            F.col("decoded.payload.currency").isNotNull()
            & ~F.col("decoded.payload.currency").rlike("^[A-Z]{3}$"),
            F.lit("INVALID_CURRENCY"),
        ),
        F.when(
            F.col("decoded.payload.gross_amount").isNotNull()
            & (F.col("decoded.payload.gross_amount").try_cast("decimal(20,2)") < 0),
            F.lit("NEGATIVE_AMOUNT"),
        ),
        F.when(
            F.col("decoded.produced_at") < F.col("decoded.occurred_at"),
            F.lit("PRODUCED_BEFORE_OCCURRED"),
        ),
    ]
    for field, data_type in {
        "gross_amount": "decimal(20,2)",
        "amount": "decimal(20,2)",
        "quantity": "long",
        "quantity_delta": "long",
        "source_lsn": "long",
        "promised_at": "timestamp",
        "created_at": "timestamp",
    }.items():
        value = F.col(f"decoded.payload.{field}")
        flags.append(
            F.when(
                value.isNotNull() & value.try_cast(data_type).isNull(),
                F.lit(f"INVALID_{field.upper()}"),
            )
        )
    return F.filter(F.array(*flags), lambda item: item.isNotNull())
