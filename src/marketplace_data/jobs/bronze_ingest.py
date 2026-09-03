"""Checkpointed Kafka-to-Iceberg ingestion with independent quarantine progress."""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame

from marketplace_data.contracts import CONTRACTS_ROOT
from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, parser, spark_for
from marketplace_data.observability import ROWS, serve_metrics
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.bronze import decode_avro, decode_json, validate_and_route


def _kafka_options(job_settings: Any) -> dict[str, str]:
    kafka = job_settings.kafka
    processing = job_settings.processing
    return {
        "kafka.bootstrap.servers": kafka.bootstrap_servers,
        "subscribe": ",".join(kafka.topics),
        "startingOffsets": kafka.starting_offsets,
        "failOnDataLoss": str(kafka.fail_on_data_loss).lower(),
        "maxOffsetsPerTrigger": str(processing.max_offsets_per_trigger),
        "kafka.security.protocol": kafka.security_protocol,
    }


def _merge_accepted(repository: IcebergRepository, batch: DataFrame, _: int) -> None:
    cached = batch.persist()
    try:
        count = cached.count()
        repository.merge(cached, "bronze", "marketplace_events", ["event_id"])
        ROWS.labels("bronze_event_ingest", "accepted").inc(count)
    finally:
        cached.unpersist()


def _merge_quarantine(repository: IcebergRepository, batch: DataFrame, _: int) -> None:
    cached = batch.persist()
    try:
        count = cached.count()
        repository.merge(
            cached,
            "quarantine",
            "marketplace_events",
            ["source_topic", "source_partition", "source_offset"],
            update_existing=False,
        )
        ROWS.labels("bronze_event_ingest", "quarantined").inc(count)
    finally:
        cached.unpersist()


def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--available-now", action="store_true")
    args = cli.parse_args(argv)
    job = context("bronze_event_ingest", args.config, args.run_id)
    spark = spark_for(job)
    bootstrap_tables(spark, job.settings.catalog.name)
    serve_metrics(job.settings.observability.metrics_port)
    repository = IcebergRepository(spark, job.settings.catalog.name)

    records = spark.readStream.format("kafka").options(**_kafka_options(job.settings)).load()
    if job.settings.kafka.encoding == "avro":
        schema = (CONTRACTS_ROOT / "event-envelope-v1.avsc").read_text(encoding="utf-8")
        decoded = decode_avro(records, json.dumps(json.loads(schema)))
    else:
        decoded = decode_json(records)
    routed = validate_and_route(decoded, job.run_id)
    trigger = (
        {"availableNow": True}
        if args.available_now
        else {"processingTime": job.settings.processing.trigger_interval}
    )
    checkpoint_root = job.settings.processing.checkpoint_root.rstrip("/")

    accepted = (
        routed.accepted.withWatermark("occurred_at", job.settings.processing.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .writeStream.queryName("bronze_event_ingest_accepted")
        .option("checkpointLocation", f"{checkpoint_root}/bronze_event_ingest/accepted")
        .trigger(**trigger)
        .foreachBatch(lambda batch, batch_id: _merge_accepted(repository, batch, batch_id))
        .start()
    )
    quarantine = (
        routed.quarantined.writeStream.queryName("bronze_event_ingest_quarantine")
        .option("checkpointLocation", f"{checkpoint_root}/bronze_event_ingest/quarantine")
        .trigger(**trigger)
        .foreachBatch(lambda batch, batch_id: _merge_quarantine(repository, batch, batch_id))
        .start()
    )
    try:
        spark.streams.awaitAnyTermination()
        accepted.awaitTermination()
        quarantine.awaitTermination()
    finally:
        for query in (accepted, quarantine):
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
