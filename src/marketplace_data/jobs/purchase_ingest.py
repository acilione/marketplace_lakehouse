"""Append-oriented purchase ingestion; retains late events and transport duplicates."""

from __future__ import annotations

from pyspark.sql import functions as F

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.bronze_ingest import _kafka_options, _merge_quarantine
from marketplace_data.jobs.common import context, observed, parser, spark_for
from marketplace_data.schemas import BRONZE_SCHEMA
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.bronze import decode_json, validate_and_route


@observed
def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--available-now", action="store_true")
    args = cli.parse_args(argv)
    job = context("purchase_ingest", args.config, args.run_id)
    spark = spark_for(job)
    spark.sparkContext.setLogLevel("WARN")
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    target = repository.table("bronze", "purchase_events")
    if not spark.catalog.tableExists(target):
        spark.createDataFrame([], BRONZE_SCHEMA).writeTo(target).using("iceberg").partitionedBy(
            F.days("ingested_at")
        ).tableProperty("format-version", "2").create()
    records = spark.readStream.format("kafka").options(**_kafka_options(job.settings)).load()
    routed = validate_and_route(decode_json(records), job.run_id)
    trigger = (
        {"availableNow": True}
        if args.available_now
        else {"processingTime": job.settings.processing.trigger_interval}
    )
    checkpoint = job.settings.processing.checkpoint_root.rstrip("/") + "/purchase_ingest_v1"
    # Native Iceberg streaming sink commits Spark epoch IDs. Unlike a foreachBatch
    # append, restart of the same checkpoint does not append an already committed epoch.
    accepted = (
        routed.accepted.where(F.col("event_type") == "order.snapshot")
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint + "/accepted")
        .trigger(**trigger)
        .toTable(target)
    )
    quarantine = (
        routed.quarantined.writeStream.option("checkpointLocation", checkpoint + "/quarantine")
        .trigger(**trigger)
        .foreachBatch(lambda batch, epoch: _merge_quarantine(repository, batch, epoch))
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
