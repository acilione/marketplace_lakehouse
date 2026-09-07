"""Snapshot-bounded, idempotent Bronze-to-Silver order conformance."""

from __future__ import annotations

from datetime import datetime, timezone

from marketplace_data.iceberg import IcebergRepository, PipelineRun
from marketplace_data.jobs.common import context, observed, parser, spark_for
from marketplace_data.quality import accepted_values, enforce, non_negative, non_null, unique
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.orders import (
    conform_order_lines,
    conform_order_status_history,
    conform_orders,
)


@observed
def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--start-snapshot-id", type=int)
    cli.add_argument("--end-snapshot-id", type=int)
    args = cli.parse_args(argv)
    job = context("silver_order_conform", args.config, args.run_id)
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    bronze_name = repository.table("bronze", "marketplace_events")
    reader = spark.read.format("iceberg")
    evidence = "full-current-snapshot"
    if args.start_snapshot_id is not None:
        reader = reader.option("start-snapshot-id", str(args.start_snapshot_id))
        evidence = f"start={args.start_snapshot_id}"
    if args.end_snapshot_id is not None:
        reader = reader.option("end-snapshot-id", str(args.end_snapshot_id))
        evidence += f",end={args.end_snapshot_id}"
    bronze = reader.load(bronze_name)
    orders = conform_orders(bronze).persist()
    history = conform_order_status_history(bronze).persist()
    order_lines = conform_order_lines(bronze).persist()
    try:
        checks = [
            non_null(orders, "order_id"),
            unique(orders, ["order_id"]),
            accepted_values(orders, "currency", {"EUR", "USD", "GBP", "CHF"}),
            non_negative(orders, "gross_amount"),
        ]
        enforce(repository.table("silver", "orders"), checks)
        rows_read = bronze.count()
        rows_written = orders.count()
        if not args.dry_run:
            repository.merge(orders, "silver", "orders", ["order_id"])
            repository.merge(history, "silver", "order_status_history", ["event_id"])
            repository.merge(order_lines, "silver", "order_lines", ["event_id"])
            snapshot = repository.current_snapshot_id("silver", "orders")
            repository.append_audit(
                PipelineRun(
                    run_id=job.run_id,
                    pipeline_name=job.name,
                    environment=job.settings.environment.value,
                    code_sha=job.code_sha,
                    config_hash=job.config_hash,
                    input_evidence=evidence,
                    output_snapshot_id=snapshot,
                    rows_read=rows_read,
                    rows_written=rows_written,
                    rows_quarantined=0,
                    business_sum_checks={},
                    quality_status="PASSED",
                    started_at=job.started_at,
                    ended_at=datetime.now(timezone.utc),
                )
            )
    finally:
        orders.unpersist()
        history.unpersist()
        order_lines.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
