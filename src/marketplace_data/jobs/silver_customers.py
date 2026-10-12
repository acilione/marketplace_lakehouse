"""Idempotent customer CDC conformance into SCD Type 2 history."""

from __future__ import annotations

from pyspark.sql import functions as F

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, observed, parser, spark_for
from marketplace_data.quality import enforce, non_null, scd2_invariants, unique
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.scd2 import build_customer_scd2


@observed
def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument(
        "--ingestion-run-id", help="Rebuild only customers touched by this ingestion run"
    )
    cli.add_argument("--full-rebuild", action="store_true", help="Explicit recovery/reference mode")
    args = cli.parse_args(argv)
    if bool(args.ingestion_run_id) == bool(args.full_rebuild):
        cli.error("choose --ingestion-run-id or --full-rebuild explicitly")
    job = context("silver_customer_scd2", args.config, args.run_id)
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    changes = spark.table(repository.table("bronze", "marketplace_events"))
    affected = (
        changes.where(F.col("ingestion_run_id") == args.ingestion_run_id)
        .where(F.col("event_type") == "customer.changed")
        .select(F.col("payload.customer_id").alias("customer_id"))
        .distinct()
    )
    if not args.full_rebuild:
        changes = changes.join(
            affected, F.col("payload.customer_id") == affected.customer_id, "left_semi"
        )
    customers = build_customer_scd2(changes).persist()
    try:
        checks = [
            non_null(customers, "customer_id"),
            unique(customers, ["customer_id", "valid_from"]),
            *scd2_invariants(customers, "customer_id"),
        ]
        enforce(repository.table("silver", "customers_scd2"), checks)
        if not args.dry_run:
            if args.full_rebuild:
                repository.replace_all(customers, "silver", "customers_scd2")
            else:
                repository.replace_keys(
                    customers,
                    affected,
                    "silver",
                    "customers_scd2",
                    ["customer_id"],
                    ["customer_id", "valid_from"],
                )
            persisted = spark.table(repository.table("silver", "customers_scd2"))
            enforce(
                repository.table("silver", "customers_scd2"),
                [
                    unique(persisted, ["customer_id", "valid_from"]),
                    *scd2_invariants(persisted, "customer_id"),
                ],
            )
    finally:
        customers.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
