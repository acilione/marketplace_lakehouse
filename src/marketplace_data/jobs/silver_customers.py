"""Idempotent customer CDC conformance into SCD Type 2 history."""

from __future__ import annotations

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, observed, parser, spark_for
from marketplace_data.quality import enforce, non_null, scd2_invariants, unique
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.scd2 import build_customer_scd2


@observed
def main(argv: list[str] | None = None) -> None:
    args = parser(__doc__).parse_args(argv)
    job = context("silver_customer_scd2", args.config, args.run_id)
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    changes = spark.table(repository.table("bronze", "marketplace_events"))
    customers = build_customer_scd2(changes).persist()
    try:
        checks = [
            non_null(customers, "customer_id"),
            unique(customers, ["customer_id", "valid_from"]),
            *scd2_invariants(customers, "customer_id"),
        ]
        enforce(repository.table("silver", "customers_scd2"), checks)
        if not args.dry_run:
            repository.replace_all(
                customers,
                "silver",
                "customers_scd2",
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
