"""Read-only local demo query for certified Gold output."""

from __future__ import annotations

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, parser, spark_for


def main(argv: list[str] | None = None) -> None:
    args = parser(__doc__).parse_args(argv)
    job = context("show_certified_results", args.config, args.run_id)
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    try:
        spark.table(repository.table("gold", "daily_marketplace_kpis")).orderBy(
            "metric_date", "market", "seller_tier"
        ).show(100, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
