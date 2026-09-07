"""Quality-gated Gold KPI build with Iceberg write-audit-publish."""

from __future__ import annotations

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, observed, parser, spark_for
from marketplace_data.quality import enforce, non_negative, non_null, unique
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.gold import daily_marketplace_kpis
from marketplace_data.util import validate_identifier


@observed
def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--candidate-branch")
    cli.add_argument("--publish", action="store_true")
    args = cli.parse_args(argv)
    job = context("gold_marketplace_kpis", args.config, args.run_id)
    branch = validate_identifier(
        args.candidate_branch or f"candidate_{job.run_id.replace('-', '_')}"
    )
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    target = repository.table("gold", "daily_marketplace_kpis")
    published_snapshot = repository.current_snapshot_id("gold", "daily_marketplace_kpis")
    kpis = daily_marketplace_kpis(spark.table(repository.table("silver", "orders"))).persist()
    try:
        checks = [
            non_null(kpis, "metric_date"),
            unique(kpis, ["metric_date", "market", "seller_tier"]),
            non_negative(kpis, "gmv"),
            non_negative(kpis, "net_revenue"),
        ]
        enforce(target, checks)
        if not args.dry_run:
            if published_snapshot is None and not args.publish:
                raise ValueError("the first atomic publication requires --publish")
            write_target = target
            if published_snapshot is not None:
                repository.create_candidate_branch("gold", "daily_marketplace_kpis", branch)
                # A branch-qualified identifier guarantees this write cannot
                # mutate the certified main reference before quality promotion.
                write_target = f"{target}.branch_{branch}"
            kpis.writeTo(write_target).overwritePartitions()
            if args.publish and published_snapshot is not None:
                repository.fast_forward("gold", "daily_marketplace_kpis", branch)
    finally:
        kpis.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
