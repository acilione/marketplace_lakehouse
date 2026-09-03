"""Build and atomically publish leakage-safe demand features."""

from __future__ import annotations

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, parser, spark_for
from marketplace_data.quality import enforce, non_negative, non_null, unique
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.gold import demand_features
from marketplace_data.util import validate_identifier


def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--candidate-branch")
    cli.add_argument("--publish", action="store_true")
    args = cli.parse_args(argv)
    job = context("demand_feature_build", args.config, args.run_id)
    branch = validate_identifier(
        args.candidate_branch or f"candidate_{job.run_id.replace('-', '_')}"
    )
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    target = repository.table("gold", "demand_features")
    current_snapshot = repository.current_snapshot_id("gold", "demand_features")
    features = demand_features(
        spark.table(repository.table("silver", "order_lines")),
        spark.table(repository.table("silver", "inventory_daily")),
    ).persist()
    try:
        enforce(
            target,
            [
                non_null(features, "sku"),
                unique(features, ["sku", "location_id", "forecast_date"]),
                non_negative(features, "demand_units"),
            ],
        )
        if not args.dry_run:
            if current_snapshot is None and not args.publish:
                raise ValueError("the first atomic publication requires --publish")
            write_target = target
            if current_snapshot is not None:
                repository.create_candidate_branch("gold", "demand_features", branch)
                write_target = f"{target}.branch_{branch}"
            features.writeTo(write_target).overwritePartitions()
            if args.publish and current_snapshot is not None:
                repository.fast_forward("gold", "demand_features", branch)
    finally:
        features.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
