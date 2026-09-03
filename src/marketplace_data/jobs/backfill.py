"""Bounded, isolated order backfill with candidate publication."""

from __future__ import annotations

from datetime import date

from pyspark.sql import functions as F

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, parser, spark_for
from marketplace_data.quality import enforce, non_null, unique
from marketplace_data.transforms.orders import conform_orders
from marketplace_data.util import validate_identifier


def _date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--start-date", required=True, type=_date)
    cli.add_argument("--end-date", required=True, type=_date)
    cli.add_argument("--candidate-branch", required=True)
    cli.add_argument("--publish", action="store_true")
    args = cli.parse_args(argv)
    if args.end_date < args.start_date or (args.end_date - args.start_date).days > 31:
        raise ValueError("backfill range must be ordered and no wider than 31 days")
    branch = validate_identifier(args.candidate_branch)
    job = context("bounded_backfill", args.config, args.run_id)
    spark = spark_for(job)
    try:
        repository = IcebergRepository(spark, job.settings.catalog.name)
        if repository.current_snapshot_id("silver", "orders") is None:
            raise ValueError("cannot backfill an uninitialized Silver table")
        source = spark.table(repository.table("bronze", "marketplace_events")).where(
            F.to_date("occurred_at").between(F.lit(args.start_date), F.lit(args.end_date))
        )
        candidate = conform_orders(source).persist()
        try:
            enforce(
                repository.table("silver", "orders"),
                [non_null(candidate, "order_id"), unique(candidate, ["order_id"])],
            )
            if not args.dry_run:
                repository.create_candidate_branch("silver", "orders", branch)
                repository.merge(
                    candidate,
                    "silver",
                    "orders",
                    ["order_id"],
                    branch=branch,
                )
                if args.publish:
                    repository.fast_forward("silver", "orders", branch)
        finally:
            candidate.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
