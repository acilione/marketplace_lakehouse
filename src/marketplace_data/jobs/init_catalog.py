"""Create governed namespaces and tables idempotently."""

from __future__ import annotations

from marketplace_data.jobs.common import context, parser, spark_for
from marketplace_data.tables import bootstrap_tables


def main(argv: list[str] | None = None) -> None:
    args = parser(__doc__).parse_args(argv)
    job = context("init_catalog", args.config, args.run_id)
    spark = spark_for(job)
    try:
        if not args.dry_run:
            bootstrap_tables(spark, job.settings.catalog.name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
