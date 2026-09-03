"""Policy-bounded Iceberg compaction, manifest rewrite, and snapshot expiry."""

from __future__ import annotations

from marketplace_data.jobs.common import context, parser, spark_for
from marketplace_data.util import qualified_table, validate_identifier


def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--namespace", required=True)
    cli.add_argument("--table", required=True)
    cli.add_argument("--retention-hours", type=int, default=168)
    args = cli.parse_args(argv)
    if not 168 <= args.retention_hours <= 8_760:
        raise ValueError("snapshot retention must be between 168 and 8760 hours")
    job = context("table_maintenance", args.config, args.run_id)
    catalog = validate_identifier(job.settings.catalog.name)
    namespace = validate_identifier(args.namespace)
    table = validate_identifier(args.table)
    spark = spark_for(job)
    name = qualified_table(catalog, namespace, table)
    try:
        if args.dry_run:
            spark.table(f"{name}.files").groupBy("content").count().show(truncate=False)
            return
        spark.sql(
            f"CALL {catalog}.system.rewrite_data_files("
            f"table => '{namespace}.{table}', options => map("
            f"'target-file-size-bytes','{job.settings.processing.target_file_size_bytes}'))"
        )
        spark.sql(f"CALL {catalog}.system.rewrite_manifests(table => '{namespace}.{table}')")
        spark.sql(
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{namespace}.{table}', older_than => "
            f"TIMESTAMPADD(HOUR, -{args.retention_hours}, CURRENT_TIMESTAMP), retain_last => 5)"
        )
        # Orphan deletion is deliberately separate: its identity and safety interval are stricter.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
