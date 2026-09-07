"""Real Iceberg regressions, using only uniquely named temporary test tables."""

import sys
import uuid
from datetime import datetime, timezone

from marketplace_data.config import load_settings
from marketplace_data.iceberg import IcebergRepository
from marketplace_data.quality import enforce, scd2_invariants
from marketplace_data.spark import build_spark_session
from marketplace_data.transforms.scd2 import build_scd2


def main() -> None:
    spark = build_spark_session(
        "iceberg_regressions", load_settings("/opt/marketplace/config/local.yaml")
    )
    spark.sparkContext.setLogLevel("WARN")
    repository = IcebergRepository(spark)
    name = f"regression_{uuid.uuid4().hex}"
    table = repository.table("ops", name)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    rows = [
        ("c1", "IT", datetime(2026, 1, 1, tzinfo=timezone.utc), 1),
        ("c1", "DE", datetime(2026, 1, 3, tzinfo=timezone.utc), 3),
    ]

    def history(values):  # type: ignore[no-untyped-def]
        return build_scd2(
            spark.createDataFrame(values, ["customer_id", "country", "valid_from", "lsn"]),
            key_columns=["customer_id"],
            attribute_columns=["country"],
            effective_column="valid_from",
            tie_breakers=["lsn"],
        )

    try:
        history(rows).writeTo(table).using("iceberg").create()
        main_snapshot = repository.current_snapshot_id("ops", name)
        repository.create_candidate_branch("ops", name, "unpublished")
        history(rows).writeTo(f"{table}.branch_unpublished").append()
        assert repository.current_snapshot_id("ops", name) == main_snapshot
        repository.create_candidate_branch("ops", name, "next_candidate")
        candidate = spark.table(f"{table}.refs").where("name = 'next_candidate'").first()
        assert candidate.snapshot_id == main_snapshot
        revised = history([*rows, ("c1", "DE", datetime(2026, 1, 2, tzinfo=timezone.utc), 2)])
        repository.replace_all(revised, "ops", name)
        persisted = spark.table(table)
        enforce(table, scd2_invariants(persisted, "customer_id"))
        assert persisted.count() == 2
        assert persisted.where("valid_from = TIMESTAMP '2026-01-03 00:00:00'").count() == 0
        repository.replace_all(revised, "ops", name)
        assert spark.table(table).count() == 2
        sys.stdout.write(
            "PASS: main reference isolation, late SCD2 reconciliation, replay idempotency\n"
        )
    finally:
        # Only this invocation's UUID-named test table can be removed.
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.stop()


if __name__ == "__main__":
    main()
