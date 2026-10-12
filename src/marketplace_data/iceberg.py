"""Validated Iceberg DDL, idempotent MERGE, audit, and branch publication."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from marketplace_data.util import qualified_table, validate_identifier


@dataclass(frozen=True)
class PipelineRun:
    run_id: str
    pipeline_name: str
    environment: str
    code_sha: str
    config_hash: str
    input_evidence: str
    output_snapshot_id: int | None
    rows_read: int
    rows_written: int
    rows_quarantined: int
    business_sum_checks: dict[str, str]
    quality_status: str
    started_at: datetime
    ended_at: datetime


PIPELINE_RUN_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("pipeline_name", StringType(), nullable=False),
        StructField("environment", StringType(), nullable=False),
        StructField("code_sha", StringType(), nullable=False),
        StructField("config_hash", StringType(), nullable=False),
        StructField("input_evidence", StringType(), nullable=False),
        StructField("output_snapshot_id", LongType(), nullable=True),
        StructField("rows_read", LongType(), nullable=False),
        StructField("rows_written", LongType(), nullable=False),
        StructField("rows_quarantined", LongType(), nullable=False),
        StructField(
            "business_sum_checks",
            MapType(StringType(), StringType(), valueContainsNull=False),
            nullable=False,
        ),
        StructField("quality_status", StringType(), nullable=False),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("ended_at", TimestampType(), nullable=False),
    ]
)


class IcebergRepository:
    def __init__(self, spark: SparkSession, catalog: str = "lakehouse") -> None:
        self.spark = spark
        self.catalog = validate_identifier(catalog)

    def table(self, namespace: str, table: str) -> str:
        return qualified_table(self.catalog, namespace, table)

    def merge(
        self,
        source: DataFrame,
        namespace: str,
        table: str,
        keys: list[str],
        *,
        update_existing: bool = True,
        branch: str | None = None,
        version_columns: list[str] | None = None,
    ) -> None:
        target = self.table(namespace, table)
        if branch is not None:
            target = f"{target}.branch_{validate_identifier(branch)}"
        safe_keys = [validate_identifier(key) for key in keys]
        view = f"candidate_{validate_identifier(table)}"
        # foreachBatch receives a DataFrame bound to an isolated Spark session in
        # Spark 4.x. The view and MERGE must execute in that same session.
        session = source.sparkSession
        # Spark 4.1 + Iceberg 1.11 cannot plan MERGE when a temp view still exposes
        # an Iceberg TableReference (SPARK-57539 / Iceberg #16451). Materializing
        # once and rebuilding from the cached RDD both truncates that SQL lineage
        # and guarantees the two source reads performed by MERGE see identical data.
        already_cached = source.is_cached
        cached = source.persist()
        cached.count()
        materialized = session.createDataFrame(cached.rdd, schema=cached.schema)
        materialized.createOrReplaceTempView(view)
        condition = " AND ".join(f"target.{key} <=> source.{key}" for key in safe_keys)
        guard = version_guard(version_columns) if version_columns else ""
        # Guard contains only validated identifiers and fixed SQL operators.
        update = f"WHEN MATCHED {guard} THEN UPDATE SET *" if update_existing else ""  # nosec B608
        try:
            session.sql(
                f"""
                MERGE INTO {target} AS target
                USING {view} AS source
                ON {condition}
                {update}
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        finally:
            session.catalog.dropTempView(view)
            if not already_cached:
                cached.unpersist()

    def replace_keys(
        self,
        source: DataFrame,
        affected: DataFrame,
        namespace: str,
        table: str,
        keys: list[str],
        row_keys: list[str],
    ) -> None:
        """One atomic MERGE replaces affected groups, including groups now empty.

        A bounded key set stays distributed. Obsolete rows are explicit tombstones;
        no driver collection or non-atomic DELETE followed by INSERT is involved.
        Callers serialize writers and provide the entire state of affected groups.
        """
        target = self.table(namespace, table)
        existing = self.spark.table(target).join(
            affected.select(*keys).distinct(), keys, "left_semi"
        )
        removed = existing.join(source.select(*row_keys), row_keys, "left_anti")
        changes = (
            source.withColumn("_remove", F.lit(False))
            .unionByName(removed.withColumn("_remove", F.lit(True)))
            .persist()
        )
        view = f"replacement_{validate_identifier(table)}"
        try:
            changes.count()
            materialized = self.spark.createDataFrame(changes.rdd, changes.schema)
            materialized.createOrReplaceTempView(view)
            condition = " AND ".join(
                f"target.{validate_identifier(key)} <=> source.{validate_identifier(key)}"
                for key in row_keys
            )
            columns = [validate_identifier(column) for column in source.columns]
            assignments = ", ".join(f"target.{c} = source.{c}" for c in columns)
            values = ", ".join(f"source.{c}" for c in columns)
            # All interpolated values are validated identifiers or SQL assembled
            # exclusively from those identifiers. No row data is interpolated.
            statement = (
                f"MERGE INTO {target} target USING {view} source ON {condition} "  # noqa: S608  # nosec B608
                "WHEN MATCHED AND source._remove THEN DELETE "
                f"WHEN MATCHED THEN UPDATE SET {assignments} "
                "WHEN NOT MATCHED AND NOT source._remove "
                f"THEN INSERT ({', '.join(columns)}) VALUES ({values})"
            )
            self.spark.sql(statement)
        finally:
            self.spark.catalog.dropTempView(view)
            changes.unpersist()

    def current_snapshot_id(self, namespace: str, table: str) -> int | None:
        refs = self.spark.table(f"{self.table(namespace, table)}.refs")
        row = refs.where(F.col("name") == "main").select("snapshot_id").first()
        return int(row.snapshot_id) if row else None

    def replace_all(self, source: DataFrame, namespace: str, table: str) -> None:
        """Atomically reconcile a complete rebuilt dataset, including obsolete rows.

        Callers must supply the full source history and serialize writers. Iceberg's
        overwrite conflict validation prevents an overlapping concurrent overwrite.
        """
        cached = source.persist()
        try:
            cached.count()
            materialized = source.sparkSession.createDataFrame(cached.rdd, cached.schema)
            materialized.writeTo(self.table(namespace, table)).overwrite(F.lit(True))
        finally:
            cached.unpersist()

    def append_audit(self, run: PipelineRun) -> None:
        values = asdict(run)
        values["business_sum_checks"] = {
            key: str(value) for key, value in run.business_sum_checks.items()
        }
        # Explicit typing is required for valid audit records that contain null
        # snapshot IDs or empty check maps; Spark cannot infer those types.
        self.spark.createDataFrame([values], schema=PIPELINE_RUN_SCHEMA).writeTo(
            self.table("ops", "pipeline_runs")
        ).append()

    def create_candidate_branch(
        self, namespace: str, table: str, branch: str, retain_days: int = 7
    ) -> None:
        safe_branch = validate_identifier(branch)
        if safe_branch == "main":
            raise ValueError("candidate branch cannot be main")
        if not 1 <= retain_days <= 30:
            raise ValueError("candidate retention must be between 1 and 30 days")
        # Each build starts from the currently certified main snapshot. Reusing a
        # stale branch can create divergent history that cannot be fast-forwarded.
        snapshot_id = self.current_snapshot_id(namespace, table)
        if snapshot_id is None:
            raise ValueError("candidate branch requires an existing table snapshot")
        self.spark.sql(
            f"ALTER TABLE {self.table(namespace, table)} "
            f"CREATE OR REPLACE BRANCH {safe_branch} AS OF VERSION {snapshot_id} "
            f"RETAIN {retain_days} DAYS"
        )

    def fast_forward(self, namespace: str, table: str, branch: str) -> None:
        safe_namespace = validate_identifier(namespace)
        safe_table = validate_identifier(table)
        safe_branch = validate_identifier(branch)
        if safe_branch == "main":
            raise ValueError("source branch cannot be main")
        self.spark.sql(
            f"CALL {self.catalog}.system.fast_forward("
            f"table => '{safe_namespace}.{safe_table}', branch => 'main', to => '{safe_branch}')"
        )


def version_guard(columns: list[str]) -> str:
    """Lexicographic, null-safe strict version comparison for mutable entities."""
    safe = [validate_identifier(column) for column in columns]
    if not safe:
        raise ValueError("version columns cannot be empty")
    newer = []
    for index, column in enumerate(safe):
        equal = [f"source.{c} <=> target.{c}" for c in safe[:index]]
        greater = (
            f"(source.{column} > target.{column} OR "
            f"(target.{column} IS NULL AND source.{column} IS NOT NULL))"
        )
        newer.append("(" + " AND ".join([*equal, greater]) + ")")
    return "AND (" + " OR ".join(newer) + ")"
