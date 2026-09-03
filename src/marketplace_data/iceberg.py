"""Validated Iceberg DDL, idempotent MERGE, audit, and branch publication."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
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

    def create_namespaces(self) -> None:
        for namespace in ("bronze", "silver", "gold", "quarantine", "ops"):
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{namespace}")

    def merge(
        self,
        source: DataFrame,
        namespace: str,
        table: str,
        keys: list[str],
        *,
        update_existing: bool = True,
        branch: str | None = None,
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
        cached = source.persist()
        cached.count()
        materialized = session.createDataFrame(cached.rdd, schema=cached.schema)
        materialized.createOrReplaceTempView(view)
        condition = " AND ".join(f"target.{key} <=> source.{key}" for key in safe_keys)
        update = "WHEN MATCHED THEN UPDATE SET *" if update_existing else ""
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
            cached.unpersist()

    def current_snapshot_id(self, namespace: str, table: str) -> int | None:
        snapshots = self.spark.table(f"{self.table(namespace, table)}.snapshots")
        row = snapshots.orderBy("committed_at", ascending=False).select("snapshot_id").first()
        return int(row.snapshot_id) if row else None

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

    @staticmethod
    def audit_json(run: PipelineRun) -> str:
        return json.dumps(asdict(run), default=str, sort_keys=True)
