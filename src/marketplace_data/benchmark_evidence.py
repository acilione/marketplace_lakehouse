"""Explicit opt-in physical evidence; never include credentials from Spark configuration."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from pyspark.sql import functions as F

from marketplace_data.iceberg import IcebergRepository


def collect(repository: IcebergRepository) -> dict[str, Any]:
    spark = repository.spark
    context = spark.sparkContext
    result: dict[str, Any] = {
        "spark_version": spark.version,
        "application_id": context.applicationId,
        "configuration": {
            key: context.getConf().get(key, "unset")
            for key in (
                "spark.master",
                "spark.executor.memory",
                "spark.executor.cores",
                "spark.cores.max",
                "spark.sql.shuffle.partitions",
            )
        },
        "tables": {},
    }
    if context.uiWebUrl:
        try:
            url = f"{context.uiWebUrl}/api/v1/applications/{context.applicationId}/stages"
            if not url.startswith(("http://", "https://")):
                raise ValueError("Spark evidence requires an HTTP UI endpoint")
            with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 # nosec B310
                stages = json.load(response)
            fields = (
                "inputBytes",
                "inputRecords",
                "shuffleReadBytes",
                "shuffleWriteBytes",
                "memoryBytesSpilled",
                "diskBytesSpilled",
                "executorRunTime",
                "executorCpuTime",
            )
            result["stage_totals"] = {
                key: sum(stage.get(key, 0) for stage in stages) for key in fields
            }
            result["retained_stages"] = len(stages)
            result["stage_metrics_note"] = (
                "Spark REST retained stages; totals include repeated reads and attempts, "
                "not unique input size."
            )
        except (OSError, ValueError) as error:
            result["stage_metrics_unavailable"] = type(error).__name__
    for namespace, table in (
        ("bronze", "purchase_events"),
        ("silver", "purchase_items"),
        ("gold", "customer_profiles"),
        ("gold", "product_daily"),
        ("gold", "customer_cohorts"),
    ):
        name = repository.table(namespace, table)
        # Metadata-only file statistics, not an additional full data count.
        row = (
            spark.table(name + ".files")
            .agg(
                F.count("*").alias("files"),
                F.sum("record_count").alias("records"),
                F.sum("file_size_in_bytes").alias("bytes"),
            )
            .first()
        )
        result["tables"][f"{namespace}.{table}"] = row.asDict() if row else {}
    return result
