"""Reproducible Spark benchmark harness with mandatory environment evidence."""

from __future__ import annotations

import argparse
import json
import os
import platform
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Evidence:
    scenario: str
    recorded_at: str
    hardware: str
    python: str
    spark: str
    input_rows: int
    input_bytes: int | None
    executor_count: int
    executor_cores: int
    executor_memory: str
    shuffle_partitions: int
    runtime_seconds: float
    output_rows: int
    physical_plan: str
    extra: dict[str, Any]


def join_scenario(spark: SparkSession, rows: int, skew: int) -> tuple[int, str]:
    facts = spark.range(rows).select(
        F.col("id"),
        F.when(F.col("id") % 100 < skew, F.lit(0))
        .otherwise(F.col("id") % 10_000)
        .alias("seller_id"),
        (F.col("id") % 1_000).alias("amount"),
    )
    sellers = spark.range(10_000).select(
        F.col("id").alias("seller_id"), F.lit("STANDARD").alias("tier")
    )
    result = facts.join(sellers, "seller_id").groupBy("tier").agg(F.sum("amount").alias("amount"))
    output_rows = result.count()
    return output_rows, result._jdf.queryExecution().executedPlan().toString()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=["join"], default="join")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--skew-percent", type=int, default=80)
    parser.add_argument("--output", default="benchmark-results/join.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows < 1 or not 0 <= args.skew_percent <= 100:
        raise ValueError("rows must be positive and skew must be between 0 and 100")
    spark = (
        SparkSession.builder.appName(f"benchmark-{args.scenario}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )
    started = time.perf_counter()
    try:
        output_rows, plan = join_scenario(spark, args.rows, args.skew_percent)
        runtime = time.perf_counter() - started
        evidence = Evidence(
            scenario=args.scenario,
            recorded_at=datetime.now(timezone.utc).isoformat(),
            hardware=platform.platform(),
            python=platform.python_version(),
            spark=spark.version,
            input_rows=args.rows,
            input_bytes=None,
            executor_count=int(spark.conf.get("spark.executor.instances", "1")),
            executor_cores=int(spark.conf.get("spark.executor.cores", "1")),
            executor_memory=spark.conf.get("spark.executor.memory", "local-default"),
            shuffle_partitions=int(spark.conf.get("spark.sql.shuffle.partitions")),
            runtime_seconds=round(runtime, 6),
            output_rows=output_rows,
            physical_plan=plan,
            extra={
                "skew_percent": args.skew_percent,
                "host_cpu_count": os.cpu_count(),
                "claim_policy": "local measurements are not extrapolated to production",
            },
        )
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(evidence), indent=2), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
