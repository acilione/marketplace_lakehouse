"""Executable quality gates for candidate datasets."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    observed: str
    severity: str = "ERROR"


class QualityGateError(RuntimeError):
    def __init__(self, dataset: str, failures: list[CheckResult]) -> None:
        self.dataset = dataset
        self.failures = failures
        details = ", ".join(f"{item.name}={item.observed}" for item in failures)
        super().__init__(f"quality gate failed for {dataset}: {details}")


def _scalar(frame: DataFrame, expression: Column, alias: str = "value") -> Any:
    # Quality aggregates intentionally materialize exactly one bounded control row.
    row = frame.agg(expression.alias(alias)).first()
    if row is None:
        raise RuntimeError("quality aggregate unexpectedly returned no control row")
    return row[alias]


def non_null(frame: DataFrame, column: str) -> CheckResult:
    failures = int(_scalar(frame, F.sum(F.col(column).isNull().cast("long"))) or 0)
    return CheckResult(f"{column}_not_null", failures == 0, str(failures))


def unique(frame: DataFrame, columns: list[str]) -> CheckResult:
    duplicates = frame.groupBy(*columns).count().where(F.col("count") > 1).limit(1).count()
    name = "_".join(columns) + "_unique"
    return CheckResult(name, duplicates == 0, str(duplicates))


def accepted_values(frame: DataFrame, column: str, values: set[str]) -> CheckResult:
    invalid = int(
        _scalar(
            frame,
            F.sum((F.col(column).isNotNull() & ~F.col(column).isin(sorted(values))).cast("long")),
        )
        or 0
    )
    return CheckResult(f"{column}_domain", invalid == 0, str(invalid))


def non_negative(frame: DataFrame, column: str) -> CheckResult:
    invalid = int(_scalar(frame, F.sum((F.col(column) < 0).cast("long"))) or 0)
    return CheckResult(f"{column}_non_negative", invalid == 0, str(invalid))


def scd2_invariants(
    frame: DataFrame, key: str, valid_from: str = "valid_from", valid_to: str = "valid_to"
) -> list[CheckResult]:
    current_duplicates = (
        frame.groupBy(key)
        .agg(F.sum(F.col("is_current").cast("int")).alias("current_count"))
        .where(F.col("current_count") != 1)
        .limit(1)
        .count()
    )
    left = frame.alias("left")
    right = frame.alias("right")
    overlaps = (
        left.join(
            right,
            (F.col(f"left.{key}") == F.col(f"right.{key}"))
            & (F.col(f"left.{valid_from}") < F.col(f"right.{valid_from}"))
            & (
                F.col(f"left.{valid_to}").isNull()
                | (F.col(f"right.{valid_from}") < F.col(f"left.{valid_to}"))
            ),
        )
        .limit(1)
        .count()
    )
    return [
        CheckResult("exactly_one_current_row", current_duplicates == 0, str(current_duplicates)),
        CheckResult("non_overlapping_intervals", overlaps == 0, str(overlaps)),
    ]


def reconciliation(
    name: str,
    source_count: int,
    target_count: int,
    source_sum: Decimal | None = None,
    target_sum: Decimal | None = None,
) -> CheckResult:
    counts_match = source_count == target_count
    sums_match = source_sum is None or target_sum is None or source_sum == target_sum
    observed = f"count={source_count}/{target_count};sum={source_sum}/{target_sum}"
    return CheckResult(name, counts_match and sums_match, observed)


def enforce(dataset: str, results: list[CheckResult]) -> None:
    failures = [result for result in results if not result.passed and result.severity == "ERROR"]
    if failures:
        raise QualityGateError(dataset, failures)
