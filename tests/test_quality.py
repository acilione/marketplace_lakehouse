from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from marketplace_data.quality import (
    QualityGateError,
    accepted_values,
    enforce,
    non_negative,
    non_null,
    reconciliation,
    scd2_invariants,
    unique,
)


def test_quality_gate_blocks_invalid_candidate(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a", 1), ("a", -1), (None, 3)], ["id", "amount"])
    results = [non_null(frame, "id"), unique(frame, ["id"]), non_negative(frame, "amount")]

    with pytest.raises(QualityGateError) as error:
        enforce("candidate", results)
    assert len(error.value.failures) == 3


def test_domain_reconciliation_and_scd2_checks(spark: SparkSession) -> None:
    domain = spark.createDataFrame([("EUR",), ("NOPE",)], ["currency"])
    assert not accepted_values(domain, "currency", {"EUR", "USD"}).passed
    assert reconciliation("control", 2, 2).passed
    assert not reconciliation("control", 2, 1).passed

    history = spark.createDataFrame(
        [
            ("c1", "2026-01-01", "2026-02-01", False),
            ("c1", "2026-02-01", None, True),
        ],
        "customer_id string, valid_from string, valid_to string, is_current boolean",
    ).selectExpr(
        "customer_id",
        "cast(valid_from as timestamp) valid_from",
        "cast(valid_to as timestamp) valid_to",
        "is_current",
    )
    assert all(result.passed for result in scd2_invariants(history, "customer_id"))
