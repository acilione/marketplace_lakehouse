from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("marketplace-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
