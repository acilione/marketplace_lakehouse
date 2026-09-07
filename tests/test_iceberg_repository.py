from unittest.mock import MagicMock

from pyspark.sql import SparkSession

from marketplace_data.iceberg import IcebergRepository


def test_current_snapshot_reads_main_not_newer_candidate(spark: SparkSession) -> None:
    session = MagicMock()
    session.table.return_value = spark.createDataFrame(
        [("main", 100), ("candidate", 200)], ["name", "snapshot_id"]
    )
    repository = IcebergRepository(session)
    assert repository.current_snapshot_id("gold", "daily_marketplace_kpis") == 100
    session.table.assert_called_once_with("lakehouse.gold.daily_marketplace_kpis.refs")


def test_replace_all_deletes_obsolete_rows_atomically(spark: SparkSession) -> None:
    source = MagicMock()
    source.persist.return_value = source
    repository = IcebergRepository(spark)
    repository.replace_all(source, "silver", "customers_scd2")
    writer = source.sparkSession.createDataFrame.return_value.writeTo
    writer.assert_called_once_with("lakehouse.silver.customers_scd2")
    writer.return_value.overwrite.assert_called_once()
    source.unpersist.assert_called_once()
