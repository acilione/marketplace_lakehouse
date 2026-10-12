"""Iceberg/PostgreSQL integration: partial failure, correction, replay and equivalence.

Run only in the local isolated demonstration. Uses a dedicated generated source;
its final order is cancelled, leaving no analytical contributions.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

from marketplace_data.analytics_contracts import PurchaseSnapshot
from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, spark_for
from marketplace_data.jobs.customer_analytics import PIPELINE, build
from marketplace_data.publication import PublicationStore
from marketplace_data.purchase_source import envelope
from marketplace_data.schemas import BRONZE_SCHEMA
from marketplace_data.transforms.analytics import (
    customer_cohorts,
    customer_daily,
    customer_profiles,
    product_daily,
)


def main() -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument(
        "--reference-only",
        action="store_true",
        help="Compare published marts with canonical facts without generating fixtures",
    )
    args = cli.parse_args()
    job = context("analytics_verification", "config/local.yaml", None)
    spark = spark_for(job)
    spark.sparkContext.setLogLevel("WARN")
    repository = IcebergRepository(spark)
    store = PublicationStore(os.environ["ANALYTICS_CONTROL_DSN"])
    source = "generated-verification-" + uuid4().hex[:8]
    previous = store.current(PIPELINE)
    if not previous or previous["data_kind"] != "generated":
        raise RuntimeError("run only after a generated analytics demonstration")
    as_of = date.fromisoformat(previous["as_of"])
    now = datetime.now(timezone.utc)

    def append(version: int, owner: str, status: str = "CONFIRMED", refund: bool = False) -> None:
        purchase = PurchaseSnapshot.model_validate(
            {
                "source": source,
                "order_id": "order-1",
                "version": version,
                "customer_id": owner,
                "currency": "EUR",
                "country": "IT",
                "purchased_at": f"{as_of}T00:00:00Z",
                "status": status,
                "items": [{"line_id": "1", "product_id": owner, "quantity": 2, "amount": "20.00"}],
                "returns": [
                    {
                        "return_id": "r1",
                        "line_id": "1",
                        "quantity": 1,
                        "amount": "10.00",
                        "occurred_at": f"{as_of}T01:00:00Z",
                    }
                ]
                if refund
                else [],
            }
        )
        event = envelope(purchase).model_dump()
        event.update(
            source_topic="integration",
            source_partition=0,
            source_offset=version,
            source_timestamp=now,
            ingested_at=now,
            ingestion_run_id=source,
            quality_flags=[],
        )
        spark.createDataFrame([event], BRONZE_SCHEMA).writeTo(
            repository.table("bronze", "purchase_events")
        ).append()

    def publish() -> None:
        build(repository, store, as_of, public_demo=True)

    try:
        with store.writer(PIPELINE):
            if args.reference_only:
                items = spark.read.option(
                    "versionAsOf", previous["snapshots"]["silver.purchase_items"]
                ).table(repository.table("silver", "purchase_items"))
                daily = customer_daily(items)
                for table, expected in (
                    ("customer_daily", daily),
                    ("customer_profiles", customer_profiles(daily, as_of)),
                    ("product_daily", product_daily(items)),
                    ("customer_cohorts", customer_cohorts(daily, as_of)),
                ):
                    actual = (
                        spark.read.option("versionAsOf", previous["snapshots"]["gold." + table])
                        .table(repository.table("gold", table))
                        .select(*expected.columns)
                    )
                    assert actual.exceptAll(expected).limit(1).count() == 0, table
                    assert expected.exceptAll(actual).limit(1).count() == 0, table
                return
            competitor = PublicationStore(os.environ["ANALYTICS_CONTROL_DSN"])
            try:
                try:
                    with competitor.writer(PIPELINE):
                        raise AssertionError("concurrent writer acquired lock")
                except RuntimeError as error:
                    assert "another writer" in str(error)
            finally:
                competitor.close()
            append(1, "original")
            append(1, "original")  # transport duplicate
            publish()
            good = store.current(PIPELINE)
            append(2, "abandoned")
            real_publish = store.publish

            def fail_pointer(*args: object) -> None:
                raise RuntimeError("injected publication failure")

            store.publish = fail_pointer  # type: ignore[method-assign]
            try:
                publish()
                raise AssertionError("fault was not injected")
            except RuntimeError as error:
                assert "injected publication failure" in str(error)
            finally:
                store.publish = real_publish  # type: ignore[method-assign]
            assert store.current(PIPELINE) == good
            append(3, "current")
            publish()
            profiles = spark.table(repository.table("gold", "customer_profiles"))
            assert (
                profiles.where(
                    F.col("customer_key").isin(source + ":original", source + ":abandoned")
                ).count()
                == 0
            )
            assert (
                profiles.where(F.col("customer_key") == source + ":current").first().orders_90d == 1
            )
            assert (
                spark.table(repository.table("gold", "product_daily"))
                .where(F.col("product_key") == source + ":abandoned")
                .count()
                == 0
            )
            # Old late version cannot regress the current owner or totals.
            append(1, "original")
            append(4, "current", refund=True)
            publish()
            assert (
                spark.table(repository.table("gold", "customer_profiles"))
                .where(F.col("customer_key") == source + ":current")
                .first()
                .net_amount_90d
                == 10
            )
            # An ambiguous same-version snapshot is quarantined; the published owner is retained.
            append(4, "conflicting")
            publish()
            assert store.current(PIPELINE)["rejected_snapshots"] == 1
            assert (
                spark.table(repository.table("bronze", "purchase_rejections"))
                .where(F.col("order_key") == source + ":order-1")
                .count()
                == 1
            )
            assert (
                spark.table(repository.table("gold", "customer_profiles"))
                .where(F.col("customer_key") == source + ":current")
                .first()
                .net_amount_90d
                == 10
            )
            append(5, "current", status="CANCELLED")
            publish()
            # A cancelled final order must remove the now-empty customer/product groups.
            assert (
                spark.table(repository.table("gold", "customer_profiles"))
                .where(F.col("customer_key").startswith(source))
                .count()
                == 0
            )
            items = spark.table(repository.table("silver", "purchase_items"))
            daily = customer_daily(items)
            for table, expected in (
                ("customer_daily", daily),
                ("customer_profiles", customer_profiles(daily, as_of)),
                ("product_daily", product_daily(items)),
                ("customer_cohorts", customer_cohorts(daily, as_of)),
            ):
                actual = spark.table(repository.table("gold", table)).select(*expected.columns)
                assert actual.exceptAll(expected).limit(1).count() == 0, table
                assert expected.exceptAll(actual).limit(1).count() == 0, table
            # No-input rerun remains equivalent, including recency and rolling windows.
            publish()
            actual = spark.table(repository.table("gold", "customer_profiles"))
            assert actual.exceptAll(customer_profiles(daily, as_of)).limit(1).count() == 0
    finally:
        store.close()
        spark.stop()


if __name__ == "__main__":
    main()
