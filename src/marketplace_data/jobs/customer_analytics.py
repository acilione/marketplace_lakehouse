"""Incremental purchases, affected-key marts and snapshot-pinned publication."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, observed, parser, spark_for
from marketplace_data.publication import PublicationStore
from marketplace_data.telemetry import record
from marketplace_data.transforms.analytics import (
    POLICY_VERSION,
    customer_cohorts,
    customer_daily,
    customer_profiles,
    latest_purchases,
    product_daily,
    purchase_changes,
    purchase_items,
)

PIPELINE = "customer_analytics_v1"


def rejected_purchases(changes: DataFrame) -> DataFrame:
    """Reject the whole order in this range if any version is invalid/ambiguous."""
    invalid = changes.where("""
        source IS NULL OR NOT source RLIKE '^[a-z][a-z0-9_-]{0,63}$'
        OR order_key IS NULL OR length(order_key) <= length(source) + 1
        OR version IS NULL OR version < 1 OR purchased_at IS NULL
        OR currency IS NULL OR NOT currency RLIKE '^[A-Z]{3}$'
        OR country IS NULL OR status IS NULL OR status NOT IN ('CONFIRMED','CANCELLED','REFUNDED')
        OR items IS NULL OR returns IS NULL
        OR invalid_money IS NULL OR invalid_money
        OR size(items) > 10000 OR size(returns) > 10000
        OR size(array_distinct(transform(items, x -> x.line_id))) <> size(items)
        OR size(array_distinct(transform(returns, x -> x.return_id))) <> size(returns)
        OR exists(items, x -> x.line_id IS NULL OR x.line_id = '' OR x.product_id IS NULL
          OR x.product_id = '' OR x.quantity IS NULL OR x.quantity <= 0
          OR x.amount IS NULL OR x.amount < 0)
        OR exists(returns, r -> r.return_id IS NULL OR r.return_id = '' OR r.line_id IS NULL
          OR r.quantity IS NULL OR r.quantity <= 0 OR r.amount IS NULL OR r.amount < 0
          OR r.occurred_at IS NULL OR r.occurred_at < purchased_at
          OR NOT exists(items, x -> x.line_id = r.line_id))
    """)
    conflict = (
        changes.groupBy("order_key", "version")
        .agg(F.countDistinct("content_hash").alias("variants"))
        .where("variants > 1")
    )
    validation_state = changes.dropDuplicates(["order_key", "version"]).withColumn(
        "status", F.lit("CONFIRMED")
    )
    invalid_returns = purchase_items(validation_state).where(
        "returned_quantity > quantity OR returned_amount > gross_amount"
    )
    reasons = (
        invalid.select("order_key")
        .withColumn("reason", F.lit("invalid purchase snapshot"))
        .union(
            conflict.select("order_key").withColumn(
                "reason", F.lit("conflicting payloads for the same order version")
            )
        )
        .union(
            invalid_returns.select("order_key").withColumn(
                "reason", F.lit("returns exceed purchased quantity or amount")
            )
        )
        .groupBy("order_key")
        .agg(F.concat_ws("; ", F.sort_array(F.collect_set("reason"))).alias("reason"))
    )
    return changes.join(reasons, "order_key")


def validate_purchases(changes: DataFrame) -> None:
    """Strict validation entry point for contracts and standalone checks."""
    row = rejected_purchases(changes).select("reason").first()
    if row:
        raise ValueError(row.reason)


def quarantine(repository: IcebergRepository, rejected: DataFrame, end: int) -> int:
    if "invalid_money" not in rejected.columns:
        rejected = rejected.withColumn("invalid_money", F.lit(False))
    records = (
        rejected.withColumn("source_snapshot", F.lit(str(end)))
        .withColumn(
            "rejection_id", F.sha2(F.concat_ws(":", "order_key", "content_hash", "reason"), 256)
        )
        .dropDuplicates(["rejection_id"])
    )
    count = records.count()
    if count:
        ensure_table(repository, records, "bronze", "purchase_rejections")
        repository.merge(records, "bronze", "purchase_rejections", ["rejection_id"])
    return count


def ensure_table(
    repository: IcebergRepository, frame: DataFrame, namespace: str, table: str
) -> None:
    name = repository.table(namespace, table)
    if not repository.spark.catalog.tableExists(name):
        # Bootstrap only the schema, not unvalidated rows. Monthly date partitions
        # suit the functional environment; evolve after measured large-scale tests.
        writer = frame.limit(0).writeTo(name).using("iceberg").tableProperty("format-version", "2")
        if "metric_date" in frame.columns:
            writer = writer.partitionedBy(F.months("metric_date"))
        writer.tableProperty("write.target-file-size-bytes", "536870912").create()


def source_range(repository: IcebergRepository, start: int | None, end: int) -> DataFrame:
    name = repository.table("bronze", "purchase_events")
    spark = repository.spark
    if start == end:
        return spark.read.option("versionAsOf", str(end)).table(name).limit(0)
    # Validate the actual ancestry, not timestamp ordering. Fail closed on expired
    # history or mutable source operations instead of silently skipping corrections.
    snapshots = {int(row.snapshot_id): row for row in spark.table(name + ".snapshots").collect()}
    cursor: int | None = end
    while cursor != start:
        if cursor is None or cursor not in snapshots:
            raise ValueError("source range no longer available; explicit recovery is required")
        row = snapshots[cursor]
        if row.operation != "append":
            raise ValueError("purchase raw source must be append-only; mutable snapshot detected")
        cursor = int(row.parent_id) if row.parent_id is not None else None
    reader = spark.read.format("iceberg")
    if start is None:
        return reader.option("versionAsOf", str(end)).load(name)
    return (
        reader.option("start-snapshot-id", str(start))
        .option("end-snapshot-id", str(end))
        .load(name)
    )


def build(
    repository: IcebergRepository,
    store: PublicationStore,
    as_of: date,
    *,
    public_demo: bool = False,
    public_dataset: bool = False,
) -> None:
    spark = repository.spark
    previous = store.current(PIPELINE)
    publication_tag = "publication_" + uuid4().hex
    end = repository.current_snapshot_id("bronze", "purchase_events")
    if end is None:
        raise ValueError("no purchase events; run purchase ingestion first")
    start = int(previous["source_snapshot"]) if previous else None
    if previous and as_of < date.fromisoformat(previous["as_of"]):
        raise ValueError("as-of date cannot move backwards; use an isolated backfill namespace")
    same_visibility = previous and (
        (previous["data_kind"] == "generated" and public_demo)
        or (previous["data_kind"] == "public_dataset" and public_dataset)
        or (previous["data_kind"] == "source" and not public_demo and not public_dataset)
    )
    if (
        previous
        and start == end
        and previous["as_of"] == as_of.isoformat()
        and same_visibility
        and "gold.customer_cohorts" in previous["snapshots"]
    ):
        # No business state, observation date or visibility change: keep the exact
        # existing publication instead of rescanning history or generating small files.
        # Do not update publication freshness: no new publication occurred.
        return
    # Restore the last published working baseline before every attempt. Otherwise
    # a failed v2 followed by v3 could leave v2-only products/customers behind.
    # Readers use pinned snapshots, so unpublished main changes are never served.
    for table in (
        "silver.purchase_orders",
        "silver.purchase_items",
        "gold.customer_daily",
        "gold.customer_profiles",
        "gold.product_daily",
        "gold.customer_cohorts",
    ):
        namespace, name = table.split(".")
        target = repository.table(namespace, name)
        if spark.catalog.tableExists(target):
            baseline = previous["snapshots"].get(table) if previous else None
            if baseline:
                current = repository.current_snapshot_id(namespace, name)
                if current != int(baseline):
                    spark.sql(
                        f"CALL {repository.catalog}.system.rollback_to_snapshot("
                        f"table => '{target}', snapshot_id => {int(baseline)})"
                    )
            else:
                repository.replace_all(spark.table(target).limit(0), namespace, name)
    changes = purchase_changes(source_range(repository, start, end)).persist()
    updates: DataFrame | None = None
    try:
        rejected = rejected_purchases(changes)
        rejection_count = quarantine(repository, rejected, end)
        accepted = changes.join(rejected.select("order_key").distinct(), "order_key", "left_anti")
        newest = latest_purchases(accepted).drop("invalid_money")
        ensure_table(repository, newest, "silver", "purchase_orders")

        # Always compare with last published state, NOT partially written main
        # from a crashed run. Every retry recomputes the same affected keys.
        def published(namespace: str, table: str) -> DataFrame:
            name = repository.table(namespace, table)
            snapshot = previous["snapshots"].get(f"{namespace}.{table}") if previous else None
            return (
                spark.read.option("versionAsOf", str(snapshot)).table(name)
                if snapshot
                else spark.table(name).limit(0)
            )

        old_orders = published("silver", "purchase_orders")
        both = newest.alias("n").join(old_orders.alias("o"), "order_key", "left")
        conflicts = both.where("n.version = o.version AND n.content_hash <> o.content_hash").select(
            "n.*"
        )
        rejection_count += quarantine(
            repository,
            conflicts.withColumn("reason", F.lit("equal-version conflict with published order")),
            end,
        )
        updates = both.where("o.version IS NULL OR n.version > o.version").select("n.*").persist()
        affected_orders = updates.select("order_key").distinct()
        old_affected = old_orders.join(affected_orders, "order_key", "left_semi")
        keys = (
            old_affected.select("customer_key")
            .union(updates.select("customer_key"))
            .where(F.col("customer_key").isNotNull())
            .distinct()
        )
        new_items = purchase_items(updates)
        ensure_table(repository, new_items, "silver", "purchase_items")
        old_items = published("silver", "purchase_items")
        old_changed_items = old_items.join(affected_orders, "order_key", "left_semi")
        products = (
            old_changed_items.select("product_key")
            .union(new_items.select("product_key"))
            .distinct()
        )
        repository.merge(
            updates, "silver", "purchase_orders", ["order_key"], version_columns=["version"]
        )
        repository.replace_keys(
            new_items,
            affected_orders,
            "silver",
            "purchase_items",
            ["order_key"],
            ["order_key", "line_id"],
        )
        # Canonical recomputation for affected keys, never additive totals.
        all_items = spark.table(repository.table("silver", "purchase_items"))
        daily = customer_daily(all_items.join(keys, "customer_key", "left_semi"))
        ensure_table(repository, daily, "gold", "customer_daily")
        repository.replace_keys(
            daily,
            keys,
            "gold",
            "customer_daily",
            ["customer_key"],
            ["customer_key", "currency", "metric_date"],
        )
        days = spark.table(repository.table("gold", "customer_daily"))
        aging = not previous or previous["as_of"] != as_of.isoformat()
        profile_keys = days.select("customer_key").union(keys).distinct() if aging else keys
        profiles = customer_profiles(days.join(profile_keys, "customer_key", "left_semi"), as_of)
        ensure_table(repository, profiles, "gold", "customer_profiles")
        repository.replace_keys(
            profiles,
            profile_keys,
            "gold",
            "customer_profiles",
            ["customer_key"],
            ["customer_key", "currency"],
        )
        product_metrics = product_daily(all_items.join(products, "product_key", "left_semi"))
        ensure_table(repository, product_metrics, "gold", "product_daily")
        repository.replace_keys(
            product_metrics,
            products,
            "gold",
            "product_daily",
            ["product_key"],
            ["product_key", "currency", "metric_date"],
        )
        cohorts = customer_cohorts(days, as_of)
        ensure_table(repository, cohorts, "gold", "customer_cohorts")
        repository.replace_all(cohorts, "gold", "customer_cohorts")
        snapshots: dict[str, str] = {}
        for namespace, table in (
            ("silver", "purchase_orders"),
            ("silver", "purchase_items"),
            ("gold", "customer_daily"),
            ("gold", "customer_profiles"),
            ("gold", "product_daily"),
            ("gold", "customer_cohorts"),
        ):
            snapshot = repository.current_snapshot_id(namespace, table)
            if snapshot is None:
                raise ValueError(f"missing output snapshot: {namespace}.{table}")
            snapshots[f"{namespace}.{table}"] = str(snapshot)
            # Named tags protect published snapshots from normal expiration.
            # A separate retention procedure must release obsolete publication tags.
            spark.sql(
                f"ALTER TABLE {repository.table(namespace, table)} CREATE OR REPLACE TAG "
                f"{publication_tag} AS OF VERSION {snapshot}"
            )
        source_table = repository.table("bronze", "purchase_events")
        spark.sql(
            f"ALTER TABLE {source_table} CREATE OR REPLACE TAG analytics_input_{end} "
            f"AS OF VERSION {end}"
        )
        real_source = (
            spark.table(repository.table("silver", "purchase_orders"))
            .where(~F.col("source").startswith("generated-"))
            .limit(1)
            .count()
        )
        only_public_dataset = (
            not spark.table(repository.table("silver", "purchase_orders"))
            .where(F.col("source") != "uci-online-retail-ii")
            .limit(1)
            .count()
        )
        document: dict[str, Any] = {
            "source_snapshot": str(end),
            "tag": publication_tag,
            "source_start": str(start) if start else None,
            "snapshots": snapshots,
            "as_of": as_of.isoformat(),
            "policy_version": POLICY_VERSION,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "data_kind": "generated" if public_demo and not real_source else "source",
            "rejected_snapshots": rejection_count,
        }
        if public_dataset and only_public_dataset:
            document["data_kind"] = "public_dataset"
            document["dataset"] = (
                "UCI Online Retail II · positive product purchases; unlinked cancellations excluded"
            )
        store.publish(PIPELINE, document)
        record(
            PIPELINE,
            rejected_snapshots=rejection_count,
            publication=datetime.now(timezone.utc).timestamp(),
        )
    finally:
        if updates is not None:
            updates.unpersist()
        changes.unpersist()


@observed
def main(argv: list[str] | None = None) -> None:
    cli = parser(__doc__)
    cli.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    cli.add_argument(
        "--public-demo",
        action="store_true",
        help="Allow generated-only publications in the local public dashboard",
    )
    cli.add_argument(
        "--public-dataset",
        action="store_true",
        help="Allow the allowlisted UCI dataset, exclusively, in guest analytics",
    )
    cli.add_argument(
        "--evidence",
        action="store_true",
        help="Emit bounded physical benchmark evidence after successful publication",
    )
    args = cli.parse_args(argv)
    if args.dry_run:
        cli.error("use transform tests for dry runs; publication requires durable candidate writes")
    job = context(PIPELINE, args.config, args.run_id)
    spark = spark_for(job)
    spark.sparkContext.setLogLevel("WARN")
    store = PublicationStore(os.environ["ANALYTICS_CONTROL_DSN"])
    try:
        with store.writer(PIPELINE):
            build(
                IcebergRepository(spark, job.settings.catalog.name),
                store,
                args.as_of,
                public_demo=args.public_demo,
                public_dataset=args.public_dataset,
            )
            if args.evidence:
                from marketplace_data.benchmark_evidence import collect

                print(  # noqa: T201
                    "ANALYTICS_EVIDENCE "
                    + json.dumps(collect(IcebergRepository(spark, job.settings.catalog.name)))
                )
    finally:
        store.close()
        spark.stop()


if __name__ == "__main__":
    main()
