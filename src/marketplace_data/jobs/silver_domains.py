"""Conform payment, shipment, and inventory domain facts."""

from __future__ import annotations

from marketplace_data.iceberg import IcebergRepository
from marketplace_data.jobs.common import context, parser, spark_for
from marketplace_data.quality import accepted_values, enforce, non_negative, non_null, unique
from marketplace_data.tables import bootstrap_tables
from marketplace_data.transforms.domains import conform_payments, conform_shipments, inventory_daily


def main(argv: list[str] | None = None) -> None:
    args = parser(__doc__).parse_args(argv)
    job = context("silver_domain_conform", args.config, args.run_id)
    spark = spark_for(job)
    repository = IcebergRepository(spark, job.settings.catalog.name)
    bootstrap_tables(spark, job.settings.catalog.name)
    bronze = spark.table(repository.table("bronze", "marketplace_events"))
    payments = conform_payments(bronze).persist()
    shipments = conform_shipments(bronze).persist()
    inventory = inventory_daily(bronze).persist()
    try:
        enforce(
            repository.table("silver", "payments"),
            [
                non_null(payments, "payment_id"),
                unique(payments, ["payment_id"]),
                non_negative(payments, "amount"),
                accepted_values(payments, "currency", {"EUR", "USD", "GBP", "CHF"}),
            ],
        )
        enforce(
            repository.table("silver", "shipments"),
            [non_null(shipments, "shipment_id"), unique(shipments, ["shipment_id"])],
        )
        enforce(
            repository.table("silver", "inventory_daily"),
            [unique(inventory, ["sku", "location_id", "date"])],
        )
        if not args.dry_run:
            repository.merge(payments, "silver", "payments", ["payment_id"])
            repository.merge(shipments, "silver", "shipments", ["shipment_id"])
            repository.merge(
                inventory,
                "silver",
                "inventory_daily",
                ["sku", "location_id", "date"],
            )
    finally:
        payments.unpersist()
        shipments.unpersist()
        inventory.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
