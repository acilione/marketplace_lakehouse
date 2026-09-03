"""Governed Iceberg table definitions and properties."""

from __future__ import annotations

from pyspark.sql import SparkSession

from marketplace_data.util import validate_identifier

TABLE_DDL = (
    """
    CREATE TABLE IF NOT EXISTS {catalog}.bronze.marketplace_events (
      event_id STRING NOT NULL, event_type STRING NOT NULL, event_version INT NOT NULL,
      occurred_at TIMESTAMP NOT NULL, produced_at TIMESTAMP NOT NULL, producer STRING NOT NULL,
      trace_id STRING NOT NULL, partition_key STRING NOT NULL, payload MAP<STRING, STRING> NOT NULL,
      producer_sequence BIGINT, source_topic STRING NOT NULL, source_partition INT NOT NULL,
      source_offset BIGINT NOT NULL, source_timestamp TIMESTAMP NOT NULL,
      ingested_at TIMESTAMP NOT NULL,
      ingestion_run_id STRING NOT NULL, quality_flags ARRAY<STRING> NOT NULL
    ) USING iceberg PARTITIONED BY (days(ingested_at))
    TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='536870912',
      'governance.owner'='data-platform', 'classification'='restricted', 'retention.days'='90',
      'freshness.slo.minutes'='3')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.quarantine.marketplace_events (
      source_topic STRING NOT NULL, source_partition INT NOT NULL, source_offset BIGINT NOT NULL,
      source_timestamp TIMESTAMP, raw_value STRING, error_code STRING NOT NULL,
      error_details ARRAY<STRING> NOT NULL, first_seen_at TIMESTAMP NOT NULL,
      ingestion_run_id STRING NOT NULL, remediation_status STRING NOT NULL,
      remediation_batch_id STRING
    ) USING iceberg PARTITIONED BY (days(first_seen_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='data-operations',
      'classification'='restricted', 'retention.days'='90')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.orders (
      order_id STRING NOT NULL, seller_id STRING NOT NULL, customer_token STRING NOT NULL,
      market STRING NOT NULL, currency STRING NOT NULL, gross_amount DECIMAL(20,2) NOT NULL,
      status STRING NOT NULL, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
      last_event_id STRING NOT NULL, source_partition INT NOT NULL, source_offset BIGINT NOT NULL
    ) USING iceberg PARTITIONED BY (months(created_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='marketplace-analytics',
      'classification'='internal', 'retention.months'='24', 'freshness.slo.minutes'='15')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.order_status_history (
      event_id STRING NOT NULL, order_id STRING NOT NULL, status STRING NOT NULL,
      occurred_at TIMESTAMP NOT NULL, source_partition INT NOT NULL, source_offset BIGINT NOT NULL
    ) USING iceberg PARTITIONED BY (months(occurred_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='marketplace-analytics',
      'classification'='internal', 'retention.months'='24')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.order_lines (
      event_id STRING NOT NULL, order_id STRING NOT NULL, sku STRING NOT NULL,
      location_id STRING NOT NULL, quantity BIGINT NOT NULL, created_at TIMESTAMP NOT NULL,
      source_partition INT NOT NULL, source_offset BIGINT NOT NULL
    ) USING iceberg PARTITIONED BY (months(created_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='marketplace-analytics',
      'classification'='internal', 'retention.months'='24')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.payments (
      payment_id STRING NOT NULL, order_id STRING NOT NULL, currency STRING NOT NULL,
      amount DECIMAL(20,2) NOT NULL, status STRING NOT NULL, updated_at TIMESTAMP NOT NULL,
      last_event_id STRING NOT NULL, source_partition INT NOT NULL, source_offset BIGINT NOT NULL
    ) USING iceberg PARTITIONED BY (months(updated_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='payments-analytics',
      'classification'='internal', 'retention.months'='24', 'freshness.slo.minutes'='15')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.shipments (
      shipment_id STRING NOT NULL, order_id STRING NOT NULL, status STRING NOT NULL,
      promised_at TIMESTAMP, delivered_at TIMESTAMP, updated_at TIMESTAMP NOT NULL,
      last_event_id STRING NOT NULL, source_partition INT NOT NULL, source_offset BIGINT NOT NULL
    ) USING iceberg PARTITIONED BY (months(updated_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='fulfilment-analytics',
      'classification'='internal', 'retention.months'='24', 'freshness.slo.minutes'='15')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.inventory_daily (
      sku STRING NOT NULL, location_id STRING NOT NULL, date DATE NOT NULL,
      daily_delta BIGINT NOT NULL, available_units BIGINT NOT NULL, is_stockout BOOLEAN NOT NULL
    ) USING iceberg PARTITIONED BY (months(date))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='inventory-analytics',
      'classification'='internal', 'retention.months'='24', 'freshness.slo.minutes'='15')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.customers_scd2 (
      customer_id STRING NOT NULL, email_token STRING, country STRING, customer_tier STRING,
      is_deleted BOOLEAN NOT NULL, valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP,
      is_current BOOLEAN NOT NULL, source_event_id STRING NOT NULL, source_lsn BIGINT,
      attribute_hash STRING NOT NULL
    ) USING iceberg PARTITIONED BY (bucket(64, customer_id))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='customer-domain',
      'classification'='confidential-tokenized', 'freshness.slo.minutes'='15')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.gold.daily_marketplace_kpis (
      metric_date DATE NOT NULL, market STRING NOT NULL, seller_tier STRING NOT NULL,
      order_count BIGINT NOT NULL, gmv DECIMAL(38,2) NOT NULL,
      net_revenue DECIMAL(38,2) NOT NULL, cancelled_orders BIGINT NOT NULL,
      cancellation_rate DECIMAL(12,6)
    ) USING iceberg PARTITIONED BY (months(metric_date))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='finance-analytics',
      'classification'='internal', 'retention.months'='36', 'freshness.slo.minutes'='360')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.gold.demand_features (
      sku STRING NOT NULL, location_id STRING NOT NULL, forecast_date DATE NOT NULL,
      demand_units BIGINT NOT NULL, demand_lag_1 BIGINT, demand_lag_7 BIGINT,
      demand_avg_7 DOUBLE, demand_avg_28 DOUBLE, available_units BIGINT,
      is_stockout BOOLEAN
    ) USING iceberg PARTITIONED BY (months(forecast_date))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='demand-science',
      'classification'='internal', 'retention.months'='18', 'freshness.slo.minutes'='1440')
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.ops.pipeline_runs (
      run_id STRING NOT NULL, pipeline_name STRING NOT NULL, environment STRING NOT NULL,
      code_sha STRING NOT NULL, config_hash STRING NOT NULL, input_evidence STRING NOT NULL,
      output_snapshot_id BIGINT, rows_read BIGINT NOT NULL, rows_written BIGINT NOT NULL,
      rows_quarantined BIGINT NOT NULL, business_sum_checks MAP<STRING, STRING> NOT NULL,
      quality_status STRING NOT NULL, started_at TIMESTAMP NOT NULL, ended_at TIMESTAMP NOT NULL
    ) USING iceberg PARTITIONED BY (days(started_at))
    TBLPROPERTIES ('format-version'='2', 'governance.owner'='data-platform',
      'classification'='internal-audit', 'write.delete.mode'='merge-on-read')
    """,
)


def bootstrap_tables(spark: SparkSession, catalog: str = "lakehouse") -> None:
    safe_catalog = validate_identifier(catalog)
    for namespace in ("bronze", "silver", "gold", "quarantine", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {safe_catalog}.{namespace}")
    for ddl in TABLE_DDL:
        spark.sql(ddl.format(catalog=safe_catalog))
