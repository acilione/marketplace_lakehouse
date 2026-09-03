"""Snapshot-ordered marketplace pipelines; transformation code remains in Spark apps."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3",
        "org.apache.spark:spark-avro_2.13:4.1.3",
    ]
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}


def spark_task(task_id: str, application: str, application_args: list[str]) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_standalone",
        application=application,
        application_args=application_args,
        packages=SPARK_PACKAGES,
        deploy_mode="client",
        conf={
            "spark.sql.session.timeZone": "UTC",
            "spark.eventLog.enabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        },
        verbose=False,
    )


with DAG(
    dag_id="marketplace_incremental",
    description="Five-minute snapshot-ordered marketplace conformance",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "spark", "iceberg"],
) as dag:
    ingest = spark_task(
        "bronze_available_now",
        "/opt/marketplace/apps/bronze_ingest/main.py",
        ["--config", "/opt/marketplace/config/local.yaml", "--available-now"],
    )
    orders = spark_task(
        "silver_orders",
        "/opt/marketplace/apps/silver_conform/orders.py",
        ["--config", "/opt/marketplace/config/local.yaml"],
    )
    customers = spark_task(
        "silver_customers",
        "/opt/marketplace/apps/silver_conform/customers.py",
        ["--config", "/opt/marketplace/config/local.yaml"],
    )
    domains = spark_task(
        "silver_domains",
        "/opt/marketplace/apps/silver_conform/domains.py",
        ["--config", "/opt/marketplace/config/local.yaml"],
    )
    gold = spark_task(
        "gold_kpis_candidate",
        "/opt/marketplace/apps/gold_marts/marketplace_kpis.py",
        [
            "--config",
            "/opt/marketplace/config/local.yaml",
            "--candidate-branch",
            "airflow_candidate_{{ ts_nodash | lower }}",
            "--publish",
        ],
    )

    ingest >> [orders, customers, domains]  # type: ignore[operator]
    [orders, domains] >> gold
