"""Spark session construction and compatibility configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession

from marketplace_data.config import AppSettings, Environment


def build_spark_session(app_name: str, settings: AppSettings) -> SparkSession:
    catalog = settings.catalog
    processing = settings.processing
    prefix = f"spark.sql.catalog.{catalog.name}"
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(processing.shuffle_partitions))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", catalog.uri)
        .config(f"{prefix}.warehouse", catalog.warehouse)
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.endpoint", catalog.s3_endpoint)
        .config(f"{prefix}.s3.region", catalog.s3_region)
        .config(f"{prefix}.s3.path-style-access", str(catalog.s3_path_style_access).lower())
        # Iceberg uses S3FileIO, while Structured Streaming checkpoints use Hadoop S3A.
        # Configure both clients explicitly so neither can silently fall back to public AWS.
        .config("spark.hadoop.fs.s3a.endpoint", catalog.s3_endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", catalog.s3_region)
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            str(catalog.s3_path_style_access).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(catalog.s3_endpoint.lower().startswith("https://")).lower(),
        )
        .config("spark.sql.defaultCatalog", catalog.name)
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "false")
    )

    # Local MinIO cannot provide workload identity. Never inject static secrets outside local/test.
    if settings.environment in {Environment.LOCAL, Environment.TEST}:
        if catalog.access_key:
            access_key = catalog.access_key.get_secret_value()
            builder = builder.config(f"{prefix}.s3.access-key-id", access_key)
            builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
        if catalog.secret_key:
            secret_key = catalog.secret_key.get_secret_value()
            builder = builder.config(f"{prefix}.s3.secret-access-key", secret_key)
            builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    return builder.getOrCreate()
