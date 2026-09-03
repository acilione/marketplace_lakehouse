"""Typed, environment-aware application configuration."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class Environment(str, Enum):
    LOCAL = "local"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "kafka:9092"
    schema_registry_url: str = "http://schema-registry:8080/apis/registry/v3"
    topics: tuple[str, ...] = (
        "marketplace.orders.v1",
        "marketplace.payments.v1",
        "marketplace.inventory.v1",
        "marketplace.shipments.v1",
        "marketplace.customers.cdc.v1",
    )
    starting_offsets: Literal["earliest", "latest"] = "earliest"
    fail_on_data_loss: bool = True
    encoding: Literal["json", "avro"] = "json"
    event_schema_id: int = Field(default=1, ge=1)
    security_protocol: Literal["PLAINTEXT", "SASL_SSL", "SSL"] = "PLAINTEXT"


class CatalogSettings(BaseModel):
    name: str = "lakehouse"
    uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://warehouse/"
    branch: str = "main"
    s3_endpoint: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    s3_path_style_access: bool = True
    access_key: SecretStr | None = None
    secret_key: SecretStr | None = None


class ProcessingSettings(BaseModel):
    checkpoint_root: str = "s3a://checkpoints"
    trigger_interval: str = "30 seconds"
    watermark: str = "24 hours"
    shuffle_partitions: int = Field(default=16, ge=1, le=100_000)
    target_file_size_bytes: int = Field(default=536_870_912, ge=16_777_216)
    max_offsets_per_trigger: int = Field(default=100_000, ge=1)
    query_timeout_seconds: int = Field(default=300, ge=30)


class ObservabilitySettings(BaseModel):
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    metrics_port: int = Field(default=8000, ge=1024, le=65535)
    service_name: str = "marketplace-lakehouse"


class AppSettings(BaseSettings):
    """Settings resolved from YAML, then overridden by ``MLH_*`` environment values."""

    model_config = SettingsConfigDict(
        env_prefix="MLH_",
        env_nested_delimiter="__",
        env_file=None,
        extra="forbid",
        case_sensitive=False,
    )

    environment: Environment = Environment.LOCAL
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    catalog: CatalogSettings = Field(default_factory=CatalogSettings)
    processing: ProcessingSettings = Field(default_factory=ProcessingSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        del settings_cls, dotenv_settings
        return env_settings, init_settings, file_secret_settings

    @model_validator(mode="after")
    def reject_insecure_production_defaults(self) -> AppSettings:
        if self.environment is Environment.PRODUCTION:
            if self.kafka.security_protocol == "PLAINTEXT":
                raise ValueError("production Kafka must use TLS")
            if self.catalog.s3_endpoint.startswith("http://"):
                raise ValueError("production object storage must use TLS")
            if self.catalog.access_key or self.catalog.secret_key:
                raise ValueError("production must use workload identity, not static S3 credentials")
        return self

    def safe_dict(self) -> dict[str, Any]:
        """Return a loggable configuration without secret values."""
        return self.model_dump(mode="json", exclude={"catalog": {"access_key", "secret_key"}})


def load_settings(path: str | Path | None = None) -> AppSettings:
    """Load a versioned YAML profile with environment variables taking precedence."""
    values: dict[str, Any] = {}
    if path:
        config_path = Path(path)
        if not config_path.is_file():
            raise FileNotFoundError(f"configuration file does not exist: {config_path}")
        parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if parsed is not None and not isinstance(parsed, dict):
            raise ValueError("configuration root must be a mapping")
        values = parsed or {}
    return AppSettings(**values)
