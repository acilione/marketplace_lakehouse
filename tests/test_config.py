from __future__ import annotations

import pytest

from marketplace_data.config import AppSettings, Environment, load_settings


def test_local_profile_loads_and_redacts_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MLH_PROCESSING__SHUFFLE_PARTITIONS", "24")
    monkeypatch.setenv("MLH_CATALOG__SECRET_KEY", "local-secret")
    settings = load_settings("config/local.yaml")

    assert settings.environment is Environment.LOCAL
    assert settings.processing.shuffle_partitions == 24
    assert "secret_key" not in settings.safe_dict()["catalog"]


def test_production_rejects_plaintext_dependencies() -> None:
    with pytest.raises(ValueError, match="production Kafka must use TLS"):
        AppSettings(environment="production")


def test_configuration_rejects_unknown_keys(tmp_path) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "invalid.yaml"
    path.write_text("unknown: value\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_settings(path)


def test_missing_configuration_is_explicit(tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(FileNotFoundError, match="does not exist"):
        load_settings(tmp_path / "missing.yaml")


@pytest.mark.parametrize(
    "value", ["{}", '{"topic": {"0": -1}}', '{"topic": {"x": 0}}', "[]", '{"topic": []}']
)
def test_rejects_invalid_explicit_kafka_offsets(value: str) -> None:
    from marketplace_data.config import KafkaSettings

    with pytest.raises(ValueError):
        KafkaSettings(starting_offsets=value)


def test_accepts_captured_offsets() -> None:
    from marketplace_data.config import KafkaSettings

    value = '{"topic": {"0": 42, "1": 0}}'
    assert KafkaSettings(starting_offsets=value).starting_offsets == value
