import json
import re
from pathlib import Path

import pytest

from marketplace_data.telemetry import PipelineCollector, record


def test_last_failure_survives_success_and_is_exported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MLH_METRICS_DIRECTORY", str(tmp_path))
    record("test_pipeline", last_failure=100, last_quality_failure=100)
    record("test_pipeline", last_success=200)
    assert json.loads((tmp_path / "test_pipeline.json").read_text())["last_failure"] == 100
    samples = [sample for family in PipelineCollector().collect() for sample in family.samples]
    assert any(
        sample.name == "marketplace_pipeline_last_quality_failure_seconds" and sample.value == 100
        for sample in samples
    )


def test_grafana_only_queries_metrics_emitted_by_durable_exporter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MLH_METRICS_DIRECTORY", str(tmp_path))
    record("test_pipeline", last_success=200)
    exported = {
        sample.name for family in PipelineCollector().collect() for sample in family.samples
    }
    root = Path(__file__).resolve().parents[1]
    dashboard = json.loads(
        (root / "observability/grafana/dashboards/marketplace-platform.json").read_text()
    )
    queries = [target["expr"] for panel in dashboard["panels"] for target in panel["targets"]]
    referenced = set(re.findall(r"\bmarketplace_[a-z_]+\b", " ".join(queries)))
    assert referenced
    assert referenced <= exported, f"Grafana queries unavailable metrics: {referenced - exported}"
