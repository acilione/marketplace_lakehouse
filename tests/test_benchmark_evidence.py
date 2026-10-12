import io
import json
from unittest.mock import MagicMock

import pytest

from marketplace_data.benchmark_evidence import collect


def test_benchmark_evidence_reports_physical_metrics_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = MagicMock()
    repository.spark.version = "4.1.3"
    context = repository.spark.sparkContext
    context.applicationId = "test-app"
    context.uiWebUrl = "http://localhost:4040"
    context.getConf.return_value.get.return_value = "configured"
    repository.spark.table.return_value.agg.return_value.first.return_value.asDict.return_value = {
        "files": 2,
        "records": 10,
        "bytes": 400,
    }
    monkeypatch.setattr("marketplace_data.benchmark_evidence.F", MagicMock())
    stages = json.dumps([{"inputBytes": 200, "shuffleReadBytes": 20}, {"inputBytes": 100}]).encode()
    monkeypatch.setattr(
        "marketplace_data.benchmark_evidence.urllib.request.urlopen",
        lambda *a, **k: io.BytesIO(stages),
    )
    evidence = collect(repository)
    assert evidence["stage_totals"]["inputBytes"] == 300
    assert evidence["stage_totals"]["shuffleReadBytes"] == 20
    assert len(evidence["tables"]) == 5
    assert not any("secret" in key or "password" in key for key in evidence["configuration"])
    context.uiWebUrl = "file:///not-allowed"
    evidence = collect(repository)
    assert evidence["stage_metrics_unavailable"] == "ValueError"
    assert "stage_totals" not in evidence
