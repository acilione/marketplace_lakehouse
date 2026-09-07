import json
from unittest.mock import MagicMock

import pytest

from marketplace_data.kafka_offsets import main
from marketplace_data.simulator import TOPICS


def test_offset_capture_reads_high_watermarks_without_consuming(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    consumer = MagicMock()
    consumer.list_topics.return_value.topics = {
        topic: MagicMock(partitions={0: None}) for topic in TOPICS.values()
    }
    consumer.get_watermark_offsets.return_value = (5, 42)
    monkeypatch.setattr("marketplace_data.kafka_offsets.Consumer", lambda _: consumer)
    monkeypatch.setattr("sys.argv", ["offsets", "kafka:9092"])
    main()
    assert json.loads(capsys.readouterr().out) == {topic: {"0": 42} for topic in TOPICS.values()}
    consumer.subscribe.assert_not_called()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
