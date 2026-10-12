from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from marketplace_data.publication import PublicationStore
from marketplace_data.purchase_source import (
    envelope,
    generated_purchases,
    main,
    publish,
    read_purchases,
)


def test_generator_is_repeatable_and_lazy() -> None:
    at = datetime(2026, 10, 12, tzinfo=timezone.utc)
    first = list(generated_purchases(20, 3, 42, at))
    assert first == list(generated_purchases(20, 3, 42, at))
    assert len({row.customer_id for row in first}) <= 3
    assert any(row.returns for row in first)
    assert any(len(row.items) > 1 for row in first)
    huge = generated_purchases(1_000_000_000, 100, 42, at)
    assert next(huge).order_id == first[0].order_id
    assert envelope(first[0]).event_id == envelope(first[0]).event_id
    with pytest.raises(ValueError):
        next(generated_purchases(0, 1, 42, at))


def test_jsonl_import_and_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "source.jsonl"
    purchase = next(generated_purchases(1, 1, 1, datetime.now(timezone.utc)))
    source.write_text("\n" + purchase.model_dump_json() + "\n", encoding="utf-8")
    assert list(read_purchases(source)) == [purchase]
    publisher = MagicMock()
    monkeypatch.setattr("marketplace_data.purchase_source.publish", publisher)
    main(["--input", str(source)])
    assert list(publisher.call_args.args[0]) == [purchase]
    main(["--generate", "1"])
    assert len(list(publisher.call_args.args[0])) == 1
    source.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="line 1"):
        list(read_purchases(source))
    with pytest.raises(SystemExit):
        main(["--generate", "1", "--as-of", "2026-01-01"])


def test_publisher_checks_acknowledgements(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = MagicMock()
    producer.flush.return_value = 0
    monkeypatch.setattr("marketplace_data.purchase_source.Producer", lambda _: producer)
    monkeypatch.setattr("marketplace_data.purchase_source.time.sleep", lambda _: None)
    at = datetime.now(timezone.utc)
    publish(generated_purchases(2, 1, 1, at), "kafka:9092", 1000)
    assert producer.produce.call_count == 2
    producer.flush.return_value = 1
    with pytest.raises(RuntimeError, match="incomplete"):
        publish(generated_purchases(1, 1, 1, at), "kafka:9092", 1000)
    with pytest.raises(ValueError, match="positive"):
        publish(iter([]), "kafka:9092", 0)
    producer.flush.return_value = 0
    producer.produce.side_effect = lambda *args, **kwargs: kwargs["on_delivery"]("rejected", None)
    with pytest.raises(RuntimeError, match="failed"):
        publish(generated_purchases(2, 1, 1, at), "kafka:9092", 1000)


def test_publication_ledger_transaction_and_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock()
    monkeypatch.setattr("marketplace_data.publication.psycopg.connect", lambda *a, **kw: connection)
    store = PublicationStore("unused")
    connection.execute.return_value.fetchone.return_value = (True,)
    with store.writer("test"):
        store.publish(
            "test", {"source_snapshot": "123", "snapshots": {"gold.customer_profiles": "456"}}
        )
    connection.transaction.assert_called_once()
    assert "pg_advisory_unlock" in connection.execute.call_args.args[0]
    connection.execute.return_value.fetchone.return_value = ({"source_snapshot": "123"},)
    assert store.current("test") == {"source_snapshot": "123"}
    connection.execute.return_value.fetchone.return_value = None
    assert store.current("test") is None
    connection.execute.return_value.fetchone.return_value = (False,)
    with pytest.raises(RuntimeError, match="another writer"), store.writer("test"):
        pytest.fail("lock must not be granted")
    store.close()
    connection.close.assert_called_once()


def test_unchanged_analytics_does_not_rewrite_tables_or_publish() -> None:
    from datetime import date

    from marketplace_data.jobs.customer_analytics import build

    repository, store = MagicMock(), MagicMock()
    repository.current_snapshot_id.return_value = 123
    store.current.return_value = {
        "source_snapshot": "123",
        "as_of": "2026-10-12",
        "data_kind": "generated",
        "snapshots": {"gold.customer_cohorts": "456"},
    }
    build(repository, store, date(2026, 10, 12), public_demo=True)
    repository.spark.catalog.tableExists.assert_not_called()
    repository.merge.assert_not_called()
    store.publish.assert_not_called()


def test_skip_publishes_only_new_generated_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    publisher = MagicMock()
    monkeypatch.setattr("marketplace_data.purchase_source.publish", publisher)
    main(["--generate", "10", "--skip", "7", "--as-of", "2026-10-12T12:00:00Z"])
    values = list(publisher.call_args.args[0])
    assert len(values) == 3
    assert values[0].order_id == "order-0000000007"
