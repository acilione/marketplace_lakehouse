from __future__ import annotations

import pytest

from marketplace_data.util import qualified_table, stable_hash, validate_identifier


def test_stable_hash_ignores_mapping_order() -> None:
    assert stable_hash({"a": 1, "b": 2}) == stable_hash({"b": 2, "a": 1})


def test_sql_identifiers_are_validated() -> None:
    assert qualified_table("lakehouse", "silver", "orders") == "lakehouse.silver.orders"
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        validate_identifier("orders; DROP TABLE users")
