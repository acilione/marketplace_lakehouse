"""Small deterministic utilities shared by job boundaries."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping
from typing import Any

_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")


def stable_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def new_run_id(pipeline: str) -> str:
    validate_identifier(pipeline)
    return f"{pipeline}-{uuid.uuid4()}"


def validate_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return value


def qualified_table(catalog: str, namespace: str, table: str) -> str:
    return ".".join(validate_identifier(part) for part in (catalog, namespace, table))
