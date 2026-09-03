"""Canonical event contract and registry-independent validation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from fastavro import parse_schema
from pydantic import BaseModel, ConfigDict, Field, field_validator

CONTRACTS_ROOT = Path(__file__).resolve().parents[2] / "contracts"
SUPPORTED_EVENT_PREFIXES = frozenset({"order", "payment", "inventory", "shipment", "customer"})


class EventEnvelope(BaseModel):
    """Versioned domain envelope used by generators and contract tests."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=10, max_length=64)
    event_type: str = Field(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")
    event_version: int = Field(ge=1)
    occurred_at: datetime
    produced_at: datetime
    producer: str = Field(min_length=1, max_length=100)
    trace_id: str = Field(min_length=8, max_length=64)
    partition_key: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any]
    producer_sequence: int | None = Field(default=None, ge=0)

    @field_validator("occurred_at", "produced_at")
    @classmethod
    def timestamps_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("timestamps must be timezone-aware UTC values")
        return value

    @field_validator("event_type")
    @classmethod
    def event_domain_must_be_supported(cls, value: str) -> str:
        if value.split(".", maxsplit=1)[0] not in SUPPORTED_EVENT_PREFIXES:
            raise ValueError(f"unsupported event domain: {value}")
        return value

    @field_validator("payload")
    @classmethod
    def money_must_not_be_float(cls, value: dict[str, Any]) -> dict[str, Any]:
        monetary_fields = {"amount", "gross_amount", "net_amount", "unit_price"}
        for name in monetary_fields & value.keys():
            if isinstance(value[name], float):
                raise ValueError(f"{name} must be encoded as a decimal string, not float")
            if value[name] is not None:
                Decimal(str(value[name]))
        return value

    def as_json(self) -> bytes:
        return self.model_dump_json(exclude_none=True).encode("utf-8")


def load_avro_schema(name: str = "event-envelope-v1.avsc") -> dict[str, Any]:
    path = (CONTRACTS_ROOT / name).resolve()
    if path.parent != CONTRACTS_ROOT.resolve() or not path.is_file():
        raise ValueError(f"unknown contract: {name}")
    raw = cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
    return cast(dict[str, Any], parse_schema(raw))
