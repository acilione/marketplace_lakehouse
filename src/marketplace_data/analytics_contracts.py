"""Versioned, full-state purchase contract, independent of Spark and storage."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class PurchaseItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    line_id: str = Field(min_length=1, max_length=100)
    product_id: str = Field(min_length=1, max_length=100)
    category: str | None = None
    quantity: int = Field(gt=0)
    amount: Decimal = Field(ge=0, max_digits=20, decimal_places=2)

    @field_validator("amount", mode="before")
    @classmethod
    def decimal_money(cls, value: object) -> object:
        if isinstance(value, float):
            raise ValueError("money must be a decimal string, not a float")
        if isinstance(value, (str, Decimal)) and "e" in str(value).lower():
            return format(Decimal(value), "f")
        return value


class PurchaseReturn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    return_id: str = Field(min_length=1, max_length=100)
    line_id: str = Field(min_length=1, max_length=100)
    quantity: int = Field(gt=0)
    amount: Decimal = Field(ge=0, max_digits=20, decimal_places=2)
    occurred_at: datetime

    _money = field_validator("amount", mode="before")(PurchaseItem.decimal_money)


class PurchaseSnapshot(BaseModel):
    """Every version supplies ALL items and returns; omission removes prior state.

    Source/version is monotonic per order. Amounts are after line discounts,
    excluding shipping and tax. Unknown customers remain null, never a shared key.
    """

    model_config = ConfigDict(extra="forbid")
    source: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    order_id: str = Field(min_length=1, max_length=100)
    version: int = Field(ge=1)
    customer_id: str | None = Field(default=None, min_length=1, max_length=100)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    country: str = Field(min_length=1, max_length=100)
    purchased_at: datetime
    status: str = Field(pattern=r"^(CONFIRMED|CANCELLED|REFUNDED)$")
    items: list[PurchaseItem] = Field(max_length=10_000)
    returns: list[PurchaseReturn] = Field(default_factory=list, max_length=10_000)

    @field_validator("purchased_at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("purchase timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def valid_lines(self) -> PurchaseSnapshot:
        lines = {line.line_id: line for line in self.items}
        if len(lines) != len(self.items):
            raise ValueError("line IDs must be unique within an order")
        if len({item.return_id for item in self.returns}) != len(self.returns):
            raise ValueError("return IDs must be unique within an order")
        for item in self.returns:
            if item.line_id not in lines:
                raise ValueError("return must reference an existing line")
            if item.occurred_at.tzinfo is None or item.occurred_at < self.purchased_at:
                raise ValueError("return timestamp must follow purchase and include timezone")
        for key, line in lines.items():
            returns = [item for item in self.returns if item.line_id == key]
            if sum(item.quantity for item in returns) > line.quantity:
                raise ValueError("returned quantity exceeds purchased quantity")
            if sum((item.amount for item in returns), Decimal(0)) > line.amount:
                raise ValueError("returned amount exceeds line amount")
        return self

    def payload(self) -> dict[str, str]:
        """Keep the existing envelope MAP<string,string> wire contract."""
        import json

        data = self.model_dump(mode="json")
        return {
            key: json.dumps(value, separators=(",", ":")) if isinstance(value, list) else str(value)
            for key, value in data.items()
            if value is not None
        }
