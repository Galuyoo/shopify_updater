from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class StockRuleConfig:
    buffer: int = 0
    max_stock: int = 5
    update_missing_as_zero: bool = False


def coerce_non_negative_int(value) -> int:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return 0
    return max(int(number), 0)


def calculate_safe_stock(supplier_stock, config: StockRuleConfig) -> int | None:
    """Return capped safe stock, or None when missing stock should not be exported."""
    if supplier_stock is None or pd.isna(supplier_stock):
        return 0 if config.update_missing_as_zero else None

    free = coerce_non_negative_int(supplier_stock)
    if free <= 0:
        return 0

    adjusted = max(free - config.buffer, 0)
    return min(adjusted, config.max_stock)
