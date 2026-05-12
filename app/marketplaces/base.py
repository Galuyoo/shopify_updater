from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class DryRunReport:
    name: str
    path: Path
    rows: int


@dataclass(frozen=True)
class DryRunSummary:
    marketplace: str
    input_listing_map: str
    out_dir: str
    prefer: str
    stock_rows: int
    listing_rows: int
    enabled_rows: int
    would_update_rows: int
    error_rows: int
    warning_rows: int
    skipped_rows: int
    unmatched_rows: int
    duplicate_amazon_seller_sku_rows: int
    duplicate_canonical_sku_rows: int
    amazon_sp_api_called: bool


class MarketplaceDryRunAdapter:
    marketplace: str

    def build_dry_run(self, stock_df: pd.DataFrame, listing_map_df: pd.DataFrame) -> dict:
        raise NotImplementedError

    def write_reports(self, result: dict, out_dir: Path) -> list[DryRunReport]:
        raise NotImplementedError

