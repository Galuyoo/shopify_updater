from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .base import DryRunReport, DryRunSummary, MarketplaceDryRunAdapter

REQUIRED_COLUMNS = [
    "canonical_sku",
    "amazon_seller_sku",
    "asin",
    "marketplace_id",
    "fulfillment_channel",
    "enabled",
]

TRUE_VALUES = {"1", "true", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "no", "n", "off", ""}


def normalize_sku(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().upper()


def normalize_enabled(value: object) -> bool:
    if value is None or pd.isna(value):
        return False

    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False

    return False


def _missing_columns(columns: Iterable[str]) -> list[str]:
    present = {str(c).strip() for c in columns}
    return [col for col in REQUIRED_COLUMNS if col not in present]


def load_listing_map(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Amazon listing map not found: {path}")

    df = pd.read_csv(path, dtype=str).fillna("")
    missing = _missing_columns(df.columns)
    if missing:
        raise ValueError(
            "Amazon listing map is missing required columns: "
            + ", ".join(missing)
        )

    return df[REQUIRED_COLUMNS].copy()


def validate_listing_map(df: pd.DataFrame) -> pd.DataFrame:
    missing = _missing_columns(df.columns)
    if missing:
        raise ValueError(
            "Amazon listing map is missing required columns: "
            + ", ".join(missing)
        )

    out = df[REQUIRED_COLUMNS].copy().fillna("")
    out["canonical_sku"] = out["canonical_sku"].astype(str).str.strip()
    out["amazon_seller_sku"] = out["amazon_seller_sku"].astype(str).str.strip()
    out["asin"] = out["asin"].astype(str).str.strip()
    out["marketplace_id"] = out["marketplace_id"].astype(str).str.strip()
    out["fulfillment_channel"] = out["fulfillment_channel"].astype(str).str.strip()
    out["enabled"] = out["enabled"].apply(normalize_enabled)
    out["canonical_sku_norm"] = out["canonical_sku"].apply(normalize_sku)
    out["amazon_seller_sku_norm"] = out["amazon_seller_sku"].apply(normalize_sku)

    return out


def build_stock_lookup(stock_df: pd.DataFrame) -> pd.DataFrame:
    required = {"sku", "free"}
    missing = required - set(stock_df.columns)
    if missing:
        raise ValueError(f"stock_df missing required columns: {sorted(missing)}")

    cols = ["sku", "free"] + (['source'] if 'source' in stock_df.columns else [])
    stock = stock_df[cols].copy()
    stock["canonical_sku_norm"] = stock["sku"].apply(normalize_sku)
    stock["supplier_free_qty"] = pd.to_numeric(stock["free"], errors="coerce").fillna(0).astype(int)
    if 'source' not in stock.columns:
        stock['source'] = ""
    stock["stock_source"] = stock['source']

    stock = stock[stock["canonical_sku_norm"] != ""].copy()
    return stock.drop_duplicates(subset=["canonical_sku_norm"], keep="last")[
        ["canonical_sku_norm", "supplier_free_qty", "stock_source"]
    ]


def build_amazon_dry_run(stock_df: pd.DataFrame, listing_map_df: pd.DataFrame) -> dict:
    listings = validate_listing_map(listing_map_df)
    stock = build_stock_lookup(stock_df)

    report = listings.merge(stock, on="canonical_sku_norm", how="left")
    report["supplier_free_qty"] = report["supplier_free_qty"].astype("Int64")
    report["stock_source"] = report["stock_source"].fillna("")
    report["status"] = "ready"
    report["reason"] = ""

    disabled = ~report["enabled"]
    has_canonical = report["canonical_sku_norm"] != ""
    has_seller_sku = report["amazon_seller_sku_norm"] != ""
    missing_canonical = report["enabled"] & ~has_canonical
    missing_seller_sku = report["enabled"] & has_canonical & ~has_seller_sku
    duplicate_seller = (
        report["enabled"]
        & has_canonical
        & has_seller_sku
    ) & report["amazon_seller_sku_norm"].duplicated(keep=False)
    unmatched = (
        report["enabled"]
        & has_canonical
        & has_seller_sku
        & ~duplicate_seller
        & report["supplier_free_qty"].isna()
    )

    # Explicit precedence: disabled -> missing fields -> duplicate seller -> unmatched -> warning -> ready.
    report.loc[disabled, ["status", "reason"]] = ["disabled", "Listing map row is disabled"]
    report.loc[missing_canonical, ["status", "reason"]] = ["error_missing_canonical_sku", "canonical_sku is required"]
    report.loc[missing_seller_sku, ["status", "reason"]] = ["error_missing_amazon_seller_sku", "amazon_seller_sku is required"]
    report.loc[duplicate_seller, ["status", "reason"]] = [
        "error_duplicate_amazon_seller_sku",
        "amazon_seller_sku appears more than once",
    ]
    report.loc[unmatched, ["status", "reason"]] = [
        "unmatched_supplier_sku",
        "canonical_sku was not found in supplier stock",
    ]

    ready_canonical_counts = report.loc[
        report["status"].eq("ready"), "canonical_sku_norm"
    ].value_counts()
    warning_mask = (
        report["status"].eq("ready")
        & report["canonical_sku_norm"].map(ready_canonical_counts).fillna(0).gt(1)
    )
    report.loc[warning_mask, ["status", "reason"]] = [
        "warning_duplicate_canonical_sku",
        "canonical_sku feeds multiple Amazon listings",
    ]

    # Phase 1 intentionally uses supplier free quantity exactly.
    # TODO: evaluate caps/buffers before any future live Amazon inventory writes.
    report["proposed_amazon_qty"] = report["supplier_free_qty"]
    report.loc[~report["status"].isin(["ready", "warning_duplicate_canonical_sku"]), "proposed_amazon_qty"] = pd.NA
    enabled_rows = int(report["enabled"].sum())

    report = report.rename(columns={"supplier_free_qty": "supplier_qty"})
    report = report[
        [
            "canonical_sku",
            "amazon_seller_sku",
            "asin",
            "marketplace_id",
            "fulfillment_channel",
            "supplier_qty",
            "proposed_amazon_qty",
            "stock_source",
            "status",
            "reason",
        ]
    ].copy()

    duplicate_seller_report = report[report["status"].eq("error_duplicate_amazon_seller_sku")].copy()
    duplicate_canonical_report = report[
        report["status"].eq("warning_duplicate_canonical_sku")
        | report["canonical_sku"].apply(normalize_sku).duplicated(keep=False)
    ].copy()
    unmatched_report = report[report["status"].eq("unmatched_supplier_sku")].copy()

    return {
        "report": report,
        "unmatched": unmatched_report,
        "duplicate_amazon_seller_sku": duplicate_seller_report,
        "duplicate_canonical_sku": duplicate_canonical_report,
        "enabled_rows": enabled_rows,
        "amazon_sp_api_called": False,
    }


def _write_csv(df: pd.DataFrame, path: Path) -> DryRunReport:
    df.to_csv(path, index=False)
    return DryRunReport(name=path.name, path=path, rows=len(df))


def _summary_from_result(
    result: dict,
    *,
    listing_map: Path,
    out_dir: Path,
    prefer: str,
    stock_rows: int,
) -> DryRunSummary:
    report = result["report"]
    statuses = report["status"].astype(str)
    return DryRunSummary(
        marketplace="amazon",
        input_listing_map=str(listing_map),
        out_dir=str(out_dir),
        prefer=prefer,
        stock_rows=int(stock_rows),
        listing_rows=int(len(report)),
        enabled_rows=int(result["enabled_rows"]),
        would_update_rows=int(statuses.eq("ready").sum()),
        error_rows=int(statuses.str.startswith("error_").sum()),
        warning_rows=int(statuses.str.startswith("warning_").sum()),
        skipped_rows=int(statuses.eq("disabled").sum()),
        unmatched_rows=int(len(result["unmatched"])),
        duplicate_amazon_seller_sku_rows=int(len(result["duplicate_amazon_seller_sku"])),
        duplicate_canonical_sku_rows=int(len(result["duplicate_canonical_sku"])),
        amazon_sp_api_called=bool(result["amazon_sp_api_called"]),
    )


def write_dry_run_reports(
    result: dict,
    out_dir: Path,
    *,
    listing_map: Path,
    prefer: str,
    stock_rows: int,
    timestamp: str | None = None,
) -> tuple[list[DryRunReport], DryRunSummary]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")

    reports = [
        _write_csv(result["report"], out_dir / f"amazon_stock_dry_run_{stamp}.csv"),
        _write_csv(result["unmatched"], out_dir / f"amazon_unmatched_skus_{stamp}.csv"),
        _write_csv(
            result["duplicate_amazon_seller_sku"],
            out_dir / f"amazon_duplicate_amazon_seller_sku_{stamp}.csv",
        ),
        _write_csv(
            result["duplicate_canonical_sku"],
            out_dir / f"amazon_duplicate_canonical_sku_{stamp}.csv",
        ),
    ]

    summary = _summary_from_result(
        result,
        listing_map=listing_map,
        out_dir=out_dir,
        prefer=prefer,
        stock_rows=stock_rows,
    )
    summary_path = out_dir / f"amazon_stock_dry_run_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary.__dict__, indent=2), encoding="utf-8")
    reports.append(DryRunReport(name=summary_path.name, path=summary_path, rows=1))

    return reports, summary


class AmazonDryRunAdapter(MarketplaceDryRunAdapter):
    marketplace = "amazon"

    def build_dry_run(self, stock_df: pd.DataFrame, listing_map_df: pd.DataFrame) -> dict:
        return build_amazon_dry_run(stock_df, listing_map_df)

    def write_reports(
        self,
        result: dict,
        out_dir: Path,
        *,
        listing_map: Path,
        prefer: str,
        stock_rows: int,
    ) -> tuple[list[DryRunReport], DryRunSummary]:
        return write_dry_run_reports(
            result,
            out_dir,
            listing_map=listing_map,
            prefer=prefer,
            stock_rows=stock_rows,
        )
