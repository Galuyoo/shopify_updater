from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
import subprocess
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_mapping import build_supplier_stock_lookup
from src.stock_rules import StockRuleConfig, calculate_safe_stock
from src.storefeeder_api import (
    StoreFeederApiClient,
    StoreFeederApiConfig,
    batch_items,
    payload_preview_to_storefeeder_items,
    supplier_payload_preview_to_items,
)
from src.storefeeder_stock_export import read_csv


TARGET_COLUMNS = [
    "ProductID",
    "SKU",
    "supplier",
    "SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "stock_location",
]

OPTIONAL_TARGET_COLUMNS = [
    "inventory_stock_location",
    "clear_stock_locations",
    "preserve_existing_locations",
    "warehouse_safe_mode",
    "skip_stock_location_update",
    "allow_stock_location_update",
    "stock_strategy",
    "sellable_stock_location",
]

PAYLOAD_COLUMNS = [
    "ProductID",
    "SKU",
    "supplier",
    "supplier_sku",
    "quantity",
    "confidence_status",
    "ProductIDType.IDType",
    "ProductIDType.Value",
    "Supplier.SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "SupplierStockLevel",
    "SupplierCosts",
]

STOCK_LOCATION_PAYLOAD_COLUMNS = [
    "SKU",
    "supplier",
    "supplier_sku",
    "stock_location",
    "quantity",
    "confidence_status",
    "ProductIDType.IDType",
    "ProductIDType.Value",
    "AdjustmentType",
    "AdjustmentAmount",
    "StockLocationID.IDType",
    "StockLocationID.Value",
    "Reason",
]

STOCK_LOCATION_MAX_RETRIES = 2
STOCK_LOCATION_RETRY_DELAY_SECONDS = 5
CHANNEL_SAFETY_SKIP_COLUMNS = ["ProductID", "SKU", "SupplierSKU", "supplier", "stock_location", "skip_reason"]
ZERO_LOCATION_PREVIEW_COLUMNS = [
    "SKU",
    "ProductID",
    "stock_strategy",
    "keep_stock_location",
    "zero_stock_location",
    "current_quantity",
    "new_quantity",
    "reason",
]
ZERO_LOCATION_SAFETY_SKIP_COLUMNS = [
    "SKU",
    "ProductID",
    "stock_strategy",
    "keep_stock_location",
    "zero_stock_location",
    "skip_reason",
]
KNOWN_STOCK_LOCATIONS = ["Ralawise", "Uneek", "Temporary stock location", "Unspecified", "Warehouse Stock"]
UNSUPPORTED_ZERO_STOCK_LOCATIONS = ["Temporary stock location"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast StoreFeeder supplier stock update without product enrichment.")
    parser.add_argument("--live-stock-update", action="store_true")
    parser.add_argument("--api-limit", type=int)
    parser.add_argument("--api-batch-size", default=50, type=int)
    parser.add_argument("--buffer", default=0, type=int)
    parser.add_argument("--max-stock", default=5, type=int)
    parser.add_argument("--targets", default=Path("data/storefeeder_supplier_stock_update_targets.csv"), type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--ralawise-stock", default=Path("data/RALAWISE_stock_lvl.csv"), type=Path)
    parser.add_argument("--uneek-stock", default=Path("data/Uneek_stock_levels.csv"), type=Path)
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--missing-as-zero", action="store_true", help="Explicitly allow missing supplier stock to become zero")
    parser.add_argument("--skip-stock-refresh", action="store_true")
    parser.add_argument(
        "--zero-other-locations-for-supplier-synced",
        action="store_true",
        help="For supplier_synced_inventory targets with explicit location permission, zero non-authoritative stock locations.",
    )
    args = parser.parse_args()
    if args.api_limit is not None and args.api_limit < 0:
        parser.error("--api-limit must be zero or greater")
    if args.api_batch_size < 1 or args.api_batch_size > 50:
        parser.error("--api-batch-size must be between 1 and 50")
    if args.live_stock_update and args.api_limit is None:
        parser.error("--live-stock-update requires explicit --api-limit for guarded runs")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(PROJECT_ROOT / ".env")
    out_dir = args.out_dir or Path("reports/fast_stock_updates") / datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_stock_refresh:
        _run(
            [
                sys.executable,
                "scripts/refresh_supplier_stock_files.py",
                "--ralawise-out",
                str(args.ralawise_stock),
                "--uneek-out",
                str(args.uneek_stock),
            ],
            "refresh supplier stock files",
        )

    targets = read_csv(args.targets)
    ralawise_stock = read_csv(args.ralawise_stock)
    uneek_stock = read_csv(args.uneek_stock)
    supplier_stock = build_supplier_stock_lookup(ralawise_stock, uneek_stock)
    (
        preview,
        stock_location_preview,
        zero_location_preview,
        invalid_rows,
        channel_safety_skips,
        zero_location_safety_skips,
    ) = build_fast_payload_preview(
        targets,
        supplier_stock,
        buffer=args.buffer,
        max_stock=args.max_stock,
        missing_as_zero=args.missing_as_zero,
        zero_other_locations_for_supplier_synced=args.zero_other_locations_for_supplier_synced,
    )
    if args.api_limit is not None:
        preview = preview.head(args.api_limit).copy()
        limited_keys = set(zip(preview["SKU"].astype(str), preview["SupplierSKU"].astype(str)))
        stock_location_preview = stock_location_preview[
            stock_location_preview.apply(lambda row: (str(row["SKU"]), str(row["supplier_sku"])) in limited_keys, axis=1)
        ].copy()
        limited_skus = set(preview["SKU"].astype(str))
        zero_location_preview = zero_location_preview[zero_location_preview["SKU"].astype(str).isin(limited_skus)].copy()
        channel_safety_skips = channel_safety_skips[
            channel_safety_skips.apply(lambda row: (str(row["SKU"]), str(row["SupplierSKU"])) in limited_keys, axis=1)
        ].copy()
        zero_location_safety_skips = zero_location_safety_skips[zero_location_safety_skips["SKU"].astype(str).isin(set(preview["SKU"].astype(str)))].copy()

    paths = {
        "fast_stock_payload_preview": out_dir / "fast_stock_payload_preview.csv",
        "fast_stock_location_payload_preview": out_dir / "fast_stock_location_payload_preview.csv",
        "fast_stock_location_zero_payload_preview": out_dir / "fast_stock_location_zero_payload_preview.csv",
        "fast_stock_location_zero_safety_skips": out_dir / "fast_stock_location_zero_safety_skips.csv",
        "fast_stock_summary": out_dir / "fast_stock_summary.csv",
        "fast_stock_invalid_rows": out_dir / "fast_stock_invalid_rows.csv",
        "fast_stock_channel_safety_skips": out_dir / "fast_stock_channel_safety_skips.csv",
    }
    preview.to_csv(paths["fast_stock_payload_preview"], index=False)
    stock_location_preview.to_csv(paths["fast_stock_location_payload_preview"], index=False)
    zero_location_preview.to_csv(paths["fast_stock_location_zero_payload_preview"], index=False)
    zero_location_safety_skips.to_csv(paths["fast_stock_location_zero_safety_skips"], index=False)
    invalid_rows.to_csv(paths["fast_stock_invalid_rows"], index=False)
    channel_safety_skips.to_csv(paths["fast_stock_channel_safety_skips"], index=False)
    summary = _summary_frame(
        targets,
        preview,
        stock_location_preview,
        invalid_rows,
        args.live_stock_update,
        channel_safety_skips=channel_safety_skips,
        zero_location_preview=zero_location_preview,
        zero_location_safety_skips=zero_location_safety_skips,
    )
    summary.to_csv(paths["fast_stock_summary"], index=False)

    print("Fast StoreFeeder stock update dry run" if not args.live_stock_update else "Fast StoreFeeder stock update live run")
    print(summary.to_string(index=False))
    print("\nWrote reports:")
    for path in paths.values():
        print(path)

    if not invalid_rows.empty:
        raise SystemExit("Blocked fast stock update because invalid target/stock rows are present.")
    if preview.empty:
        raise SystemExit("Blocked fast stock update because payload preview is empty.")
    if not args.live_stock_update:
        print("\nDry-run only. No StoreFeeder API calls were made.")
        return 0

    items = supplier_payload_preview_to_items(preview)
    batches = batch_items(items, args.api_batch_size)
    stock_location_items = payload_preview_to_storefeeder_items(stock_location_preview)
    stock_location_batches = batch_items(stock_location_items, args.api_batch_size)
    zero_location_items = payload_preview_to_storefeeder_items(_zero_preview_to_stock_location_payload(zero_location_preview))
    zero_location_batches = batch_items(zero_location_items, args.api_batch_size)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    live_paths = _send_fast_stock_batches(client, batches, out_dir)
    location_live_paths, stock_location_retry_count = _send_fast_stock_location_batches(client, stock_location_batches, out_dir)
    zero_location_live_paths, zero_location_retry_count = _send_fast_stock_location_batches(
        client,
        zero_location_batches,
        out_dir,
        file_prefix="fast_stock_location_zero_update",
    )
    supplier_update_failures = _csv_count(live_paths["fast_stock_update_failures"])
    stock_location_update_failures = _csv_count(location_live_paths["fast_stock_location_update_failures"])
    zero_location_update_failures = _csv_count(zero_location_live_paths["fast_stock_location_zero_update_failures"])
    summary = _summary_frame(
        targets,
        preview,
        stock_location_preview,
        invalid_rows,
        args.live_stock_update,
        supplier_update_success=_csv_count(live_paths["fast_stock_update_success"]),
        supplier_update_failures=supplier_update_failures,
        stock_location_update_success=_csv_count(location_live_paths["fast_stock_location_update_success"]),
        stock_location_update_failures=stock_location_update_failures,
        retry_count=stock_location_retry_count + zero_location_retry_count,
        channel_safety_skips=channel_safety_skips,
        zero_location_preview=zero_location_preview,
        zero_location_safety_skips=zero_location_safety_skips,
        zero_other_locations_success=_csv_count(zero_location_live_paths["fast_stock_location_zero_update_success"]),
        zero_other_locations_failures=zero_location_update_failures,
    )
    summary.to_csv(paths["fast_stock_summary"], index=False)
    print("\nLive stock update reports:")
    for path in [*live_paths.values(), *location_live_paths.values(), *zero_location_live_paths.values()]:
        print(path)
    if supplier_update_failures or stock_location_update_failures or zero_location_update_failures:
        raise SystemExit(
            "Fast stock update completed with failures: "
            f"supplier_update_failures={supplier_update_failures}, "
            f"stock_location_update_failures={stock_location_update_failures}, "
            f"zero_other_locations_failures={zero_location_update_failures}"
        )
    return 0


def build_fast_payload_preview(
    targets: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    *,
    buffer: int,
    max_stock: int,
    missing_as_zero: bool,
    zero_other_locations_for_supplier_synced: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _require_columns(targets, TARGET_COLUMNS, "target file")
    if targets.empty:
        invalid = pd.DataFrame([{"invalid_reason": "target_file_empty"}])
        return (
            pd.DataFrame(columns=PAYLOAD_COLUMNS),
            pd.DataFrame(columns=STOCK_LOCATION_PAYLOAD_COLUMNS),
            pd.DataFrame(columns=ZERO_LOCATION_PREVIEW_COLUMNS),
            invalid,
            pd.DataFrame(columns=CHANNEL_SAFETY_SKIP_COLUMNS),
            pd.DataFrame(columns=ZERO_LOCATION_SAFETY_SKIP_COLUMNS),
        )

    rows = targets[TARGET_COLUMNS + [column for column in OPTIONAL_TARGET_COLUMNS if column in targets.columns]].copy()
    for column in TARGET_COLUMNS + [column for column in OPTIONAL_TARGET_COLUMNS if column in rows.columns]:
        rows[column] = rows[column].fillna("").astype(str).str.strip()
    if "inventory_stock_location" not in rows.columns:
        rows["inventory_stock_location"] = rows["supplier"]
    rows["inventory_stock_location"] = rows["inventory_stock_location"].where(
        rows["inventory_stock_location"].str.strip().ne(""),
        rows["supplier"],
    )
    if "preserve_existing_locations" not in rows.columns:
        rows["preserve_existing_locations"] = "no"
    if "warehouse_safe_mode" not in rows.columns:
        rows["warehouse_safe_mode"] = "no"
    if "skip_stock_location_update" not in rows.columns:
        rows["skip_stock_location_update"] = "no"
    if "allow_stock_location_update" not in rows.columns:
        rows["allow_stock_location_update"] = "no"
    if "stock_strategy" not in rows.columns:
        rows["stock_strategy"] = ""
    if "sellable_stock_location" not in rows.columns:
        rows["sellable_stock_location"] = ""
    supplier_synced_mask = rows["stock_strategy"].str.casefold().eq("supplier_synced_inventory")
    sellable_mask = supplier_synced_mask & rows["sellable_stock_location"].str.strip().ne("")
    rows.loc[sellable_mask, "inventory_stock_location"] = rows.loc[sellable_mask, "sellable_stock_location"]
    rows.loc[~supplier_synced_mask, "sellable_stock_location"] = ""

    preserve_mask = (
        rows["preserve_existing_locations"].str.casefold().isin(["yes", "true", "1", "y"])
        | rows["warehouse_safe_mode"].str.casefold().isin(["yes", "true", "1", "y"])
    )

    if "clear_stock_locations" not in rows.columns:
        rows["clear_stock_locations"] = ""
    rows["clear_stock_locations"] = rows["clear_stock_locations"].where(
        rows["clear_stock_locations"].str.strip().ne(""),
        rows["supplier"].map(_default_clear_stock_locations),
    )
    rows.loc[preserve_mask, "clear_stock_locations"] = ""

    rows["SupplierSKU"] = rows["SupplierSKU"].str.upper()
    rows["_supplier_key"] = rows["supplier"].str.casefold()
    rows["_supplier_sku_key"] = rows["SupplierSKU"].str.casefold()
    rows["_duplicate_key"] = rows["ProductID"] + "|" + rows["SupplierID"] + "|" + rows["SupplierSKU"]

    stock = supplier_stock.copy()
    stock["_supplier_key"] = stock["supplier"].fillna("").astype(str).str.casefold()
    stock["_supplier_sku_key"] = stock["supplier_sku"].fillna("").astype(str).str.casefold()
    rows = rows.merge(
        stock[["_supplier_key", "_supplier_sku_key", "supplier_free_stock"]],
        how="left",
        on=["_supplier_key", "_supplier_sku_key"],
    )

    config = StockRuleConfig(buffer=buffer, max_stock=max_stock, update_missing_as_zero=missing_as_zero)
    rows["quantity"] = rows["supplier_free_stock"].map(lambda value: calculate_safe_stock(value, config))
    rows["invalid_reason"] = rows.apply(_invalid_target_reason, axis=1)

    duplicate_mask = rows["_duplicate_key"].duplicated(keep=False)
    rows.loc[duplicate_mask, "invalid_reason"] = rows.loc[duplicate_mask, "invalid_reason"].map(
        lambda reason: _append_reason(reason, "duplicate_ProductID_SupplierID_SupplierSKU")
    )

    invalid_rows = rows[rows["invalid_reason"].ne("")].copy()
    valid = rows[rows["invalid_reason"].eq("")].copy()
    if valid.empty:
        return (
            pd.DataFrame(columns=PAYLOAD_COLUMNS),
            pd.DataFrame(columns=STOCK_LOCATION_PAYLOAD_COLUMNS),
            pd.DataFrame(columns=ZERO_LOCATION_PREVIEW_COLUMNS),
            invalid_rows.reset_index(drop=True),
            pd.DataFrame(columns=CHANNEL_SAFETY_SKIP_COLUMNS),
            pd.DataFrame(columns=ZERO_LOCATION_SAFETY_SKIP_COLUMNS),
        )

    valid["_channel_decorated"] = valid["SKU"].astype(str).str.strip().str.casefold().ne(
        valid["SupplierSKU"].astype(str).str.strip().str.casefold()
    )
    valid["_allow_stock_location_update"] = valid["allow_stock_location_update"].astype(str).str.strip().str.casefold().isin(["yes", "true", "1", "y"])
    valid["_explicit_skip_stock_location_update"] = valid["skip_stock_location_update"].astype(str).str.strip().str.casefold().isin(["yes", "true", "1", "y"])
    valid["_stock_location_skip_reason"] = ""
    valid.loc[valid["_explicit_skip_stock_location_update"], "_stock_location_skip_reason"] = "explicit_skip_stock_location_update"
    warehouse_only_mask = valid["stock_strategy"].astype(str).str.strip().str.casefold().eq("warehouse_only")
    valid.loc[warehouse_only_mask & valid["_stock_location_skip_reason"].eq(""), "_stock_location_skip_reason"] = "warehouse_only_stock_location_protected"
    implicit_channel_skip = (
        valid["_channel_decorated"]
        & ~valid["_allow_stock_location_update"]
        & valid["_stock_location_skip_reason"].eq("")
    )
    valid.loc[implicit_channel_skip, "_stock_location_skip_reason"] = "implicit_channel_decorated_stock_location_skip"

    channel_safety_skips = valid[valid["_stock_location_skip_reason"].ne("")].copy()
    if channel_safety_skips.empty:
        channel_safety_skip_report = pd.DataFrame(columns=CHANNEL_SAFETY_SKIP_COLUMNS)
    else:
        channel_safety_skip_report = pd.DataFrame(
            {
                "ProductID": channel_safety_skips["ProductID"],
                "SKU": channel_safety_skips["SKU"],
                "SupplierSKU": channel_safety_skips["SupplierSKU"],
                "supplier": channel_safety_skips["supplier"],
                "stock_location": channel_safety_skips["stock_location"],
                "skip_reason": channel_safety_skips["_stock_location_skip_reason"],
            },
            columns=CHANNEL_SAFETY_SKIP_COLUMNS,
        )

    preview = pd.DataFrame(
        {
            "ProductID": valid["ProductID"],
            "SKU": valid["SKU"],
            "supplier": valid["supplier"],
            "supplier_sku": valid["SupplierSKU"],
            "quantity": valid["quantity"].astype(int),
            "confidence_status": "update_ready",
            "ProductIDType.IDType": "ID",
            "ProductIDType.Value": valid["ProductID"],
            "Supplier.SupplierID": valid["SupplierID"].astype(int),
            "Supplier.Name": valid["Supplier.Name"],
            "SupplierSKU": valid["SupplierSKU"],
            "SupplierStockLevel": valid["quantity"].astype(int),
            "SupplierCosts": 0,
        },
        columns=PAYLOAD_COLUMNS,
    )
    stock_location_preview = _stock_location_payload_preview(valid)
    if zero_other_locations_for_supplier_synced:
        zero_location_preview, zero_location_safety_skips = _zero_other_locations_preview(valid)
    else:
        zero_location_preview = pd.DataFrame(columns=ZERO_LOCATION_PREVIEW_COLUMNS)
        zero_location_safety_skips = pd.DataFrame(columns=ZERO_LOCATION_SAFETY_SKIP_COLUMNS)
    return (
        preview.reset_index(drop=True),
        stock_location_preview.reset_index(drop=True),
        zero_location_preview.reset_index(drop=True),
        invalid_rows.reset_index(drop=True),
        channel_safety_skip_report.reset_index(drop=True),
        zero_location_safety_skips.reset_index(drop=True),
    )


def _default_clear_stock_locations(supplier: str) -> str:
    supplier_key = str(supplier).strip().casefold()
    if supplier_key == "ralawise":
        return "Uneek|Temporary stock location|Unspecified"
    if supplier_key == "uneek":
        return "Ralawise|Temporary stock location|Unspecified"
    return "Ralawise|Uneek|Temporary stock location|Unspecified"


def _stock_location_payload_preview(valid: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in valid.iterrows():
        sku = str(row["SKU"]).strip()
        supplier = str(row["supplier"]).strip()
        supplier_sku = str(row["SupplierSKU"]).strip()
        target_location = str(row["inventory_stock_location"]).strip()
        quantity = int(row["quantity"])
        if str(row.get("_stock_location_skip_reason", "")).strip():
            continue
        if target_location:
            rows.append(_stock_location_payload_row(sku, supplier, supplier_sku, target_location, quantity))
        for clear_location in _pipe_values(row["clear_stock_locations"]):
            if clear_location.casefold() == target_location.casefold():
                continue
            rows.append(_stock_location_payload_row(sku, supplier, supplier_sku, clear_location, 0))
    return pd.DataFrame(rows, columns=STOCK_LOCATION_PAYLOAD_COLUMNS)


def _stock_location_payload_row(sku: str, supplier: str, supplier_sku: str, stock_location: str, quantity: int) -> dict[str, Any]:
    return {
        "SKU": sku,
        "supplier": supplier,
        "supplier_sku": supplier_sku,
        "stock_location": stock_location,
        "quantity": int(quantity),
        "confidence_status": "update_ready",
        "ProductIDType.IDType": "SKU",
        "ProductIDType.Value": sku,
        "AdjustmentType": "AbsoluteAdjustment",
        "AdjustmentAmount": int(quantity),
        "StockLocationID.IDType": "StockLocationReference",
        "StockLocationID.Value": stock_location,
        "Reason": "Supplier stock sync",
    }


def _zero_other_locations_preview(valid: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    zero_rows: list[dict[str, Any]] = []
    skip_rows: list[dict[str, Any]] = []
    for _, row in valid.iterrows():
        sku = str(row.get("SKU", "")).strip()
        product_id = str(row.get("ProductID", "")).strip()
        strategy = str(row.get("stock_strategy", "")).strip().casefold()
        keep_location = str(row.get("inventory_stock_location", "")).strip() or str(row.get("stock_location", "")).strip()
        allow_location_update = bool(row.get("_allow_stock_location_update", False))
        explicit_skip = bool(row.get("_explicit_skip_stock_location_update", False))

        if strategy == "warehouse_only":
            continue
        if strategy != "supplier_synced_inventory":
            skip_rows.append(_zero_location_skip_row(row, keep_location, "ambiguous_or_missing_stock_strategy"))
            continue
        if not allow_location_update:
            skip_rows.append(_zero_location_skip_row(row, keep_location, "allow_stock_location_update_not_enabled"))
            continue
        if explicit_skip or str(row.get("_stock_location_skip_reason", "")).strip():
            skip_rows.append(_zero_location_skip_row(row, keep_location, "stock_location_update_is_skipped"))
            continue
        if not keep_location:
            skip_rows.append(_zero_location_skip_row(row, keep_location, "missing_authoritative_stock_location"))
            continue

        zero_locations = _zero_candidate_locations(row, keep_location)
        for zero_location in zero_locations:
            if zero_location.casefold() == keep_location.casefold():
                continue
            if _contains_casefold(UNSUPPORTED_ZERO_STOCK_LOCATIONS, zero_location):
                skip_rows.append(
                    _zero_location_skip_row(
                        row,
                        keep_location,
                        "unsupported_or_inaccessible_stock_location",
                        zero_location,
                    )
                )
                continue
            if zero_location.casefold() == "warehouse stock" and strategy != "supplier_synced_inventory":
                skip_rows.append(_zero_location_skip_row(row, keep_location, "warehouse_stock_zero_not_unambiguously_supplier_synced", zero_location))
                continue
            zero_rows.append(
                {
                    "SKU": sku,
                    "ProductID": product_id,
                    "stock_strategy": "supplier_synced_inventory",
                    "keep_stock_location": keep_location,
                    "zero_stock_location": zero_location,
                    "current_quantity": "",
                    "new_quantity": 0,
                    "reason": "supplier_synced_inventory_authoritative_zero_other_locations",
                }
            )
    return (
        pd.DataFrame(zero_rows, columns=ZERO_LOCATION_PREVIEW_COLUMNS),
        pd.DataFrame(skip_rows, columns=ZERO_LOCATION_SAFETY_SKIP_COLUMNS),
    )


def _zero_candidate_locations(row: pd.Series, keep_location: str) -> list[str]:
    candidates: list[str] = []
    for value in [row.get("clear_stock_locations", ""), "|".join(KNOWN_STOCK_LOCATIONS)]:
        for location in _pipe_values(value):
            if location and location.casefold() != keep_location.casefold() and not _contains_casefold(candidates, location):
                candidates.append(location)
    return candidates


def _contains_casefold(values: list[str], value: str) -> bool:
    return any(existing.casefold() == value.casefold() for existing in values)


def _zero_location_skip_row(row: pd.Series, keep_location: str, reason: str, zero_location: str = "") -> dict[str, Any]:
    return {
        "SKU": str(row.get("SKU", "")).strip(),
        "ProductID": str(row.get("ProductID", "")).strip(),
        "stock_strategy": str(row.get("stock_strategy", "")).strip(),
        "keep_stock_location": keep_location,
        "zero_stock_location": zero_location,
        "skip_reason": reason,
    }


def _zero_preview_to_stock_location_payload(zero_preview: pd.DataFrame) -> pd.DataFrame:
    if zero_preview.empty:
        return pd.DataFrame(columns=STOCK_LOCATION_PAYLOAD_COLUMNS)
    rows = pd.DataFrame(
        {
            "SKU": zero_preview["SKU"],
            "supplier": "",
            "supplier_sku": "",
            "stock_location": zero_preview["zero_stock_location"],
            "quantity": zero_preview["new_quantity"].astype(int),
            "confidence_status": "update_ready",
            "ProductIDType.IDType": "SKU",
            "ProductIDType.Value": zero_preview["SKU"],
            "AdjustmentType": "AbsoluteAdjustment",
            "AdjustmentAmount": zero_preview["new_quantity"].astype(int),
            "StockLocationID.IDType": "StockLocationReference",
            "StockLocationID.Value": zero_preview["zero_stock_location"],
            "Reason": "Supplier-synced inventory authoritative zero other locations",
        }
    )
    return rows[STOCK_LOCATION_PAYLOAD_COLUMNS]


def _pipe_values(value: Any) -> list[str]:
    return [part.strip() for part in str(value).split("|") if part.strip()]


def _invalid_target_reason(row: pd.Series) -> str:
    reasons = []
    for column in ["ProductID", "SKU", "supplier", "SupplierID", "Supplier.Name", "SupplierSKU"]:
        if not str(row.get(column, "")).strip():
            reasons.append("missing_" + column.replace(".", "_"))
    if str(row.get("ProductID", "")).strip() and not str(row.get("ProductID", "")).strip().isdigit():
        reasons.append("non_integer_ProductID")
    if str(row.get("SupplierID", "")).strip() and not str(row.get("SupplierID", "")).strip().isdigit():
        reasons.append("non_integer_SupplierID")
    if pd.isna(row.get("supplier_free_stock")):
        reasons.append("missing_supplier_stock")
    if row.get("quantity") is None or pd.isna(row.get("quantity")):
        reasons.append("missing_safe_stock")
    return "|".join(reasons)


def _send_fast_stock_batches(client: StoreFeederApiClient, batches: list[list[dict[str, Any]]], out_dir: Path) -> dict[str, Path]:
    paths = {
        "fast_stock_update_success": out_dir / "fast_stock_update_success.csv",
        "fast_stock_update_failures": out_dir / "fast_stock_update_failures.csv",
        "fast_stock_update_batches": out_dir / "fast_stock_update_batches.csv",
        "fast_stock_update_raw_responses": out_dir / "fast_stock_update_raw_responses.json",
    }
    success_rows = []
    failure_rows = []
    batch_rows = []
    raw_responses = []
    report_columns = ["batch_number", "status_code", "ProductID", "SupplierID", "Supplier.Name", "SupplierSKU", "SupplierStockLevel", "SupplierCosts"]
    for batch_number, batch in enumerate(batches, start=1):
        result = client.update_product_supplier_inventory_cost(batch, batch_number=batch_number)
        batch_rows.append(
            {
                "batch_number": result.batch_number,
                "requested_count": result.requested_count,
                "status_code": result.status_code,
                "total_processed": result.total_processed,
                "successful": result.successful,
                "failed": result.failed,
            }
        )
        raw_responses.append({"batch_number": result.batch_number, "status_code": result.status_code, "response": result.response_json})
        target_rows = failure_rows if result.status_code >= 400 or result.failed else success_rows
        for item in batch:
            target_rows.append(_supplier_item_report_row(batch_number, item, result.status_code))

    pd.DataFrame(success_rows, columns=report_columns).to_csv(paths["fast_stock_update_success"], index=False)
    pd.DataFrame(failure_rows, columns=report_columns).to_csv(paths["fast_stock_update_failures"], index=False)
    pd.DataFrame(batch_rows).to_csv(paths["fast_stock_update_batches"], index=False)
    paths["fast_stock_update_raw_responses"].write_text(json.dumps(raw_responses, indent=2), encoding="utf-8")
    return paths


def _send_fast_stock_location_batches(
    client: StoreFeederApiClient,
    batches: list[list[dict[str, Any]]],
    out_dir: Path,
    *,
    file_prefix: str = "fast_stock_location_update",
) -> tuple[dict[str, Path], int]:
    paths = {
        f"{file_prefix}_success": out_dir / f"{file_prefix}_success.csv",
        f"{file_prefix}_failures": out_dir / f"{file_prefix}_failures.csv",
        f"{file_prefix}_batches": out_dir / f"{file_prefix}_batches.csv",
        f"{file_prefix}_raw_responses": out_dir / f"{file_prefix}_raw_responses.json",
    }
    success_rows = []
    failure_rows = []
    batch_rows = []
    raw_responses = []
    retry_count = 0
    report_columns = [
        "batch_number",
        "status_code",
        "SKU",
        "stock_location_id_type",
        "stock_location_id_value",
        "adjustment_type",
        "adjustment_amount",
        "success",
        "error",
    ]
    for batch_number, batch in enumerate(batches, start=1):
        attempt = 0
        while True:
            result = client.update_stock_location_inventory(batch, batch_number=batch_number)
            should_retry = result.status_code >= 500 and attempt < STOCK_LOCATION_MAX_RETRIES
            batch_rows.append(
                {
                    "batch_number": result.batch_number,
                    "attempt": attempt + 1,
                    "requested_count": result.requested_count,
                    "status_code": result.status_code,
                    "total_processed": result.total_processed,
                    "successful": result.successful,
                    "failed": result.failed,
                    "retried": "yes" if should_retry else "no",
                }
            )
            raw_responses.append(
                {
                    "batch_number": result.batch_number,
                    "attempt": attempt + 1,
                    "status_code": result.status_code,
                    "response": result.response_json,
                }
            )
            if not should_retry:
                break
            retry_count += 1
            attempt += 1
            time.sleep(STOCK_LOCATION_RETRY_DELAY_SECONDS)

        _append_stock_location_response_rows(
            success_rows,
            failure_rows,
            batch_number,
            batch,
            result.status_code,
            result.response_json,
        )

    pd.DataFrame(success_rows, columns=report_columns).to_csv(paths[f"{file_prefix}_success"], index=False)
    pd.DataFrame(failure_rows, columns=report_columns).to_csv(paths[f"{file_prefix}_failures"], index=False)
    pd.DataFrame(batch_rows).to_csv(paths[f"{file_prefix}_batches"], index=False)
    paths[f"{file_prefix}_raw_responses"].write_text(json.dumps(raw_responses, indent=2), encoding="utf-8")
    return paths, retry_count


def _supplier_item_report_row(batch_number: int, item: dict[str, Any], status_code: int) -> dict[str, Any]:
    return {
        "batch_number": batch_number,
        "status_code": status_code,
        "ProductID": item["ProductIDType"]["Value"],
        "SupplierID": item["Supplier"]["SupplierID"],
        "Supplier.Name": item["Supplier"]["Name"],
        "SupplierSKU": item["SupplierSKU"],
        "SupplierStockLevel": item["SupplierStockLevel"],
        "SupplierCosts": item["SupplierCosts"],
    }


def _append_stock_location_response_rows(
    success_rows: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
    batch_number: int,
    batch: list[dict[str, Any]],
    status_code: int,
    response_json: dict[str, Any],
) -> None:
    responses = response_json.get("Responses")
    if not isinstance(responses, list) or len(responses) != len(batch):
        batch_success = status_code < 400 and not _response_failed(response_json)
        target_rows = success_rows if batch_success else failure_rows
        for item in batch:
            target_rows.append(
                _stock_location_item_report_row(
                    batch_number,
                    item,
                    status_code,
                    success=batch_success,
                    error=_response_error(response_json) if not batch_success else "",
                )
            )
        return

    for item, item_response in zip(batch, responses):
        if not isinstance(item_response, dict):
            success = False
            error = str(item_response)
        else:
            success = _truthy(item_response.get("Success"))
            error = _response_error(item_response)
        target_rows = success_rows if success else failure_rows
        target_rows.append(_stock_location_item_report_row(batch_number, item, status_code, success=success, error=error))


def _stock_location_item_report_row(
    batch_number: int,
    item: dict[str, Any],
    status_code: int,
    *,
    success: bool,
    error: str,
) -> dict[str, Any]:
    return {
        "batch_number": batch_number,
        "status_code": status_code,
        "SKU": item["ProductIDType"]["Value"],
        "stock_location_id_type": item["StockLocationID"]["IDType"],
        "stock_location_id_value": item["StockLocationID"]["Value"],
        "adjustment_type": item["AdjustmentType"],
        "adjustment_amount": item["AdjustmentAmount"],
        "success": "yes" if success else "no",
        "error": error,
    }


def _response_failed(response_json: dict[str, Any]) -> bool:
    try:
        return int(response_json.get("Failed", 0)) > 0
    except (TypeError, ValueError):
        return False


def _response_error(response_json: dict[str, Any]) -> str:
    for key in ["Error", "Errors", "Message", "ExceptionMessage", "raw_text"]:
        value = response_json.get(key)
        if value:
            return str(value)
    return ""


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in ["true", "1", "yes", "y", "success"]


def _summary_frame(
    targets: pd.DataFrame,
    preview: pd.DataFrame,
    stock_location_preview: pd.DataFrame,
    invalid_rows: pd.DataFrame,
    live: bool,
    *,
    supplier_update_success: int = 0,
    supplier_update_failures: int = 0,
    stock_location_update_success: int = 0,
    stock_location_update_failures: int = 0,
    retry_count: int = 0,
    channel_safety_skips: pd.DataFrame | None = None,
    zero_location_preview: pd.DataFrame | None = None,
    zero_location_safety_skips: pd.DataFrame | None = None,
    zero_other_locations_success: int = 0,
    zero_other_locations_failures: int = 0,
) -> pd.DataFrame:
    channel_safety_skips = channel_safety_skips if channel_safety_skips is not None else pd.DataFrame(columns=CHANNEL_SAFETY_SKIP_COLUMNS)
    zero_location_preview = zero_location_preview if zero_location_preview is not None else pd.DataFrame(columns=ZERO_LOCATION_PREVIEW_COLUMNS)
    zero_location_safety_skips = zero_location_safety_skips if zero_location_safety_skips is not None else pd.DataFrame(columns=ZERO_LOCATION_SAFETY_SKIP_COLUMNS)
    return pd.DataFrame(
        [
            {"metric": "dry_run", "value": "no" if live else "yes"},
            {"metric": "target_rows", "value": len(targets)},
            {"metric": "supplier_payload_rows", "value": len(preview)},
            {"metric": "stock_location_payload_rows", "value": len(stock_location_preview)},
            {"metric": "zero_other_locations_payload_rows", "value": len(zero_location_preview)},
            {"metric": "zero_other_locations_safety_skips", "value": len(zero_location_safety_skips)},
            {"metric": "invalid_rows", "value": len(invalid_rows)},
            {"metric": "channel_decorated_rows", "value": _channel_decorated_count(targets)},
            {
                "metric": "implicit_channel_stock_location_skips",
                "value": _skip_reason_count(channel_safety_skips, "implicit_channel_decorated_stock_location_skip"),
            },
            {"metric": "explicit_stock_location_allowed_rows", "value": _explicit_stock_location_allowed_count(targets)},
            {"metric": "supplier_update_success", "value": supplier_update_success},
            {"metric": "supplier_update_failures", "value": supplier_update_failures},
            {"metric": "stock_location_update_success", "value": stock_location_update_success},
            {"metric": "stock_location_update_failures", "value": stock_location_update_failures},
            {"metric": "zero_other_locations_success", "value": zero_other_locations_success},
            {"metric": "zero_other_locations_failures", "value": zero_other_locations_failures},
            {"metric": "retry_count", "value": retry_count},
            {"metric": "live_stock_update", "value": "yes" if live else "no"},
        ]
    )


def _require_columns(df: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: " + ", ".join(missing))


def _channel_decorated_count(targets: pd.DataFrame) -> int:
    if "SKU" not in targets.columns or "SupplierSKU" not in targets.columns:
        return 0
    return int(
        targets["SKU"].fillna("").astype(str).str.strip().str.casefold().ne(
            targets["SupplierSKU"].fillna("").astype(str).str.strip().str.casefold()
        ).sum()
    )


def _explicit_stock_location_allowed_count(targets: pd.DataFrame) -> int:
    if "SKU" not in targets.columns or "SupplierSKU" not in targets.columns or "allow_stock_location_update" not in targets.columns:
        return 0
    decorated = targets["SKU"].fillna("").astype(str).str.strip().str.casefold().ne(
        targets["SupplierSKU"].fillna("").astype(str).str.strip().str.casefold()
    )
    allowed = targets["allow_stock_location_update"].fillna("").astype(str).str.strip().str.casefold().isin(["yes", "true", "1", "y"])
    return int((decorated & allowed).sum())


def _skip_reason_count(channel_safety_skips: pd.DataFrame, reason: str) -> int:
    if channel_safety_skips.empty or "skip_reason" not in channel_safety_skips.columns:
        return 0
    return int(channel_safety_skips["skip_reason"].astype(str).eq(reason).sum())


def _append_reason(existing: str, reason: str) -> str:
    return reason if not str(existing).strip() else str(existing).strip() + "|" + reason


def _csv_count(path: Path) -> int:
    try:
        return len(pd.read_csv(path, dtype=str, keep_default_na=False))
    except pd.errors.EmptyDataError:
        return 0


def _run(command: list[str], label: str) -> None:
    print(f"\n== {label} ==")
    print(" ".join(command))
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        if name and name not in os.environ:
            os.environ[name] = value


if __name__ == "__main__":
    raise SystemExit(main())
