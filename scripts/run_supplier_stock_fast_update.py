from __future__ import annotations

import argparse
import json
import os
import shutil
import time
import traceback
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
from scripts.run_new_product_onboarding_delta import _load_products as _load_live_products


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
    "ProductID",
    "SKU",
    "SupplierSKU",
    "supplier",
    "supplier_id",
    "supplier_sku",
    "stock_location",
    "stock_strategy",
    "skip_stock_location_update",
    "allow_stock_location_update",
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
    "SupplierSKU",
    "supplier",
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
PRODUCT_ID_RECONCILIATION_COLUMNS = [
    "SKU",
    "SupplierSKU",
    "supplier",
    "stock_strategy",
    "old_ProductID",
    "new_ProductID",
    "reason",
]
PRODUCT_ID_RECONCILIATION_QUARANTINE_COLUMNS = [
    "SKU",
    "SupplierSKU",
    "supplier",
    "stock_strategy",
    "ProductID",
    "reason",
]


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
    parser.add_argument("--scheduled-run", action="store_true", help="Enable scheduled-run stale report guard fields.")
    parser.add_argument("--verify-live-sample", type=int, default=0)
    parser.add_argument("--verify-live-strict", action="store_true")
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
    if args.verify_live_sample < 0:
        parser.error("--verify-live-sample must be zero or greater")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(PROJECT_ROOT / ".env")
    out_dir = args.out_dir or Path("reports/fast_stock_updates") / datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    target_file_mtime = _file_mtime(args.targets)
    current_target_rows = _csv_count(args.targets)

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
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    live_products = _load_live_products(client, page_size=100, limit=None)
    product_id_reconciliation, product_id_quarantine, targets = _reconcile_runtime_product_ids(targets, live_products)
    if args.live_stock_update and not product_id_reconciliation.empty:
        _backup_and_write_targets(args.targets, targets, out_dir.name)
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
        "supplier_info_only_payload_preview": out_dir / "supplier_info_only_payload_preview.csv",
        "fast_stock_supplier_info_only_payload_preview": out_dir / "fast_stock_supplier_info_only_payload_preview.csv",
        "fast_stock_location_payload_preview": out_dir / "fast_stock_location_payload_preview.csv",
        "fast_stock_location_zero_payload_preview": out_dir / "fast_stock_location_zero_payload_preview.csv",
        "fast_stock_location_zero_safety_skips": out_dir / "fast_stock_location_zero_safety_skips.csv",
        "fast_stock_summary": out_dir / "fast_stock_summary.csv",
        "fast_stock_invalid_rows": out_dir / "fast_stock_invalid_rows.csv",
        "fast_stock_channel_safety_skips": out_dir / "fast_stock_channel_safety_skips.csv",
        "stale_fast_sync_guard": out_dir / "stale_fast_sync_guard.csv",
        "fast_stock_product_id_reconciliation": out_dir / "fast_stock_product_id_reconciliation.csv",
        "fast_stock_product_id_quarantine": out_dir / "fast_stock_product_id_quarantine.csv",
    }
    preview.to_csv(paths["fast_stock_payload_preview"], index=False)
    supplier_info_only_preview = _supplier_info_only_payload_preview(preview, targets)
    supplier_info_only_preview.to_csv(paths["supplier_info_only_payload_preview"], index=False)
    supplier_info_only_preview.to_csv(paths["fast_stock_supplier_info_only_payload_preview"], index=False)
    stock_location_preview.to_csv(paths["fast_stock_location_payload_preview"], index=False)
    zero_location_preview.to_csv(paths["fast_stock_location_zero_payload_preview"], index=False)
    zero_location_safety_skips.to_csv(paths["fast_stock_location_zero_safety_skips"], index=False)
    if not product_id_quarantine.empty:
        product_id_invalid = product_id_quarantine.rename(columns={"reason": "invalid_reason"}).copy()
        invalid_rows = pd.concat([invalid_rows, product_id_invalid], ignore_index=True, sort=False)
    invalid_rows.to_csv(paths["fast_stock_invalid_rows"], index=False)
    channel_safety_skips.to_csv(paths["fast_stock_channel_safety_skips"], index=False)
    product_id_reconciliation.to_csv(paths["fast_stock_product_id_reconciliation"], index=False)
    product_id_quarantine.to_csv(paths["fast_stock_product_id_quarantine"], index=False)
    summary = _summary_frame(
        targets,
        preview,
        stock_location_preview,
        invalid_rows,
        args.live_stock_update,
        channel_safety_skips=channel_safety_skips,
        zero_location_preview=zero_location_preview,
        zero_location_safety_skips=zero_location_safety_skips,
        supplier_info_only_preview=supplier_info_only_preview,
        current_target_rows=current_target_rows,
        target_file_mtime=target_file_mtime,
        scheduled_run=args.scheduled_run,
        product_id_reconciliation=product_id_reconciliation,
        product_id_quarantine=product_id_quarantine,
    )
    stale_guard = _stale_fast_sync_guard(args.targets, current_target_rows, len(targets), target_file_mtime, args.scheduled_run)
    stale_guard.to_csv(paths["stale_fast_sync_guard"], index=False)
    summary.to_csv(paths["fast_stock_summary"], index=False)

    print("Fast StoreFeeder stock update dry run" if not args.live_stock_update else "Fast StoreFeeder stock update live run")
    print(summary.to_string(index=False))
    print("\nWrote reports:")
    for path in paths.values():
        print(path)

    if not invalid_rows.empty:
        raise SystemExit("Blocked fast stock update because invalid target/stock rows are present.")
    if current_target_rows != len(targets):
        raise SystemExit(
            f"Critical target row count mismatch: current_target_rows={current_target_rows}, "
            f"target_rows_loaded_this_run={len(targets)}"
        )
    if preview.empty:
        raise SystemExit("Blocked fast stock update because payload preview is empty.")
    if not args.live_stock_update:
        print("\nDry-run only. No StoreFeeder write API calls were made.")
        return 0

    items = supplier_payload_preview_to_items(preview)
    batches = batch_items(items, args.api_batch_size)
    stock_location_items = payload_preview_to_storefeeder_items(stock_location_preview)
    stock_location_batches = batch_items(stock_location_items, args.api_batch_size)
    zero_location_items = payload_preview_to_storefeeder_items(_zero_preview_to_stock_location_payload(zero_location_preview))
    zero_location_batches = batch_items(zero_location_items, args.api_batch_size)
    live_paths = _send_fast_stock_batches(client, batches, out_dir)
    location_live_paths, stock_location_retry_count = _send_fast_stock_location_batches(
        client,
        stock_location_batches,
        out_dir,
        source_preview=stock_location_preview,
    )
    zero_location_live_paths, zero_location_retry_count = _send_fast_stock_location_batches(
        client,
        zero_location_batches,
        out_dir,
        file_prefix="fast_stock_location_zero_update",
        source_preview=_zero_preview_to_stock_location_payload(zero_location_preview),
    )
    verification_sample = pd.DataFrame()
    verification_failures = pd.DataFrame()
    if args.verify_live_sample:
        verification_sample, verification_failures = _verify_live_sample(client, stock_location_preview, args.verify_live_sample)
    verification_sample_path = out_dir / "fast_stock_live_verification_sample.csv"
    verification_failures_path = out_dir / "fast_stock_live_verification_failures.csv"
    verification_sample.to_csv(verification_sample_path, index=False)
    verification_failures.to_csv(verification_failures_path, index=False)
    supplier_update_failures = _csv_count(live_paths["fast_stock_update_failures"])
    supplier_retry_success = _csv_count(live_paths["fast_stock_supplier_update_retry_success"])
    supplier_retry_failures = _csv_count(live_paths["fast_stock_supplier_update_retry_failures"])
    supplier_missing_recovered = _csv_count(live_paths["fast_stock_supplier_missing_product_supplier_recovered"])
    supplier_info_only_success = _supplier_info_only_update_count(live_paths["fast_stock_update_success"], supplier_info_only_preview)
    supplier_info_only_failures = _supplier_info_only_update_count(live_paths["fast_stock_update_failures"], supplier_info_only_preview)
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
        supplier_info_only_preview=supplier_info_only_preview,
        zero_other_locations_success=_csv_count(zero_location_live_paths["fast_stock_location_zero_update_success"]),
        zero_other_locations_failures=zero_location_update_failures,
        supplier_update_initial_failures=supplier_retry_success + supplier_retry_failures,
        supplier_update_retry_success=supplier_retry_success,
        supplier_update_retry_failures=supplier_retry_failures,
        supplier_update_missing_product_supplier_recovered=supplier_missing_recovered,
        supplier_update_persistent_failures=supplier_update_failures,
        supplier_info_only_supplier_update_success=supplier_info_only_success,
        supplier_info_only_supplier_update_failures=supplier_info_only_failures,
        current_target_rows=current_target_rows,
        target_file_mtime=target_file_mtime,
        scheduled_run=args.scheduled_run,
        product_id_reconciliation=product_id_reconciliation,
        product_id_quarantine=product_id_quarantine,
    )
    summary.to_csv(paths["fast_stock_summary"], index=False)
    print("\nLive stock update reports:")
    for path in [*live_paths.values(), *location_live_paths.values(), *zero_location_live_paths.values()]:
        print(path)
    if args.verify_live_sample:
        print(verification_sample_path)
        print(verification_failures_path)
    if args.verify_live_strict and not verification_failures.empty:
        raise SystemExit(f"Fast stock live verification failed for {len(verification_failures)} sampled rows")
    if supplier_update_failures or stock_location_update_failures or zero_location_update_failures:
        raise SystemExit(
            "Fast stock update completed with failures: "
            f"supplier_update_failures={supplier_update_failures}, "
            f"stock_location_update_failures={stock_location_update_failures}, "
            f"zero_other_locations_failures={zero_location_update_failures}"
        )
    return 0


def _reconcile_runtime_product_ids(targets: pd.DataFrame, products: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if targets.empty:
        return (
            pd.DataFrame(columns=PRODUCT_ID_RECONCILIATION_COLUMNS),
            pd.DataFrame(columns=PRODUCT_ID_RECONCILIATION_QUARANTINE_COLUMNS),
            targets.copy(),
        )
    if "SKU" not in targets.columns or "ProductID" not in targets.columns:
        return (
            pd.DataFrame(columns=PRODUCT_ID_RECONCILIATION_COLUMNS),
            pd.DataFrame(
                [{"reason": "target_file_missing_SKU_or_ProductID_columns"}],
                columns=PRODUCT_ID_RECONCILIATION_QUARANTINE_COLUMNS,
            ),
            targets.iloc[0:0].copy(),
        )

    products = products.copy()
    if products.empty or "SKU" not in products.columns or "ID" not in products.columns:
        quarantine = targets.apply(
            lambda row: _product_id_quarantine_row(row, "live_product_catalogue_unavailable"),
            axis=1,
        ).tolist()
        return (
            pd.DataFrame(columns=PRODUCT_ID_RECONCILIATION_COLUMNS),
            pd.DataFrame(quarantine, columns=PRODUCT_ID_RECONCILIATION_QUARANTINE_COLUMNS),
            targets.iloc[0:0].copy(),
        )

    products["_sku_key"] = products["SKU"].fillna("").astype(str).str.strip().str.casefold()
    products["_product_id"] = products["ID"].fillna("").astype(str).str.strip()
    products = products[products["_sku_key"].ne("") & products["_product_id"].ne("")].copy()
    live_counts = products["_sku_key"].value_counts().to_dict()
    live_unique = products[~products["_sku_key"].duplicated(keep=False)].set_index("_sku_key")["_product_id"].to_dict()

    reconciled = targets.copy()
    keep_mask = pd.Series(True, index=reconciled.index)
    reconciliation_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []

    for index, row in reconciled.iterrows():
        sku = str(row.get("SKU", "")).strip()
        sku_key = sku.casefold()
        old_product_id = str(row.get("ProductID", "")).strip()
        if not sku_key:
            quarantine_rows.append(_product_id_quarantine_row(row, "missing_target_sku"))
            keep_mask.loc[index] = False
            continue
        if live_counts.get(sku_key, 0) > 1:
            quarantine_rows.append(_product_id_quarantine_row(row, "duplicate_live_storefeeder_sku"))
            keep_mask.loc[index] = False
            continue
        new_product_id = live_unique.get(sku_key, "")
        if not new_product_id:
            quarantine_rows.append(_product_id_quarantine_row(row, "missing_live_storefeeder_sku"))
            keep_mask.loc[index] = False
            continue
        if old_product_id != new_product_id:
            reconciliation_rows.append(
                {
                    "SKU": sku,
                    "SupplierSKU": str(row.get("SupplierSKU", "")).strip(),
                    "supplier": str(row.get("supplier", "")).strip(),
                    "stock_strategy": str(row.get("stock_strategy", "")).strip(),
                    "old_ProductID": old_product_id,
                    "new_ProductID": new_product_id,
                    "reason": "exact_unique_live_sku_productid_changed",
                }
            )
            reconciled.at[index, "ProductID"] = new_product_id

    return (
        pd.DataFrame(reconciliation_rows, columns=PRODUCT_ID_RECONCILIATION_COLUMNS),
        pd.DataFrame(quarantine_rows, columns=PRODUCT_ID_RECONCILIATION_QUARANTINE_COLUMNS),
        reconciled[keep_mask].reset_index(drop=True),
    )


def _product_id_quarantine_row(row: pd.Series, reason: str) -> dict[str, str]:
    return {
        "SKU": str(row.get("SKU", "")).strip(),
        "SupplierSKU": str(row.get("SupplierSKU", "")).strip(),
        "supplier": str(row.get("supplier", "")).strip(),
        "stock_strategy": str(row.get("stock_strategy", "")).strip(),
        "ProductID": str(row.get("ProductID", "")).strip(),
        "reason": reason,
    }


def _backup_and_write_targets(target_path: Path, targets: pd.DataFrame, run_id: str) -> None:
    backup_path = target_path.with_name(f"{target_path.stem}.backup_{run_id}{target_path.suffix}")
    shutil.copy2(target_path, backup_path)
    targets.to_csv(target_path, index=False)


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
    strategy_key = valid["stock_strategy"].astype(str).str.strip().str.casefold()
    warehouse_only_mask = strategy_key.eq("warehouse_only")
    supplier_info_only_mask = strategy_key.eq("supplier_info_only_manual_inventory")
    valid.loc[warehouse_only_mask & valid["_stock_location_skip_reason"].eq(""), "_stock_location_skip_reason"] = "warehouse_only_stock_location_protected"
    valid.loc[supplier_info_only_mask & valid["_stock_location_skip_reason"].eq(""), "_stock_location_skip_reason"] = "supplier_info_only_manual_inventory_skip"
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
        product_id = str(row["ProductID"]).strip()
        sku = str(row["SKU"]).strip()
        supplier = str(row["supplier"]).strip()
        supplier_sku = str(row["SupplierSKU"]).strip()
        supplier_id = str(row["SupplierID"]).strip()
        stock_strategy = str(row.get("stock_strategy", "")).strip()
        skip_stock_location_update = str(row.get("skip_stock_location_update", "")).strip()
        allow_stock_location_update = str(row.get("allow_stock_location_update", "")).strip()
        target_location = str(row["inventory_stock_location"]).strip()
        quantity = int(row["quantity"])
        if str(row.get("_stock_location_skip_reason", "")).strip():
            continue
        if target_location:
            rows.append(
                _stock_location_payload_row(
                    product_id,
                    sku,
                    supplier,
                    supplier_id,
                    supplier_sku,
                    stock_strategy,
                    skip_stock_location_update,
                    allow_stock_location_update,
                    target_location,
                    quantity,
                )
            )
        for clear_location in _pipe_values(row["clear_stock_locations"]):
            if clear_location.casefold() == target_location.casefold():
                continue
            rows.append(
                _stock_location_payload_row(
                    product_id,
                    sku,
                    supplier,
                    supplier_id,
                    supplier_sku,
                    stock_strategy,
                    skip_stock_location_update,
                    allow_stock_location_update,
                    clear_location,
                    0,
                )
            )
    return pd.DataFrame(rows, columns=STOCK_LOCATION_PAYLOAD_COLUMNS)


def _stock_location_payload_row(
    product_id: str,
    sku: str,
    supplier: str,
    supplier_id: str,
    supplier_sku: str,
    stock_strategy: str,
    skip_stock_location_update: str,
    allow_stock_location_update: str,
    stock_location: str,
    quantity: int,
) -> dict[str, Any]:
    return {
        "ProductID": product_id,
        "SKU": sku,
        "SupplierSKU": supplier_sku,
        "supplier": supplier,
        "supplier_id": supplier_id,
        "supplier_sku": supplier_sku,
        "stock_location": stock_location,
        "stock_strategy": stock_strategy,
        "skip_stock_location_update": skip_stock_location_update,
        "allow_stock_location_update": allow_stock_location_update,
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

        if strategy in {"warehouse_only", "supplier_info_only_manual_inventory"}:
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
                    "SupplierSKU": str(row.get("SupplierSKU", "")).strip(),
                    "supplier": str(row.get("supplier", "")).strip(),
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
            "ProductID": zero_preview.get("ProductID", ""),
            "SKU": zero_preview["SKU"],
            "SupplierSKU": zero_preview.get("SupplierSKU", ""),
            "supplier": zero_preview.get("supplier", ""),
            "supplier_id": "",
            "supplier_sku": zero_preview.get("SupplierSKU", ""),
            "stock_location": zero_preview["zero_stock_location"],
            "stock_strategy": zero_preview.get("stock_strategy", ""),
            "skip_stock_location_update": "",
            "allow_stock_location_update": "",
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
        "fast_stock_supplier_update_retry_success": out_dir / "fast_stock_supplier_update_retry_success.csv",
        "fast_stock_supplier_update_retry_failures": out_dir / "fast_stock_supplier_update_retry_failures.csv",
        "fast_stock_supplier_missing_product_supplier_recovered": out_dir / "fast_stock_supplier_missing_product_supplier_recovered.csv",
    }
    success_rows = []
    initial_failure_rows = []
    retry_success_rows = []
    retry_failure_rows = []
    batch_rows = []
    raw_responses = []
    report_columns = ["batch_number", "status_code", "ProductID", "SupplierID", "Supplier.Name", "SupplierSKU", "SupplierStockLevel", "SupplierCosts", "success", "error"]
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
                "retry": "no",
            }
        )
        raw_responses.append({"batch_number": result.batch_number, "status_code": result.status_code, "response": result.response_json})
        failed_items = _append_supplier_response_rows(success_rows, initial_failure_rows, batch_number, batch, result.status_code, result.response_json)
        for retry_index, item in enumerate(failed_items, start=1):
            retry_batch_number = (batch_number * 1000) + retry_index
            retry_result = client.update_product_supplier_inventory_cost([item], batch_number=retry_batch_number)
            batch_rows.append(
                {
                    "batch_number": retry_result.batch_number,
                    "requested_count": retry_result.requested_count,
                    "status_code": retry_result.status_code,
                    "total_processed": retry_result.total_processed,
                    "successful": retry_result.successful,
                    "failed": retry_result.failed,
                    "retry": "individual",
                }
            )
            raw_responses.append({"batch_number": retry_result.batch_number, "status_code": retry_result.status_code, "response": retry_result.response_json})
            _append_supplier_response_rows(retry_success_rows, retry_failure_rows, retry_batch_number, [item], retry_result.status_code, retry_result.response_json)

    final_failure_rows = retry_failure_rows if retry_failure_rows else initial_failure_rows
    pd.DataFrame(success_rows, columns=report_columns).to_csv(paths["fast_stock_update_success"], index=False)
    pd.DataFrame(final_failure_rows, columns=report_columns).to_csv(paths["fast_stock_update_failures"], index=False)
    pd.DataFrame(retry_success_rows, columns=report_columns).to_csv(paths["fast_stock_supplier_update_retry_success"], index=False)
    pd.DataFrame(retry_failure_rows, columns=report_columns).to_csv(paths["fast_stock_supplier_update_retry_failures"], index=False)
    pd.DataFrame(columns=report_columns).to_csv(paths["fast_stock_supplier_missing_product_supplier_recovered"], index=False)
    pd.DataFrame(batch_rows).to_csv(paths["fast_stock_update_batches"], index=False)
    paths["fast_stock_update_raw_responses"].write_text(json.dumps(raw_responses, indent=2), encoding="utf-8")
    return paths


def _append_supplier_response_rows(
    success_rows: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
    batch_number: int,
    batch: list[dict[str, Any]],
    status_code: int,
    response_json: dict[str, Any],
) -> list[dict[str, Any]]:
    failed_items: list[dict[str, Any]] = []
    responses = response_json.get("Responses")
    if not isinstance(responses, list) or len(responses) != len(batch):
        batch_success = status_code < 400 and not _response_failed(response_json)
        target_rows = success_rows if batch_success else failure_rows
        error = "" if batch_success else _response_error(response_json)
        for item in batch:
            target_rows.append(_supplier_item_report_row(batch_number, item, status_code, success=batch_success, error=error))
            if not batch_success:
                failed_items.append(item)
        return failed_items
    for item, item_response in zip(batch, responses):
        if isinstance(item_response, dict):
            success = _truthy(item_response.get("Success"))
            error = _response_error(item_response)
        else:
            success = False
            error = str(item_response)
        target_rows = success_rows if success else failure_rows
        target_rows.append(_supplier_item_report_row(batch_number, item, status_code, success=success, error=error))
        if not success:
            failed_items.append(item)
    return failed_items

def _send_fast_stock_location_batches(
    client: StoreFeederApiClient,
    batches: list[list[dict[str, Any]]],
    out_dir: Path,
    *,
    file_prefix: str = "fast_stock_location_update",
    source_preview: pd.DataFrame | None = None,
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
        "ProductID",
        "SKU",
        "SupplierSKU",
        "supplier",
        "supplier_id",
        "stock_strategy",
        "skip_stock_location_update",
        "allow_stock_location_update",
        "stock_location_id_type",
        "stock_location_id_value",
        "adjustment_type",
        "adjustment_amount",
        "success",
        "error",
    ]
    metadata = _stock_location_metadata(source_preview)
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
            metadata,
        )

    pd.DataFrame(success_rows, columns=report_columns).to_csv(paths[f"{file_prefix}_success"], index=False)
    pd.DataFrame(failure_rows, columns=report_columns).to_csv(paths[f"{file_prefix}_failures"], index=False)
    pd.DataFrame(batch_rows).to_csv(paths[f"{file_prefix}_batches"], index=False)
    paths[f"{file_prefix}_raw_responses"].write_text(json.dumps(raw_responses, indent=2), encoding="utf-8")
    return paths, retry_count


def _supplier_item_report_row(batch_number: int, item: dict[str, Any], status_code: int, *, success: bool, error: str) -> dict[str, Any]:
    return {
        "batch_number": batch_number,
        "status_code": status_code,
        "ProductID": item["ProductIDType"]["Value"],
        "SupplierID": item["Supplier"]["SupplierID"],
        "Supplier.Name": item["Supplier"]["Name"],
        "SupplierSKU": item["SupplierSKU"],
        "SupplierStockLevel": item["SupplierStockLevel"],
        "SupplierCosts": item["SupplierCosts"],
        "success": "yes" if success else "no",
        "error": error,
    }


def _append_stock_location_response_rows(
    success_rows: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
    batch_number: int,
    batch: list[dict[str, Any]],
    status_code: int,
    response_json: dict[str, Any],
    metadata: dict[tuple[str, str, int], dict[str, Any]] | None = None,
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
                    metadata=metadata,
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
        target_rows.append(_stock_location_item_report_row(batch_number, item, status_code, success=success, error=error, metadata=metadata))


def _stock_location_item_report_row(
    batch_number: int,
    item: dict[str, Any],
    status_code: int,
    *,
    success: bool,
    error: str,
    metadata: dict[tuple[str, str, int], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    meta = _stock_location_item_metadata(item, metadata)
    return {
        "batch_number": batch_number,
        "status_code": status_code,
        "ProductID": meta.get("ProductID", ""),
        "SKU": item["ProductIDType"]["Value"],
        "SupplierSKU": meta.get("SupplierSKU", ""),
        "supplier": meta.get("supplier", ""),
        "supplier_id": meta.get("supplier_id", ""),
        "stock_strategy": meta.get("stock_strategy", ""),
        "skip_stock_location_update": meta.get("skip_stock_location_update", ""),
        "allow_stock_location_update": meta.get("allow_stock_location_update", ""),
        "stock_location_id_type": item["StockLocationID"]["IDType"],
        "stock_location_id_value": item["StockLocationID"]["Value"],
        "adjustment_type": item["AdjustmentType"],
        "adjustment_amount": item["AdjustmentAmount"],
        "success": "yes" if success else "no",
        "error": error,
    }


def _stock_location_metadata(preview: pd.DataFrame | None) -> dict[tuple[str, str, int], dict[str, Any]]:
    if preview is None or preview.empty:
        return {}
    rows: dict[tuple[str, str, int], dict[str, Any]] = {}
    for _, row in preview.iterrows():
        sku = str(row.get("ProductIDType.Value", row.get("SKU", ""))).strip()
        location = str(row.get("StockLocationID.Value", row.get("stock_location", ""))).strip()
        try:
            amount = int(row.get("AdjustmentAmount", row.get("quantity", 0)))
        except (TypeError, ValueError):
            amount = 0
        rows[(sku.casefold(), location.casefold(), amount)] = {
            "ProductID": str(row.get("ProductID", "")).strip(),
            "SupplierSKU": str(row.get("SupplierSKU", row.get("supplier_sku", ""))).strip(),
            "supplier": str(row.get("supplier", "")).strip(),
            "supplier_id": str(row.get("supplier_id", row.get("SupplierID", ""))).strip(),
            "stock_strategy": str(row.get("stock_strategy", "")).strip(),
            "skip_stock_location_update": str(row.get("skip_stock_location_update", "")).strip(),
            "allow_stock_location_update": str(row.get("allow_stock_location_update", "")).strip(),
        }
    return rows


def _stock_location_item_metadata(item: dict[str, Any], metadata: dict[tuple[str, str, int], dict[str, Any]] | None) -> dict[str, Any]:
    if not metadata:
        return {}
    try:
        amount = int(item.get("AdjustmentAmount", 0))
    except (TypeError, ValueError):
        amount = 0
    key = (
        str(item.get("ProductIDType", {}).get("Value", "")).strip().casefold(),
        str(item.get("StockLocationID", {}).get("Value", "")).strip().casefold(),
        amount,
    )
    return metadata.get(key, {})


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



def _supplier_info_only_payload_preview(preview: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    if preview.empty or targets.empty or "stock_strategy" not in targets.columns:
        return pd.DataFrame(columns=PAYLOAD_COLUMNS)
    target_keys = targets[targets["stock_strategy"].fillna("").astype(str).str.strip().str.casefold().eq("supplier_info_only_manual_inventory")].copy()
    if target_keys.empty:
        return pd.DataFrame(columns=PAYLOAD_COLUMNS)
    keys = set(zip(target_keys["ProductID"].astype(str), target_keys["SupplierSKU"].astype(str).str.upper()))
    subset = preview[preview.apply(lambda row: (str(row["ProductID"]), str(row["SupplierSKU"]).upper()) in keys, axis=1)].copy()
    return subset.reindex(columns=PAYLOAD_COLUMNS, fill_value="")

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
    supplier_info_only_preview: pd.DataFrame | None = None,
    zero_other_locations_success: int = 0,
    zero_other_locations_failures: int = 0,
    supplier_update_initial_failures: int = 0,
    supplier_update_retry_success: int = 0,
    supplier_update_retry_failures: int = 0,
    supplier_update_missing_product_supplier_recovered: int = 0,
    supplier_update_persistent_failures: int = 0,
    supplier_info_only_supplier_update_success: int = 0,
    supplier_info_only_supplier_update_failures: int = 0,
    current_target_rows: int | None = None,
    target_file_mtime: str = "",
    scheduled_run: bool = False,
    product_id_reconciliation: pd.DataFrame | None = None,
    product_id_quarantine: pd.DataFrame | None = None,
) -> pd.DataFrame:
    channel_safety_skips = channel_safety_skips if channel_safety_skips is not None else pd.DataFrame(columns=CHANNEL_SAFETY_SKIP_COLUMNS)
    zero_location_preview = zero_location_preview if zero_location_preview is not None else pd.DataFrame(columns=ZERO_LOCATION_PREVIEW_COLUMNS)
    zero_location_safety_skips = zero_location_safety_skips if zero_location_safety_skips is not None else pd.DataFrame(columns=ZERO_LOCATION_SAFETY_SKIP_COLUMNS)
    supplier_info_only_preview = supplier_info_only_preview if supplier_info_only_preview is not None else pd.DataFrame(columns=PAYLOAD_COLUMNS)
    product_id_reconciliation = product_id_reconciliation if product_id_reconciliation is not None else pd.DataFrame(columns=PRODUCT_ID_RECONCILIATION_COLUMNS)
    product_id_quarantine = product_id_quarantine if product_id_quarantine is not None else pd.DataFrame(columns=PRODUCT_ID_RECONCILIATION_QUARANTINE_COLUMNS)
    return pd.DataFrame(
        [
            {"metric": "dry_run", "value": "no" if live else "yes"},
            {"metric": "target_rows", "value": len(targets)},
            {"metric": "current_target_rows", "value": current_target_rows if current_target_rows is not None else len(targets)},
            {"metric": "target_rows_loaded_this_run", "value": len(targets)},
            {"metric": "target_file_mtime", "value": target_file_mtime},
            {"metric": "scheduled_run", "value": "yes" if scheduled_run else "no"},
            {"metric": "supplier_payload_rows", "value": len(preview)},
            {"metric": "runtime_product_id_reconciliations", "value": len(product_id_reconciliation)},
            {"metric": "runtime_product_id_quarantine_rows", "value": len(product_id_quarantine)},
            {"metric": "supplier_info_only_target_rows", "value": _supplier_info_only_target_count(targets)},
            {"metric": "supplier_info_only_rows", "value": len(supplier_info_only_preview)},
            {"metric": "supplier_info_only_payload_rows", "value": len(supplier_info_only_preview)},
            {"metric": "supplier_info_only_supplier_updates", "value": len(supplier_info_only_preview)},
            {"metric": "supplier_info_only_supplier_update_success", "value": supplier_info_only_supplier_update_success},
            {"metric": "supplier_info_only_supplier_update_failures", "value": supplier_info_only_supplier_update_failures},
            {"metric": "supplier_info_only_inventory_skips", "value": len(supplier_info_only_preview)},
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
            {"metric": "supplier_update_initial_failures", "value": supplier_update_initial_failures},
            {"metric": "supplier_update_retry_success", "value": supplier_update_retry_success},
            {"metric": "supplier_update_retry_failures", "value": supplier_update_retry_failures},
            {"metric": "supplier_update_missing_product_supplier_recovered", "value": supplier_update_missing_product_supplier_recovered},
            {"metric": "supplier_update_persistent_failures", "value": supplier_update_persistent_failures},
            {"metric": "stock_location_update_success", "value": stock_location_update_success},
            {"metric": "stock_location_update_failures", "value": stock_location_update_failures},
            {"metric": "zero_other_locations_success", "value": zero_other_locations_success},
            {"metric": "zero_other_locations_failures", "value": zero_other_locations_failures},
            {"metric": "retry_count", "value": retry_count},
            {"metric": "live_stock_update", "value": "yes" if live else "no"},
        ]
    )


def _stale_fast_sync_guard(target_path: Path, current_target_rows: int, loaded_target_rows: int, target_file_mtime: str, scheduled_run: bool) -> pd.DataFrame:
    rows = [
        {"metric": "current_target_rows", "value": current_target_rows},
        {"metric": "target_rows_loaded_this_run", "value": loaded_target_rows},
        {"metric": "target_file_mtime", "value": target_file_mtime},
        {"metric": "scheduled_run", "value": "yes" if scheduled_run else "no"},
        {"metric": "row_count_match", "value": "yes" if current_target_rows == loaded_target_rows else "no"},
        {"metric": "target_file", "value": str(target_path)},
    ]
    stale = False
    if scheduled_run:
        latest_onboarding = _latest_report_file(Path("reports/new_product_onboarding_delta"), "onboarding_summary.csv")
        latest_fast = _latest_report_file(Path("reports/scheduled_fast_stock_sync"), "fast_stock_summary.csv")
        latest_onboarding_mtime = _file_mtime(latest_onboarding) if latest_onboarding else ""
        latest_fast_mtime = _file_mtime(latest_fast) if latest_fast else ""
        if latest_onboarding and latest_fast and latest_onboarding.stat().st_mtime > latest_fast.stat().st_mtime:
            stale = True
        if latest_fast and target_path.exists() and target_path.stat().st_mtime > latest_fast.stat().st_mtime:
            stale = True
        rows.extend(
            [
                {"metric": "latest_onboarding_summary", "value": str(latest_onboarding or "")},
                {"metric": "latest_onboarding_summary_mtime", "value": latest_onboarding_mtime},
                {"metric": "latest_fast_summary", "value": str(latest_fast or "")},
                {"metric": "latest_fast_summary_mtime", "value": latest_fast_mtime},
                {"metric": "stale_fast_sync_detected", "value": "yes" if stale else "no"},
            ]
        )
    else:
        rows.append({"metric": "stale_fast_sync_detected", "value": "no"})
    return pd.DataFrame(rows)


def _latest_report_file(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    files = [path / filename for path in root.iterdir() if path.is_dir() and (path / filename).exists()]
    return max(files, key=lambda path: path.stat().st_mtime) if files else None


def _file_mtime(path: Path | None) -> str:
    if not path or not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


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


def _supplier_info_only_target_count(targets: pd.DataFrame) -> int:
    if "stock_strategy" not in targets.columns:
        return 0
    return int(
        targets["stock_strategy"].fillna("").astype(str).str.strip().str.casefold().eq("supplier_info_only_manual_inventory").sum()
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


def _supplier_info_only_update_count(report_path: Path, supplier_info_only_preview: pd.DataFrame) -> int:
    if supplier_info_only_preview.empty or not report_path.exists():
        return 0
    try:
        report = pd.read_csv(report_path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return 0
    if report.empty:
        return 0
    keys = set(
        zip(
            supplier_info_only_preview["ProductID"].astype(str).str.strip(),
            supplier_info_only_preview["SupplierSKU"].astype(str).str.strip().str.casefold(),
        )
    )
    if not keys:
        return 0
    return int(
        report.apply(
            lambda row: (
                str(row.get("ProductID", "")).strip(),
                str(row.get("SupplierSKU", "")).strip().casefold(),
            )
            in keys,
            axis=1,
        ).sum()
    )


def _verify_live_sample(client: StoreFeederApiClient, stock_location_preview: pd.DataFrame, sample_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = ["ProductID", "SKU", "stock_location", "intended_quantity", "live_quantity", "verification_status", "reason"]
    if stock_location_preview.empty or sample_size <= 0:
        empty = pd.DataFrame(columns=columns)
        return empty, empty.copy()
    sample = stock_location_preview.drop_duplicates(subset=["ProductID", "SKU", "stock_location"]).head(sample_size).copy()
    rows: list[dict[str, Any]] = []
    for _, row in sample.iterrows():
        product_id = str(row.get("ProductID", "")).strip()
        sku = str(row.get("SKU", "")).strip()
        location = str(row.get("stock_location", "")).strip()
        intended = str(row.get("quantity", "")).strip()
        live_quantity = ""
        status = "not_verified"
        reason = ""
        if not product_id:
            reason = "missing_product_id"
        else:
            try:
                wrapper = client.get_product(product_id)
                if int(wrapper.get("_status_code", 0)) >= 400:
                    reason = f"GET product failed: {wrapper.get('_status_code')}"
                else:
                    live_quantity = _live_stock_location_quantity(wrapper.get("response", {}), location)
                    if live_quantity == "":
                        status = "failed"
                        reason = "stock_location_not_present_in_live_readback"
                    elif str(live_quantity).strip() == intended:
                        status = "verified"
                        reason = "live quantity matches intended quantity"
                    else:
                        status = "failed"
                        reason = "live quantity differs from intended quantity"
            except Exception as exc:
                status = "failed"
                reason = str(exc)
        rows.append(
            {
                "ProductID": product_id,
                "SKU": sku,
                "stock_location": location,
                "intended_quantity": intended,
                "live_quantity": live_quantity,
                "verification_status": status,
                "reason": reason,
            }
        )
    sample_report = pd.DataFrame(rows, columns=columns)
    failures = sample_report[sample_report["verification_status"].ne("verified")].copy()
    return sample_report, failures


def _live_stock_location_quantity(payload: Any, location: str) -> str:
    if not isinstance(payload, dict) or not location:
        return ""
    records: list[dict[str, Any]] = []
    for container in [payload.get("WarehouseInformation"), payload.get("warehouseInformation"), payload]:
        if not isinstance(container, dict):
            continue
        for key in ["StockLocations", "stockLocations", "Locations", "locations"]:
            value = container.get(key)
            if isinstance(value, list):
                records.extend([item for item in value if isinstance(item, dict)])
    for record in records:
        name = str(
            record.get("StockLocationReference")
            or record.get("Reference")
            or record.get("Name")
            or record.get("StockLocation", {}).get("Name", "") if isinstance(record.get("StockLocation"), dict) else ""
        ).strip()
        if name.casefold() != location.casefold():
            continue
        for key in ["CurrentInventory", "Inventory", "Quantity", "Available", "Stock"]:
            value = record.get(key)
            if value not in [None, ""]:
                return str(value).strip()
    return ""


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


def _out_dir_from_argv(argv: list[str]) -> Path:
    for index, value in enumerate(argv):
        if value == "--out-dir" and index + 1 < len(argv):
            return Path(argv[index + 1])
        if value.startswith("--out-dir="):
            return Path(value.split("=", 1)[1])
    return Path("reports/fast_stock_updates") / datetime.now().strftime("%Y%m%d_%H%M%S_exception")


def _write_exception_artifacts(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    traceback_path = out_dir / "exception_traceback.txt"
    traceback_path.write_text(traceback.format_exc(), encoding="utf-8")
    summary_path = out_dir / "fast_stock_summary.csv"
    exception_rows = pd.DataFrame(
        [
            {"metric": "wrapper_failed", "value": "yes"},
            {"metric": "python_failed", "value": "yes"},
            {"metric": "failure_stage", "value": "python_exception"},
            {"metric": "exception_traceback_path", "value": str(traceback_path)},
            {"metric": "live_stock_update", "value": "unknown"},
        ]
    )
    if summary_path.exists():
        try:
            existing = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
        except Exception:
            existing = pd.DataFrame(columns=["metric", "value"])
        if {"metric", "value"}.issubset(existing.columns):
            existing = existing[~existing["metric"].isin(exception_rows["metric"])].copy()
            summary = pd.concat([existing[["metric", "value"]], exception_rows], ignore_index=True)
        else:
            summary = exception_rows
    else:
        summary = exception_rows
    summary.to_csv(summary_path, index=False)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit as exc:
        raise exc
    except Exception:
        _write_exception_artifacts(_out_dir_from_argv(sys.argv[1:]))
        raise

