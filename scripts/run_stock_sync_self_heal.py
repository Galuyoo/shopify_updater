from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.promote_exact_supplier_matches import (
    TARGET_COLUMNS,
    _append_targets,
    _create_and_verify_product_supplier,
    _exact_supplier_matches,
    _readback_contains_supplier,
    _target_row,
)
from scripts.run_new_product_onboarding_delta import (
    _load_products,
    _load_protection_rules,
    _load_supplier_ids,
    _load_supplier_info_only_map,
    _supplier_info_only_matches,
    _supplier_info_only_target_row,
)
from src.stock_mapping import build_supplier_stock_lookup
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv


SUMMARY_COLUMNS = ["metric", "value"]
QUARANTINE_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "lane", "reason"]
TARGET_GAP_COLUMNS = [
    "ProductID",
    "SKU",
    "Parent SKU",
    "Name",
    "supplier",
    "SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "stock_strategy",
    "reason",
]
SUPPLIER_SETUP_COLUMNS = ["ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU", "stock_strategy", "status", "status_code", "response"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Self-heal StoreFeeder stock-sync enrichment with exact matching only.")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fix-product-ids", action="store_true")
    parser.add_argument("--create-missing-product-suppliers", action="store_true")
    parser.add_argument("--repair-invalid-targets", action="store_true")
    parser.add_argument("--parent-sku")
    parser.add_argument("--target-file", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--supplier-info-only-sku-map", type=Path, default=Path("data/supplier_info_only_sku_map.csv"))
    parser.add_argument("--warehouse-only-rules", type=Path, default=Path("data/warehouse_only_prime_sku_rules.csv"))
    parser.add_argument("--prime-parent-registry", type=Path, default=Path("data/amazon_prime_parent_skus.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/stock_sync_self_heal"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--supplier-costs", type=int, default=0)
    args = parser.parse_args()
    if args.execute and args.dry_run:
        parser.error("--execute and --dry-run cannot be used together")
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    products = _load_products(client, args.page_size, None)
    products = _filter_parent(products, args.parent_sku)
    targets = _read_targets(args.target_file)
    targets = _filter_targets_to_products(targets, products, args.parent_sku)
    supplier_ids = _load_supplier_ids(args.supplier_id_map)
    supplier_stock = build_supplier_stock_lookup(read_csv(args.ralawise_stock), read_csv(args.uneek_stock))
    supplier_info_only_map = _load_supplier_info_only_map(args.supplier_info_only_sku_map)
    protection_rules = _load_protection_rules(args.warehouse_only_rules, args.prime_parent_registry)
    latest_supplier_failures = _latest_supplier_update_failures()

    live_duplicates = _live_sku_duplicates(products)
    live_index = _unique_live_index(products, live_duplicates)
    cleanup_detected, cleanup_repaired, cleanup_disabled, cleaned_targets, cleanup_summary = _repair_invalid_targets(
        targets=targets,
        live_index=live_index,
        live_duplicates=live_duplicates,
        supplier_ids=supplier_ids,
        supplier_info_only_map=supplier_info_only_map,
    )
    cleanup_applied = False
    if args.execute and args.repair_invalid_targets and not args.parent_sku and (
        not cleanup_repaired.empty or not cleanup_disabled.empty
    ):
        _backup_and_write_targets(args.target_file, cleaned_targets, run_id)
        targets = cleaned_targets.copy()
        cleanup_applied = True
    elif args.repair_invalid_targets:
        targets = cleaned_targets.copy()
    target_reconciliation, stale_quarantine, reconciled_targets = _reconcile_target_product_ids(targets, live_index, live_duplicates)
    feed_missing, feed_duplicate = _target_supplier_feed_quarantine(reconciled_targets, supplier_stock)
    missing_supplier, created_supplier, create_failures = _heal_missing_product_suppliers(
        client,
        reconciled_targets,
        live_index,
        supplier_stock,
        supplier_info_only_map,
        latest_supplier_failures,
        target_reconciliation,
        scan_all_targets=args.create_missing_product_suppliers,
        execute=args.execute and args.create_missing_product_suppliers,
        supplier_costs=args.supplier_costs,
    )
    new_variants = _new_variants_detected(products, reconciled_targets)
    normal_missing_targets, normal_quarantine, supplier_info_only_family_base_suppressed = _normal_target_gaps(
        products,
        reconciled_targets,
        supplier_ids,
        supplier_stock,
        protection_rules,
        supplier_info_only_map,
    )
    info_ready, info_quarantine, manual_profile_needed = _supplier_info_only_gaps(
        products,
        reconciled_targets,
        supplier_info_only_map,
        supplier_ids,
        supplier_stock,
        protection_rules,
    )
    append_ready = pd.concat([normal_missing_targets, info_ready], ignore_index=True)
    supplier_feed_missing_report = pd.concat(
        [
            feed_missing,
            normal_quarantine[normal_quarantine["reason"].fillna("").astype(str).str.contains("no_exact_supplier_feed_match", case=False, na=False)] if not normal_quarantine.empty else pd.DataFrame(columns=QUARANTINE_COLUMNS),
        ],
        ignore_index=True,
    )
    supplier_feed_duplicate_report = pd.concat(
        [
            feed_duplicate,
            normal_quarantine[normal_quarantine["reason"].fillna("").astype(str).str.contains("duplicate_supplier_feed_match", case=False, na=False)] if not normal_quarantine.empty else pd.DataFrame(columns=QUARANTINE_COLUMNS),
        ],
        ignore_index=True,
    )

    appended = pd.DataFrame(columns=TARGET_COLUMNS)
    fixed_count = 0
    appended_count = 0
    target_updates_applied = pd.DataFrame(columns=target_reconciliation.columns)
    if args.execute:
        if args.fix_product_ids and not target_reconciliation.empty:
            _backup_and_write_targets(args.target_file, reconciled_targets, run_id)
            fixed_count = len(target_reconciliation)
            target_updates_applied = target_reconciliation.copy()
        if not append_ready.empty:
            appended_count = _append_targets(args.target_file, append_ready[TARGET_COLUMNS].copy(), out_dir, run_id)
            appended_path = out_dir / "target_rows_appended_to_file.csv"
            appended = read_csv(appended_path) if appended_path.exists() else pd.DataFrame(columns=TARGET_COLUMNS)
            if args.create_missing_product_suppliers and not appended.empty:
                appended_missing, appended_created, appended_failures = _verify_or_create_product_suppliers_for_targets(
                    client=client,
                    targets_to_check=appended,
                    live_index=live_index,
                    supplier_stock=supplier_stock,
                    supplier_info_only_map=supplier_info_only_map,
                    execute=True,
                    supplier_costs=args.supplier_costs,
                )
                missing_supplier = pd.concat([missing_supplier, appended_missing], ignore_index=True)
                created_supplier = pd.concat([created_supplier, appended_created], ignore_index=True)
                create_failures = pd.concat([create_failures, appended_failures], ignore_index=True)

    quarantine_rows = pd.concat(
        [
            stale_quarantine,
            feed_missing,
            feed_duplicate,
            normal_quarantine,
            info_quarantine,
            _create_failures_to_quarantine(create_failures),
        ],
        ignore_index=True,
    )
    safe_to_run_fast_sync = "yes" if _critical_quarantine_count(quarantine_rows) == 0 else "no"

    reports = {
        "self_heal_summary": _summary(
            products=products,
            targets=targets,
            stale=target_reconciliation,
            fixed_count=fixed_count,
            missing_supplier=missing_supplier,
            created_supplier=created_supplier,
            target_missing=append_ready,
            appended_count=appended_count,
            new_variants=new_variants,
            info_ready=info_ready,
            info_appended=appended[appended.get("stock_strategy", pd.Series(dtype=str)).astype(str).str.casefold().eq("supplier_info_only_manual_inventory")] if not appended.empty else appended,
            manual_profile_needed=manual_profile_needed,
            quarantine=quarantine_rows,
            safe_to_run_fast_sync=safe_to_run_fast_sync,
            execute=args.execute,
            invalid_cleanup=cleanup_summary,
            invalid_cleanup_applied=cleanup_applied,
            supplier_info_only_family_base_suppressed=supplier_info_only_family_base_suppressed,
        ),
        "target_cleanup_summary": cleanup_summary,
        "invalid_targets_detected": cleanup_detected,
        "invalid_targets_repaired": cleanup_repaired,
        "invalid_targets_disabled": cleanup_disabled,
        "live_sku_index_duplicates": live_duplicates,
        "stale_product_id_reconciliations": target_reconciliation,
        "target_product_id_updates_applied": target_updates_applied,
        "stale_product_id_quarantine": stale_quarantine,
        "product_suppliers_missing_detected": missing_supplier,
        "missing_product_supplier_detected": missing_supplier,
        "product_suppliers_created": created_supplier,
        "missing_product_supplier_created": created_supplier,
        "product_supplier_create_failures": create_failures,
        "target_rows_missing_detected": append_ready,
        "supplier_info_only_family_base_targets_suppressed": supplier_info_only_family_base_suppressed,
        "target_rows_appended": appended,
        "new_variants_detected": new_variants,
        "supplier_feed_missing_quarantine": supplier_feed_missing_report,
        "supplier_feed_duplicate_quarantine": supplier_feed_duplicate_report,
        "supplier_info_only_mapping_ready": info_ready,
        "supplier_info_only_mapping_quarantine": info_quarantine,
        "instock_supplier_info_only_ready": _instock_target_rows(info_ready),
        "instock_supplier_info_only_appended": _instock_target_rows(appended),
        "instock_supplier_info_only_quarantine": _instock_quarantine_rows(pd.concat([info_quarantine, quarantine_rows], ignore_index=True)),
        "manual_profile_mapping_needed": manual_profile_needed,
        "quarantine": quarantine_rows,
        "recommended_user_actions": _recommended_actions(quarantine_rows, manual_profile_needed, safe_to_run_fast_sync),
    }

    for name, frame in reports.items():
        frame.to_csv(out_dir / f"{name}.csv", index=False)
    _write_brief(out_dir / "CHATGPT_BRIEF.txt", reports["self_heal_summary"], reports["recommended_user_actions"])

    print("StoreFeeder stock sync self-heal")
    print(reports["self_heal_summary"].to_string(index=False))
    print()
    print("Reports:")
    for name in reports:
        print(out_dir / f"{name}.csv")
    print(out_dir / "CHATGPT_BRIEF.txt")
    return 0


def _filter_parent(products: pd.DataFrame, parent_sku: str | None) -> pd.DataFrame:
    if not parent_sku or products.empty:
        return products
    key = parent_sku.strip().casefold()
    sku = products["SKU"].fillna("").astype(str).str.strip().str.casefold()
    parent = products["Parent SKU"].fillna("").astype(str).str.strip().str.casefold()
    return products[sku.eq(key) | parent.eq(key) | sku.str.startswith(key + "-")].copy()


def _filter_targets_to_products(targets: pd.DataFrame, products: pd.DataFrame, parent_sku: str | None) -> pd.DataFrame:
    if not parent_sku or targets.empty or products.empty:
        return targets
    live_skus = {str(value).strip().casefold() for value in products["SKU"].fillna("").astype(str) if str(value).strip()}
    return targets[targets["SKU"].fillna("").astype(str).str.strip().str.casefold().isin(live_skus)].copy()


def _read_targets(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=TARGET_COLUMNS)
    targets = read_csv(path)
    for column in TARGET_COLUMNS:
        if column not in targets.columns:
            targets[column] = ""
    return targets.copy()


def _repair_invalid_targets(
    *,
    targets: pd.DataFrame,
    live_index: dict[str, pd.Series],
    live_duplicates: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_info_only_map: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    detected: list[dict[str, Any]] = []
    repaired: list[dict[str, Any]] = []
    disabled: list[dict[str, Any]] = []
    cleaned = targets.copy()
    disabled_indexes: set[int] = set()
    duplicate_skus = _duplicate_live_sku_keys(live_duplicates)
    supplier_id_by_name = _supplier_id_lookup(supplier_ids)
    explicit_info_only = _supplier_info_only_target_keys(supplier_info_only_map)
    info_only_supplier_prefixes = _supplier_info_only_supplier_prefixes(supplier_info_only_map)

    for index, row in targets.iterrows():
        sku = str(row.get("SKU", "")).strip()
        supplier = str(row.get("supplier", "")).strip()
        supplier_name = str(row.get("Supplier.Name", "")).strip() or supplier
        supplier_sku = str(row.get("SupplierSKU", "")).strip()
        supplier_id = str(row.get("SupplierID", "")).strip()
        strategy = str(row.get("stock_strategy", "")).strip()
        strategy_key = strategy.casefold()
        sku_key = sku.casefold()
        is_explicit_info_only = (
            _is_instock_sku(sku)
            or strategy_key == "supplier_info_only_manual_inventory"
            or (sku_key, supplier_sku.casefold()) in explicit_info_only
        )
        is_info_only_family_base_target = (
            strategy_key == "supplier_synced_inventory"
            and sku
            and supplier_sku
            and sku.casefold() == supplier_sku.casefold()
            and _supplier_sku_prefix(supplier_sku).casefold() in info_only_supplier_prefixes
        )
        reasons = []
        if not sku:
            reasons.append("missing_sku")
        elif sku_key in duplicate_skus:
            reasons.append("duplicate_live_storefeeder_sku")
        elif sku_key not in live_index:
            reasons.append("missing_live_storefeeder_sku")
        if not supplier_sku:
            reasons.append("missing_supplier_sku")
        if not supplier_id:
            reasons.append("missing_supplier_id")
        if strategy_key not in {"supplier_synced_inventory", "supplier_info_only_manual_inventory", "warehouse_only"}:
            reasons.append("invalid_stock_strategy")
        if is_explicit_info_only and (
            strategy_key != "supplier_info_only_manual_inventory"
            or str(row.get("skip_stock_location_update", "")).strip().casefold() != "yes"
            or str(row.get("allow_stock_location_update", "")).strip().casefold() != "no"
            or str(row.get("stock_location", "")).strip()
            or str(row.get("sellable_stock_location", "")).strip()
        ):
            reasons.append("supplier_info_only_lane_fields_invalid")
        if reasons:
            detected.append(_target_cleanup_row(row, ";".join(reasons), "detected"))

        if is_info_only_family_base_target:
            disabled.append(_target_cleanup_row(row, "disabled_supplier_info_only_family_base_target", "disabled"))
            disabled_indexes.add(index)
            continue
        if sku and (sku_key in duplicate_skus or sku_key not in live_index) and not is_explicit_info_only:
            disabled.append(_target_cleanup_row(row, "disabled_missing_or_duplicate_live_storefeeder_sku", "disabled"))
            disabled_indexes.add(index)
            continue

        changed = False
        if not supplier_id and supplier_name.casefold() in supplier_id_by_name:
            cleaned.loc[index, "SupplierID"] = supplier_id_by_name[supplier_name.casefold()]
            changed = True
        if is_explicit_info_only:
            info_values = {
                "stock_strategy": "supplier_info_only_manual_inventory",
                "skip_stock_location_update": "yes",
                "allow_stock_location_update": "no",
                "stock_location": "",
                "sellable_stock_location": "",
            }
            for column, value in info_values.items():
                if column in cleaned.columns and str(cleaned.at[index, column]).strip() != value:
                    cleaned.loc[index, column] = value
                    changed = True
        if changed:
            repaired.append(_target_cleanup_row(cleaned.loc[index], "deterministic_target_repair", "repaired"))

    if disabled_indexes:
        cleaned = cleaned.drop(index=list(disabled_indexes)).reset_index(drop=True)
    summary = pd.DataFrame(
        [
            {"metric": "invalid_targets_detected", "value": len(detected)},
            {"metric": "invalid_targets_repaired", "value": len(repaired)},
            {"metric": "invalid_targets_disabled", "value": len(disabled)},
            {"metric": "post_cleanup_target_rows", "value": len(cleaned)},
        ],
        columns=SUMMARY_COLUMNS,
    )
    return (
        pd.DataFrame(detected, columns=_target_cleanup_columns()),
        pd.DataFrame(repaired, columns=_target_cleanup_columns()),
        pd.DataFrame(disabled, columns=_target_cleanup_columns()),
        cleaned,
        summary,
    )


def _target_cleanup_columns() -> list[str]:
    return [
        "action",
        "reason",
        "ProductID",
        "SKU",
        "supplier",
        "SupplierID",
        "Supplier.Name",
        "SupplierSKU",
        "stock_strategy",
        "skip_stock_location_update",
        "allow_stock_location_update",
        "stock_location",
        "sellable_stock_location",
    ]


def _target_cleanup_row(row: pd.Series, reason: str, action: str) -> dict[str, str]:
    return {
        "action": action,
        "reason": reason,
        "ProductID": str(row.get("ProductID", "")).strip(),
        "SKU": str(row.get("SKU", "")).strip(),
        "supplier": str(row.get("supplier", "")).strip(),
        "SupplierID": str(row.get("SupplierID", "")).strip(),
        "Supplier.Name": str(row.get("Supplier.Name", "")).strip(),
        "SupplierSKU": str(row.get("SupplierSKU", "")).strip(),
        "stock_strategy": str(row.get("stock_strategy", "")).strip(),
        "skip_stock_location_update": str(row.get("skip_stock_location_update", "")).strip(),
        "allow_stock_location_update": str(row.get("allow_stock_location_update", "")).strip(),
        "stock_location": str(row.get("stock_location", "")).strip(),
        "sellable_stock_location": str(row.get("sellable_stock_location", "")).strip(),
    }


def _supplier_id_lookup(supplier_ids: pd.DataFrame) -> dict[str, str]:
    lookup = {}
    for _, row in supplier_ids.iterrows():
        supplier_id = str(row.get("SupplierID", "")).strip()
        for column in ["supplier", "Supplier.Name"]:
            name = str(row.get(column, "")).strip()
            if name and supplier_id:
                lookup[name.casefold()] = supplier_id
    return lookup


def _supplier_info_only_target_keys(mapping: pd.DataFrame) -> set[tuple[str, str]]:
    if mapping.empty or "child_sku" not in mapping.columns or "supplier_sku" not in mapping.columns:
        return set()
    mode = mapping.get("stock_update_mode", pd.Series("", index=mapping.index)).fillna("").astype(str).str.strip().str.casefold()
    rows = mapping[mode.eq("supplier_info_only_manual_inventory")]
    return {
        (str(row.get("child_sku", "")).strip().casefold(), str(row.get("supplier_sku", "")).strip().casefold())
        for _, row in rows.iterrows()
        if str(row.get("child_sku", "")).strip() and str(row.get("supplier_sku", "")).strip()
    }


def _supplier_info_only_supplier_prefixes(mapping: pd.DataFrame) -> set[str]:
    if mapping.empty or "supplier_sku" not in mapping.columns:
        return set()
    mode = mapping.get("stock_update_mode", pd.Series("", index=mapping.index)).fillna("").astype(str).str.strip().str.casefold()
    rows = mapping[mode.eq("supplier_info_only_manual_inventory")]
    return {
        prefix.casefold()
        for value in rows["supplier_sku"].fillna("").astype(str)
        for prefix in [_supplier_sku_prefix(value)]
        if prefix
    }


def _supplier_sku_prefix(supplier_sku: str) -> str:
    value = str(supplier_sku).strip().upper()
    match = re.match(r"^([A-Z]+\d+)", value)
    return match.group(1) if match else ""


def _is_supplier_info_only_family_base_sku(sku: str, candidate: dict[str, Any], info_only_supplier_prefixes: set[str]) -> bool:
    supplier_sku = str(candidate.get("supplier_sku", "")).strip()
    if not sku or not supplier_sku or str(sku).strip().casefold() != supplier_sku.casefold():
        return False
    prefix = _supplier_sku_prefix(supplier_sku).casefold()
    return bool(prefix and prefix in info_only_supplier_prefixes)


def _duplicate_live_sku_keys(live_duplicates: pd.DataFrame) -> set[str]:
    keys: set[str] = set()
    if live_duplicates.empty or "SKU" not in live_duplicates.columns:
        return keys
    for value in live_duplicates["SKU"].fillna("").astype(str):
        for sku in value.split("|"):
            if sku.strip():
                keys.add(sku.strip().casefold())
    return keys


def _live_sku_duplicates(products: pd.DataFrame) -> pd.DataFrame:
    counts = products.groupby(products["SKU"].fillna("").astype(str).str.strip().str.casefold(), dropna=False).size().reset_index(name="live_sku_count")
    counts = counts[(counts["SKU"].astype(str).ne("")) & (counts["live_sku_count"] > 1)].copy()
    if counts.empty:
        return pd.DataFrame(columns=["SKU", "live_sku_count"])
    counts = counts.rename(columns={"SKU": "sku_key"})
    rows = []
    for _, row in counts.iterrows():
        matches = products[products["SKU"].fillna("").astype(str).str.strip().str.casefold().eq(str(row["sku_key"]))]
        rows.append({"SKU": "|".join(matches["SKU"].astype(str)), "ProductIDs": "|".join(matches["ID"].astype(str)), "live_sku_count": row["live_sku_count"]})
    return pd.DataFrame(rows)


def _unique_live_index(products: pd.DataFrame, duplicates: pd.DataFrame) -> dict[str, pd.Series]:
    duplicate_keys = set()
    if not duplicates.empty:
        for value in duplicates["SKU"].astype(str):
            for sku in value.split("|"):
                duplicate_keys.add(sku.strip().casefold())
    out: dict[str, pd.Series] = {}
    for _, product in products.iterrows():
        sku = str(product.get("SKU", "")).strip()
        if not sku or sku.casefold() in duplicate_keys:
            continue
        out[sku.casefold()] = product
    return out


def _reconcile_target_product_ids(targets: pd.DataFrame, live_index: dict[str, pd.Series], duplicates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = []
    quarantine = []
    reconciled = targets.copy()
    duplicate_keys = set()
    if not duplicates.empty:
        for value in duplicates["SKU"].astype(str):
            duplicate_keys.update(part.strip().casefold() for part in value.split("|"))
    for index, target in targets.iterrows():
        sku = str(target.get("SKU", "")).strip()
        if not sku:
            continue
        if sku.casefold() in duplicate_keys:
            quarantine.append(_quarantine_from_target(target, "target_productid_reconciliation", "duplicate_live_storefeeder_sku"))
            continue
        live = live_index.get(sku.casefold())
        if live is None:
            quarantine.append(_quarantine_from_target(target, "target_productid_reconciliation", "missing_live_storefeeder_sku"))
            continue
        live_product_id = str(live.get("ID", "")).strip()
        old_product_id = str(target.get("ProductID", "")).strip()
        if live_product_id and old_product_id != live_product_id:
            rows.append({"SKU": sku, "old_ProductID": old_product_id, "new_ProductID": live_product_id, "stock_strategy": str(target.get("stock_strategy", "")).strip(), "reason": "exact_unique_live_sku_productid_changed"})
            reconciled.loc[index, "ProductID"] = live_product_id
    return pd.DataFrame(rows), pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS), reconciled


def _target_supplier_feed_quarantine(targets: pd.DataFrame, supplier_stock: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing = []
    duplicate = []
    for _, target in targets.iterrows():
        supplier = str(target.get("supplier", "")).strip()
        supplier_sku = str(target.get("SupplierSKU", "")).strip()
        if not supplier or not supplier_sku:
            continue
        matches = supplier_stock[
            supplier_stock["supplier"].fillna("").astype(str).str.casefold().eq(supplier.casefold())
            & supplier_stock["supplier_sku"].fillna("").astype(str).str.casefold().eq(supplier_sku.casefold())
        ]
        if len(matches) == 0:
            missing.append(_quarantine_from_target(target, str(target.get("stock_strategy", "")).strip(), "supplier_feed_sku_missing"))
        elif len(matches) > 1:
            duplicate.append(_quarantine_from_target(target, str(target.get("stock_strategy", "")).strip(), "supplier_feed_sku_duplicate"))
    return pd.DataFrame(missing, columns=QUARANTINE_COLUMNS), pd.DataFrame(duplicate, columns=QUARANTINE_COLUMNS)


def _heal_missing_product_suppliers(
    client: StoreFeederApiClient,
    targets: pd.DataFrame,
    live_index: dict[str, pd.Series],
    supplier_stock: pd.DataFrame,
    supplier_info_only_map: pd.DataFrame,
    latest_supplier_failures: pd.DataFrame,
    target_reconciliation: pd.DataFrame,
    *,
    scan_all_targets: bool,
    execute: bool,
    supplier_costs: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing_rows = []
    created_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    targets_to_check = (
        _targets_needing_product_supplier_scan(targets, latest_supplier_failures, target_reconciliation)
        if scan_all_targets
        else _targets_with_recent_supplier_failures(targets, latest_supplier_failures)
    )
    return _verify_or_create_product_suppliers_for_targets(
        client=client,
        targets_to_check=targets_to_check,
        live_index=live_index,
        supplier_stock=supplier_stock,
        supplier_info_only_map=supplier_info_only_map,
        execute=execute,
        supplier_costs=supplier_costs,
    )


def _verify_or_create_product_suppliers_for_targets(
    *,
    client: StoreFeederApiClient,
    targets_to_check: pd.DataFrame,
    live_index: dict[str, pd.Series],
    supplier_stock: pd.DataFrame,
    supplier_info_only_map: pd.DataFrame,
    execute: bool,
    supplier_costs: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing_rows = []
    created_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    for _, target in targets_to_check.iterrows():
        strategy = str(target.get("stock_strategy", "")).strip()
        if not _can_create_product_supplier_for_strategy(strategy):
            continue
        if strategy.casefold() == "supplier_info_only_manual_inventory" and not (
            _supplier_info_only_mapping_allows_target(target, supplier_info_only_map)
            or _instock_target_allows_supplier_info_only(target)
        ):
            continue
        product = live_index.get(str(target.get("SKU", "")).strip().casefold())
        if product is None:
            continue
        candidate = _candidate_from_target(target, supplier_stock)
        if candidate is None:
            continue
        product_id = str(product.get("ID", "")).strip()
        if _readback_contains_supplier(client.get_product_suppliers(product_id), candidate):
            continue
        missing_rows.append(_target_gap_row(product, candidate, strategy, "ProductSupplier missing for exact target row"))
        if execute:
            success_count_before = len(created_rows)
            failure_count_before = len(failure_rows)
            _create_and_verify_product_supplier(
                client=client,
                product_id=product_id,
                product=product,
                candidate=candidate,
                supplier_costs=supplier_costs,
                success_rows=created_rows,
                failure_rows=failure_rows,
            )
            for success in created_rows[success_count_before:]:
                success["stock_strategy"] = strategy
            for failure in failure_rows[failure_count_before:]:
                failure["stock_strategy"] = strategy
    created = pd.DataFrame(created_rows, columns=SUPPLIER_SETUP_COLUMNS)
    failures = pd.DataFrame(failure_rows, columns=SUPPLIER_SETUP_COLUMNS)
    return pd.DataFrame(missing_rows, columns=TARGET_GAP_COLUMNS), created, failures


def _latest_supplier_update_failures() -> pd.DataFrame:
    roots = [Path("reports/scheduled_fast_stock_sync"), Path("reports/fast_stock_updates")]
    candidates: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        candidates.extend(root.glob("*/fast_stock_update_failures.csv"))
    if not candidates:
        return pd.DataFrame()
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    try:
        return pd.read_csv(latest, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _targets_needing_product_supplier_scan(targets: pd.DataFrame, failures: pd.DataFrame, reconciliations: pd.DataFrame) -> pd.DataFrame:
    if targets.empty:
        return targets.iloc[0:0].copy()
    sku_keys = set()
    supplier_sku_keys = set()
    if not failures.empty:
        if "SKU" in failures.columns:
            sku_keys.update(
                str(value).strip().casefold()
                for value in failures["SKU"].fillna("").astype(str)
                if str(value).strip()
            )
        if "SupplierSKU" in failures.columns:
            supplier_sku_keys.update(
                str(value).strip().casefold()
                for value in failures["SupplierSKU"].fillna("").astype(str)
                if str(value).strip()
            )
    if not reconciliations.empty and "SKU" in reconciliations.columns:
        sku_keys.update(
            str(value).strip().casefold()
            for value in reconciliations["SKU"].fillna("").astype(str)
            if str(value).strip()
        )
    if not sku_keys and not supplier_sku_keys:
        return targets.iloc[0:0].copy()
    mask = targets.apply(
        lambda row: (
            str(row.get("SKU", "")).strip().casefold() in sku_keys
            or str(row.get("SupplierSKU", "")).strip().casefold() in supplier_sku_keys
        ),
        axis=1,
    )
    return targets[mask].copy()


def _targets_with_recent_supplier_failures(targets: pd.DataFrame, failures: pd.DataFrame) -> pd.DataFrame:
    if targets.empty or failures.empty:
        return targets.iloc[0:0].copy()
    sku_keys = set()
    supplier_sku_keys = set()
    if "SKU" in failures.columns:
        sku_keys.update(str(value).strip().casefold() for value in failures["SKU"].fillna("").astype(str) if str(value).strip())
    if "SupplierSKU" in failures.columns:
        supplier_sku_keys.update(str(value).strip().casefold() for value in failures["SupplierSKU"].fillna("").astype(str) if str(value).strip())
    if not sku_keys and not supplier_sku_keys:
        return targets.iloc[0:0].copy()
    mask = targets.apply(
        lambda row: (
            str(row.get("SKU", "")).strip().casefold() in sku_keys
            or str(row.get("SupplierSKU", "")).strip().casefold() in supplier_sku_keys
        ),
        axis=1,
    )
    return targets[mask].copy()


def _new_variants_detected(products: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    target_skus = {str(value).strip().casefold() for value in targets["SKU"].fillna("").astype(str) if str(value).strip()}
    child_rows = products[products["Parent SKU"].fillna("").astype(str).str.strip().ne("")].copy()
    child_rows = child_rows[~child_rows["SKU"].fillna("").astype(str).str.strip().str.casefold().isin(target_skus)]
    return child_rows[["ID", "SKU", "Parent SKU", "Name"]].rename(columns={"ID": "ProductID"}).reset_index(drop=True)


def _normal_target_gaps(
    products: pd.DataFrame,
    targets: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    protection_rules: list[dict[str, str]],
    supplier_info_only_map: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    target_skus = {str(value).strip().casefold() for value in targets["SKU"].fillna("").astype(str) if str(value).strip()}
    info_only_supplier_prefixes = _supplier_info_only_supplier_prefixes(supplier_info_only_map)
    rows = []
    quarantine = []
    suppressed = []
    for _, product in products.iterrows():
        sku = str(product.get("SKU", "")).strip()
        parent_sku = str(product.get("Parent SKU", "")).strip()
        if not sku or sku.casefold() in target_skus or not parent_sku:
            continue
        if _is_instock_sku(sku):
            continue
        if _protected_by_rules(sku, parent_sku, protection_rules):
            continue
        matches = _exact_supplier_matches(sku, supplier_ids, supplier_stock)
        if len(matches) == 1:
            if _is_supplier_info_only_family_base_sku(sku, matches[0], info_only_supplier_prefixes):
                suppressed.append(_target_gap_row(product, matches[0], "supplier_info_only_manual_inventory", "raw supplier SKU family is controlled by supplier_info_only_sku_map.csv"))
                continue
            rows.append(_target_row(product, matches[0]))
        elif len(matches) == 0:
            quarantine.append(_quarantine_from_product(product, "supplier_synced_inventory", "no_exact_supplier_feed_match_for_new_variant"))
        else:
            quarantine.append(_quarantine_from_product(product, "supplier_synced_inventory", "duplicate_supplier_feed_match_for_new_variant"))
    return (
        pd.DataFrame(rows, columns=TARGET_COLUMNS),
        pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS),
        pd.DataFrame(suppressed, columns=TARGET_GAP_COLUMNS),
    )


def _supplier_info_only_gaps(
    products: pd.DataFrame,
    targets: pd.DataFrame,
    mapping: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    protection_rules: list[dict[str, str]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ready = []
    quarantine = []
    manual_needed = []
    target_keys = _target_lane_keys(targets)
    planned_keys: set[tuple[str, str, str, str]] = set()
    by_sku = {str(row.get("SKU", "")).strip().casefold(): row for _, row in products.iterrows() if str(row.get("SKU", "")).strip()}
    for _, product in products.iterrows():
        sku = str(product.get("SKU", "")).strip()
        if not _is_instock_sku(sku):
            continue
        base_sku = _instock_base_sku(sku)
        if _is_numeric_parent_sku(base_sku):
            quarantine.append(_quarantine_from_product(product, "supplier_info_only_manual_inventory", "instock_parent_or_aggregate"))
            continue
        candidate_rows = _instock_supplier_info_only_matches(product, supplier_ids, supplier_stock)
        if len(candidate_rows) == 0:
            quarantine.append(_quarantine_from_product(product, "supplier_info_only_manual_inventory", "instock_supplier_feed_match_missing"))
            continue
        if len(candidate_rows) > 1:
            quarantine.append(_quarantine_from_product(product, "supplier_info_only_manual_inventory", "instock_supplier_feed_match_duplicate"))
            continue
        candidate = candidate_rows[0]
        key = (sku.casefold(), str(candidate["supplier_sku"]).strip().casefold(), str(candidate["SupplierID"]).strip(), "supplier_info_only_manual_inventory")
        if key in target_keys or key in planned_keys:
            continue
        ready.append(_supplier_info_only_target_row(product, candidate))
        planned_keys.add(key)
    if mapping.empty:
        return pd.DataFrame(ready, columns=TARGET_COLUMNS), pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS), pd.DataFrame(columns=["parent_sku", "child_sku", "parsed_internal_colour", "available_supplier_colour_candidates", "quarantine_reason"])
    for _, row in mapping.iterrows():
        if str(row.get("stock_update_mode", "")).strip().casefold() != "supplier_info_only_manual_inventory":
            continue
        child_sku = str(row.get("child_sku", "")).strip()
        product = by_sku.get(child_sku.casefold())
        if product is None:
            quarantine.append({"ProductID": "", "SKU": child_sku, "Parent SKU": str(row.get("parent_sku", "")).strip(), "Name": "", "lane": "supplier_info_only_manual_inventory", "reason": "mapped_child_sku_missing_from_live_storefeeder"})
            continue
        candidate_rows = _supplier_info_only_matches(product, {}, mapping, supplier_stock)
        if len(candidate_rows) != 1:
            quarantine.append(_quarantine_from_product(product, "supplier_info_only_manual_inventory", "supplier_info_only_mapping_missing_or_duplicate_supplier_feed_match"))
            continue
        candidate = candidate_rows[0]
        key = (child_sku.casefold(), str(candidate["supplier_sku"]).strip().casefold(), str(candidate["SupplierID"]).strip(), "supplier_info_only_manual_inventory")
        if key in target_keys or key in planned_keys:
            continue
        ready.append(_supplier_info_only_target_row(product, candidate))
        planned_keys.add(key)
    protected = products[products["SKU"].fillna("").astype(str).map(lambda sku: any(_rule_matches(str(sku), "", rule) for rule in protection_rules))].copy()
    mapped_children = {str(value).strip().casefold() for value in mapping["child_sku"].fillna("").astype(str)}
    for _, product in protected.iterrows():
        sku = str(product.get("SKU", "")).strip()
        if sku and sku.casefold() not in mapped_children:
            manual_needed.append({"parent_sku": str(product.get("Parent SKU", "")).strip(), "child_sku": sku, "parsed_internal_colour": _parse_internal_colour(sku), "available_supplier_colour_candidates": "", "quarantine_reason": "manual_supplier_info_only_mapping_missing"})
    return pd.DataFrame(ready, columns=TARGET_COLUMNS), pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS), pd.DataFrame(manual_needed)


def _candidate_from_target(target: pd.Series, supplier_stock: pd.DataFrame) -> dict[str, Any] | None:
    supplier = str(target.get("supplier", "")).strip()
    supplier_sku = str(target.get("SupplierSKU", "")).strip()
    supplier_id = str(target.get("SupplierID", "")).strip()
    supplier_name = str(target.get("Supplier.Name", supplier)).strip()
    matches = supplier_stock[
        supplier_stock["supplier"].fillna("").astype(str).str.casefold().eq(supplier.casefold())
        & supplier_stock["supplier_sku"].fillna("").astype(str).str.casefold().eq(supplier_sku.casefold())
    ]
    if len(matches) != 1 or not supplier or not supplier_sku or not supplier_id:
        return None
    return {"supplier": supplier, "supplier_sku": supplier_sku, "SupplierID": supplier_id, "Supplier.Name": supplier_name, "supplier_free_stock": str(matches.iloc[0].get("supplier_free_stock", "")).strip()}


def _supplier_info_only_mapping_allows_target(target: pd.Series, mapping: pd.DataFrame) -> bool:
    if mapping.empty:
        return False
    required = ["child_sku", "supplier_sku", "supplier_id", "stock_update_mode"]
    if any(column not in mapping.columns for column in required):
        return False
    child_sku = str(target.get("SKU", "")).strip().casefold()
    supplier_sku = str(target.get("SupplierSKU", "")).strip().casefold()
    supplier_id = str(target.get("SupplierID", "")).strip()
    mode = "supplier_info_only_manual_inventory"
    rows = mapping[
        mapping["child_sku"].fillna("").astype(str).str.strip().str.casefold().eq(child_sku)
        & mapping["supplier_sku"].fillna("").astype(str).str.strip().str.casefold().eq(supplier_sku)
        & mapping["supplier_id"].fillna("").astype(str).str.strip().eq(supplier_id)
        & mapping["stock_update_mode"].fillna("").astype(str).str.strip().str.casefold().eq(mode)
    ]
    return len(rows) == 1


def _is_instock_sku(sku: str) -> bool:
    return str(sku).strip().upper().endswith("_INSTOCK")


def _instock_base_sku(sku: str) -> str:
    value = str(sku).strip()
    return value[:-8] if value.upper().endswith("_INSTOCK") else value


def _is_numeric_parent_sku(sku: str) -> bool:
    value = str(sku).strip()
    return bool(value) and value.isdigit()


def _instock_supplier_info_only_matches(product: pd.Series, supplier_ids: pd.DataFrame, supplier_stock: pd.DataFrame) -> list[dict[str, Any]]:
    sku = str(product.get("SKU", "")).strip()
    if not _is_instock_sku(sku):
        return []
    base_sku = _instock_base_sku(sku)
    if not base_sku or _is_numeric_parent_sku(base_sku):
        return []
    matches = _exact_supplier_matches(base_sku, supplier_ids, supplier_stock)
    for match in matches:
        match["supplier_sku"] = base_sku
    return matches


def _instock_target_allows_supplier_info_only(target: pd.Series) -> bool:
    sku = str(target.get("SKU", "")).strip()
    supplier_sku = str(target.get("SupplierSKU", "")).strip()
    strategy = str(target.get("stock_strategy", "")).strip().casefold()
    return (
        strategy == "supplier_info_only_manual_inventory"
        and _is_instock_sku(sku)
        and supplier_sku.casefold() == _instock_base_sku(sku).casefold()
        and not _is_numeric_parent_sku(_instock_base_sku(sku))
    )


def _instock_target_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    return rows[rows["SKU"].fillna("").astype(str).map(_is_instock_sku)].copy()


def _instock_quarantine_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    reason = rows["reason"].fillna("").astype(str)
    return rows[
        rows["SKU"].fillna("").astype(str).map(_is_instock_sku)
        | reason.str.contains("instock", case=False, na=False)
    ].copy()


def _target_lane_keys(targets: pd.DataFrame) -> set[tuple[str, str, str, str]]:
    keys = set()
    for _, row in targets.iterrows():
        keys.add((str(row.get("SKU", "")).strip().casefold(), str(row.get("SupplierSKU", "")).strip().casefold(), str(row.get("SupplierID", "")).strip(), str(row.get("stock_strategy", "")).strip().casefold()))
    return keys


def _target_gap_row(product: pd.Series, candidate: dict[str, Any], strategy: str, reason: str) -> dict[str, Any]:
    return {"ProductID": str(product.get("ID", "")).strip(), "SKU": str(product.get("SKU", "")).strip(), "Parent SKU": str(product.get("Parent SKU", "")).strip(), "Name": str(product.get("Name", "")).strip(), "supplier": candidate.get("supplier", ""), "SupplierID": candidate.get("SupplierID", ""), "Supplier.Name": candidate.get("Supplier.Name", ""), "SupplierSKU": candidate.get("supplier_sku", ""), "stock_strategy": strategy, "reason": reason}


def _quarantine_from_target(target: pd.Series, lane: str, reason: str) -> dict[str, str]:
    return {"ProductID": str(target.get("ProductID", "")).strip(), "SKU": str(target.get("SKU", "")).strip(), "Parent SKU": "", "Name": "", "lane": lane, "reason": reason}


def _quarantine_from_product(product: pd.Series, lane: str, reason: str) -> dict[str, str]:
    return {"ProductID": str(product.get("ID", "")).strip(), "SKU": str(product.get("SKU", "")).strip(), "Parent SKU": str(product.get("Parent SKU", "")).strip(), "Name": str(product.get("Name", "")).strip(), "lane": lane, "reason": reason}


def _create_failures_to_quarantine(failures: pd.DataFrame) -> pd.DataFrame:
    if failures.empty:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    rows = []
    for _, row in failures.iterrows():
        lane = str(row.get("stock_strategy", "")).strip() or "supplier_synced_inventory"
        rows.append(
            {
                "ProductID": str(row.get("ProductID", "")).strip(),
                "SKU": str(row.get("SKU", "")).strip(),
                "Parent SKU": "",
                "Name": "",
                "lane": lane,
                "reason": "product_supplier_create_failed",
            }
        )
    return pd.DataFrame(rows, columns=QUARANTINE_COLUMNS)


def _protected_by_rules(sku: str, parent_sku: str, rules: list[dict[str, str]]) -> bool:
    return any(_rule_matches(sku, parent_sku, rule) for rule in rules)


def _rule_matches(sku: str, parent_sku: str, rule: dict[str, str]) -> bool:
    value = str(rule.get("value", "")).strip().casefold()
    if not value:
        return False
    for candidate in [sku, parent_sku]:
        key = str(candidate).strip().casefold()
        if rule.get("match_type") == "exact" and key == value:
            return True
        if rule.get("match_type") == "prefix" and (key == value or key.startswith(value + "-")):
            return True
    return False


def _can_create_product_supplier_for_strategy(strategy: str) -> bool:
    return strategy.strip().casefold() in {"supplier_synced_inventory", "supplier_info_only_manual_inventory"}


def _parse_internal_colour(sku: str) -> str:
    parts = [part for part in str(sku).split("-") if part]
    return parts[-2] if len(parts) >= 2 else ""


def _backup_and_write_targets(path: Path, targets: pd.DataFrame, run_id: str) -> None:
    backup = path.with_name(path.stem + f".backup_before_self_heal_{run_id}" + path.suffix)
    shutil.copy2(path, backup)
    targets.to_csv(path, index=False)


def _critical_quarantine_count(quarantine: pd.DataFrame) -> int:
    if quarantine.empty:
        return 0
    lane = quarantine["lane"].fillna("").astype(str).str.casefold()
    reason = quarantine["reason"].fillna("").astype(str).str.casefold()
    non_blocking_new_variant = reason.isin(
        [
            "no_exact_supplier_feed_match_for_new_variant",
            "duplicate_supplier_feed_match_for_new_variant",
        ]
    )
    critical = lane.eq("target_productid_reconciliation") | (
        lane.eq("supplier_synced_inventory") & ~non_blocking_new_variant
    )
    return int(critical.sum())


def _summary(**kwargs: Any) -> pd.DataFrame:
    rows = [
        ("execute_mode", "yes" if kwargs["execute"] else "no"),
        ("live_products_scanned", len(kwargs["products"])),
        ("target_rows_scanned", len(kwargs["targets"])),
        ("stale_product_id_rows_detected", len(kwargs["stale"])),
        ("stale_product_id_rows_fixed", kwargs["fixed_count"]),
        ("product_suppliers_missing_detected", len(kwargs["missing_supplier"])),
        ("product_suppliers_created", len(kwargs["created_supplier"])),
        ("target_rows_missing_detected", len(kwargs["target_missing"])),
        ("supplier_info_only_family_base_targets_suppressed", len(kwargs.get("supplier_info_only_family_base_suppressed", []))),
        ("target_rows_appended", kwargs["appended_count"]),
        ("new_variants_detected", len(kwargs["new_variants"])),
        ("supplier_info_only_rows_ready", len(kwargs["info_ready"])),
        ("supplier_info_only_rows_appended", len(kwargs["info_appended"])),
        ("instock_supplier_info_only_ready", len(_instock_target_rows(kwargs["info_ready"]))),
        ("instock_supplier_info_only_appended", len(_instock_target_rows(kwargs["info_appended"]))),
        ("instock_supplier_info_only_quarantine", len(_instock_quarantine_rows(kwargs["quarantine"]))),
        ("instock_product_suppliers_created", int(kwargs["created_supplier"]["SKU"].fillna("").astype(str).map(_is_instock_sku).sum()) if not kwargs["created_supplier"].empty else 0),
        ("invalid_target_cleanup_applied", "yes" if kwargs.get("invalid_cleanup_applied") else "no"),
        ("invalid_targets_detected", _summary_metric(kwargs.get("invalid_cleanup"), "invalid_targets_detected")),
        ("invalid_targets_repaired", _summary_metric(kwargs.get("invalid_cleanup"), "invalid_targets_repaired")),
        ("invalid_targets_disabled", _summary_metric(kwargs.get("invalid_cleanup"), "invalid_targets_disabled")),
        ("manual_profile_mapping_needed_rows", len(kwargs["manual_profile_needed"])),
        ("quarantine_rows", len(kwargs["quarantine"])),
        ("safe_to_run_fast_sync", kwargs["safe_to_run_fast_sync"]),
    ]
    return pd.DataFrame([{"metric": metric, "value": value} for metric, value in rows], columns=SUMMARY_COLUMNS)


def _summary_metric(summary: pd.DataFrame | None, metric: str) -> int:
    if summary is None or summary.empty:
        return 0
    rows = summary[summary["metric"].astype(str).eq(metric)]
    if rows.empty:
        return 0
    try:
        return int(rows.iloc[0]["value"])
    except (TypeError, ValueError):
        return 0


def _recommended_actions(quarantine: pd.DataFrame, manual_profile_needed: pd.DataFrame, safe_to_run_fast_sync: str) -> pd.DataFrame:
    rows = []
    if safe_to_run_fast_sync != "yes":
        rows.append({"priority": "high", "action": "Review critical normal supplier quarantine before fast sync.", "reason": "safe_to_run_fast_sync=no"})
    if not manual_profile_needed.empty:
        rows.append({"priority": "medium", "action": "Add explicit supplier-info-only profile mappings for manual/Amazon variants.", "reason": f"{len(manual_profile_needed)} rows need manual mapping"})
    if quarantine.empty and manual_profile_needed.empty:
        rows.append({"priority": "info", "action": "No manual action required.", "reason": "self-heal reports are clean"})
    return pd.DataFrame(rows)


def _write_brief(path: Path, summary: pd.DataFrame, actions: pd.DataFrame) -> None:
    lines = ["STOCK SYNC SELF-HEAL BRIEF", "", "SUMMARY"]
    for _, row in summary.iterrows():
        lines.append(f"- {row['metric']}: {row['value']}")
    lines.append("")
    lines.append("RECOMMENDED USER ACTIONS")
    for _, row in actions.iterrows():
        lines.append(f"- [{row['priority']}] {row['action']} ({row['reason']})")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        os.environ.setdefault(name.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
