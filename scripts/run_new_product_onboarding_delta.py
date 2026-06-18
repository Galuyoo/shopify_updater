from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import hashlib
import shutil

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.export_storefeeder_products import fetch_products
from scripts.promote_exact_supplier_matches import (
    TARGET_COLUMNS,
    _append_targets,
    _create_and_verify_product_supplier,
    _exact_supplier_matches,
    _readback_contains_supplier,
    _target_row,
)
from src.stock_mapping import build_supplier_stock_lookup
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv

SUMMARY_COLUMNS = ["metric", "value"]
PROTECTED_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "matched_rule", "reason", "state_action"]
PARENT_COLUMNS = ["ProductID", "SKU", "Name", "classification", "family_signature", "child_count", "state_action", "reason"]
QUARANTINE_COLUMNS = [
    "ProductID",
    "SKU",
    "Parent SKU",
    "Name",
    "work_source",
    "supplier_match_status",
    "product_supplier_status",
    "target_action",
    "reason",
]
PROMOTED_COLUMNS = [
    "ProductID",
    "SKU",
    "Parent SKU",
    "Name",
    "supplier",
    "SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "supplier_free_stock",
    "product_supplier_status",
    "target_action",
    "reason",
]
SETUP_SUCCESS_COLUMNS = ["ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU", "status", "status_code", "response"]
SETUP_FAILURE_COLUMNS = ["ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU", "status", "status_code", "response"]
RULE_COLUMNS = ["match_type", "value", "reason"]
PRIME_REGISTRY_COLUMNS = ["parent_sku", "reason", "registered_at", "ProductID", "Name"]
STATE_VERSION = 1
CATALOGUE_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "row_signature", "family_signature"]
DELTA_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "delta_status", "delta_reason"]
NEW_PARENT_COLUMNS = ["ProductID", "SKU", "Name", "child_count", "delta_status", "delta_reason"]
NEW_CHILD_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "delta_status", "delta_reason"]
TARGET_GAP_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "supplier", "SupplierID", "Supplier.Name", "SupplierSKU", "supplier_free_stock", "recovery_reason"]
MISSING_PRODUCT_SUPPLIER_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "supplier", "SupplierID", "Supplier.Name", "SupplierSKU", "status", "action", "reason"]
PENDING_RETRY_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "pending_status", "pending_reason", "work_source"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delta onboarding for new StoreFeeder products. No stock updates.")
    parser.add_argument("--target-file", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--state-file", type=Path, default=Path("data/new_product_onboarding_state.json"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--warehouse-only-rules", type=Path, default=Path("data/warehouse_only_prime_sku_rules.csv"))
    parser.add_argument("--prime-parent-registry", type=Path, default=Path("data/amazon_prime_parent_skus.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/new_product_onboarding_delta"))
    parser.add_argument("--catalogue-snapshot-dir", type=Path, default=Path("data/storefeeder_catalogue_snapshots"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--only-sku", help="Runtime filter for manual testing. Not a code rule.")
    parser.add_argument("--refresh-catalogue-snapshot-only", action="store_true")
    parser.add_argument("--force-save-catalogue-snapshot", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--create-missing-product-suppliers", action="store_true")
    parser.add_argument("--supplier-costs", type=int, default=0)
    args = parser.parse_args()
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be zero or greater")
    if args.create_missing_product_suppliers and not args.execute:
        parser.error("--create-missing-product-suppliers requires --execute")
    if args.refresh_catalogue_snapshot_only and args.only_sku:
        parser.error("--refresh-catalogue-snapshot-only cannot be combined with --only-sku")
    if args.refresh_catalogue_snapshot_only and args.execute:
        parser.error("--refresh-catalogue-snapshot-only cannot be combined with --execute")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    full_products = _load_products(client, args.page_size, args.limit)
    current_catalogue = _build_catalogue_snapshot(full_products)
    previous_catalogue = _read_catalogue_snapshot(args.catalogue_snapshot_dir / "current_catalogue_snapshot.csv")
    if args.refresh_catalogue_snapshot_only:
        _save_catalogue_snapshot(args.catalogue_snapshot_dir, current_catalogue, run_id, force=args.force_save_catalogue_snapshot)
        print("StoreFeeder catalogue snapshot refreshed")
        print(_snapshot_summary(current_catalogue, previous_catalogue, args.catalogue_snapshot_dir).to_string(index=False))
        return 0
    products = _filter_only_sku(full_products, args.only_sku)
    catalogue_delta = _catalogue_delta(current_catalogue, previous_catalogue)
    new_parent_families = _new_parent_families_detected(current_catalogue, catalogue_delta)
    changed_parent_families = _changed_parent_families_detected(current_catalogue, catalogue_delta)
    new_children = _new_children_detected(catalogue_delta)
    targets = _read_targets(args.target_file)
    supplier_ids = _load_supplier_ids(args.supplier_id_map)
    supplier_stock = build_supplier_stock_lookup(read_csv(args.ralawise_stock), read_csv(args.uneek_stock))
    warehouse_rules = _load_protection_rules(args.warehouse_only_rules, args.prime_parent_registry)
    state = _read_state(args.state_file)

    result = run_delta_onboarding(
        client=client,
        products=products,
        targets=targets,
        supplier_ids=supplier_ids,
        supplier_stock=supplier_stock,
        warehouse_rules=warehouse_rules,
        state=state,
        execute=args.execute,
        create_missing_product_suppliers=args.create_missing_product_suppliers,
        supplier_costs=args.supplier_costs,
        run_id=run_id,
        ignore_success_state=bool(args.only_sku),
        catalogue_delta=catalogue_delta,
        use_catalogue_delta=not bool(args.only_sku),
    )

    promoted = result["promoted"]
    protected = result["protected"]
    parents = result["parents"]
    quarantine = result["quarantine"]
    setup_success = result["setup_success"]
    setup_failures = result["setup_failures"]
    append_ready = result["append_ready"]
    summary = result["summary"]
    new_state = result["state"]

    appended_count = 0
    target_rows_appended = pd.DataFrame(columns=TARGET_COLUMNS)
    if args.execute and not append_ready.empty and setup_failures.empty:
        appended_count = _append_targets(args.target_file, append_ready[TARGET_COLUMNS].copy(), out_dir, run_id)
        target_rows_appended = _read_appended_report(out_dir)
    elif args.execute and not setup_failures.empty:
        summary = _replace_summary_metric(summary, "append_blocked_reason", "product_supplier_setup_failures_present")

    if args.execute:
        _replace_pending_state(new_state, quarantine)
        _write_state(args.state_file, new_state)

    summary = _replace_summary_metric(summary, "target_rows_appended", appended_count)
    if args.execute:
        summary = _replace_summary_metric(summary, "target_gap_rows_appended", result.get("target_gap_rows_ready", 0) if appended_count else 0)
    summary = _replace_summary_metric(summary, "execute_mode", "yes" if args.execute else "no")
    summary = _replace_summary_metric(summary, "create_missing_product_suppliers", "yes" if args.create_missing_product_suppliers else "no")

    paths = {
        "onboarding_summary": out_dir / "onboarding_summary.csv",
        "current_catalogue_snapshot": out_dir / "current_catalogue_snapshot.csv",
        "previous_catalogue_snapshot": out_dir / "previous_catalogue_snapshot.csv",
        "catalogue_delta": out_dir / "catalogue_delta.csv",
        "new_parent_families_detected": out_dir / "new_parent_families_detected.csv",
        "changed_parent_families_detected": out_dir / "changed_parent_families_detected.csv",
        "new_children_detected": out_dir / "new_children_detected.csv",
        "recovered_target_gaps": out_dir / "recovered_target_gaps.csv",
        "recovered_missing_product_suppliers": out_dir / "recovered_missing_product_suppliers.csv",
        "pending_retry_processed": out_dir / "pending_retry_processed.csv",
        "promoted_supplier_synced": out_dir / "promoted_supplier_synced.csv",
        "protected_warehouse_only": out_dir / "protected_warehouse_only.csv",
        "parent_aggregates": out_dir / "parent_aggregates.csv",
        "quarantine": out_dir / "quarantine.csv",
        "product_supplier_setup_success": out_dir / "product_supplier_setup_success.csv",
        "product_supplier_setup_failures": out_dir / "product_supplier_setup_failures.csv",
        "target_rows_appended": out_dir / "target_rows_appended.csv",
    }
    summary.to_csv(paths["onboarding_summary"], index=False)
    current_catalogue.to_csv(paths["current_catalogue_snapshot"], index=False)
    previous_catalogue.to_csv(paths["previous_catalogue_snapshot"], index=False)
    catalogue_delta.to_csv(paths["catalogue_delta"], index=False)
    new_parent_families.to_csv(paths["new_parent_families_detected"], index=False)
    changed_parent_families.to_csv(paths["changed_parent_families_detected"], index=False)
    new_children.to_csv(paths["new_children_detected"], index=False)
    result["target_gaps"].to_csv(paths["recovered_target_gaps"], index=False)
    result["missing_product_suppliers"].to_csv(paths["recovered_missing_product_suppliers"], index=False)
    result["pending_retry_processed"].to_csv(paths["pending_retry_processed"], index=False)
    promoted.to_csv(paths["promoted_supplier_synced"], index=False)
    protected.to_csv(paths["protected_warehouse_only"], index=False)
    parents.to_csv(paths["parent_aggregates"], index=False)
    quarantine.to_csv(paths["quarantine"], index=False)
    setup_success.to_csv(paths["product_supplier_setup_success"], index=False)
    setup_failures.to_csv(paths["product_supplier_setup_failures"], index=False)
    target_rows_appended.to_csv(paths["target_rows_appended"], index=False)

    print("StoreFeeder new product onboarding delta")
    print(summary.to_string(index=False))
    print()
    print("Reports:")
    for path in paths.values():
        print(path)

    if not setup_failures.empty:
        raise SystemExit(f"Stopped because ProductSupplier setup failures were found: {len(setup_failures)}")
    if args.execute and not args.only_sku:
        _save_catalogue_snapshot(args.catalogue_snapshot_dir, current_catalogue, run_id, force=args.force_save_catalogue_snapshot)
    return 0


def run_delta_onboarding(
    *,
    client: StoreFeederApiClient,
    products: pd.DataFrame,
    targets: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    warehouse_rules: list[dict[str, str]],
    state: dict[str, Any],
    execute: bool,
    create_missing_product_suppliers: bool,
    supplier_costs: int,
    run_id: str,
    ignore_success_state: bool = False,
    catalogue_delta: pd.DataFrame | None = None,
    use_catalogue_delta: bool = True,
) -> dict[str, pd.DataFrame | dict[str, Any]]:
    required_product_columns = ["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"]
    _require_columns(products, required_product_columns, "StoreFeeder products")
    products = products.copy()
    for column in required_product_columns:
        products[column] = products[column].fillna("").astype(str).str.strip()

    existing_product_ids = set(targets["ProductID"].fillna("").astype(str).str.strip())
    existing_skus = {value.casefold() for value in targets["SKU"].fillna("").astype(str).str.strip() if value}
    children_by_parent = _children_by_parent(products)
    parent_skus = set(children_by_parent.keys())
    target_gaps = _target_gap_candidates(products, targets, supplier_ids, supplier_stock, warehouse_rules, children_by_parent)
    state = _normalize_state(state)
    processed = state.setdefault("processed", {})
    pending = state.setdefault("pending", {})
    if use_catalogue_delta:
        products, delta_stats = _filter_products_for_catalogue_delta(products, catalogue_delta, children_by_parent, pending, target_gaps)
    else:
        delta_stats = {
            "catalogue_delta_enabled": "no",
            "catalogue_delta_rows": 0,
            "catalogue_delta_products_selected": len(products),
            "catalogue_delta_new_products": 0,
            "catalogue_delta_changed_products": 0,
            "catalogue_delta_removed_products": 0,
            "catalogue_delta_new_numeric_parent_families": 0,
            "pending_retry_rows_selected": 0,
            "target_gap_rows_detected": len(target_gaps),
        }

    pending_retry_processed = _pending_retry_processed_report(products, pending)
    promoted_rows: list[dict[str, Any]] = []
    protected_rows: list[dict[str, Any]] = []
    parent_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    setup_success_rows: list[dict[str, Any]] = []
    setup_failure_rows: list[dict[str, Any]] = []
    append_rows: list[dict[str, Any]] = []
    missing_product_supplier_rows: list[dict[str, Any]] = []
    target_gap_rows_ready = 0

    scanned_variants = 0
    skipped_existing = 0
    skipped_state = 0
    processed_new = 0

    for _, product in products.iterrows():
        product_id = str(product["ID"]).strip()
        sku = str(product["SKU"]).strip()
        parent_sku = str(product["Parent SKU"]).strip()
        name = str(product["Name"]).strip()

        if not product_id or not sku:
            quarantine_rows.append(_quarantine_row(product, "missing_sku_or_product_id", "missing_sku_or_product_id"))
            continue

        state_key = _state_key(product_id, sku)
        product_state = processed.get(state_key, {})

        if product_id in existing_product_ids or sku.casefold() in existing_skus:
            skipped_existing += 1
            if execute:
                _clear_pending(pending, product_id, sku)
            continue

        matched_rule = _match_warehouse_only_rule(sku, parent_sku, warehouse_rules)
        if matched_rule:
            if not ignore_success_state and _state_success(product_state, "protected"):
                skipped_state += 1
                continue
            row = {
                "ProductID": product_id,
                "SKU": sku,
                "Parent SKU": parent_sku,
                "Name": name,
                "matched_rule": matched_rule,
                "reason": "protected_by_prime_or_warehouse_only_registry",
                "state_action": "save_protected" if execute else "dry_run_only",
            }
            protected_rows.append(row)
            processed_new += 1
            if execute:
                _save_processed(processed, state_key, product_id, sku, "protected", row["reason"], run_id)
                _clear_pending(pending, product_id, sku)
            continue

        is_parent = sku.casefold() in parent_skus
        if is_parent:
            signature = _family_signature(children_by_parent.get(sku.casefold(), pd.DataFrame()))
            is_supplier_family_candidate = _is_numeric_parent_sku(sku)
            if not ignore_success_state and _state_success(product_state, "parent_aggregate") and product_state.get("family_signature") == signature:
                skipped_state += 1
                continue
            child_count = int(len(children_by_parent.get(sku.casefold(), pd.DataFrame())))
            row = {
                "ProductID": product_id,
                "SKU": sku,
                "Name": name,
                "classification": "supplier_family_candidate" if is_supplier_family_candidate else "parent_aggregate",
                "family_signature": signature,
                "child_count": child_count,
                "state_action": "save_parent_aggregate" if execute else "dry_run_only",
                "reason": "numeric supplier-family parent; children are processed separately" if is_supplier_family_candidate else "parent product; children are processed separately",
            }
            parent_rows.append(row)
            processed_new += 1
            if execute:
                _save_processed(processed, state_key, product_id, sku, "parent_aggregate", row["reason"], run_id, family_signature=signature)
                _clear_pending(pending, product_id, sku)
            continue

        scanned_variants += 1
        if not ignore_success_state and _state_success(product_state, "promoted"):
            skipped_state += 1
            continue

        exact_matches = _exact_supplier_matches(sku, supplier_ids, supplier_stock)
        if len(exact_matches) == 0:
            quarantine_rows.append(_quarantine_row(product, "no_exact_supplier_sku_match", "StoreFeeder SKU was not found exactly once in supplier feeds"))
            continue
        if len(exact_matches) > 1:
            quarantine_rows.append(_quarantine_row(product, "ambiguous_exact_supplier_sku_match", "StoreFeeder SKU matched multiple supplier feeds or duplicate supplier rows"))
            continue

        candidate = exact_matches[0]
        already_attached = _product_has_supplier(client, product_id, candidate)
        setup_ok = already_attached
        setup_status = "already_attached" if already_attached else "missing_product_supplier_preview_only"
        if not already_attached:
            missing_product_supplier_rows.append(_missing_product_supplier_row(product, candidate, setup_status, "create_in_execute" if execute and create_missing_product_suppliers else "preview_only", "exact supplier match but ProductSupplier is missing"))
        if not already_attached and not execute:
            promoted_rows.append(
                _promoted_row(
                    product,
                    candidate,
                    setup_status,
                    "preview_create_product_supplier_then_append_target",
                    "exact unique supplier SKU match; ProductSupplier missing and would be created in execute mode",
                )
            )
            processed_new += 1
            continue
        if not already_attached and execute and create_missing_product_suppliers:
            setup_ok = _create_and_verify_product_supplier(
                client=client,
                product_id=product_id,
                product=product,
                candidate=candidate,
                supplier_costs=supplier_costs,
                success_rows=setup_success_rows,
                failure_rows=setup_failure_rows,
            )
            setup_status = "created_and_verified" if setup_ok else "setup_failed"
        elif not already_attached and execute and not create_missing_product_suppliers:
            setup_status = "missing_product_supplier_execute_blocked"

        if setup_ok:
            target_row = _target_row(product, candidate)
            append_rows.append(target_row)
            if str(product.get("_work_source", "")).strip() == "target_gap_recovery":
                target_gap_rows_ready += 1
            promoted_rows.append(
                _promoted_row(
                    product,
                    candidate,
                    setup_status,
                    "append_to_normal_supplier_target",
                    "exact unique supplier SKU match; ProductSupplier ready; append target even when supplier_free_stock is 0",
                )
            )
            processed_new += 1
            if execute:
                _save_processed(processed, state_key, product_id, sku, "promoted", "exact_unique_supplier_match_promoted", run_id)
                _clear_pending(pending, product_id, sku)
        else:
            quarantine_rows.append(_quarantine_row(product, setup_status, "ProductSupplier relationship missing or failed"))

    promoted = pd.DataFrame(promoted_rows, columns=PROMOTED_COLUMNS)
    protected = pd.DataFrame(protected_rows, columns=PROTECTED_COLUMNS)
    parents = pd.DataFrame(parent_rows, columns=PARENT_COLUMNS)
    quarantine = pd.DataFrame(quarantine_rows, columns=QUARANTINE_COLUMNS)
    setup_success = pd.DataFrame(setup_success_rows, columns=SETUP_SUCCESS_COLUMNS)
    setup_failures = pd.DataFrame(setup_failure_rows, columns=SETUP_FAILURE_COLUMNS)
    append_ready = pd.DataFrame(append_rows, columns=TARGET_COLUMNS)
    missing_product_suppliers = pd.DataFrame(missing_product_supplier_rows, columns=MISSING_PRODUCT_SUPPLIER_COLUMNS)

    summary = pd.DataFrame(
        [
            {"metric": "execute_mode", "value": "yes" if execute else "no"},
            {"metric": "create_missing_product_suppliers", "value": "yes" if create_missing_product_suppliers else "no"},
            {"metric": "catalogue_delta_enabled", "value": delta_stats["catalogue_delta_enabled"]},
            {"metric": "catalogue_delta_rows", "value": delta_stats["catalogue_delta_rows"]},
            {"metric": "catalogue_delta_products_selected", "value": delta_stats["catalogue_delta_products_selected"]},
            {"metric": "catalogue_delta_new_products", "value": delta_stats["catalogue_delta_new_products"]},
            {"metric": "catalogue_delta_changed_products", "value": delta_stats["catalogue_delta_changed_products"]},
            {"metric": "catalogue_delta_removed_products", "value": delta_stats["catalogue_delta_removed_products"]},
            {"metric": "catalogue_delta_new_numeric_parent_families", "value": delta_stats["catalogue_delta_new_numeric_parent_families"]},
            {"metric": "pending_retry_rows_selected", "value": delta_stats["pending_retry_rows_selected"]},
            {"metric": "target_gap_rows_detected", "value": delta_stats["target_gap_rows_detected"]},
            {"metric": "target_gap_rows_appended", "value": 0},
            {"metric": "missing_product_supplier_rows_detected", "value": len(missing_product_suppliers)},
            {"metric": "missing_product_supplier_rows_created", "value": len(setup_success)},
            {"metric": "changed_parent_families_detected", "value": delta_stats.get("changed_parent_families_detected", 0)},
            {"metric": "scanned_products", "value": len(products)},
            {"metric": "scanned_variants", "value": scanned_variants},
            {"metric": "existing_target_rows", "value": len(targets)},
            {"metric": "skipped_existing_target_rows", "value": skipped_existing},
            {"metric": "skipped_success_state_rows", "value": skipped_state},
            {"metric": "new_rows_processed_this_run", "value": processed_new},
            {"metric": "promoted_supplier_synced_rows", "value": len(promoted)},
            {"metric": "protected_warehouse_only_rows", "value": len(protected)},
            {"metric": "parent_aggregate_rows", "value": len(parents)},
            {"metric": "quarantine_rows", "value": len(quarantine)},
            {"metric": "product_supplier_setup_success_rows", "value": len(setup_success)},
            {"metric": "product_supplier_setup_failure_rows", "value": len(setup_failures)},
            {"metric": "target_rows_ready_to_append", "value": len(append_ready)},
            {"metric": "target_rows_appended", "value": 0},
        ],
        columns=SUMMARY_COLUMNS,
    )

    return {
        "promoted": promoted,
        "protected": protected,
        "parents": parents,
        "quarantine": quarantine,
        "setup_success": setup_success,
        "setup_failures": setup_failures,
        "append_ready": append_ready,
        "target_gaps": target_gaps,
        "missing_product_suppliers": missing_product_suppliers,
        "pending_retry_processed": pending_retry_processed,
        "target_gap_rows_ready": target_gap_rows_ready,
        "summary": summary,
        "state": state,
    }



def _build_catalogue_snapshot(products: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    if products.empty:
        return pd.DataFrame(columns=CATALOGUE_COLUMNS)
    working = products.copy()
    for column in ["ID", "SKU", "Parent SKU", "Name"]:
        if column not in working.columns:
            working[column] = ""
        working[column] = working[column].fillna("").astype(str).str.strip()
    children_by_parent = _children_by_parent(working)
    for _, product in working.iterrows():
        product_id = str(product.get("ID", "")).strip()
        sku = str(product.get("SKU", "")).strip()
        parent_sku = str(product.get("Parent SKU", "")).strip()
        name = str(product.get("Name", "")).strip()
        row_signature = _hash_payload({"ProductID": product_id, "SKU": sku, "Parent SKU": parent_sku, "Name": name})
        family_signature = ""
        if sku and sku.casefold() in children_by_parent:
            family_signature = _family_signature(children_by_parent[sku.casefold()])
        rows.append(
            {
                "ProductID": product_id,
                "SKU": sku,
                "Parent SKU": parent_sku,
                "Name": name,
                "row_signature": row_signature,
                "family_signature": family_signature,
            }
        )
    return pd.DataFrame(rows, columns=CATALOGUE_COLUMNS).sort_values(["ProductID", "SKU"], kind="stable").reset_index(drop=True)


def _read_catalogue_snapshot(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=CATALOGUE_COLUMNS)
    snapshot = read_csv(path)
    for column in CATALOGUE_COLUMNS:
        if column not in snapshot.columns:
            snapshot[column] = ""
        snapshot[column] = snapshot[column].fillna("").astype(str).str.strip()
    return snapshot[CATALOGUE_COLUMNS].copy()


def _save_catalogue_snapshot(snapshot_dir: Path, current_catalogue: pd.DataFrame, run_id: str, *, force: bool = False) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    current_path = snapshot_dir / "current_catalogue_snapshot.csv"
    previous_path = snapshot_dir / "previous_catalogue_snapshot.csv"
    timestamped_path = snapshot_dir / f"current_catalogue_snapshot_{run_id}.csv"
    previous_catalogue = _read_catalogue_snapshot(current_path)
    if not force and not previous_catalogue.empty:
        previous_count = len(previous_catalogue)
        current_count = len(current_catalogue)
        minimum_safe_count = max(1, int(previous_count * 0.8))
        if current_count < minimum_safe_count:
            raise SystemExit(
                "Refusing to save catalogue snapshot because current row count "
                f"({current_count}) is suspiciously small compared with previous snapshot "
                f"({previous_count}). Use --force-save-catalogue-snapshot to override."
            )
    if current_path.exists():
        shutil.copy2(current_path, previous_path)
    current_catalogue.to_csv(current_path, index=False)
    current_catalogue.to_csv(timestamped_path, index=False)


def _snapshot_summary(current_catalogue: pd.DataFrame, previous_catalogue: pd.DataFrame, snapshot_dir: Path) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"metric": "current_rows", "value": len(current_catalogue)},
            {"metric": "previous_rows", "value": len(previous_catalogue)},
            {"metric": "snapshot_dir", "value": str(snapshot_dir)},
            {"metric": "current_snapshot", "value": str(snapshot_dir / "current_catalogue_snapshot.csv")},
        ],
        columns=SUMMARY_COLUMNS,
    )


def _catalogue_delta(current: pd.DataFrame, previous: pd.DataFrame) -> pd.DataFrame:
    if current.empty:
        return pd.DataFrame(columns=DELTA_COLUMNS)
    current = current.copy()
    previous = previous.copy()
    for frame in [current, previous]:
        for column in CATALOGUE_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""
            frame[column] = frame[column].fillna("").astype(str).str.strip()
        frame["_key"] = frame.apply(_catalogue_key, axis=1)
    previous_by_key = {str(row["_key"]): row for _, row in previous.iterrows() if str(row.get("_key", "")).strip()}
    current_keys = {str(row["_key"]) for _, row in current.iterrows() if str(row.get("_key", "")).strip()}
    rows: list[dict[str, str]] = []
    for _, row in current.iterrows():
        key = str(row.get("_key", "")).strip()
        if not key:
            continue
        old = previous_by_key.get(key)
        status = "unchanged"
        reason = "present_in_previous_snapshot"
        if old is None:
            status = "new"
            reason = "new_product_id_or_sku"
        elif str(old.get("row_signature", "")) != str(row.get("row_signature", "")) or str(old.get("family_signature", "")) != str(row.get("family_signature", "")):
            status = "changed"
            reason = "catalogue_row_or_family_signature_changed"
        if status != "unchanged":
            rows.append(
                {
                    "ProductID": str(row.get("ProductID", "")).strip(),
                    "SKU": str(row.get("SKU", "")).strip(),
                    "Parent SKU": str(row.get("Parent SKU", "")).strip(),
                    "Name": str(row.get("Name", "")).strip(),
                    "delta_status": status,
                    "delta_reason": reason,
                }
            )
    removed_keys = sorted(set(previous_by_key) - current_keys)
    for key in removed_keys:
        old = previous_by_key[key]
        rows.append(
            {
                "ProductID": str(old.get("ProductID", "")).strip(),
                "SKU": str(old.get("SKU", "")).strip(),
                "Parent SKU": str(old.get("Parent SKU", "")).strip(),
                "Name": str(old.get("Name", "")).strip(),
                "delta_status": "removed",
                "delta_reason": "not_present_in_current_snapshot",
            }
        )
    return pd.DataFrame(rows, columns=DELTA_COLUMNS)



def _target_gap_candidates(
    products: pd.DataFrame,
    targets: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    warehouse_rules: list[dict[str, str]],
    children_by_parent: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if products.empty:
        return pd.DataFrame(columns=TARGET_GAP_COLUMNS)
    existing_product_ids = set(targets.get("ProductID", pd.Series(dtype=str)).fillna("").astype(str).str.strip())
    existing_skus = {value.casefold() for value in targets.get("SKU", pd.Series(dtype=str)).fillna("").astype(str).str.strip() if value}
    numeric_parents = {parent for parent in children_by_parent if _is_numeric_parent_sku(parent)}
    rows: list[dict[str, Any]] = []
    for _, product in products.iterrows():
        product_id = str(product.get("ID", "")).strip()
        sku = str(product.get("SKU", "")).strip()
        parent_sku = str(product.get("Parent SKU", "")).strip()
        if not product_id or not sku or not parent_sku:
            continue
        if product_id in existing_product_ids or sku.casefold() in existing_skus:
            continue
        if parent_sku.casefold() not in numeric_parents:
            continue
        if _match_warehouse_only_rule(sku, parent_sku, warehouse_rules):
            continue
        exact_matches = _exact_supplier_matches(sku, supplier_ids, supplier_stock)
        if len(exact_matches) != 1:
            continue
        candidate = exact_matches[0]
        rows.append(
            {
                "ProductID": product_id,
                "SKU": sku,
                "Parent SKU": parent_sku,
                "Name": str(product.get("Name", "")).strip(),
                "supplier": candidate.get("supplier", ""),
                "SupplierID": candidate.get("SupplierID", ""),
                "Supplier.Name": candidate.get("Supplier.Name", ""),
                "SupplierSKU": candidate.get("supplier_sku", ""),
                "supplier_free_stock": candidate.get("supplier_free_stock", ""),
                "recovery_reason": "numeric supplier-family child exact-matches supplier feed but is missing from target file",
            }
        )
    return pd.DataFrame(rows, columns=TARGET_GAP_COLUMNS)


def _missing_product_supplier_row(product: pd.Series, candidate: dict[str, Any], status: str, action: str, reason: str) -> dict[str, Any]:
    return {
        "ProductID": str(product.get("ID", "")).strip(),
        "SKU": str(product.get("SKU", "")).strip(),
        "Parent SKU": str(product.get("Parent SKU", "")).strip(),
        "Name": str(product.get("Name", "")).strip(),
        "supplier": candidate.get("supplier", ""),
        "SupplierID": candidate.get("SupplierID", ""),
        "Supplier.Name": candidate.get("Supplier.Name", ""),
        "SupplierSKU": candidate.get("supplier_sku", ""),
        "status": status,
        "action": action,
        "reason": reason,
    }


def _pending_retry_processed_report(products: pd.DataFrame, pending: dict[str, Any]) -> pd.DataFrame:
    if products.empty or "_work_source" not in products.columns:
        return pd.DataFrame(columns=PENDING_RETRY_COLUMNS)
    rows: list[dict[str, Any]] = []
    pending_products = products[products["_work_source"].fillna("").astype(str).eq("pending_retry")]
    for _, product in pending_products.iterrows():
        product_id = str(product.get("ID", "")).strip()
        sku = str(product.get("SKU", "")).strip()
        pending_row = pending.get(_state_key(product_id, sku), {}) if isinstance(pending, dict) else {}
        rows.append(
            {
                "ProductID": product_id,
                "SKU": sku,
                "Parent SKU": str(product.get("Parent SKU", "")).strip(),
                "Name": str(product.get("Name", "")).strip(),
                "pending_status": str(pending_row.get("supplier_match_status", "")).strip(),
                "pending_reason": str(pending_row.get("reason", "")).strip(),
                "work_source": "pending_retry",
            }
        )
    return pd.DataFrame(rows, columns=PENDING_RETRY_COLUMNS)


def _changed_parent_families_detected(current_catalogue: pd.DataFrame, catalogue_delta: pd.DataFrame) -> pd.DataFrame:
    if catalogue_delta.empty:
        return pd.DataFrame(columns=NEW_PARENT_COLUMNS)
    changed = catalogue_delta[catalogue_delta["delta_status"].eq("changed")].copy()
    changed = changed[changed["SKU"].fillna("").astype(str).map(_is_numeric_parent_sku)].copy()
    if changed.empty:
        return pd.DataFrame(columns=NEW_PARENT_COLUMNS)
    child_counts = current_catalogue[current_catalogue["Parent SKU"].fillna("").astype(str).str.strip().ne("")].groupby(
        current_catalogue["Parent SKU"].fillna("").astype(str).str.strip().str.casefold()
    ).size().to_dict()
    rows = []
    for _, row in changed.iterrows():
        sku = str(row.get("SKU", "")).strip()
        rows.append(
            {
                "ProductID": str(row.get("ProductID", "")).strip(),
                "SKU": sku,
                "Name": str(row.get("Name", "")).strip(),
                "child_count": int(child_counts.get(sku.casefold(), 0)),
                "delta_status": str(row.get("delta_status", "")).strip(),
                "delta_reason": str(row.get("delta_reason", "")).strip(),
            }
        )
    return pd.DataFrame(rows, columns=NEW_PARENT_COLUMNS)


def _count_changed_numeric_parent_families(catalogue_delta: pd.DataFrame) -> int:
    if catalogue_delta is None or catalogue_delta.empty:
        return 0
    changed = catalogue_delta[catalogue_delta["delta_status"].eq("changed")].copy()
    return int(changed["SKU"].fillna("").astype(str).map(_is_numeric_parent_sku).sum())

def _filter_products_for_catalogue_delta(
    products: pd.DataFrame,
    catalogue_delta: pd.DataFrame | None,
    children_by_parent: dict[str, pd.DataFrame],
    pending: dict[str, Any] | None,
    target_gaps: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    products = products.copy()
    products["_work_source"] = "catalogue_delta"
    if catalogue_delta is None:
        catalogue_delta = pd.DataFrame(columns=DELTA_COLUMNS)
    pending = pending if isinstance(pending, dict) else {}

    active_delta = catalogue_delta[catalogue_delta.get("delta_status", pd.Series(dtype=str)).isin(["new", "changed"])].copy()
    selected_product_ids = {str(value).strip() for value in active_delta.get("ProductID", pd.Series(dtype=str)).fillna("").astype(str) if str(value).strip()}
    selected_skus = {str(value).strip().casefold() for value in active_delta.get("SKU", pd.Series(dtype=str)).fillna("").astype(str) if str(value).strip()}
    selected_parent_skus = {str(value).strip().casefold() for value in active_delta.get("Parent SKU", pd.Series(dtype=str)).fillna("").astype(str) if str(value).strip()}
    source_by_key: dict[str, str] = {}
    for product_id in selected_product_ids:
        source_by_key["ProductID:" + product_id] = "catalogue_delta"
    for sku in selected_skus:
        source_by_key["SKU:" + sku] = "catalogue_delta"

    numeric_parent_families = {
        sku for sku in selected_skus
        if _is_numeric_parent_sku(sku) and sku in children_by_parent
    }
    impacted_parent_skus = selected_parent_skus | numeric_parent_families
    for parent_sku in list(impacted_parent_skus):
        if parent_sku in children_by_parent:
            child_group = children_by_parent[parent_sku]
            selected_product_ids.update(str(value).strip() for value in child_group["ID"].fillna("").astype(str) if str(value).strip())
            selected_skus.update(str(value).strip().casefold() for value in child_group["SKU"].fillna("").astype(str) if str(value).strip())
            selected_skus.add(parent_sku)
            source_by_key["SKU:" + parent_sku] = "catalogue_delta_parent_family"
            for _, child in child_group.iterrows():
                child_id = str(child.get("ID", "")).strip()
                child_sku = str(child.get("SKU", "")).strip().casefold()
                if child_id:
                    source_by_key.setdefault("ProductID:" + child_id, "catalogue_delta_family_child")
                if child_sku:
                    source_by_key.setdefault("SKU:" + child_sku, "catalogue_delta_family_child")

    for _, gap in target_gaps.iterrows():
        product_id = str(gap.get("ProductID", "")).strip()
        sku = str(gap.get("SKU", "")).strip()
        if product_id:
            selected_product_ids.add(product_id)
            source_by_key["ProductID:" + product_id] = "target_gap_recovery"
        if sku:
            selected_skus.add(sku.casefold())
            source_by_key["SKU:" + sku.casefold()] = "target_gap_recovery"

    pending_count = 0
    for item in pending.values():
        if not isinstance(item, dict):
            continue
        product_id = str(item.get("product_id", "")).strip()
        sku = str(item.get("sku", "")).strip()
        parent_sku = str(item.get("parent_sku", "")).strip()
        if product_id:
            selected_product_ids.add(product_id)
            source_by_key["ProductID:" + product_id] = "pending_retry"
            pending_count += 1
        if sku:
            selected_skus.add(sku.casefold())
            source_by_key["SKU:" + sku.casefold()] = "pending_retry"
        if parent_sku:
            selected_parent_skus.add(parent_sku.casefold())

    if catalogue_delta.empty and not selected_product_ids and not selected_skus and not selected_parent_skus:
        selected = products.iloc[0:0].copy()
    else:
        product_ids = products["ID"].fillna("").astype(str).str.strip()
        skus = products["SKU"].fillna("").astype(str).str.strip().str.casefold()
        parents = products["Parent SKU"].fillna("").astype(str).str.strip().str.casefold()
        mask = product_ids.isin(selected_product_ids) | skus.isin(selected_skus) | parents.isin(impacted_parent_skus | selected_parent_skus)
        selected = products[mask].copy()
        selected["_work_source"] = selected.apply(lambda row: _work_source_for_row(row, source_by_key), axis=1)

    stats = {
        "catalogue_delta_enabled": "yes",
        "catalogue_delta_rows": len(catalogue_delta),
        "catalogue_delta_products_selected": len(selected),
        "catalogue_delta_new_products": int(catalogue_delta.get("delta_status", pd.Series(dtype=str)).eq("new").sum()) if not catalogue_delta.empty else 0,
        "catalogue_delta_changed_products": int(catalogue_delta.get("delta_status", pd.Series(dtype=str)).eq("changed").sum()) if not catalogue_delta.empty else 0,
        "catalogue_delta_removed_products": int(catalogue_delta.get("delta_status", pd.Series(dtype=str)).eq("removed").sum()) if not catalogue_delta.empty else 0,
        "catalogue_delta_new_numeric_parent_families": len(numeric_parent_families),
        "pending_retry_rows_selected": pending_count,
        "target_gap_rows_detected": len(target_gaps),
        "changed_parent_families_detected": _count_changed_numeric_parent_families(catalogue_delta),
    }
    return selected, stats


def _work_source_for_row(row: pd.Series, source_by_key: dict[str, str]) -> str:
    product_id = str(row.get("ID", "")).strip()
    sku = str(row.get("SKU", "")).strip().casefold()
    if product_id and "ProductID:" + product_id in source_by_key:
        return source_by_key["ProductID:" + product_id]
    if sku and "SKU:" + sku in source_by_key:
        return source_by_key["SKU:" + sku]
    parent_sku = str(row.get("Parent SKU", "")).strip().casefold()
    if parent_sku and "SKU:" + parent_sku in source_by_key:
        return "catalogue_delta_family_child"
    return "catalogue_delta"


def _new_parent_families_detected(current_catalogue: pd.DataFrame, catalogue_delta: pd.DataFrame) -> pd.DataFrame:
    if catalogue_delta.empty:
        return pd.DataFrame(columns=NEW_PARENT_COLUMNS)
    active = catalogue_delta[catalogue_delta["delta_status"].isin(["new", "changed"])].copy()
    active = active[active["SKU"].fillna("").astype(str).map(_is_numeric_parent_sku)].copy()
    if active.empty:
        return pd.DataFrame(columns=NEW_PARENT_COLUMNS)
    child_counts = current_catalogue[current_catalogue["Parent SKU"].fillna("").astype(str).str.strip().ne("")].groupby(
        current_catalogue["Parent SKU"].fillna("").astype(str).str.strip().str.casefold()
    ).size().to_dict()
    rows = []
    for _, row in active.iterrows():
        sku = str(row.get("SKU", "")).strip()
        rows.append(
            {
                "ProductID": str(row.get("ProductID", "")).strip(),
                "SKU": sku,
                "Name": str(row.get("Name", "")).strip(),
                "child_count": int(child_counts.get(sku.casefold(), 0)),
                "delta_status": str(row.get("delta_status", "")).strip(),
                "delta_reason": str(row.get("delta_reason", "")).strip(),
            }
        )
    return pd.DataFrame(rows, columns=NEW_PARENT_COLUMNS)


def _new_children_detected(catalogue_delta: pd.DataFrame) -> pd.DataFrame:
    if catalogue_delta.empty:
        return pd.DataFrame(columns=NEW_CHILD_COLUMNS)
    children = catalogue_delta[
        catalogue_delta["delta_status"].isin(["new", "changed"])
        & catalogue_delta["Parent SKU"].fillna("").astype(str).str.strip().ne("")
    ].copy()
    if children.empty:
        return pd.DataFrame(columns=NEW_CHILD_COLUMNS)
    return children[NEW_CHILD_COLUMNS].copy()

def _catalogue_key(row: pd.Series) -> str:
    product_id = str(row.get("ProductID", "")).strip()
    if product_id:
        return "ProductID:" + product_id
    sku = str(row.get("SKU", "")).strip()
    return "SKU:" + sku.casefold()


def _hash_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _is_numeric_parent_sku(sku: str) -> bool:
    value = str(sku).strip()
    return bool(value) and value.isdigit()

def _load_products(client: StoreFeederApiClient, page_size: int, limit: int | None) -> pd.DataFrame:
    products = fetch_products(client, page_size=page_size, limit=limit)
    rows = []
    for product in products:
        product_id = _first_value(product, ["ID", "ProductID", "ProductId"])
        sku = _first_value(product, ["SKU", "Sku"])
        parent_sku = _first_value(product, ["Parent SKU", "ParentSKU", "ParentSku", "ParentProductSKU"])
        name = _first_value(product, ["Name", "ProductName", "Title"])
        rows.append(
            {
                "ID": product_id,
                "SKU": sku,
                "Parent SKU": parent_sku,
                "Name": name,
                "Suppliers": "",
                "Supplier SKUs": "",
            }
        )
        if product_id and _is_numeric_parent_sku(sku):
            rows.extend(_product_detail_child_rows(client, product_id, sku, name))
    frame = pd.DataFrame(rows, columns=["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"])
    return _dedupe_product_rows(frame)


def _product_detail_child_rows(client: StoreFeederApiClient, product_id: str, parent_sku: str, parent_name: str) -> list[dict[str, str]]:
    try:
        detail = client.get_product(product_id).get("response", {})
    except Exception:
        return []
    children = detail.get("Children", []) if isinstance(detail, dict) else []
    rows: list[dict[str, str]] = []
    if not isinstance(children, list):
        return rows
    for child in children:
        if not isinstance(child, dict):
            continue
        child_product = child.get("Product", {})
        if not isinstance(child_product, dict):
            continue
        child_id = _first_value(child_product, ["ID", "ProductID", "ProductId"])
        child_sku = _first_value(child_product, ["SKU", "Sku"])
        child_name = _first_value(child_product, ["Name", "ProductName", "Title"]) or parent_name
        if not child_id or not child_sku:
            continue
        rows.append(
            {
                "ID": child_id,
                "SKU": child_sku,
                "Parent SKU": parent_sku,
                "Name": child_name,
                "Suppliers": "",
                "Supplier SKUs": "",
            }
        )
    return rows


def _dedupe_product_rows(products: pd.DataFrame) -> pd.DataFrame:
    if products.empty:
        return products
    working = products.copy()
    for column in ["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"]:
        if column not in working.columns:
            working[column] = ""
        working[column] = working[column].fillna("").astype(str).str.strip()
    working["_dedupe_key"] = working.apply(lambda row: _state_key(str(row.get("ID", "")).strip(), str(row.get("SKU", "")).strip()), axis=1)
    working = working.drop_duplicates("_dedupe_key", keep="first").drop(columns=["_dedupe_key"])
    return working[["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"]].reset_index(drop=True)


def _first_value(payload: dict[str, Any], names: list[str]) -> str:
    for name in names:
        value = payload.get(name)
        if value not in [None, ""]:
            return str(value).strip()
    return ""


def _product_has_supplier(client: StoreFeederApiClient, product_id: str, candidate: dict[str, Any]) -> bool:
    try:
        return _readback_contains_supplier(client.get_product_suppliers(product_id), candidate)
    except Exception:
        return False


def _filter_only_sku(products: pd.DataFrame, only_sku: str | None) -> pd.DataFrame:
    if not only_sku or products.empty:
        return products
    key = only_sku.strip().casefold()
    if not key:
        return products
    sku = products.get("SKU", pd.Series(dtype=str)).fillna("").astype(str).str.strip().str.casefold()
    parent = products.get("Parent SKU", pd.Series(dtype=str)).fillna("").astype(str).str.strip().str.casefold()
    return products[sku.eq(key) | parent.eq(key) | sku.str.startswith(key + "-")].copy()


def _read_targets(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=TARGET_COLUMNS)
    targets = read_csv(path)
    for column in TARGET_COLUMNS:
        if column not in targets.columns:
            targets[column] = ""
    return targets


def _load_supplier_ids(path: Path) -> pd.DataFrame:
    supplier_ids = read_csv(path)
    _require_columns(supplier_ids, ["supplier", "SupplierID", "Supplier.Name"], "supplier ID map")
    supplier_ids = supplier_ids.copy()
    for column in ["supplier", "SupplierID", "Supplier.Name"]:
        supplier_ids[column] = supplier_ids[column].fillna("").astype(str).str.strip()
    supplier_ids["_supplier_key"] = supplier_ids["supplier"].str.casefold()
    return supplier_ids


def _load_protection_rules(warehouse_rules_path: Path, prime_registry_path: Path) -> list[dict[str, str]]:
    rules = _load_warehouse_only_rules(warehouse_rules_path)
    if prime_registry_path.exists():
        with prime_registry_path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                parent_sku = str(row.get("parent_sku", "")).strip()
                reason = str(row.get("reason", "")).strip() or "prime_parent_registry"
                if parent_sku:
                    rules.append({"match_type": "prefix", "value": parent_sku, "reason": reason})
                    rules.append({"match_type": "exact", "value": parent_sku, "reason": reason})
    return _dedupe_rules(rules)


def _load_warehouse_only_rules(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            match_type = str(row.get("match_type", "")).strip().casefold()
            value = str(row.get("value", "")).strip()
            reason = str(row.get("reason", "")).strip()
            if match_type in {"exact", "prefix"} and value:
                rows.append({"match_type": match_type, "value": value, "reason": reason})
    return rows


def _dedupe_rules(rules: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for rule in rules:
        key = (rule["match_type"].casefold(), rule["value"].casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append(rule)
    return out


def _match_warehouse_only_rule(sku: str, parent_sku: str, rules: list[dict[str, str]]) -> str:
    values = [sku.strip(), parent_sku.strip()]
    for rule in rules:
        rule_value = rule["value"].strip()
        rule_cf = rule_value.casefold()
        for value in values:
            value_cf = value.casefold()
            if rule["match_type"] == "exact" and value_cf == rule_cf:
                return rule_value
            if rule["match_type"] == "prefix" and (value_cf == rule_cf or value_cf.startswith(rule_cf + "-")):
                return rule_value
    return ""


def _children_by_parent(products: pd.DataFrame) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    if "Parent SKU" not in products.columns:
        return result
    for parent_sku, group in products[products["Parent SKU"].fillna("").astype(str).str.strip().ne("")].groupby(products["Parent SKU"].str.casefold()):
        result[str(parent_sku)] = group.copy()
    return result


def _family_signature(children: pd.DataFrame) -> str:
    rows = []
    if children is not None and not children.empty:
        for _, child in children.iterrows():
            rows.append({
                "ProductID": str(child.get("ID", "")).strip(),
                "SKU": str(child.get("SKU", "")).strip(),
                "Parent SKU": str(child.get("Parent SKU", "")).strip(),
            })
    raw = json.dumps(sorted(rows, key=lambda row: (row["ProductID"], row["SKU"])), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _promoted_row(product: pd.Series, candidate: dict[str, Any], setup_status: str, target_action: str, reason: str) -> dict[str, Any]:
    return {
        "ProductID": str(product.get("ID", "")).strip(),
        "SKU": str(product.get("SKU", "")).strip(),
        "Parent SKU": str(product.get("Parent SKU", "")).strip(),
        "Name": str(product.get("Name", "")).strip(),
        "supplier": candidate.get("supplier", ""),
        "SupplierID": candidate.get("SupplierID", ""),
        "Supplier.Name": candidate.get("Supplier.Name", ""),
        "SupplierSKU": candidate.get("supplier_sku", ""),
        "supplier_free_stock": candidate.get("supplier_free_stock", ""),
        "product_supplier_status": setup_status,
        "target_action": target_action,
        "reason": reason,
    }


def _quarantine_row(product: pd.Series, status: str, reason: str) -> dict[str, Any]:
    return {
        "ProductID": str(product.get("ID", "")).strip(),
        "SKU": str(product.get("SKU", "")).strip(),
        "Parent SKU": str(product.get("Parent SKU", "")).strip(),
        "Name": str(product.get("Name", "")).strip(),
        "work_source": str(product.get("_work_source", "catalogue_delta")).strip() or "catalogue_delta",
        "supplier_match_status": status,
        "product_supplier_status": "not_applicable",
        "target_action": "quarantine",
        "reason": reason,
    }


def _state_key(product_id: str, sku: str) -> str:
    if product_id:
        return "ProductID:" + product_id
    return "SKU:" + sku.casefold()


def _normalize_state(state: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(state, dict):
        state = {}
    if state.get("version") != STATE_VERSION:
        state.setdefault("version", STATE_VERSION)
    if not isinstance(state.get("processed"), dict):
        state["processed"] = {}
    if not isinstance(state.get("pending"), dict):
        state["pending"] = {}
    return state


def _state_success(row: Any, status: str) -> bool:
    return isinstance(row, dict) and str(row.get("status", "")).strip() == status


def _save_processed(
    processed: dict[str, Any],
    key: str,
    product_id: str,
    sku: str,
    status: str,
    reason: str,
    run_id: str,
    *,
    family_signature: str = "",
) -> None:
    row = {
        "product_id": product_id,
        "sku": sku,
        "status": status,
        "reason": reason,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
    }
    if family_signature:
        row["family_signature"] = family_signature
    processed[key] = row



def _clear_pending(pending: dict[str, Any], product_id: str, sku: str) -> None:
    for key in [_state_key(product_id, sku), "SKU:" + str(sku).strip().casefold()]:
        pending.pop(key, None)


def _replace_pending_state(state: dict[str, Any], quarantine: pd.DataFrame) -> None:
    pending: dict[str, Any] = {}
    if not quarantine.empty:
        for _, row in quarantine.iterrows():
            product_id = str(row.get("ProductID", "")).strip()
            sku = str(row.get("SKU", "")).strip()
            key = _state_key(product_id, sku)
            pending[key] = {
                "product_id": product_id,
                "sku": sku,
                "parent_sku": str(row.get("Parent SKU", "")).strip(),
                "name": str(row.get("Name", "")).strip(),
                "status": "pending_retry",
                "supplier_match_status": str(row.get("supplier_match_status", "")).strip(),
                "reason": str(row.get("reason", "")).strip(),
                "last_seen_at": datetime.now(timezone.utc).isoformat(),
            }
    state["pending"] = pending

def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": STATE_VERSION, "processed": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": STATE_VERSION, "processed": {}}


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _read_appended_report(out_dir: Path) -> pd.DataFrame:
    path = out_dir / "target_rows_appended_to_file.csv"
    if not path.exists():
        return pd.DataFrame(columns=TARGET_COLUMNS)
    return read_csv(path)


def _replace_summary_metric(summary: pd.DataFrame, metric: str, value: Any) -> pd.DataFrame:
    summary = summary.copy()
    if metric in set(summary["metric"]):
        summary.loc[summary["metric"].eq(metric), "value"] = value
    else:
        summary = pd.concat([summary, pd.DataFrame([{"metric": metric, "value": value}])], ignore_index=True)
    return summary


def _require_columns(df: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: " + ", ".join(missing))


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