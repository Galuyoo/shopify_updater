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

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.export_storefeeder_products import fetch_products
from scripts.promote_exact_supplier_matches import (
    TARGET_COLUMNS,
    _append_targets,
    _create_and_verify_product_supplier,
    _exact_supplier_matches,
    _has_stock_location,
    _qty_to_int,
    _readback_contains_supplier,
    _target_row,
)
from src.stock_mapping import build_supplier_stock_lookup
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv

SUMMARY_COLUMNS = ["metric", "value"]
PROTECTED_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "matched_rule", "reason", "state_action"]
PARENT_COLUMNS = ["ProductID", "SKU", "Name", "family_signature", "child_count", "state_action", "reason"]
QUARANTINE_COLUMNS = [
    "ProductID",
    "SKU",
    "Parent SKU",
    "Name",
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
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--only-sku", help="Runtime filter for manual testing. Not a code rule.")
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
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    products = _load_products(client, args.page_size, args.limit)
    products = _filter_only_sku(products, args.only_sku)
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
        _write_state(args.state_file, new_state)

    summary = _replace_summary_metric(summary, "target_rows_appended", appended_count)
    summary = _replace_summary_metric(summary, "execute_mode", "yes" if args.execute else "no")
    summary = _replace_summary_metric(summary, "create_missing_product_suppliers", "yes" if args.create_missing_product_suppliers else "no")

    paths = {
        "onboarding_summary": out_dir / "onboarding_summary.csv",
        "promoted_supplier_synced": out_dir / "promoted_supplier_synced.csv",
        "protected_warehouse_only": out_dir / "protected_warehouse_only.csv",
        "parent_aggregates": out_dir / "parent_aggregates.csv",
        "quarantine": out_dir / "quarantine.csv",
        "product_supplier_setup_success": out_dir / "product_supplier_setup_success.csv",
        "product_supplier_setup_failures": out_dir / "product_supplier_setup_failures.csv",
        "target_rows_appended": out_dir / "target_rows_appended.csv",
    }
    summary.to_csv(paths["onboarding_summary"], index=False)
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

    state = _normalize_state(state)
    processed = state.setdefault("processed", {})

    promoted_rows: list[dict[str, Any]] = []
    protected_rows: list[dict[str, Any]] = []
    parent_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    setup_success_rows: list[dict[str, Any]] = []
    setup_failure_rows: list[dict[str, Any]] = []
    append_rows: list[dict[str, Any]] = []

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
            continue

        is_parent = sku.casefold() in parent_skus
        if is_parent:
            signature = _family_signature(children_by_parent.get(sku.casefold(), pd.DataFrame()))
            if not ignore_success_state and _state_success(product_state, "parent_aggregate") and product_state.get("family_signature") == signature:
                skipped_state += 1
                continue
            child_count = int(len(children_by_parent.get(sku.casefold(), pd.DataFrame())))
            row = {
                "ProductID": product_id,
                "SKU": sku,
                "Name": name,
                "family_signature": signature,
                "child_count": child_count,
                "state_action": "save_parent_aggregate" if execute else "dry_run_only",
                "reason": "parent product; children are processed separately",
            }
            parent_rows.append(row)
            processed_new += 1
            if execute:
                _save_processed(processed, state_key, product_id, sku, "parent_aggregate", row["reason"], run_id, family_signature=signature)
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

        append_allowed = True
        stock_location_status = "warehouse_stock_update_allowed"
        if setup_ok and _qty_to_int(candidate.get("supplier_free_stock", 0)) == 0:
            detail = client.get_product(product_id).get("response", {})
            if not _has_stock_location(detail, "Warehouse Stock"):
                stock_location_status = "zero_stock_no_warehouse_stock_row"
                append_allowed = False

        if setup_ok and append_allowed:
            target_row = _target_row(product, candidate)
            append_rows.append(target_row)
            promoted_rows.append(_promoted_row(product, candidate, setup_status, "append_to_normal_supplier_target", "exact unique supplier SKU match; ProductSupplier ready"))
            processed_new += 1
            if execute:
                _save_processed(processed, state_key, product_id, sku, "promoted", "exact_unique_supplier_match_promoted", run_id)
        elif setup_ok and not append_allowed:
            quarantine_rows.append(_quarantine_row(product, stock_location_status, "supplier_setup_only_no_target_append: zero stock has no Warehouse Stock row; target append delayed until safe"))
        else:
            quarantine_rows.append(_quarantine_row(product, setup_status, "ProductSupplier relationship missing or failed"))

    promoted = pd.DataFrame(promoted_rows, columns=PROMOTED_COLUMNS)
    protected = pd.DataFrame(protected_rows, columns=PROTECTED_COLUMNS)
    parents = pd.DataFrame(parent_rows, columns=PARENT_COLUMNS)
    quarantine = pd.DataFrame(quarantine_rows, columns=QUARANTINE_COLUMNS)
    setup_success = pd.DataFrame(setup_success_rows, columns=SETUP_SUCCESS_COLUMNS)
    setup_failures = pd.DataFrame(setup_failure_rows, columns=SETUP_FAILURE_COLUMNS)
    append_ready = pd.DataFrame(append_rows, columns=TARGET_COLUMNS)

    summary = pd.DataFrame(
        [
            {"metric": "execute_mode", "value": "yes" if execute else "no"},
            {"metric": "create_missing_product_suppliers", "value": "yes" if create_missing_product_suppliers else "no"},
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
        "summary": summary,
        "state": state,
    }


def _load_products(client: StoreFeederApiClient, page_size: int, limit: int | None) -> pd.DataFrame:
    products = fetch_products(client, page_size=page_size, limit=limit)
    rows = []
    for product in products:
        rows.append(
            {
                "ID": _first_value(product, ["ID", "ProductID", "ProductId"]),
                "SKU": _first_value(product, ["SKU", "Sku"]),
                "Parent SKU": _first_value(product, ["Parent SKU", "ParentSKU", "ParentSku", "ParentProductSKU"]),
                "Name": _first_value(product, ["Name", "ProductName", "Title"]),
                "Suppliers": "",
                "Supplier SKUs": "",
            }
        )
    return pd.DataFrame(rows, columns=["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"])


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