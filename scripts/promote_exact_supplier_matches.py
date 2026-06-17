from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.export_storefeeder_products import build_snapshot_rows, fetch_products
from src.stock_mapping import build_supplier_stock_lookup
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv


TARGET_COLUMNS = [
    "ProductID",
    "SKU",
    "supplier",
    "SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "stock_location",
    "preserve_existing_locations",
    "warehouse_safe_mode",
    "skip_stock_location_update",
    "allow_stock_location_update",
    "stock_strategy",
    "sellable_stock_location",
]

CANDIDATE_COLUMNS = [
    "ProductID",
    "SKU",
    "Parent SKU",
    "Name",
    "supplier",
    "SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "supplier_free_stock",
    "supplier_match_status",
    "product_supplier_status",
    "stock_location_status",
    "target_action",
    "reason",
]

SUMMARY_COLUMNS = ["metric", "value"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generic exact-SKU StoreFeeder supplier promoter. No product-specific rules."
    )
    parser.add_argument("--target-file", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--warehouse-only-rules", type=Path, default=Path("data/warehouse_only_prime_sku_rules.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/exact_supplier_promoter"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--create-missing-product-suppliers", action="store_true")
    parser.add_argument("--supplier-costs", type=int, default=0)
    args = parser.parse_args()

    if args.create_missing_product_suppliers and not args.execute:
        parser.error("--create-missing-product-suppliers requires --execute")

    return args


def main() -> int:
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    _load_env_file(args.env_file)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))

    products = _load_products(client, args.page_size, args.limit)
    targets = _read_targets(args.target_file)
    supplier_ids = _load_supplier_ids(args.supplier_id_map)
    supplier_stock = build_supplier_stock_lookup(
        read_csv(args.ralawise_stock),
        read_csv(args.uneek_stock),
    )
    warehouse_only_rules = _load_warehouse_only_rules(args.warehouse_only_rules)

    result = promote_exact_supplier_matches(
        client=client,
        products=products,
        targets=targets,
        supplier_ids=supplier_ids,
        supplier_stock=supplier_stock,
        warehouse_only_rules=warehouse_only_rules,
        execute=args.execute,
        create_missing_product_suppliers=args.create_missing_product_suppliers,
        supplier_costs=args.supplier_costs,
    )

    candidates = result["candidates"]
    appended = result["appended"]
    quarantine = result["quarantine"]
    setup_success = result["setup_success"]
    setup_failures = result["setup_failures"]
    summary = result["summary"]

    candidates_path = out_dir / "exact_supplier_candidates.csv"
    appended_path = out_dir / "exact_supplier_targets_appended.csv"
    quarantine_path = out_dir / "exact_supplier_quarantine.csv"
    setup_success_path = out_dir / "product_supplier_setup_success.csv"
    setup_failures_path = out_dir / "product_supplier_setup_failures.csv"
    summary_path = out_dir / "exact_supplier_promotion_summary.csv"

    candidates.to_csv(candidates_path, index=False)
    appended.to_csv(appended_path, index=False)
    quarantine.to_csv(quarantine_path, index=False)
    setup_success.to_csv(setup_success_path, index=False)
    setup_failures.to_csv(setup_failures_path, index=False)
    summary.to_csv(summary_path, index=False)

    if args.execute and not appended.empty:
        appended_count = _append_targets(args.target_file, appended[TARGET_COLUMNS].copy(), out_dir, run_id)
        summary = _replace_summary_metric(summary, "target_rows_appended_to_file", appended_count)
        summary.to_csv(summary_path, index=False)

    print("Generic exact-SKU supplier promotion")
    print(summary.to_string(index=False))
    print()
    print("Reports:")
    print(summary_path)
    print(candidates_path)
    print(appended_path)
    print(quarantine_path)
    print(setup_success_path)
    print(setup_failures_path)

    setup_failure_count = len(setup_failures)
    if setup_failure_count:
        raise SystemExit(f"Stopped because product supplier setup failures were found: {setup_failure_count}")

    return 0


def promote_exact_supplier_matches(
    *,
    client: StoreFeederApiClient,
    products: pd.DataFrame,
    targets: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    warehouse_only_rules: list[dict[str, str]],
    execute: bool,
    create_missing_product_suppliers: bool,
    supplier_costs: int,
) -> dict[str, pd.DataFrame]:
    required_product_columns = ["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"]
    _require_columns(products, required_product_columns, "StoreFeeder products")

    products = products.copy()
    for column in required_product_columns:
        products[column] = products[column].fillna("").astype(str).str.strip()

    targets = targets.copy()
    for column in TARGET_COLUMNS:
        if column not in targets.columns:
            targets[column] = ""

    existing_product_ids = set(targets["ProductID"].fillna("").astype(str).str.strip())
    existing_skus = {
        value.casefold()
        for value in targets["SKU"].fillna("").astype(str).str.strip()
        if value
    }
    parent_skus = {
        value.casefold()
        for value in products["Parent SKU"].fillna("").astype(str).str.strip()
        if value
    }

    candidate_rows: list[dict[str, Any]] = []
    append_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    setup_success_rows: list[dict[str, Any]] = []
    setup_failure_rows: list[dict[str, Any]] = []

    scanned_variants = 0
    skipped_existing = 0
    skipped_parent = 0
    warehouse_only_protected = 0

    for _, product in products.iterrows():
        product_id = str(product["ID"]).strip()
        sku = str(product["SKU"]).strip()
        parent_sku = str(product["Parent SKU"]).strip()
        name = str(product["Name"]).strip()

        if not sku or not product_id:
            continue

        is_parent = sku.casefold() in parent_skus
        if is_parent:
            skipped_parent += 1
            continue

        scanned_variants += 1

        if product_id in existing_product_ids or sku.casefold() in existing_skus:
            skipped_existing += 1
            continue

        warehouse_rule = _match_warehouse_only_rule(sku, parent_sku, name, warehouse_only_rules)
        if warehouse_rule:
            warehouse_only_protected += 1
            quarantine_rows.append(_candidate_row(
                product=product,
                candidate=None,
                supplier_match_status="protected_warehouse_only",
                product_supplier_status="not_applicable",
                stock_location_status="not_applicable",
                target_action="quarantine",
                reason="warehouse_only_rule_matched:" + warehouse_rule,
            ))
            continue

        exact_matches = _exact_supplier_matches(sku, supplier_ids, supplier_stock)

        if len(exact_matches) == 0:
            quarantine_rows.append(_candidate_row(
                product=product,
                candidate=None,
                supplier_match_status="no_exact_supplier_sku_match",
                product_supplier_status="not_applicable",
                stock_location_status="not_applicable",
                target_action="quarantine",
                reason="StoreFeeder SKU was not found exactly once in supplier feeds",
            ))
            continue

        if len(exact_matches) > 1:
            quarantine_rows.append(_candidate_row(
                product=product,
                candidate=None,
                supplier_match_status="ambiguous_exact_supplier_sku_match",
                product_supplier_status="not_applicable",
                stock_location_status="not_applicable",
                target_action="quarantine",
                reason="StoreFeeder SKU matched multiple supplier feeds or duplicate supplier rows",
            ))
            continue

        candidate = exact_matches[0]

        already_attached = _product_export_has_supplier(product, candidate["supplier"], candidate["supplier_sku"])
        supplier_setup_status = "already_attached" if already_attached else "missing_product_supplier"

        setup_ok = already_attached
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
            supplier_setup_status = "created_and_verified" if setup_ok else "setup_failed"
        elif not already_attached and not create_missing_product_suppliers:
            supplier_setup_status = "missing_product_supplier_preview_only"

        supplier_qty = _qty_to_int(candidate["supplier_free_stock"])
        stock_location_status = "warehouse_stock_update_allowed"

        append_allowed = True
        if supplier_qty == 0:
            detail = client.get_product(product_id).get("response", {})
            if not _has_stock_location(detail, "Warehouse Stock"):
                stock_location_status = "zero_stock_no_warehouse_stock_row"
                append_allowed = False

        if setup_ok and append_allowed:
            target_action = "append_to_normal_supplier_target"
            reason = "exact StoreFeeder SKU to supplier SKU match; ProductSupplier ready"
            append_rows.append(_target_row(product, candidate))
        elif setup_ok and not append_allowed:
            target_action = "supplier_setup_only_no_target_append"
            reason = "supplier is set up, but zero stock has no Warehouse Stock row; target append delayed until safe"
        else:
            target_action = "quarantine"
            reason = "ProductSupplier relationship missing or failed"

        row = _candidate_row(
            product=product,
            candidate=candidate,
            supplier_match_status="exact_unique_supplier_sku_match",
            product_supplier_status=supplier_setup_status,
            stock_location_status=stock_location_status,
            target_action=target_action,
            reason=reason,
        )
        candidate_rows.append(row)

        if target_action == "quarantine":
            quarantine_rows.append(row)

    candidates = pd.DataFrame(candidate_rows, columns=CANDIDATE_COLUMNS)
    appended = pd.DataFrame(append_rows, columns=TARGET_COLUMNS)
    quarantine = pd.DataFrame(quarantine_rows, columns=CANDIDATE_COLUMNS)
    setup_success = pd.DataFrame(setup_success_rows)
    setup_failures = pd.DataFrame(setup_failure_rows)

    if setup_success.empty:
        setup_success = pd.DataFrame(columns=["ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU", "status", "status_code"])
    if setup_failures.empty:
        setup_failures = pd.DataFrame(columns=["ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU", "status", "status_code", "response"])

    summary = pd.DataFrame(
        [
            {"metric": "execute_mode", "value": "yes" if execute else "no"},
            {"metric": "create_missing_product_suppliers", "value": "yes" if create_missing_product_suppliers else "no"},
            {"metric": "scanned_products", "value": len(products)},
            {"metric": "scanned_variants", "value": scanned_variants},
            {"metric": "existing_target_rows", "value": len(targets)},
            {"metric": "skipped_existing_target_rows", "value": skipped_existing},
            {"metric": "skipped_parent_products", "value": skipped_parent},
            {"metric": "warehouse_only_protected_rows", "value": warehouse_only_protected},
            {"metric": "exact_supplier_candidate_rows", "value": len(candidates)},
            {"metric": "target_rows_ready_to_append", "value": len(appended)},
            {"metric": "quarantine_rows", "value": len(quarantine)},
            {"metric": "product_supplier_setup_success_rows", "value": len(setup_success)},
            {"metric": "product_supplier_setup_failure_rows", "value": len(setup_failures)},
            {"metric": "target_rows_appended_to_file", "value": 0},
        ],
        columns=SUMMARY_COLUMNS,
    )

    return {
        "candidates": candidates,
        "appended": appended,
        "quarantine": quarantine,
        "setup_success": setup_success,
        "setup_failures": setup_failures,
        "summary": summary,
    }


def _load_products(client: StoreFeederApiClient, page_size: int, limit: int | None) -> pd.DataFrame:
    products = fetch_products(client, page_size=page_size, limit=limit)
    return pd.DataFrame(build_snapshot_rows(client, products))


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


def _load_warehouse_only_rules(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            match_type = str(row.get("match_type", "")).strip().casefold()
            value = str(row.get("value", "")).strip()
            reason = str(row.get("reason", "")).strip()
            if match_type in {"exact", "prefix"} and value:
                rows.append({"match_type": match_type, "value": value, "reason": reason})
    return rows


def _match_warehouse_only_rule(sku: str, parent_sku: str, name: str, rules: list[dict[str, str]]) -> str:
    values = [sku.strip(), parent_sku.strip()]
    for rule in rules:
        rule_value = rule["value"].strip()
        rule_cf = rule_value.casefold()
        for value in values:
            value_cf = value.casefold()
            if rule["match_type"] == "exact" and value_cf == rule_cf:
                return rule_value
            if rule["match_type"] == "prefix" and value_cf.startswith(rule_cf):
                return rule_value
    return ""


def _exact_supplier_matches(sku: str, supplier_ids: pd.DataFrame, supplier_stock: pd.DataFrame) -> list[dict[str, Any]]:
    sku_key = sku.strip().casefold()
    stock = supplier_stock.copy()
    for column in ["supplier", "supplier_sku", "supplier_free_stock"]:
        if column not in stock.columns:
            return []

    matches = stock[stock["supplier_sku"].fillna("").astype(str).str.strip().str.casefold().eq(sku_key)].copy()
    if matches.empty:
        return []

    matches["_dedupe_key"] = list(
        zip(
            matches["supplier"].fillna("").astype(str).str.casefold(),
            matches["supplier_sku"].fillna("").astype(str).str.casefold(),
        )
    )
    matches = matches.drop_duplicates(subset=["_dedupe_key"], keep="first")

    out: list[dict[str, Any]] = []
    for _, stock_row in matches.iterrows():
        supplier = str(stock_row.get("supplier", "")).strip()
        supplier_sku = str(stock_row.get("supplier_sku", "")).strip()
        supplier_row = supplier_ids[supplier_ids["_supplier_key"].eq(supplier.casefold())]
        if len(supplier_row) != 1:
            continue
        supplier_id = supplier_row.iloc[0]
        out.append(
            {
                "supplier": supplier,
                "supplier_sku": supplier_sku,
                "SupplierID": str(supplier_id["SupplierID"]).strip(),
                "Supplier.Name": str(supplier_id["Supplier.Name"]).strip(),
                "supplier_free_stock": str(stock_row.get("supplier_free_stock", "")).strip(),
            }
        )
    return out


def _product_export_has_supplier(product: pd.Series, supplier: str, supplier_sku: str) -> bool:
    suppliers = _pipe_values(product.get("Suppliers", ""))
    supplier_skus = _pipe_values(product.get("Supplier SKUs", ""))
    supplier_key = supplier.strip().casefold()
    supplier_sku_key = supplier_sku.strip().casefold()

    for index, existing_supplier in enumerate(suppliers):
        existing_supplier_sku = supplier_skus[index] if index < len(supplier_skus) else ""
        if existing_supplier.strip().casefold() == supplier_key and existing_supplier_sku.strip().casefold() == supplier_sku_key:
            return True
    return False


def _create_and_verify_product_supplier(
    *,
    client: StoreFeederApiClient,
    product_id: str,
    product: pd.Series,
    candidate: dict[str, Any],
    supplier_costs: int,
    success_rows: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
) -> bool:
    sku = str(product.get("SKU", "")).strip()
    item = {
        "Supplier": {
            "SupplierID": int(candidate["SupplierID"]),
            "Name": str(candidate["Supplier.Name"]).strip(),
        },
        "SupplierSKU": str(candidate["supplier_sku"]).strip(),
        "SupplierStockLevel": _qty_to_int(candidate["supplier_free_stock"]),
        "SupplierCosts": int(supplier_costs),
    }

    try:
        response = client.create_product_supplier(product_id, item)
        status_code = int(response.get("_status_code", 0))
        readback = client.get_product_suppliers(product_id)
        verified = _readback_contains_supplier(readback, candidate)
        ok = status_code < 400 and verified
        row = {
            "ProductID": product_id,
            "SKU": sku,
            "supplier": candidate["supplier"],
            "SupplierID": candidate["SupplierID"],
            "SupplierSKU": candidate["supplier_sku"],
            "status": "created_and_verified" if ok else "create_or_readback_failed",
            "status_code": status_code,
            "response": json.dumps({"setup": response, "readback": readback})[:2000],
        }
        if ok:
            success_rows.append(row)
            return True
        failure_rows.append(row)
        return False
    except Exception as exc:
        failure_rows.append(
            {
                "ProductID": product_id,
                "SKU": sku,
                "supplier": candidate["supplier"],
                "SupplierID": candidate["SupplierID"],
                "SupplierSKU": candidate["supplier_sku"],
                "status": "exception",
                "status_code": "",
                "response": str(exc)[:2000],
            }
        )
        return False


def _readback_contains_supplier(readback: dict[str, Any], candidate: dict[str, Any]) -> bool:
    if int(readback.get("_status_code", 0)) >= 400:
        return False

    payload = readback.get("response")
    if isinstance(payload, dict):
        records = payload.get("value", payload.get("Suppliers", []))
    elif isinstance(payload, list):
        records = payload
    else:
        records = []

    if isinstance(records, dict):
        records = [records]

    expected_supplier_id = str(candidate["SupplierID"]).strip()
    expected_supplier_sku = str(candidate["supplier_sku"]).strip().casefold()

    for record in records:
        supplier = record.get("Supplier") if isinstance(record, dict) else {}
        supplier_id = str((supplier or {}).get("SupplierID", "")).strip()
        supplier_sku = str(record.get("SupplierSKU", "")).strip().casefold()
        if supplier_id == expected_supplier_id and supplier_sku == expected_supplier_sku:
            return True

    return False


def _has_stock_location(detail: dict[str, Any], stock_location: str) -> bool:
    for loc in (detail.get("WarehouseInformation", {}) or {}).get("StockLocations", []) or []:
        ref = ((loc.get("StockLocation") or {}).get("StockLocationReference") or "").strip()
        if ref.casefold() == stock_location.casefold():
            return True
    return False


def _target_row(product: pd.Series, candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "ProductID": str(product["ID"]).strip(),
        "SKU": str(product["SKU"]).strip(),
        "supplier": candidate["supplier"],
        "SupplierID": candidate["SupplierID"],
        "Supplier.Name": candidate["Supplier.Name"],
        "SupplierSKU": candidate["supplier_sku"],
        "stock_location": "Warehouse Stock",
        "preserve_existing_locations": "yes",
        "warehouse_safe_mode": "yes",
        "skip_stock_location_update": "no",
        "allow_stock_location_update": "yes",
        "stock_strategy": "supplier_synced_inventory",
        "sellable_stock_location": "Warehouse Stock",
    }


def _candidate_row(
    *,
    product: pd.Series,
    candidate: dict[str, Any] | None,
    supplier_match_status: str,
    product_supplier_status: str,
    stock_location_status: str,
    target_action: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "ProductID": str(product.get("ID", "")).strip(),
        "SKU": str(product.get("SKU", "")).strip(),
        "Parent SKU": str(product.get("Parent SKU", "")).strip(),
        "Name": str(product.get("Name", "")).strip(),
        "supplier": "" if candidate is None else candidate.get("supplier", ""),
        "SupplierID": "" if candidate is None else candidate.get("SupplierID", ""),
        "Supplier.Name": "" if candidate is None else candidate.get("Supplier.Name", ""),
        "SupplierSKU": "" if candidate is None else candidate.get("supplier_sku", ""),
        "supplier_free_stock": "" if candidate is None else candidate.get("supplier_free_stock", ""),
        "supplier_match_status": supplier_match_status,
        "product_supplier_status": product_supplier_status,
        "stock_location_status": stock_location_status,
        "target_action": target_action,
        "reason": reason,
    }


def _append_targets(target_path: Path, append: pd.DataFrame, out_dir: Path, run_id: str) -> int:
    if append.empty:
        return 0

    current = _read_targets(target_path)
    for column in TARGET_COLUMNS:
        if column not in current.columns:
            current[column] = ""
        if column not in append.columns:
            append[column] = ""

    current_keys = set(
        zip(
            current["ProductID"].fillna("").astype(str).str.strip(),
            current["SupplierID"].fillna("").astype(str).str.strip(),
            current["SupplierSKU"].fillna("").astype(str).str.strip().str.casefold(),
        )
    )

    append = append.copy()
    append["_key"] = list(
        zip(
            append["ProductID"].fillna("").astype(str).str.strip(),
            append["SupplierID"].fillna("").astype(str).str.strip(),
            append["SupplierSKU"].fillna("").astype(str).str.strip().str.casefold(),
        )
    )
    append = append[~append["_key"].isin(current_keys)].drop(columns=["_key"]).copy()
    append = append.drop_duplicates(subset=["ProductID", "SupplierID", "SupplierSKU"], keep="first")

    if append.empty:
        return 0

    target_columns = list(current.columns)
    for column in append.columns:
        if column not in target_columns:
            target_columns.append(column)
            current[column] = ""

    for column in target_columns:
        if column not in append.columns:
            append[column] = ""

    backup = target_path.with_name(target_path.stem + f".backup_before_exact_supplier_promoter_{run_id}" + target_path.suffix)
    shutil.copy2(target_path, backup)

    combined = pd.concat(
        [current.reindex(columns=target_columns, fill_value=""), append.reindex(columns=target_columns, fill_value="")],
        ignore_index=True,
    )
    combined.to_csv(target_path, index=False)
    append.reindex(columns=target_columns, fill_value="").to_csv(out_dir / "target_rows_appended_to_file.csv", index=False)
    return len(append)


def _qty_to_int(value: Any) -> int:
    text = str(value or "0").strip()
    if text == "":
        return 0
    return max(0, int(float(text)))


def _pipe_values(value: Any) -> list[str]:
    return [part.strip() for part in str(value).split("|") if part.strip()]


def _require_columns(df: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: " + ", ".join(missing))


def _replace_summary_metric(summary: pd.DataFrame, metric: str, value: Any) -> pd.DataFrame:
    summary = summary.copy()
    if metric in set(summary["metric"]):
        summary.loc[summary["metric"].eq(metric), "value"] = value
    else:
        summary = pd.concat([summary, pd.DataFrame([{"metric": metric, "value": value}])], ignore_index=True)
    return summary


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
