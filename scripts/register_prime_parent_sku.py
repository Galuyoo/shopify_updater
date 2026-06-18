from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.export_storefeeder_products import fetch_products
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv

REGISTRY_COLUMNS = ["parent_sku", "reason", "registered_at", "ProductID", "Name", "stock_update_mode"]
RULE_COLUMNS = ["match_type", "value", "reason", "stock_update_mode"]
VALID_MODES = {"full_protect", "supplier_info_only_manual_inventory"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register an Amazon Prime parent SKU as warehouse-only protected inventory.")
    parser.add_argument("--parent-sku", required=True)
    parser.add_argument("--reason", default="amazon_prime_warehouse_only")
    parser.add_argument("--mode", choices=sorted(VALID_MODES), default="full_protect")
    parser.add_argument("--registry-file", type=Path, default=Path("data/amazon_prime_parent_skus.csv"))
    parser.add_argument("--warehouse-only-rules", type=Path, default=Path("data/warehouse_only_prime_sku_rules.csv"))
    parser.add_argument("--target-file", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/prime_parent_registry"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--page-size", type=int, default=100)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parent_sku = args.parent_sku.strip()
    if not parent_sku:
        raise SystemExit("--parent-sku cannot be blank")

    _load_env_file(args.env_file)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))

    # Lightweight list only. Do not call build_snapshot_rows here.
    products = fetch_products(client, page_size=args.page_size, limit=None)

    parent = _find_product_by_sku(products, parent_sku)
    parent_id = _product_id(parent)
    parent_name = _product_name(parent)

    detail: dict[str, Any] = {}
    if parent_id:
        detail = client.get_product(parent_id).get("response", {}) or {}

    family = _build_family(parent_sku, parent_id, parent_name, products, detail)

    _upsert_registry(args.registry_file, parent_sku, args.reason, parent_id, parent_name, args.mode)
    _ensure_prefix_rule(args.warehouse_only_rules, parent_sku, args.reason, args.mode)

    supplier_conflicts = _supplier_feed_conflicts(family, args.ralawise_stock, args.uneek_stock)
    target_conflicts = _target_conflicts(family, args.target_file)

    family_path = out_dir / "protected_prime_family.csv"
    supplier_conflicts_path = out_dir / "supplier_feed_conflicts.csv"
    target_conflicts_path = out_dir / "target_conflicts.csv"
    summary_path = out_dir / "prime_parent_registration_summary.csv"

    pd.DataFrame(family).to_csv(family_path, index=False)
    pd.DataFrame(supplier_conflicts).to_csv(supplier_conflicts_path, index=False)
    pd.DataFrame(target_conflicts).to_csv(target_conflicts_path, index=False)

    summary = pd.DataFrame([
        {"metric": "parent_sku", "value": parent_sku},
        {"metric": "reason", "value": args.reason},
        {"metric": "stock_update_mode", "value": args.mode},
        {"metric": "parent_found_in_storefeeder", "value": "yes" if parent_id else "no"},
        {"metric": "ProductID", "value": parent_id},
        {"metric": "protected_family_rows", "value": len(family)},
        {"metric": "supplier_feed_conflict_rows", "value": len(supplier_conflicts)},
        {"metric": "target_conflict_rows", "value": len(target_conflicts)},
        {"metric": "rule_written", "value": "prefix"},
        {"metric": "registry_file", "value": str(args.registry_file)},
        {"metric": "warehouse_only_rules", "value": str(args.warehouse_only_rules)},
    ])
    summary.to_csv(summary_path, index=False)

    print("Prime parent SKU registration")
    print(summary.to_string(index=False))
    print()
    print("Reports:")
    print(summary_path)
    print(family_path)
    print(supplier_conflicts_path)
    print(target_conflicts_path)

    if target_conflicts:
        print()
        print("WARNING: protected family has rows in supplier target. Review target_conflicts.csv before next live sync.")

    if supplier_conflicts:
        print()
        print()
        print("WARNING: protected family has exact supplier-feed SKU matches. It is still protected by prefix rule.")

    return 0


def _find_product_by_sku(products: list[dict[str, Any]], sku: str) -> dict[str, Any]:
    sku_key = sku.casefold()
    for product in products:
        if _product_sku(product).casefold() == sku_key:
            return product
    return {}


def _build_family(parent_sku: str, parent_id: str, parent_name: str, products: list[dict[str, Any]], detail: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(product_id: str, sku: str, parent: str, name: str, role: str) -> None:
        sku = str(sku or "").strip()
        if not sku or sku.casefold() in seen:
            return
        seen.add(sku.casefold())
        rows.append({
            "ProductID": str(product_id or "").strip(),
            "SKU": sku,
            "Parent SKU": str(parent or "").strip(),
            "Name": str(name or "").strip(),
            "family_role": role,
            "protected_by": "prime_parent_prefix",
        })

    add(parent_id, parent_sku, "", parent_name, "parent")

    for child in detail.get("Children", []) or []:
        add(
            _product_id(child),
            _product_sku(child),
            parent_sku,
            _product_name(child),
            "child",
        )

    # Fallback from lightweight list: protect obvious family SKUs by prefix.
    prefix = parent_sku.casefold() + "-"
    for product in products:
        sku = _product_sku(product)
        if sku.casefold().startswith(prefix):
            add(
                _product_id(product),
                sku,
                parent_sku,
                _product_name(product),
                "child_prefix_match",
            )

    return rows


def _upsert_registry(path: Path, parent_sku: str, reason: str, product_id: str, name: str, mode: str) -> None:
    rows = _read_csv_rows(path, REGISTRY_COLUMNS)
    now = datetime.now(timezone.utc).isoformat()

    found = False
    for row in rows:
        if str(row.get("parent_sku", "")).strip().casefold() == parent_sku.casefold():
            row["reason"] = reason
            row["registered_at"] = now
            row["ProductID"] = product_id
            row["Name"] = name
            row["stock_update_mode"] = mode
            found = True

    if not found:
        rows.append({
            "parent_sku": parent_sku,
            "reason": reason,
            "registered_at": now,
            "ProductID": product_id,
            "Name": name,
            "stock_update_mode": mode,
        })

    _write_csv_rows(path, REGISTRY_COLUMNS, rows)


def _ensure_prefix_rule(path: Path, parent_sku: str, reason: str, mode: str) -> None:
    rows = _read_csv_rows(path, RULE_COLUMNS)

    # Remove exact duplicate rows for same value, then guarantee one prefix rule.
    clean_rows: list[dict[str, str]] = []
    prefix_exists = False

    for row in rows:
        match_type = str(row.get("match_type", "")).strip().casefold()
        value = str(row.get("value", "")).strip()
        if value.casefold() == parent_sku.casefold() and match_type == "prefix":
            if not prefix_exists:
                clean_rows.append({"match_type": "prefix", "value": parent_sku, "reason": reason, "stock_update_mode": mode})
                prefix_exists = True
            continue
        clean_rows.append(row)

    if not prefix_exists:
        clean_rows.append({"match_type": "prefix", "value": parent_sku, "reason": reason, "stock_update_mode": mode})

    _write_csv_rows(path, RULE_COLUMNS, clean_rows)


def _supplier_feed_conflicts(family: list[dict[str, str]], ralawise_path: Path, uneek_path: Path) -> list[dict[str, str]]:
    family_skus = {row["SKU"].strip().casefold(): row for row in family if row.get("SKU", "").strip()}
    conflicts: list[dict[str, str]] = []

    for supplier, path in [("Ralawise", ralawise_path), ("Uneek", uneek_path)]:
        if not path.exists():
            continue

        df = read_csv(path)
        sku_col = _first_col(df, ["SKU", "sku", "SupplierSKU", "supplier_sku"])
        free_col = _first_col(df, ["free", "Free", "supplier_free_stock", "SupplierStockLevel"])

        if not sku_col:
            continue

        for _, feed_row in df.iterrows():
            sku = str(feed_row.get(sku_col, "")).strip()
            if not sku:
                continue

            family_row = family_skus.get(sku.casefold())
            if not family_row:
                continue

            conflicts.append({
                "ProductID": family_row.get("ProductID", ""),
                "SKU": family_row.get("SKU", ""),
                "family_role": family_row.get("family_role", ""),
                "supplier": supplier,
                "supplier_sku": sku,
                "free": str(feed_row.get(free_col, "")).strip() if free_col else "",
                "warning": "protected_prime_family_sku_exists_in_supplier_feed",
            })

    return conflicts


def _target_conflicts(family: list[dict[str, str]], target_path: Path) -> list[dict[str, str]]:
    if not target_path.exists():
        return []

    family_skus = {row["SKU"].strip().casefold(): row for row in family if row.get("SKU", "").strip()}
    family_ids = {row["ProductID"].strip(): row for row in family if row.get("ProductID", "").strip()}

    df = read_csv(target_path)
    conflicts: list[dict[str, str]] = []

    for _, target_row in df.iterrows():
        sku = str(target_row.get("SKU", "")).strip()
        product_id = str(target_row.get("ProductID", "")).strip()
        family_row = family_skus.get(sku.casefold()) or family_ids.get(product_id)

        if not family_row:
            continue

        conflicts.append({
            "ProductID": product_id,
            "SKU": sku,
            "supplier": str(target_row.get("supplier", "")).strip(),
            "SupplierSKU": str(target_row.get("SupplierSKU", "")).strip(),
            "stock_strategy": str(target_row.get("stock_strategy", "")).strip(),
            "warning": "protected_prime_family_row_exists_in_supplier_target",
        })

    return conflicts


def _product_id(product: dict[str, Any]) -> str:
    return str(product.get("ProductID", product.get("ID", "")) or "").strip()


def _product_sku(product: dict[str, Any]) -> str:
    return str(product.get("SKU", product.get("Sku", "")) or "").strip()


def _product_name(product: dict[str, Any]) -> str:
    return str(product.get("Name", product.get("Title", "")) or "").strip()


def _read_csv_rows(path: Path, columns: list[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []

    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = {col: str(row.get(col, "")).strip() for col in columns}
            if "stock_update_mode" in columns and not normalized.get("stock_update_mode"):
                normalized["stock_update_mode"] = "full_protect"
            rows.append(normalized)
    return rows


def _write_csv_rows(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in columns})


def _first_col(df: pd.DataFrame, names: list[str]) -> str:
    lookup = {c.casefold(): c for c in df.columns}
    for name in names:
        if name.casefold() in lookup:
            return lookup[name.casefold()]
    return ""


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
