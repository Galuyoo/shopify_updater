from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.export_storefeeder_products import fetch_products
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv

MAP_COLUMNS = ["parent_sku", "child_sku", "supplier_name", "supplier_id", "supplier_sku", "stock_update_mode", "notes"]
COLOUR_COLUMNS = ["garment_code", "internal_colour_code", "supplier_colour_code", "notes"]
CANDIDATE_COLUMNS = [
    "parent_sku",
    "child_sku",
    "ProductID",
    "parsed_colour_code",
    "supplier_name",
    "supplier_id",
    "supplier_sku",
    "stock_update_mode",
    "candidate_status",
    "reason",
]
SUMMARY_COLUMNS = ["metric", "value"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build explicit supplier-info-only SKU map candidates. No writes unless --execute.")
    parser.add_argument("--parent-sku", required=True)
    parser.add_argument("--supplier", required=True)
    parser.add_argument("--supplier-id", required=True)
    parser.add_argument("--garment-code", required=True)
    parser.add_argument("--mode", default="supplier_info_only_manual_inventory", choices=["supplier_info_only_manual_inventory"])
    parser.add_argument("--colour-map", type=Path, default=Path("data/internal_colour_to_supplier_colour_map.csv"))
    parser.add_argument("--supplier-info-map", type=Path, default=Path("data/supplier_info_only_sku_map.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/supplier_info_only_mapping"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    products = fetch_products(client, page_size=args.page_size, limit=None)
    parent = _find_product(products, args.parent_sku)
    parent_id = _product_id(parent)
    detail = client.get_product(parent_id).get("response", {}) if parent_id else {}
    children = _children_from_detail(detail, args.parent_sku)

    colour_map = _load_colour_map(args.colour_map, args.garment_code)
    supplier_stock = _load_supplier_stock(args.supplier, args.ralawise_stock, args.uneek_stock)

    candidate_rows: list[dict[str, Any]] = []
    ready_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    for child in children:
        row = _candidate_row(child, args, colour_map, supplier_stock)
        candidate_rows.append(row)
        if row["candidate_status"] == "mapping_ready":
            ready_rows.append(_map_row(row))
        else:
            quarantine_rows.append(row)

    candidates = pd.DataFrame(candidate_rows, columns=CANDIDATE_COLUMNS)
    ready = pd.DataFrame(ready_rows, columns=MAP_COLUMNS)
    quarantine = pd.DataFrame(quarantine_rows, columns=CANDIDATE_COLUMNS)

    paths = {
        "mapping_candidates": out_dir / "mapping_candidates.csv",
        "mapping_ready_to_write": out_dir / "mapping_ready_to_write.csv",
        "mapping_quarantine": out_dir / "mapping_quarantine.csv",
        "mapping_summary": out_dir / "mapping_summary.csv",
    }
    candidates.to_csv(paths["mapping_candidates"], index=False)
    ready.to_csv(paths["mapping_ready_to_write"], index=False)
    quarantine.to_csv(paths["mapping_quarantine"], index=False)

    written = 0
    if args.execute and not ready.empty:
        written = _append_map_rows(args.supplier_info_map, ready)

    summary = pd.DataFrame(
        [
            {"metric": "execute", "value": "yes" if args.execute else "no"},
            {"metric": "parent_sku", "value": args.parent_sku},
            {"metric": "parent_found", "value": "yes" if parent_id else "no"},
            {"metric": "children_found", "value": len(children)},
            {"metric": "supplier_info_only_mapping_candidates", "value": len(candidates)},
            {"metric": "supplier_info_only_mapping_ready", "value": len(ready)},
            {"metric": "supplier_info_only_mapping_quarantine", "value": len(quarantine)},
            {"metric": "rows_written", "value": written},
            {"metric": "supplier_info_map", "value": str(args.supplier_info_map)},
        ],
        columns=SUMMARY_COLUMNS,
    )
    summary.to_csv(paths["mapping_summary"], index=False)

    print("Supplier-info-only SKU map builder")
    print(summary.to_string(index=False))
    print("\nReports:")
    for path in paths.values():
        print(path)
    if not args.execute:
        print("\nDry-run only. No mapping file was modified.")
    return 0


def _candidate_row(child: dict[str, str], args: argparse.Namespace, colour_map: dict[str, dict[str, str]], supplier_stock: pd.DataFrame) -> dict[str, Any]:
    child_sku = child["SKU"]
    colour = _parse_colour(args.parent_sku, child_sku)
    base = {
        "parent_sku": args.parent_sku,
        "child_sku": child_sku,
        "ProductID": child["ProductID"],
        "parsed_colour_code": colour,
        "supplier_name": args.supplier,
        "supplier_id": args.supplier_id,
        "supplier_sku": "",
        "stock_update_mode": args.mode,
        "candidate_status": "manual_review",
        "reason": "",
    }
    if not child_sku:
        base.update(candidate_status="quarantine", reason="missing_storefeeder_child_sku")
        return base
    if not colour:
        base.update(candidate_status="quarantine", reason="could_not_parse_internal_colour_code")
        return base
    colour_row = colour_map.get(colour.casefold())
    if not colour_row:
        base.update(candidate_status="quarantine", reason="unknown_internal_colour_code")
        return base
    supplier_sku = str(args.garment_code).strip().upper() + colour_row["supplier_colour_code"].upper()
    base["supplier_sku"] = supplier_sku
    matches = supplier_stock[supplier_stock["supplier_sku"].astype(str).str.strip().str.casefold().eq(supplier_sku.casefold())]
    if len(matches) == 0:
        base.update(candidate_status="quarantine", reason="supplier_sku_not_found_in_supplier_feed")
        return base
    if len(matches) > 1:
        base.update(candidate_status="quarantine", reason="duplicate_supplier_sku_in_supplier_feed")
        return base
    base.update(candidate_status="mapping_ready", reason="exact supplier SKU validated once in supplier feed")
    return base


def _map_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "parent_sku": str(row["parent_sku"]),
        "child_sku": str(row["child_sku"]),
        "supplier_name": str(row["supplier_name"]),
        "supplier_id": str(row["supplier_id"]),
        "supplier_sku": str(row["supplier_sku"]),
        "stock_update_mode": str(row["stock_update_mode"]),
        "notes": str(row["reason"]),
    }


def _parse_colour(parent_sku: str, child_sku: str) -> str:
    prefix = parent_sku.strip() + "-"
    if not child_sku.casefold().startswith(prefix.casefold()):
        return ""
    rest = child_sku[len(prefix):]
    parts = [part.strip() for part in rest.split("-") if part.strip()]
    if len(parts) != 2 or parts[-1].casefold() != "one":
        return ""
    return parts[0]


def _load_colour_map(path: Path, garment_code: str) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    df = read_csv(path)
    for col in COLOUR_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    garment_key = garment_code.strip().casefold()
    out = {}
    for _, row in df.iterrows():
        if str(row.get("garment_code", "")).strip().casefold() != garment_key:
            continue
        internal = str(row.get("internal_colour_code", "")).strip()
        supplier = str(row.get("supplier_colour_code", "")).strip()
        if internal and supplier:
            out[internal.casefold()] = {"supplier_colour_code": supplier, "notes": str(row.get("notes", "")).strip()}
    return out


def _load_supplier_stock(supplier: str, ralawise_path: Path, uneek_path: Path) -> pd.DataFrame:
    path = ralawise_path if supplier.strip().casefold() == "ralawise" else uneek_path
    df = read_csv(path)
    sku_col = _first_col(df, ["SKU", "supplier_sku", "SupplierSKU", "ItemNo", "ProductCode"])
    if not sku_col:
        return pd.DataFrame(columns=["supplier_sku"])
    return pd.DataFrame({"supplier_sku": df[sku_col].fillna("").astype(str).str.strip()})


def _children_from_detail(detail: dict[str, Any], parent_sku: str) -> list[dict[str, str]]:
    rows = []
    for child in detail.get("Children", []) or []:
        product = child.get("Product", {}) if isinstance(child, dict) else {}
        if not isinstance(product, dict):
            continue
        sku = _product_sku(product)
        product_id = _product_id(product)
        if sku:
            rows.append({"ProductID": product_id, "SKU": sku, "Parent SKU": parent_sku})
    return rows


def _append_map_rows(path: Path, ready: pd.DataFrame) -> int:
    existing = read_csv(path) if path.exists() else pd.DataFrame(columns=MAP_COLUMNS)
    for col in MAP_COLUMNS:
        if col not in existing.columns:
            existing[col] = ""
        if col not in ready.columns:
            ready[col] = ""
    keys = set(zip(existing["parent_sku"].astype(str).str.casefold(), existing["child_sku"].astype(str).str.casefold()))
    append = ready[~ready.apply(lambda row: (str(row["parent_sku"]).casefold(), str(row["child_sku"]).casefold()) in keys, axis=1)].copy()
    if append.empty:
        return 0
    combined = pd.concat([existing[MAP_COLUMNS], append[MAP_COLUMNS]], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(path, index=False)
    return len(append)


def _find_product(products: list[dict[str, Any]], sku: str) -> dict[str, Any]:
    key = sku.strip().casefold()
    for product in products:
        if _product_sku(product).casefold() == key:
            return product
    return {}


def _product_id(product: dict[str, Any]) -> str:
    return str(product.get("ProductID", product.get("ID", "")) or "").strip()


def _product_sku(product: dict[str, Any]) -> str:
    return str(product.get("SKU", product.get("Sku", "")) or "").strip()


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
