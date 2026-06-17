from __future__ import annotations

import argparse
import hashlib
import json
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Skip expensive enrichment when StoreFeeder/supplier structure is unchanged.")
    parser.add_argument("--state-file", type=Path, default=Path("data/enrichment_change_gate_state.json"))
    parser.add_argument("--target-file", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--warehouse-only-rules", type=Path, default=Path("data/warehouse_only_prime_sku_rules.csv"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--update-state", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)

    current = build_fingerprint(args)
    previous = _read_json(args.state_file)

    changed = previous.get("fingerprint") != current["fingerprint"]

    print("Enrichment change gate")
    print("changed:", "yes" if changed else "no")
    print("fingerprint:", current["fingerprint"])
    print("previous:", previous.get("fingerprint", ""))

    if args.update_state:
        args.state_file.parent.mkdir(parents=True, exist_ok=True)
        args.state_file.write_text(json.dumps(current, indent=2), encoding="utf-8")
        print("state_updated:", args.state_file)

    return 2 if changed else 0


def build_fingerprint(args: argparse.Namespace) -> dict[str, Any]:
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))

    products = fetch_products(client, page_size=args.page_size, limit=None)
    product_rows = []
    for p in products:
        product_rows.append({
            "id": str(p.get("ProductID", p.get("ID", ""))).strip(),
            "sku": str(p.get("SKU", "")).strip(),
            "name": str(p.get("Name", "")).strip(),
            "product_type": str(p.get("ProductType", "")).strip(),
            "parent": str(p.get("ParentSKU", p.get("Parent SKU", ""))).strip(),
        })

    payload = {
        "products": sorted(product_rows, key=lambda r: (r["id"], r["sku"])),
        "supplier_sku_universe": _supplier_sku_universe(args.ralawise_stock, "Ralawise") + _supplier_sku_universe(args.uneek_stock, "Uneek"),
        "target_rows": _target_rows(args.target_file),
        "warehouse_rules": _file_text(args.warehouse_only_rules),
    }

    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "fingerprint": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "counts": {
            "products": len(payload["products"]),
            "supplier_sku_universe": len(payload["supplier_sku_universe"]),
            "target_rows": len(payload["target_rows"]),
        },
    }


def _supplier_sku_universe(path: Path, supplier: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    df = read_csv(path)
    sku_col = _first_col(df, ["SKU", "sku", "SupplierSKU", "supplier_sku"])
    free_col = _first_col(df, ["free", "Free", "supplier_free_stock", "SupplierStockLevel"])
    if not sku_col:
        return []

    rows = []
    for _, row in df.iterrows():
        sku = str(row.get(sku_col, "")).strip().upper()
        if not sku:
            continue
        free = _qty(row.get(free_col, 0)) if free_col else 0
        rows.append({
            "supplier": supplier,
            "sku": sku,
            "stock_state": "positive" if free > 0 else "zero",
        })

    unique = {(r["supplier"], r["sku"], r["stock_state"]) for r in rows}
    return [
        {"supplier": supplier, "sku": sku, "stock_state": state}
        for supplier, sku, state in sorted(unique)
    ]


def _target_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    df = read_csv(path)
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "ProductID": str(row.get("ProductID", "")).strip(),
            "SKU": str(row.get("SKU", "")).strip(),
            "supplier": str(row.get("supplier", "")).strip(),
            "SupplierSKU": str(row.get("SupplierSKU", "")).strip(),
            "stock_strategy": str(row.get("stock_strategy", "")).strip(),
        })
    return sorted(rows, key=lambda r: (r["ProductID"], r["SKU"], r["SupplierSKU"]))


def _first_col(df: pd.DataFrame, names: list[str]) -> str:
    lookup = {c.casefold(): c for c in df.columns}
    for name in names:
        if name.casefold() in lookup:
            return lookup[name.casefold()]
    return ""


def _qty(value: Any) -> int:
    try:
        return max(0, int(float(str(value or "0").strip() or "0")))
    except Exception:
        return 0


def _file_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8-sig")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


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
