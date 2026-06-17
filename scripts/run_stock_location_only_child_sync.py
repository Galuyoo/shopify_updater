import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load .env
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from scripts.run_supplier_stock_fast_update import (
    STOCK_LOCATION_PAYLOAD_COLUMNS,
    _stock_location_payload_row,
    payload_preview_to_storefeeder_items,
    batch_items,
    _send_fast_stock_location_batches,
    _csv_count,
)

import pandas as pd


def read_csv_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def qty_to_int(value) -> int:
    text = str(value or "0").strip()
    if text == "":
        return 0
    return max(0, int(float(text)))


def get_warehouse_stock_from_detail(detail: dict, stock_location: str):
    for loc in (detail.get("WarehouseInformation", {}) or {}).get("StockLocations", []) or []:
        ref = ((loc.get("StockLocation") or {}).get("StockLocationReference") or "").strip()
        if ref == stock_location:
            return qty_to_int(loc.get("PhysicalStock"))
    return None


def get_inventory_from_detail(detail: dict) -> int:
    return qty_to_int((detail.get("InventoryInformation", {}) or {}).get("Inventory"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules", default="data/stock_location_only_parent_rules.csv")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--api-batch-size", type=int, default=50)
    args = parser.parse_args()

    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig())

    rules = read_csv_rows(ROOT / args.rules)

    preview_rows = []
    skipped_rows = []
    candidate_rows = []

    for rule in rules:
        parent_product_id = str(rule.get("parent_product_id", "")).strip()
        parent_sku = str(rule.get("parent_sku", "")).strip()
        supplier = str(rule.get("supplier", "")).strip()
        stock_file = ROOT / str(rule.get("stock_file", "")).strip()
        stock_sku_column = str(rule.get("stock_sku_column", "")).strip()
        stock_qty_column = str(rule.get("stock_qty_column", "")).strip()
        stock_location = str(rule.get("stock_location", "")).strip() or "Warehouse Stock"

        if not parent_product_id:
            raise RuntimeError("Missing parent_product_id in rule")
        if not stock_file.exists():
            raise RuntimeError(f"Missing stock file: {stock_file}")

        stock_rows = read_csv_rows(stock_file)
        stock_index = {}
        for row in stock_rows:
            sku = str(row.get(stock_sku_column, "")).strip()
            if sku:
                stock_index[sku] = row

        parent_detail = client.get_product(parent_product_id).get("response", {})
        children = parent_detail.get("Children", []) or []

        for child in children:
            product = child.get("Product") or {}
            product_id = str(product.get("ProductID") or "").strip()
            sku = str(product.get("SKU") or "").strip()

            if not product_id or not sku:
                continue

            if sku not in stock_index:
                skipped_rows.append({
                    "parent_product_id": parent_product_id,
                    "parent_sku": parent_sku,
                    "ProductID": product_id,
                    "SKU": sku,
                    "reason": "child_sku_not_found_in_supplier_stock_file",
                })
                continue

            supplier_qty = qty_to_int(stock_index[sku].get(stock_qty_column))

            # For zero quantity, avoid creating noisy failures when StoreFeeder has no Warehouse Stock row
            # and inventory is already effectively zero.
            should_send = True
            live_inventory = ""
            live_warehouse_stock = ""

            if supplier_qty == 0:
                child_detail = client.get_product(product_id).get("response", {})
                live_inventory_int = get_inventory_from_detail(child_detail)
                live_warehouse_stock_value = get_warehouse_stock_from_detail(child_detail, stock_location)

                live_inventory = str(live_inventory_int)
                live_warehouse_stock = "" if live_warehouse_stock_value is None else str(live_warehouse_stock_value)

                if live_inventory_int == 0 and live_warehouse_stock_value is None:
                    should_send = False
                    skipped_rows.append({
                        "parent_product_id": parent_product_id,
                        "parent_sku": parent_sku,
                        "ProductID": product_id,
                        "SKU": sku,
                        "supplier_qty": supplier_qty,
                        "live_inventory": live_inventory,
                        "live_warehouse_stock": live_warehouse_stock,
                        "reason": "zero_stock_already_effective_no_warehouse_stock_row",
                    })

            candidate_rows.append({
                "parent_product_id": parent_product_id,
                "parent_sku": parent_sku,
                "ProductID": product_id,
                "SKU": sku,
                "supplier": supplier,
                "SupplierSKU": sku,
                "supplier_qty": supplier_qty,
                "stock_location": stock_location,
                "live_inventory": live_inventory,
                "live_warehouse_stock": live_warehouse_stock,
                "send_update": "yes" if should_send else "no",
            })

            if should_send:
                preview_rows.append(
                    _stock_location_payload_row(
                        sku=sku,
                        supplier=supplier,
                        supplier_sku=sku,
                        stock_location=stock_location,
                        quantity=supplier_qty,
                    )
                )

    candidates_path = out_dir / "stock_location_only_candidates.csv"
    skipped_path = out_dir / "stock_location_only_skips.csv"
    preview_path = out_dir / "stock_location_only_payload_preview.csv"

    pd.DataFrame(candidate_rows).to_csv(candidates_path, index=False)
    pd.DataFrame(skipped_rows).to_csv(skipped_path, index=False)

    preview = pd.DataFrame(preview_rows, columns=STOCK_LOCATION_PAYLOAD_COLUMNS)
    preview.to_csv(preview_path, index=False)

    items = payload_preview_to_storefeeder_items(preview)
    batches = batch_items(items, args.api_batch_size)

    live_paths, retry_count = _send_fast_stock_location_batches(
        client,
        batches,
        out_dir,
        file_prefix="stock_location_only_update",
    )

    success_count = _csv_count(live_paths["stock_location_only_update_success"])
    failure_count = _csv_count(live_paths["stock_location_only_update_failures"])

    summary = pd.DataFrame([
        {"metric": "run_time", "value": datetime.now().isoformat(timespec="seconds")},
        {"metric": "rules", "value": len(rules)},
        {"metric": "candidate_rows", "value": len(candidate_rows)},
        {"metric": "payload_rows", "value": len(preview_rows)},
        {"metric": "skipped_rows", "value": len(skipped_rows)},
        {"metric": "success_rows", "value": success_count},
        {"metric": "failure_rows", "value": failure_count},
        {"metric": "retry_count", "value": retry_count},
    ])
    summary_path = out_dir / "stock_location_only_summary.csv"
    summary.to_csv(summary_path, index=False)

    print(summary.to_string(index=False))
    print()
    print("Reports:")
    print(summary_path)
    print(candidates_path)
    print(skipped_path)
    print(preview_path)
    for path in live_paths.values():
        print(path)

    if failure_count:
        raise SystemExit(f"Stock-location-only sync completed with failures: {failure_count}")


if __name__ == "__main__":
    main()
