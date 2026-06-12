from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ["Items", "items", "Data", "data", "Results", "results", "Products", "products"]:
        if isinstance(payload.get(key), list):
            return payload[key]
    return []


def first_value(obj: Any, keys: list[str]) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in keys:
        if key in obj and obj[key] not in [None, ""]:
            return str(obj[key]).strip()
    return ""


def stock_locations_from_detail(detail: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for loc in detail.get("WarehouseInformation", {}).get("StockLocations", []):
        if not isinstance(loc, dict):
            continue
        stock_location = loc.get("StockLocation", {}) if isinstance(loc.get("StockLocation"), dict) else {}
        warehouse = stock_location.get("Warehouse", {}) if isinstance(stock_location.get("Warehouse"), dict) else {}
        rows.append(
            {
                "stock_location": str(stock_location.get("StockLocationReference", "")).strip(),
                "stock_location_type": str(stock_location.get("StockLocationType", "")).strip(),
                "warehouse": str(warehouse.get("WarehouseName", "")).strip(),
                "available": str(loc.get("Available", "")).strip(),
                "physical_stock": str(loc.get("PhysicalStock", "")).strip(),
            }
        )
    return rows


def parse_int(value: Any) -> int | None:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only warehouse-only stock verification pipeline.")
    parser.add_argument("--family-code", required=True)
    parser.add_argument("--sku-prefix", required=True)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--stock-location", default="Warehouse Stock")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--out-root", type=Path, default=Path("reports/warehouse_stock"))
    parser.add_argument("--targets", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=500)
    args = parser.parse_args()

    if not args.verify:
        raise SystemExit("BLOCKED: warehouse_stock_pipeline requires --verify. No write mode exists.")

    load_env(args.env_file)
    from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / args.family_code / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = pd.read_csv(args.manifest, dtype=str, keep_default_na=False)
    required = ["SKU", "physical_stock", "stock_location", "enabled_for_same_day", "notes"]
    missing = [column for column in required if column not in manifest.columns]
    if missing:
        raise SystemExit("BLOCKED: manifest missing columns: " + ", ".join(missing))
    for column in required:
        manifest[column] = manifest[column].fillna("").astype(str).str.strip()
    manifest["_sku_key"] = manifest["SKU"].str.upper()

    targets = pd.read_csv(args.targets, dtype=str, keep_default_na=False) if args.targets.exists() else pd.DataFrame(columns=["SKU"])
    target_skus = set(targets.get("SKU", pd.Series(dtype=str)).fillna("").astype(str).str.strip().str.upper())

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig())

    scan_rows: list[dict[str, str]] = []
    detail_by_sku: dict[str, dict[str, Any]] = {}
    page = 1
    while page <= args.max_pages:
        result = client.get_products_page(page=page, page_size=args.page_size)
        items = extract_items(result.get("response", {}))
        for item in items:
            sku = first_value(item, ["SKU", "Sku", "sku"])
            product_id = first_value(item, ["ProductID", "ProductId", "ID", "Id", "id"])
            if not sku.upper().startswith(args.sku_prefix.upper()):
                continue
            detail = client.get_product(product_id).get("response", {}) if product_id else {}
            locations = stock_locations_from_detail(detail)
            detail_by_sku[sku.upper()] = {"product_id": product_id, "sku": sku, "detail": detail, "locations": locations}
            scan_rows.append(
                {
                    "ProductID": product_id,
                    "SKU": sku,
                    "Name": first_value(item, ["Name", "ProductName", "Title"]),
                    "stock_locations": "|".join(location["stock_location"] for location in locations),
                    "raw_location_json": json.dumps(locations, default=str),
                }
            )
        print(f"products page {page}: scanned {len(items)}, prefix matches {len(scan_rows)}")
        if len(items) < args.page_size:
            break
        page += 1

    scan_df = pd.DataFrame(scan_rows, columns=["ProductID", "SKU", "Name", "stock_locations", "raw_location_json"])
    write_csv(scan_df, out_dir / "01_storefeeder_scan.csv")

    blockers: list[dict[str, str]] = []
    verify_rows: list[dict[str, str]] = []
    exclusion_rows: list[dict[str, str]] = []

    manifest_keys = set(manifest["_sku_key"])
    for _, row in manifest.iterrows():
        sku = row["SKU"]
        sku_key = row["_sku_key"]
        expected_qty = parse_int(row["physical_stock"])
        product = detail_by_sku.get(sku_key)
        found_location = {}
        if not product:
            blockers.append({"stage": "manifest", "SKU": sku, "reason": "manifest_sku_missing_from_storefeeder"})
        else:
            matches = [
                location
                for location in product["locations"]
                if location["stock_location"].casefold() == args.stock_location.casefold()
            ]
            if not matches:
                blockers.append({"stage": "manifest", "SKU": sku, "reason": "missing_expected_warehouse_stock_location"})
            else:
                found_location = matches[0]
                available = parse_int(found_location.get("available"))
                physical = parse_int(found_location.get("physical_stock"))
                if expected_qty is None:
                    blockers.append({"stage": "manifest", "SKU": sku, "reason": "invalid_manifest_physical_stock"})
                elif physical is not None and physical != expected_qty:
                    blockers.append({"stage": "manifest", "SKU": sku, "reason": "physical_stock_mismatch"})
                elif available is not None and available != expected_qty:
                    blockers.append({"stage": "manifest", "SKU": sku, "reason": "available_stock_mismatch"})

        in_supplier_sync = sku_key in target_skus
        if in_supplier_sync:
            blockers.append({"stage": "supplier_sync_exclusion", "SKU": sku, "reason": "warehouse_sku_present_in_supplier_sync_targets"})
        exclusion_rows.append(
            {
                "SKU": sku,
                "in_supplier_sync_targets": "yes" if in_supplier_sync else "no",
                "status": "blocked" if in_supplier_sync else "ok",
            }
        )
        verify_rows.append(
            {
                "SKU": sku,
                "ProductID": product.get("product_id", "") if product else "",
                "expected_physical_stock": row["physical_stock"],
                "expected_stock_location": args.stock_location,
                "found_stock_location": found_location.get("stock_location", ""),
                "api_available": found_location.get("available", ""),
                "api_physical_stock": found_location.get("physical_stock", ""),
                "enabled_for_same_day": row["enabled_for_same_day"],
                "notes": row["notes"],
            }
        )

    for _, row in scan_df.iterrows():
        sku = str(row["SKU"]).strip()
        if sku.upper() not in manifest_keys:
            verify_rows.append(
                {
                    "SKU": sku,
                    "ProductID": row["ProductID"],
                    "expected_physical_stock": "",
                    "expected_stock_location": args.stock_location,
                    "found_stock_location": "",
                    "api_available": "",
                    "api_physical_stock": "",
                    "enabled_for_same_day": "no",
                    "notes": "same-prefix non-manifest variant; not enabled",
                }
            )

    verification_df = pd.DataFrame(verify_rows)
    exclusion_df = pd.DataFrame(exclusion_rows)
    blocker_df = pd.DataFrame(blockers, columns=["stage", "SKU", "reason"])
    write_csv(verification_df, out_dir / "02_manifest_verification.csv")
    write_csv(exclusion_df, out_dir / "03_supplier_sync_exclusion_check.csv")
    write_csv(blocker_df, out_dir / "BLOCKERS.csv")

    summary = pd.DataFrame(
        [
            {"metric": "family_code", "value": args.family_code},
            {"metric": "sku_prefix", "value": args.sku_prefix},
            {"metric": "manifest_rows", "value": len(manifest)},
            {"metric": "storefeeder_prefix_rows", "value": len(scan_df)},
            {"metric": "supplier_sync_exclusion_violations", "value": int(exclusion_df["in_supplier_sync_targets"].eq("yes").sum()) if not exclusion_df.empty else 0},
            {"metric": "blockers", "value": len(blocker_df)},
            {"metric": "verify_passed", "value": "yes" if blocker_df.empty else "no"},
        ]
    )
    write_csv(summary, out_dir / "SUMMARY.csv")

    brief = [
        f"FAMILY: {args.family_code}",
        f"RUN: {run_id}",
        f"VERIFY_PASSED: {'yes' if blocker_df.empty else 'no'}",
        f"MANIFEST_ROWS: {len(manifest)}",
        f"STOREFEEDER_PREFIX_ROWS: {len(scan_df)}",
        f"BLOCKERS: {len(blocker_df)}",
        f"OUT_DIR: {out_dir}",
        "",
        "Warehouse-only SKUs must not be added to supplier sync targets.",
        "No StoreFeeder writes were made.",
    ]
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(brief), encoding="utf-8")

    print("\nWAREHOUSE STOCK VERIFY SUMMARY")
    print(summary.to_string(index=False))
    print("\nReports:", out_dir)
    if not blocker_df.empty:
        print("\nVERIFY BLOCKED")
        return 2
    print("\nVERIFY PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
