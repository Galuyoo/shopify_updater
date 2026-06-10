from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig


OUTPUT_COLUMNS = [
    "ID",
    "SKU",
    "Parent SKU",
    "Name",
    "Suppliers",
    "Supplier SKUs",
    "Stock Locations",
    "Stock Location Type",
    "Stock Location Current Inventories",
    "Current Inventories",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export current StoreFeeder products with read-only API calls.")
    parser.add_argument("--out-xlsx", default=Path("data/storefeeder_products_latest.xlsx"), type=Path)
    parser.add_argument("--out-csv", default=Path("data/storefeeder_products_latest.csv"), type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--page-size", default=100, type=int)
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", default=Path(".env"), type=Path)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be zero or greater")
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    products = fetch_products(client, page_size=args.page_size, limit=args.limit)
    rows = build_snapshot_rows(client, products)
    snapshot = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    args.out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_csv(args.out_csv, index=False)
    with pd.ExcelWriter(args.out_xlsx, engine="openpyxl") as writer:
        snapshot.to_excel(writer, index=False, sheet_name="StoreFeeder Products")

    summary = {
        "total_products": len(products),
        "rows_written": len(snapshot),
        "products_with_suppliers": int(snapshot["Suppliers"].astype(str).str.strip().ne("").sum()),
        "products_with_stock_locations": int(snapshot["Stock Locations"].astype(str).str.strip().ne("").sum()),
        "out_xlsx": str(args.out_xlsx),
        "out_csv": str(args.out_csv),
    }
    print("StoreFeeder product snapshot export")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print("\nRead-only only. No StoreFeeder write endpoints were called.")
    return 0


def fetch_products(client: StoreFeederApiClient, *, page_size: int, limit: int | None) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    page = 1
    while True:
        wrapper = client.get_products_page(page=page, page_size=page_size)
        status_code = int(wrapper.get("_status_code", 0))
        if status_code >= 400:
            raise RuntimeError(f"StoreFeeder product list request failed {status_code}: {wrapper.get('response')}")
        payload = wrapper.get("response", {})
        page_products = _extract_records(payload)
        if not page_products:
            break
        print(f"Fetched product page {page}: {len(page_products)} rows", flush=True)
        for product in page_products:
            if isinstance(product, dict):
                products.append(product)
                if limit is not None and len(products) >= limit:
                    return products
        if _is_last_page(payload, page, page_size, len(page_products)):
            break
        page += 1
    return products


def build_snapshot_rows(client: StoreFeederApiClient, products: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    total = len(products)
    for index, product in enumerate(products, start=1):
        if index == 1 or index % 25 == 0 or index == total:
            print(f"Enriching product {index}/{total}", flush=True)
        product_id = _first_text(product, ["ID", "Id", "ProductID", "ProductId", "ProductIDType.Value"])
        detail = product
        if product_id and (_needs_product_detail(product) or _needs_supplier_readback(product)):
            detail_wrapper = client.get_product(product_id)
            if int(detail_wrapper.get("_status_code", 0)) < 400:
                detail_payload = detail_wrapper.get("response", {})
                detail_record = _first_record(detail_payload)
                if detail_record:
                    detail = {**product, **detail_record}

        suppliers, supplier_skus = _supplier_pipes(detail)
        if product_id and (not suppliers or not supplier_skus):
            readback = client.get_product_suppliers(product_id)
            if int(readback.get("_status_code", 0)) < 400:
                rb_suppliers, rb_supplier_skus = _supplier_pipes(readback.get("response", {}))
                suppliers = suppliers or rb_suppliers
                supplier_skus = supplier_skus or rb_supplier_skus

        stock_locations, stock_location_types, inventories = _stock_location_pipes(detail)
        rows.append(
            {
                "ID": product_id,
                "SKU": _first_text(detail, ["SKU", "Sku", "ProductSKU", "ProductSku"]),
                "Parent SKU": _first_text(detail, ["Parent SKU", "ParentSKU", "ParentSku", "ParentProductSKU"]),
                "Name": _first_text(detail, ["Name", "ProductName", "Title", "Description"]),
                "Suppliers": suppliers,
                "Supplier SKUs": supplier_skus,
                "Stock Locations": stock_locations,
                "Stock Location Type": stock_location_types,
                "Stock Location Current Inventories": inventories,
                "Current Inventories": inventories,
            }
        )
    return rows


def _extract_records(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ["Items", "items", "Products", "products", "Data", "data", "Results", "results", "value", "Value"]:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _first_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        records = _extract_records(payload)
        if records and isinstance(records[0], dict):
            return records[0]
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return {}


def _is_last_page(payload: Any, page: int, page_size: int, count: int) -> bool:
    if isinstance(payload, dict):
        for key in ["TotalPages", "totalPages", "PageCount", "pageCount"]:
            if key in payload:
                try:
                    return page >= int(payload[key])
                except (TypeError, ValueError):
                    pass
        for key in ["HasNextPage", "hasNextPage", "HasMore", "hasMore"]:
            if key in payload:
                return not bool(payload[key])
    return count < page_size


def _needs_product_detail(product: dict[str, Any]) -> bool:
    stock_locations, _, inventories = _stock_location_pipes(product)
    return not stock_locations or not inventories


def _needs_supplier_readback(product: dict[str, Any]) -> bool:
    suppliers, supplier_skus = _supplier_pipes(product)
    return not suppliers or not supplier_skus


def _supplier_pipes(payload: Any) -> tuple[str, str]:
    records = _supplier_records(payload)
    if records:
        names = []
        skus = []
        for record in records:
            supplier = record.get("Supplier", {}) if isinstance(record, dict) else {}
            if not isinstance(supplier, dict):
                supplier = {}
            names.append(
                _first_text(record, ["Supplier.Name", "SupplierName", "Name", "Supplier"])
                or _first_text(supplier, ["Name", "SupplierName"])
            )
            skus.append(_first_text(record, ["SupplierSKU", "SupplierSku", "SKU", "Supplier SKUs"]))
        return _pipe(names), _pipe(skus)
    if isinstance(payload, dict):
        return (
            _first_text(payload, ["Suppliers", "SupplierNames", "Supplier Names"]),
            _first_text(payload, ["Supplier SKUs", "SupplierSKUs", "SupplierSku", "SupplierSKU"]),
        )
    return "", ""


def _supplier_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ["ProductSuppliers", "productSuppliers", "Suppliers", "suppliers", "value", "Value", "Items", "items"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        if "Supplier" in payload or "SupplierSKU" in payload or "SupplierSku" in payload:
            return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _stock_location_pipes(payload: Any) -> tuple[str, str, str]:
    if not isinstance(payload, dict):
        return "", "", ""
    existing_locations = _first_text(payload, ["Stock Locations", "StockLocations"])
    existing_types = _first_text(payload, ["Stock Location Type", "StockLocationType"])
    existing_inventories = _first_text(
        payload,
        ["Stock Location Current Inventories", "Current Inventories", "StockLocationCurrentInventories"],
    )
    if existing_locations and existing_inventories and "|" in existing_locations:
        return existing_locations, existing_types, existing_inventories

    records = _stock_location_records(payload)
    if not records:
        return existing_locations, existing_types, existing_inventories
    locations = []
    types = []
    inventories = []
    for record in records:
        location = record.get("StockLocation", {}) if isinstance(record.get("StockLocation"), dict) else {}
        locations.append(
            _first_text(record, ["StockLocationReference", "StockLocationName", "Name", "Reference", "Stock Location"])
            or _first_text(location, ["Name", "Reference", "StockLocationReference"])
        )
        types.append(_first_text(record, ["StockLocationType", "Type", "Stock Location Type"]) or _first_text(location, ["Type"]))
        if not types[-1]:
            types[-1] = _first_text(location, ["StockLocationType", "Type"])
        inventories.append(
            _first_text(
                record,
                ["CurrentInventory", "Inventory", "Quantity", "Stock", "Available", "PhysicalStock", "Current Inventories"],
            )
        )
    return _pipe(locations), _pipe(types), _pipe(inventories)


def _stock_location_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in [
        "StockLocationInventory",
        "StockLocationInventories",
        "StockLocations",
        "stockLocations",
        "ProductStockLocations",
        "Inventory",
        "Inventories",
    ]:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    warehouse_information = payload.get("WarehouseInformation")
    if isinstance(warehouse_information, dict):
        value = warehouse_information.get("StockLocations")
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _first_text(payload: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = _nested_value(payload, key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _nested_value(payload: dict[str, Any], key: str) -> Any:
    current: Any = payload
    for part in key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _pipe(values: list[Any]) -> str:
    return "|".join(str(value).strip() for value in values)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    import os

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        if name and name not in os.environ:
            os.environ[name] = value


if __name__ == "__main__":
    raise SystemExit(main())
