from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

KEYWORDS = [
    "variant",
    "variants",
    "variation",
    "child",
    "children",
    "options",
    "option",
    "productoptions",
    "productvariations",
    "supplier",
    "suppliers",
    "stock",
    "sku",
    "barcode",
]

CANDIDATE_COLUMNS = [
    "path",
    "node_type",
    "matched_key",
    "ProductID",
    "VariantID",
    "SKU",
    "Name",
    "SupplierSKU",
    "Barcode",
    "child_count",
    "preview_json",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only StoreFeeder product detail inspector.")
    parser.add_argument("--product-id")
    parser.add_argument("--sku")
    parser.add_argument("--out-root", type=Path, default=Path("reports/product_detail_inspection"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=500)
    args = parser.parse_args()
    if not args.product_id and not args.sku:
        parser.error("either --product-id or --sku is required")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))

    product_id = str(args.product_id or "").strip()
    sku = str(args.sku or "").strip()
    resolved_from_sku = "no"
    if not product_id:
        product_id = _find_product_id_by_sku(client, sku, page_size=args.page_size, max_pages=args.max_pages)
        resolved_from_sku = "yes"
    if not product_id:
        raise SystemExit(f"ProductID not found for SKU {sku!r}")

    wrapper = client.get_product(product_id)
    status_code = int(wrapper.get("_status_code", 0))
    payload = wrapper.get("response", {})

    run_label = f"{product_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out_dir = args.out_root / run_label
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "01_raw_product_detail.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    flat = pd.DataFrame(_flatten_keys(payload))
    flat.to_csv(out_dir / "02_flattened_keys.csv", index=False)

    candidates = pd.DataFrame(_candidate_nodes(payload), columns=CANDIDATE_COLUMNS)
    candidates.to_csv(out_dir / "03_candidate_variant_nodes.csv", index=False)

    product_sku = _first_text(payload, ["SKU", "Sku", "ProductSKU", "ProductSku", "sku"])
    brief = [
        "StoreFeeder product detail inspection",
        f"PRODUCT_ID: {product_id}",
        f"REQUESTED_SKU: {sku}",
        f"PRODUCT_SKU: {product_sku}",
        f"STATUS_CODE: {status_code}",
        f"RESOLVED_FROM_SKU: {resolved_from_sku}",
        f"FLATTENED_KEYS: {len(flat)}",
        f"CANDIDATE_VARIANT_NODES: {len(candidates)}",
        f"OUT_DIR: {out_dir}",
        "",
        "Safety: read-only only. Used GET product/list endpoints only; no POST/PUT/PATCH/DELETE.",
    ]
    if not candidates.empty:
        brief.append("")
        brief.append("Top candidate paths:")
        for _, row in candidates.head(20).iterrows():
            brief.append(f"- {row.get('path', '')}: SKU={row.get('SKU', '')} SupplierSKU={row.get('SupplierSKU', '')}")
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(brief), encoding="utf-8")

    print("Product detail inspection")
    for line in brief[:9]:
        print(line)
    print("Reports:", out_dir)
    return 0 if status_code < 400 else 2


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _find_product_id_by_sku(client: StoreFeederApiClient, sku: str, *, page_size: int, max_pages: int) -> str:
    needle = str(sku).strip().casefold()
    for page in range(1, max_pages + 1):
        wrapper = client.get_products_page(page=page, page_size=page_size)
        payload = wrapper.get("response", {})
        for item in _extract_records(payload):
            item_sku = _first_text(item, ["SKU", "Sku", "ProductSKU", "ProductSku", "sku"])
            if item_sku.casefold() == needle:
                return _first_text(item, ["ID", "Id", "ProductID", "ProductId", "productId"])
        if _is_last_page(payload, page, page_size, len(_extract_records(payload))):
            break
    return ""


def _flatten_keys(payload: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                next_path = f"{path}.{key}" if path else str(key)
                rows.append(
                    {
                        "path": next_path,
                        "key": str(key),
                        "value_type": type(value).__name__,
                        "value_preview": _preview(value),
                    }
                )
                walk(value, next_path)
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]")

    walk(payload, "")
    return rows


def _candidate_nodes(payload: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def walk(node: Any, path: str, key_name: str = "") -> None:
        key_l = key_name.casefold()
        matched = next((kw for kw in KEYWORDS if kw in key_l), "")
        if matched and isinstance(node, (dict, list)):
            rows.append(_candidate_node_row(node, path, matched))
        if isinstance(node, dict):
            has_candidate_field = any(_contains_keyword(k) for k in node.keys())
            has_sku = bool(_first_text(node, ["SKU", "Sku", "ProductSKU", "ProductSku", "SupplierSKU", "SupplierSku", "Barcode", "barcode"]))
            if (has_candidate_field or has_sku) and not matched:
                rows.append(_candidate_node_row(node, path, "field_scan"))
            for key, value in node.items():
                walk(value, f"{path}.{key}" if path else str(key), str(key))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]", key_name)

    walk(payload, "$", "")
    seen: set[str] = set()
    unique = []
    for row in rows:
        key = row["path"] + "|" + row["matched_key"]
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _candidate_node_row(node: Any, path: str, matched_key: str) -> dict[str, str]:
    record = node if isinstance(node, dict) else {}
    return {
        "path": path,
        "node_type": type(node).__name__,
        "matched_key": matched_key,
        "ProductID": _first_text(record, ["ProductID", "ProductId", "ID", "Id", "productId"]),
        "VariantID": _first_text(record, ["VariantID", "VariantId", "VariationID", "VariationId", "OptionID", "OptionId"]),
        "SKU": _first_text(record, ["SKU", "Sku", "ProductSKU", "ProductSku", "sku"]),
        "Name": _first_text(record, ["Name", "ProductName", "Title", "Description"]),
        "SupplierSKU": _first_text(record, ["SupplierSKU", "SupplierSku", "Supplier SKUs", "SupplierSkuCode"]),
        "Barcode": _first_text(record, ["Barcode", "barcode", "EAN", "Ean", "UPC", "Upc"]),
        "child_count": str(len(node)) if isinstance(node, list) else "",
        "preview_json": _preview(node),
    }


def _contains_keyword(key: str) -> bool:
    key_l = str(key).casefold()
    return any(keyword in key_l for keyword in KEYWORDS)


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ["Items", "items", "Products", "products", "Data", "data", "Results", "results", "value", "Value"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


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


def _first_text(payload: Any, keys: list[str]) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        if key in payload and payload[key] is not None:
            value = payload[key]
            if isinstance(value, dict):
                nested = _first_text(value, ["Value", "Name", "SKU", "Id", "ID"])
                if nested:
                    return nested
            elif not isinstance(value, (list, tuple)):
                text = str(value).strip()
                if text:
                    return text
    return ""


def _preview(value: Any) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, default=str, ensure_ascii=False)
    else:
        text = str(value)
    return text[:1000]


if __name__ == "__main__":
    raise SystemExit(main())
