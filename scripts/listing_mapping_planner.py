from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

MANIFEST_COLUMNS = ["parent_sku", "product_name", "stock_strategy"]
PRODUCT_COLUMNS = [
    "ProductID",
    "SKU",
    "Name",
    "ParentProductID",
    "ParentSKU",
    "TargetParentSKU",
    "stock_strategy",
    "is_priority_product",
    "variant_attributes",
    "raw_json",
]
LISTING_COLUMNS = [
    "Channel",
    "ListingID",
    "ListingVariantID",
    "ListingSKU",
    "ListingTitle",
    "CurrentProductID",
    "CurrentProductSKU",
    "ListingStatus",
    "Marketplace",
    "ASIN",
    "raw_json",
]
CANDIDATE_COLUMNS = [
    "Channel",
    "ListingID",
    "ListingVariantID",
    "ListingSKU",
    "ListingTitle",
    "CurrentProductID",
    "CurrentProductSKU",
    "TargetProductID",
    "TargetProductSKU",
    "TargetParentSKU",
    "stock_strategy",
    "confidence",
    "can_map",
    "reason",
]
BLOCKER_COLUMNS = ["stage", "ListingID", "ListingSKU", "reason"]

PRIORITY_PARENT_SKU = "EMB-CSTMINST-BC045"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only StoreFeeder listing-to-clean-product mapping planner.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--verify", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--channel", default="Amazon")
    parser.add_argument("--manifest", type=Path, default=Path("data/clean_product_stock_strategy_manifest.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/listing_mapping_clean"))
    parser.add_argument("--mapping-manifest", type=Path, help="Existing 04_mapping_manifest_ready.csv for --verify/--execute")
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=500)
    args = parser.parse_args()
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    if args.max_pages < 1:
        parser.error("--max-pages must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))

    if args.verify:
        return _verify(args, client)
    if args.execute:
        return _execute(args)
    return _plan(args, client)


def _plan(args: argparse.Namespace, client: StoreFeederApiClient) -> int:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = _read_csv(args.manifest)
    _require_columns(manifest, MANIFEST_COLUMNS, str(args.manifest))
    manifest = _normalize_manifest(manifest)

    products = _fetch_products(client, page_size=args.page_size, max_pages=args.max_pages)
    product_index = _build_product_index(products)
    clean_products = _build_clean_products(client, manifest, product_index)
    clean_products_df = pd.DataFrame(clean_products, columns=PRODUCT_COLUMNS)
    _write_csv(clean_products_df, out_dir / "01_products.csv")

    listings = _fetch_listings(client, args.channel, page_size=args.page_size, max_pages=args.max_pages)
    listings_df = pd.DataFrame(listings, columns=LISTING_COLUMNS)
    _write_csv(listings_df, out_dir / "02_listings.csv")

    candidates, blockers = _build_mapping_candidates(listings_df, clean_products_df, args.channel)
    candidates_df = pd.DataFrame(candidates, columns=CANDIDATE_COLUMNS)
    ready_df = candidates_df[candidates_df["can_map"].astype(str).str.casefold().eq("yes")].copy()
    blockers_df = pd.DataFrame(blockers, columns=BLOCKER_COLUMNS)

    _write_csv(candidates_df, out_dir / "03_mapping_candidates.csv")
    _write_csv(ready_df, out_dir / "04_mapping_manifest_ready.csv")
    _write_csv(blockers_df, out_dir / "BLOCKERS.csv")

    summary = pd.DataFrame([
        {"metric": "mode", "value": "plan"},
        {"metric": "run_id", "value": run_id},
        {"metric": "channel", "value": args.channel},
        {"metric": "storefeeder_product_rows_scanned", "value": len(products)},
        {"metric": "clean_product_variant_rows", "value": len(clean_products_df)},
        {"metric": "supplier_synced_product_rows", "value": _count_eq(clean_products_df, "stock_strategy", "supplier_synced_inventory")},
        {"metric": "warehouse_only_product_rows", "value": _count_eq(clean_products_df, "stock_strategy", "warehouse_only")},
        {"metric": "listing_rows", "value": len(listings_df)},
        {"metric": "candidate_rows", "value": len(candidates_df)},
        {"metric": "mapping_manifest_ready_rows", "value": len(ready_df)},
        {"metric": "blocker_rows", "value": len(blockers_df)},
        {"metric": "execute_supported", "value": "no"},
        {"metric": "out_dir", "value": str(out_dir)},
    ])
    _write_csv(summary, out_dir / "SUMMARY.csv")
    _write_brief(out_dir, summary, blockers_df)

    print("Listing mapping planner")
    print(summary.to_string(index=False))
    print("Reports:", out_dir)
    print("Read-only plan only. No StoreFeeder write endpoints were called.")
    return 0


def _verify(args: argparse.Namespace, client: StoreFeederApiClient) -> int:
    manifest_path = args.mapping_manifest or _latest_manifest(args.out_root)
    if not manifest_path or not manifest_path.exists():
        raise SystemExit("No mapping manifest found for --verify")
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / f"verify_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = _read_csv(manifest_path)
    listings = pd.DataFrame(_fetch_listings(client, args.channel, page_size=args.page_size, max_pages=args.max_pages), columns=LISTING_COLUMNS)
    current_by_id = {str(row["ListingID"]): row for _, row in listings.iterrows()}
    rows = []
    for _, row in manifest.iterrows():
        listing_id = str(row.get("ListingID", "")).strip()
        current = current_by_id.get(listing_id)
        current_product_id = str(current.get("CurrentProductID", "")).strip() if current is not None else ""
        target_product_id = str(row.get("TargetProductID", "")).strip()
        rows.append({
            "ListingID": listing_id,
            "ListingSKU": row.get("ListingSKU", ""),
            "TargetProductID": target_product_id,
            "CurrentProductID": current_product_id,
            "verified_mapped_to_target": "yes" if current_product_id and current_product_id == target_product_id else "no",
            "verification_reason": "matched" if current_product_id and current_product_id == target_product_id else "current_mapping_differs_or_not_visible",
        })
    verification = pd.DataFrame(rows)
    _write_csv(verification, out_dir / "05_mapping_verification.csv")
    print("Verification report:", out_dir / "05_mapping_verification.csv")
    return 0


def _execute(args: argparse.Namespace) -> int:
    manifest_path = args.mapping_manifest or _latest_manifest(args.out_root)
    if not manifest_path or not manifest_path.exists():
        raise SystemExit("No mapping manifest found for --execute")
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / f"execute_blocked_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = _read_csv(manifest_path)
    work_order = manifest.copy()
    work_order["manual_action"] = "Map listing to TargetProductID/TargetProductSKU in StoreFeeder UI or approved bulk import"
    work_order["execute_status"] = "blocked_no_confirmed_safe_listing_remap_endpoint"
    _write_csv(work_order, out_dir / "06_manual_mapping_work_order.csv")
    print("BLOCKED: no confirmed safe StoreFeeder listing remap write endpoint exists.")
    print("Manual work order:", out_dir / "06_manual_mapping_work_order.csv")
    return 2


def _build_mapping_candidates(listings: pd.DataFrame, products: pd.DataFrame, channel: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    if listings.empty:
        return candidates, [{"stage": "listings", "ListingID": "", "ListingSKU": "", "reason": "no_listing_rows_found"}]
    if products.empty:
        return candidates, [{"stage": "products", "ListingID": "", "ListingSKU": "", "reason": "no_clean_product_rows_found"}]

    for _, listing in listings.iterrows():
        listing_sku = str(listing.get("ListingSKU", "")).strip()
        if not listing_sku:
            blockers.append(_blocker("match", listing, "missing_listing_sku"))
            candidates.append(_candidate_row(channel, listing, None, "none", "no", "missing_listing_sku"))
            continue
        is_priority = _is_priority_listing(listing)
        allowed = products[products["stock_strategy"].eq("warehouse_only" if is_priority else "supplier_synced_inventory")].copy()
        blocked_cross = _cross_lane_product(listing, products, is_priority)
        if blocked_cross:
            reason = "priority_listing_would_match_normal_product" if is_priority else "normal_listing_would_match_priority_product"
            blockers.append(_blocker("safety", listing, reason))
            candidates.append(_candidate_row(channel, listing, blocked_cross, "blocked_cross_lane", "no", reason))
            continue
        matches = _target_matches(listing, allowed)
        if len(matches) == 1:
            candidates.append(_candidate_row(channel, listing, matches[0]["product"], matches[0]["confidence"], "yes", matches[0]["reason"]))
        elif len(matches) == 0:
            reason = "no_deterministic_target_product_match"
            blockers.append(_blocker("match", listing, reason))
            candidates.append(_candidate_row(channel, listing, None, "none", "no", reason))
        else:
            reason = "ambiguous_multiple_target_product_matches"
            blockers.append(_blocker("match", listing, reason))
            best = matches[0]["product"]
            candidates.append(_candidate_row(channel, listing, best, "ambiguous", "no", reason))
    return candidates, blockers


def _target_matches(listing: pd.Series, allowed_products: pd.DataFrame) -> list[dict[str, Any]]:
    listing_sku = str(listing.get("ListingSKU", "")).strip().upper()
    current_sku = str(listing.get("CurrentProductSKU", "")).strip().upper()
    haystack = _listing_haystack(listing)
    matches: list[dict[str, Any]] = []
    for _, product in allowed_products.iterrows():
        target_sku = str(product.get("SKU", "")).strip().upper()
        if not target_sku:
            continue
        if listing_sku == target_sku:
            matches.append({"product": product, "confidence": "exact", "reason": "exact_listing_sku_to_target_sku"})
        elif current_sku and current_sku == target_sku:
            matches.append({"product": product, "confidence": "exact", "reason": "current_product_sku_matches_target_sku"})
        elif _bounded_contains(haystack, target_sku):
            matches.append({"product": product, "confidence": "strong", "reason": "target_sku_token_visible_in_listing"})
    return _dedupe_matches(matches)


def _cross_lane_product(listing: pd.Series, products: pd.DataFrame, is_priority: bool) -> pd.Series | None:
    disallowed_strategy = "supplier_synced_inventory" if is_priority else "warehouse_only"
    matches = _target_matches(listing, products[products["stock_strategy"].eq(disallowed_strategy)].copy())
    if len(matches) == 1:
        return matches[0]["product"]
    return None


def _candidate_row(channel: str, listing: pd.Series, product: pd.Series | None, confidence: str, can_map: str, reason: str) -> dict[str, Any]:
    product = product if product is not None else pd.Series(dtype=str)
    return {
        "Channel": str(listing.get("Channel", "")).strip() or channel,
        "ListingID": listing.get("ListingID", ""),
        "ListingVariantID": listing.get("ListingVariantID", ""),
        "ListingSKU": listing.get("ListingSKU", ""),
        "ListingTitle": listing.get("ListingTitle", ""),
        "CurrentProductID": listing.get("CurrentProductID", ""),
        "CurrentProductSKU": listing.get("CurrentProductSKU", ""),
        "TargetProductID": product.get("ProductID", ""),
        "TargetProductSKU": product.get("SKU", ""),
        "TargetParentSKU": product.get("TargetParentSKU", ""),
        "stock_strategy": product.get("stock_strategy", ""),
        "confidence": confidence,
        "can_map": can_map,
        "reason": reason,
    }


def _blocker(stage: str, listing: pd.Series, reason: str) -> dict[str, Any]:
    return {"stage": stage, "ListingID": listing.get("ListingID", ""), "ListingSKU": listing.get("ListingSKU", ""), "reason": reason}


def _is_priority_listing(listing: pd.Series) -> bool:
    text = _listing_haystack(listing).casefold()
    return any(token in text for token in ["emb-cstminst-bc045", "cstminst", "same day", "same-day", "prime", "warehouse stock"])


def _listing_haystack(listing: pd.Series) -> str:
    return " ".join(str(listing.get(column, "")) for column in ["ListingSKU", "ListingTitle", "CurrentProductSKU", "raw_json"] if column in listing)


def _bounded_contains(text: str, token: str) -> bool:
    if not token:
        return False
    return re.search(rf"(?<![A-Z0-9]){re.escape(token.upper())}(?![A-Z0-9])", text.upper()) is not None


def _dedupe_matches(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out = []
    rank = {"exact": 0, "strong": 1, "ambiguous": 2, "none": 3}
    for match in sorted(matches, key=lambda item: rank.get(item["confidence"], 9)):
        product = match["product"]
        key = (str(product.get("ProductID", "")), str(product.get("SKU", "")))
        if key in seen:
            continue
        seen.add(key)
        out.append(match)
    return out


def _build_clean_products(client: StoreFeederApiClient, manifest: pd.DataFrame, product_index: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, manifest_row in manifest.iterrows():
        parent_sku = str(manifest_row["parent_sku"]).strip()
        parent_matches = product_index["by_sku"].get(parent_sku.casefold(), [])
        for parent in parent_matches:
            detail_wrapper = client.get_product(parent["ProductID"])
            if int(detail_wrapper.get("_status_code", 0)) >= 400:
                continue
            detail = _first_record(detail_wrapper.get("response", {}))
            children = _child_product_rows(detail, parent_sku, parent.get("ProductID", ""))
            if not children:
                children = [{**parent, "ParentProductID": parent.get("ProductID", ""), "Parent SKU": parent_sku, "variant_attributes": "", "raw_json": ""}]
            for child in children:
                rows.append({
                    "ProductID": child.get("ProductID", ""),
                    "SKU": child.get("SKU", ""),
                    "Name": child.get("Name", ""),
                    "ParentProductID": child.get("ParentProductID", parent.get("ProductID", "")),
                    "ParentSKU": parent.get("SKU", parent_sku),
                    "TargetParentSKU": parent_sku,
                    "stock_strategy": manifest_row.get("stock_strategy", ""),
                    "is_priority_product": "yes" if parent_sku == PRIORITY_PARENT_SKU else "no",
                    "variant_attributes": child.get("variant_attributes", ""),
                    "raw_json": child.get("raw_json", ""),
                })
    return rows


def _fetch_products(client: StoreFeederApiClient, *, page_size: int, max_pages: int) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        wrapper = client.get_products_page(page=page, page_size=page_size)
        payload = wrapper.get("response", {})
        items = _extract_records(payload)
        if not items:
            break
        products.extend(items)
        print(f"products page {page}: {len(items)}", flush=True)
        if _is_last_page(payload, page, page_size, len(items)):
            break
    return products


def _fetch_listings(client: StoreFeederApiClient, channel: str, *, page_size: int, max_pages: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        wrapper = client.get_path("/listings", params={"page": page, "pageSize": page_size})
        if int(wrapper.get("_status_code", 0)) >= 400:
            break
        payload = wrapper.get("response", {})
        items = _extract_records(payload)
        if not items:
            break
        for item in items:
            row = _listing_row(item)
            if _channel_matches(row, channel):
                rows.append(row)
        print(f"listings page {page}: {len(items)} scanned, {len(rows)} kept", flush=True)
        if _is_last_page(payload, page, page_size, len(items)):
            break
    return rows


def _channel_matches(row: dict[str, str], channel: str) -> bool:
    if not channel:
        return True
    text = " ".join([row.get("Channel", ""), row.get("Marketplace", ""), row.get("raw_json", "")]).casefold()
    return channel.casefold() in text


def _build_product_index(products: list[dict[str, Any]]) -> dict[str, Any]:
    by_sku: dict[str, list[dict[str, str]]] = {}
    for product in products:
        row = _product_row(product)
        if row["SKU"]:
            by_sku.setdefault(row["SKU"].casefold(), []).append(row)
    return {"by_sku": by_sku}


def _product_row(item: dict[str, Any]) -> dict[str, str]:
    return {
        "ProductID": _first_text(item, ["ID", "Id", "ProductID", "ProductId", "productId"]),
        "SKU": _first_text(item, ["SKU", "Sku", "ProductSKU", "ProductSku", "sku"]),
        "Name": _first_text(item, ["Name", "ProductName", "Title", "Description"]),
    }


def _listing_row(item: dict[str, Any]) -> dict[str, str]:
    return {
        "Channel": _first_text(item, ["Channel", "channel", "ChannelName", "Integration", "IntegrationName", "Marketplace", "MarketplaceName"]),
        "ListingID": _first_text(item, ["ListingID", "ListingId", "listingId", "ID", "Id", "id"]),
        "ListingVariantID": _first_text(item, ["ListingVariantID", "ListingVariantId", "VariantID", "VariantId"]),
        "ListingSKU": _first_text(item, ["SKU", "Sku", "sku", "ListingSKU", "ListingSku", "ChannelSKU", "ChannelSku", "SellerSKU", "SellerSku", "ExternalSKU", "ExternalSku"]),
        "ListingTitle": _first_text(item, ["Title", "title", "Name", "name", "ListingTitle", "ProductName"]),
        "CurrentProductID": _first_text(item, ["ProductID", "ProductId", "MappedProductID", "MappedProductId", "StoreFeederProductID", "StoreFeederProductId"]),
        "CurrentProductSKU": _first_text(item, ["ProductSKU", "ProductSku", "MappedProductSKU", "MappedProductSku"]),
        "ListingStatus": _first_text(item, ["Status", "ListingStatus", "state"]),
        "Marketplace": _first_text(item, ["Marketplace", "MarketplaceName", "MarketPlace"]),
        "ASIN": _first_text(item, ["ASIN", "Asin", "asin"]),
        "raw_json": json.dumps(item, default=str, ensure_ascii=False),
    }


def _child_product_rows(detail: dict[str, Any], parent_sku: str, parent_id: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in ["Variants", "variants", "Children", "children", "ChildProducts", "childProducts", "ProductVariants", "productVariants"]:
        value = detail.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            if not isinstance(item, dict):
                continue
            product_node = item.get("Product") if isinstance(item.get("Product"), dict) else item
            row = _product_row(product_node)
            if not row["SKU"]:
                continue
            row["ParentProductID"] = parent_id
            row["Parent SKU"] = parent_sku
            row["variant_attributes"] = _variant_attributes(item)
            row["raw_json"] = json.dumps(item, default=str, ensure_ascii=False)
            rows.append(row)
    return _dedupe_product_rows(rows)


def _variant_attributes(item: dict[str, Any]) -> str:
    attrs = item.get("VariantAttributes") or item.get("variantAttributes") or item.get("Attributes") or []
    if not isinstance(attrs, list):
        return json.dumps(attrs, default=str, ensure_ascii=False)
    parts = []
    for attr in attrs:
        if not isinstance(attr, dict):
            continue
        name = _first_text(attr, ["Name", "AttributeName", "OptionName", "Key"])
        value = _first_text(attr, ["Value", "AttributeValue", "OptionValue"])
        if name or value:
            parts.append(f"{name}:{value}" if name else value)
    return "|".join(parts)


def _dedupe_product_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out = []
    for row in rows:
        key = (row.get("ProductID", ""), row.get("SKU", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _normalize_manifest(manifest: pd.DataFrame) -> pd.DataFrame:
    rows = manifest.copy()
    for column in MANIFEST_COLUMNS:
        rows[column] = rows[column].fillna("").astype(str).str.strip()
    return rows[rows["parent_sku"].ne("")].reset_index(drop=True)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _require_columns(df: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise SystemExit(f"{label} missing columns: {', '.join(missing)}")


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ["Items", "items", "Data", "data", "Results", "results", "Listings", "listings", "Products", "products", "value", "Value"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _first_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        records = _extract_records(payload)
        if records:
            return records[0]
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return {}


def _first_text(payload: Any, keys: list[str]) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        if key in payload and payload[key] not in [None, ""]:
            value = payload[key]
            if isinstance(value, dict):
                nested = _first_text(value, ["Value", "Name", "SKU", "Id", "ID"])
                if nested:
                    return nested
            elif not isinstance(value, (list, tuple)):
                return str(value).strip()
    for value in payload.values():
        if isinstance(value, dict):
            found = _first_text(value, keys)
            if found:
                return found
    return ""


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


def _latest_manifest(out_root: Path) -> Path | None:
    candidates = sorted(out_root.glob("*/04_mapping_manifest_ready.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _count_eq(df: pd.DataFrame, column: str, value: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    return int(df[column].astype(str).eq(value).sum())


def _write_brief(out_dir: Path, summary: pd.DataFrame, blockers: pd.DataFrame) -> None:
    values = {str(row["metric"]): str(row["value"]) for _, row in summary.iterrows()}
    lines = [
        "Listing mapping clean planner brief",
        f"RUN_ID: {values.get('run_id', '')}",
        f"CHANNEL: {values.get('channel', '')}",
        f"LISTING_ROWS: {values.get('listing_rows', '0')}",
        f"CLEAN_PRODUCT_VARIANT_ROWS: {values.get('clean_product_variant_rows', '0')}",
        f"MAPPING_MANIFEST_READY_ROWS: {values.get('mapping_manifest_ready_rows', '0')}",
        f"BLOCKER_ROWS: {values.get('blocker_rows', '0')}",
        f"EXECUTE_SUPPORTED: {values.get('execute_supported', 'no')}",
        f"OUT_DIR: {out_dir}",
        "",
        "Safety: read-only plan. Do not execute listing writes until a safe remap endpoint is confirmed.",
    ]
    if not blockers.empty:
        lines.append("")
        lines.append("Top blockers:")
        for _, row in blockers.head(20).iterrows():
            lines.append(f"- {row.get('ListingSKU', '')}: {row.get('reason', '')}")
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
