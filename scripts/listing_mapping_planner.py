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
LISTING_SOURCE_COLUMNS = LISTING_COLUMNS + ["source"]
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
LOOKUP_DEBUG_COLUMNS = [
    "lookup_value",
    "lookup_type",
    "found_yes_no",
    "endpoint_source_used",
    "channel",
    "listing_id",
    "channel_identifier",
    "listing_sku",
    "product_sku",
    "mapping_status",
    "reason_if_missing",
]
LISTING_FETCH_PAGE_COLUMNS = ["page", "status_code", "row_count", "first_listing_id", "last_listing_id", "channel_counts", "stop_reason"]
ENDPOINT_PROBE_COLUMNS = ["endpoint", "params_json", "status_code", "row_count", "contains_lookup", "matched_terms", "reason"]
OVERVIEW_ENDPOINT_PROBE_COLUMNS = [
    "endpoint",
    "params_json",
    "status_code",
    "row_count",
    "contains_listing_sku",
    "contains_channel_identifier",
    "error_or_reason",
    "sample_keys",
    "detected_columns",
]
COVERAGE_SUMMARY_COLUMNS = ["metric", "value"]

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
    parser.add_argument("--max-pages", type=int, default=None, help="Optional safety cap for paged API reads")
    parser.add_argument("--fetch-all-listings", action="store_true")
    parser.add_argument("--listing-filter-after-fetch", action="store_true")
    parser.add_argument("--max-listing-rows", type=int, default=0, help="Optional listing row cap; 0 means no cap")
    parser.add_argument("--no-raw-listings-report", dest="raw_listings_report", action="store_false")
    parser.add_argument("--listing-sku", action="append", default=[], help="Targeted listing SKU lookup diagnostic")
    parser.add_argument("--channel-identifier", action="append", default=[], help="Targeted channel identifier/ASIN lookup diagnostic")
    parser.add_argument("--listing-id", action="append", default=[], help="Optional targeted listing ID/detail lookup diagnostic")
    parser.add_argument("--listings-overview-export", type=Path, help="Optional StoreFeeder Listings Overview CSV export")
    parser.add_argument("--overview-search-sku", action="append", default=[], help="Targeted Listings Overview SKU search diagnostic")
    parser.add_argument("--overview-search-channel-identifier", action="append", default=[], help="Targeted Listings Overview ASIN/channel identifier search diagnostic")
    parser.add_argument("--overview-search-parent", action="append", default=[], help="Targeted Listings Overview parent/family search diagnostic")
    parser.set_defaults(raw_listings_report=True)
    args = parser.parse_args()
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    if args.max_pages is not None and args.max_pages < 1:
        parser.error("--max-pages must be at least 1")
    if args.max_listing_rows < 0:
        parser.error("--max-listing-rows must be zero or greater")
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

    products = _fetch_products(client, page_size=args.page_size, max_pages=args.max_pages or 500)
    product_index = _build_product_index(products)
    clean_products = _build_clean_products(client, manifest, product_index)
    clean_products_df = pd.DataFrame(clean_products, columns=PRODUCT_COLUMNS)
    _write_csv(clean_products_df, out_dir / "01_products.csv")

    all_listings, listings, listing_fetch_meta, listing_fetch_pages = _fetch_listings(
        client,
        args.channel,
        page_size=args.page_size,
        max_pages=args.max_pages,
        fetch_all=args.fetch_all_listings,
        filter_after_fetch=args.listing_filter_after_fetch or args.fetch_all_listings,
        max_listing_rows=args.max_listing_rows,
    )
    api_all_listings_df = pd.DataFrame(all_listings, columns=LISTING_COLUMNS)
    overview_listings_df = _load_listings_overview_export(args.listings_overview_export)
    overview_probe_results, overview_lookup_debug, overview_fetch_pages, overview_probe_listings_df = _probe_overview_sources(
        client,
        search_skus=args.overview_search_sku,
        channel_identifiers=args.overview_search_channel_identifier,
        parent_terms=args.overview_search_parent,
        channel=args.channel,
    )
    if not overview_probe_listings_df.empty:
        overview_listings_df = pd.concat([overview_listings_df, overview_probe_listings_df], ignore_index=True)
    if args.listings_overview_export:
        _write_csv(overview_listings_df, out_dir / "listing_overview_export_normalized.csv")
    elif not overview_listings_df.empty:
        _write_csv(overview_listings_df, out_dir / "listing_overview_export_normalized.csv")
    all_listings_df = _combine_listing_sources(api_all_listings_df, overview_listings_df)
    listings_df = all_listings_df[all_listings_df.apply(lambda row: _channel_matches(row.to_dict(), args.channel), axis=1)].copy()
    listings_df = listings_df.reindex(columns=LISTING_SOURCE_COLUMNS, fill_value="")
    if args.raw_listings_report:
        _write_csv(all_listings_df, out_dir / "02_all_listings_raw.csv")
    _write_csv(listings_df, out_dir / "02_listings.csv")
    unique_channels = _unique_listing_channels(all_listings_df)
    search_hits = _listing_channel_search_hits(all_listings_df)
    _write_csv(unique_channels, out_dir / "unique_listing_channels.csv")
    _write_csv(search_hits, out_dir / "listing_channel_search_hits.csv")
    _write_csv(pd.DataFrame(listing_fetch_pages, columns=LISTING_FETCH_PAGE_COLUMNS), out_dir / "listing_fetch_pages.csv")
    endpoint_probe_results = _probe_listing_endpoints(
        client,
        listing_skus=args.listing_sku,
        channel_identifiers=args.channel_identifier,
        channel=args.channel,
    )
    _write_csv(endpoint_probe_results, out_dir / "listing_endpoint_probe_results.csv")
    _write_csv(overview_probe_results, out_dir / "overview_endpoint_probe_results.csv")
    _write_csv(overview_lookup_debug, out_dir / "overview_lookup_debug.csv")
    _write_csv(overview_fetch_pages, out_dir / "overview_fetch_pages.csv")
    lookup_debug = _listing_lookup_debug(
        client,
        all_listings_df.to_dict("records"),
        listing_skus=args.listing_sku,
        channel_identifiers=args.channel_identifier,
        listing_ids=args.listing_id,
    )
    _write_csv(lookup_debug, out_dir / "listing_lookup_debug.csv")
    coverage_summary = _listing_source_coverage_summary(
        api_all_listings_df,
        overview_listings_df,
        listings_df,
        lookup_debug,
        endpoint_probe_results,
        overview_probe_results=overview_probe_results,
        overview_lookup_debug=overview_lookup_debug,
    )
    _write_csv(coverage_summary, out_dir / "listing_source_coverage_summary.csv")

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
        {"metric": "total_listing_rows_fetched", "value": len(all_listings_df)},
        {"metric": "filtered_listing_rows", "value": len(listings_df)},
        {"metric": "unique_channel_count", "value": len(unique_channels)},
        {"metric": "fetch_all_listings", "value": "yes" if args.fetch_all_listings else "no"},
        {"metric": "listing_pages_scanned", "value": listing_fetch_meta.get("pages_scanned", 0)},
        {"metric": "listing_fetch_stop_reason", "value": listing_fetch_meta.get("stop_reason", "")},
        {"metric": "targeted_lookup_rows", "value": len(lookup_debug)},
        {"metric": "targeted_lookup_found_rows", "value": _count_eq(lookup_debug, "found_yes_no", "yes")},
        {"metric": "api_listing_rows", "value": len(api_all_listings_df)},
        {"metric": "overview_export_listing_rows", "value": len(overview_listings_df)},
        {"metric": "endpoint_probe_rows", "value": len(endpoint_probe_results)},
        {"metric": "endpoint_probe_contains_lookup_rows", "value": _count_eq(endpoint_probe_results, "contains_lookup", "yes")},
        {"metric": "overview_endpoint_probe_rows", "value": len(overview_probe_results)},
        {"metric": "overview_endpoint_probe_contains_listing_sku_rows", "value": _count_eq(overview_probe_results, "contains_listing_sku", "yes")},
        {"metric": "overview_endpoint_probe_contains_channel_identifier_rows", "value": _count_eq(overview_probe_results, "contains_channel_identifier", "yes")},
        {"metric": "overview_lookup_found_rows", "value": _count_eq(overview_lookup_debug, "found_yes_no", "yes")},
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
    _, filtered_listings, _, _ = _fetch_listings(
        client,
        args.channel,
        page_size=args.page_size,
        max_pages=args.max_pages,
        fetch_all=args.fetch_all_listings,
        filter_after_fetch=args.listing_filter_after_fetch or args.fetch_all_listings,
        max_listing_rows=args.max_listing_rows,
    )
    listings = pd.DataFrame(filtered_listings, columns=LISTING_COLUMNS)
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


def _fetch_listings(
    client: StoreFeederApiClient,
    channel: str,
    *,
    page_size: int,
    max_pages: int | None,
    fetch_all: bool,
    filter_after_fetch: bool,
    max_listing_rows: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    all_rows: list[dict[str, Any]] = []
    filtered_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    page = 1
    stop_reason = "not_started"
    while True:
        if max_pages is not None and page > max_pages:
            stop_reason = "max_pages_reached"
            break
        wrapper = client.get_path("/listings", params={"page": page, "pageSize": page_size})
        status_code = int(wrapper.get("_status_code", 0))
        if status_code >= 400:
            stop_reason = f"http_{status_code}"
            page_rows.append(_listing_fetch_page_row(page, status_code, [], stop_reason))
            break
        payload = wrapper.get("response", {})
        items = _extract_records(payload)
        if not items:
            stop_reason = "empty_page"
            page_rows.append(_listing_fetch_page_row(page, status_code, [], stop_reason))
            print(f"listings page {page}: 0 scanned, total {len(all_rows)}", flush=True)
            break
        page_listing_rows = [_listing_row(item) for item in items]
        for item in items:
            row = _listing_row(item)
            all_rows.append(row)
            if not filter_after_fetch and _channel_matches(row, channel):
                filtered_rows.append(row)
            if max_listing_rows and len(all_rows) >= max_listing_rows:
                stop_reason = "max_listing_rows_reached"
                break
        print(f"listings page {page}: {len(items)} scanned, total {len(all_rows)}", flush=True)
        if stop_reason == "max_listing_rows_reached":
            page_rows.append(_listing_fetch_page_row(page, status_code, page_listing_rows, stop_reason))
            break
        if len(items) < page_size:
            stop_reason = "short_page"
            page_rows.append(_listing_fetch_page_row(page, status_code, page_listing_rows, stop_reason))
            break
        if not fetch_all and _is_last_page(payload, page, page_size, len(items)):
            stop_reason = "api_last_page"
            page_rows.append(_listing_fetch_page_row(page, status_code, page_listing_rows, stop_reason))
            break
        page_rows.append(_listing_fetch_page_row(page, status_code, page_listing_rows, ""))
        page += 1
    if filter_after_fetch:
        filtered_rows = [row for row in all_rows if _channel_matches(row, channel)]
    meta = {"pages_scanned": page if stop_reason != "not_started" else 0, "stop_reason": stop_reason}
    if page_rows and not page_rows[-1].get("stop_reason"):
        page_rows[-1]["stop_reason"] = stop_reason
    return all_rows, filtered_rows, meta, page_rows


def _combine_listing_sources(api_listings: pd.DataFrame, overview_listings: pd.DataFrame) -> pd.DataFrame:
    api = api_listings.copy()
    if api.empty:
        api = pd.DataFrame(columns=LISTING_COLUMNS)
    api["source"] = "/listings"
    overview = overview_listings.copy()
    if overview.empty:
        overview = pd.DataFrame(columns=LISTING_SOURCE_COLUMNS)
    for frame in [api, overview]:
        for column in LISTING_SOURCE_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""
    combined = pd.concat([api[LISTING_SOURCE_COLUMNS], overview[LISTING_SOURCE_COLUMNS]], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=LISTING_SOURCE_COLUMNS)
    return combined.drop_duplicates(subset=["source", "ListingID", "ListingSKU", "ASIN", "raw_json"], keep="first").reset_index(drop=True)


def _load_listings_overview_export(path: Path | None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=LISTING_SOURCE_COLUMNS)
    if not path.exists():
        raise SystemExit(f"Listings Overview export not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    rows = []
    for _, item in df.iterrows():
        payload = item.to_dict()
        row = {
            "Channel": _first_text(payload, ["Channel", "Channel Name", "Sales Channel", "Marketplace"]),
            "ListingID": _first_text(payload, ["ListingID", "Listing ID", "ID", "Id"]),
            "ListingVariantID": _first_text(payload, ["ListingVariantID", "Listing Variant ID", "VariantID", "Variant ID"]),
            "ListingSKU": _first_text(payload, ["Listing SKU", "ListingSKU", "SKU", "Channel SKU", "Seller SKU"]),
            "ListingTitle": _first_text(payload, ["Title", "Listing Title", "Name", "Product Name"]),
            "CurrentProductID": _first_text(payload, ["ProductID", "Product ID", "StoreFeeder Product ID", "Mapped Product ID"]),
            "CurrentProductSKU": _first_text(payload, ["Product SKU", "ProductSKU", "Mapped Product SKU", "StoreFeeder SKU"]),
            "ListingStatus": _first_text(payload, ["Status", "Listing Status", "Mapping Status"]),
            "Marketplace": _first_text(payload, ["Marketplace", "Market Place"]),
            "ASIN": _first_text(payload, ["ASIN", "Channel Identifier", "ChannelIdentifier", "Channel ID"]),
            "raw_json": json.dumps(payload, default=str, ensure_ascii=False),
            "source": f"ui_export:{path}",
        }
        rows.append(row)
    return pd.DataFrame(rows, columns=LISTING_SOURCE_COLUMNS)


def _probe_listing_endpoints(
    client: StoreFeederApiClient,
    *,
    listing_skus: list[str],
    channel_identifiers: list[str],
    channel: str,
) -> pd.DataFrame:
    lookup_terms = [value for value in [*listing_skus, *channel_identifiers, channel] if str(value).strip()]
    if not lookup_terms:
        return pd.DataFrame(columns=ENDPOINT_PROBE_COLUMNS)
    probes: list[tuple[str, dict[str, Any]]] = []
    endpoints = ["/listings", "/channel-listings", "/channellistings", "/channel/listings", "/amazon/listings"]
    for endpoint in endpoints:
        probes.append((endpoint, {"page": 1, "pageSize": 100}))
        for sku in listing_skus:
            if not str(sku).strip():
                continue
            probes.extend(
                [
                    (endpoint, {"page": 1, "pageSize": 100, "sku": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "SKU": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "listingSku": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "search": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "q": sku}),
                ]
            )
        for identifier in channel_identifiers:
            if not str(identifier).strip():
                continue
            probes.extend(
                [
                    (endpoint, {"page": 1, "pageSize": 100, "asin": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "ASIN": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "channelIdentifier": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "search": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "q": identifier}),
                ]
            )
        if channel:
            probes.extend(
                [
                    (endpoint, {"page": 1, "pageSize": 100, "channel": channel}),
                    (endpoint, {"page": 1, "pageSize": 100, "channelName": channel}),
                    (endpoint, {"page": 1, "pageSize": 100, "marketplace": channel}),
                    (endpoint, {"page": 1, "pageSize": 100, "mapped": "false"}),
                    (endpoint, {"page": 1, "pageSize": 100, "unmapped": "true"}),
                    (endpoint, {"page": 1, "pageSize": 100, "productSku": "Undefined"}),
                    (endpoint, {"page": 1, "pageSize": 100, "includeInactive": "true"}),
                ]
            )
    rows = []
    seen: set[tuple[str, str]] = set()
    for endpoint, params in probes:
        key = (endpoint, json.dumps(params, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        try:
            wrapper = client.get_path(endpoint, params=params)
            status = int(wrapper.get("_status_code", 0))
            payload = wrapper.get("response", {})
            raw = json.dumps(payload, default=str, ensure_ascii=False)
            records = _extract_records(payload)
            matched_terms = [term for term in lookup_terms if term.casefold() in raw.casefold()]
            rows.append(
                {
                    "endpoint": endpoint,
                    "params_json": json.dumps(params, sort_keys=True),
                    "status_code": status,
                    "row_count": len(records),
                    "contains_lookup": "yes" if matched_terms else "no",
                    "matched_terms": "|".join(matched_terms),
                    "reason": "" if status < 400 else f"http_{status}",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "endpoint": endpoint,
                    "params_json": json.dumps(params, sort_keys=True),
                    "status_code": "",
                    "row_count": 0,
                    "contains_lookup": "no",
                    "matched_terms": "",
                    "reason": f"exception:{exc}",
                }
            )
    return pd.DataFrame(rows, columns=ENDPOINT_PROBE_COLUMNS)


def _probe_overview_sources(
    client: StoreFeederApiClient,
    *,
    search_skus: list[str],
    channel_identifiers: list[str],
    parent_terms: list[str],
    channel: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    lookup_skus = [str(value).strip() for value in [*search_skus, *parent_terms] if str(value).strip()]
    lookup_identifiers = [str(value).strip() for value in channel_identifiers if str(value).strip()]
    if not lookup_skus and not lookup_identifiers:
        return (
            pd.DataFrame(columns=OVERVIEW_ENDPOINT_PROBE_COLUMNS),
            pd.DataFrame(columns=LOOKUP_DEBUG_COLUMNS),
            pd.DataFrame(columns=LISTING_FETCH_PAGE_COLUMNS),
            pd.DataFrame(columns=LISTING_SOURCE_COLUMNS),
        )
    endpoints = [
        "/listings/overview",
        "/Listings/Overview",
        "/Listings/Overview.aspx",
        "/listings/search",
        "/Listings/Search",
        "/listing-overview",
        "/listingoverview",
        "/channel-listings",
        "/channel-listings/search",
        "/channellistings",
        "/channellistings/search",
        "/marketplace-listings",
        "/marketplace-listings/search",
        "/marketplacelistings",
        "/amazon/listings",
        "/amazon/listings/search",
    ]
    probes: list[tuple[str, dict[str, Any]]] = []
    for endpoint in endpoints:
        base_params = {"page": 1, "pageSize": 100}
        probes.append((endpoint, base_params))
        for sku in lookup_skus:
            probes.extend(
                [
                    (endpoint, {"page": 1, "pageSize": 100, "sku": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "SKU": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "listingSku": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "listingSKU": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "search": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "q": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "parent": sku}),
                    (endpoint, {"page": 1, "pageSize": 100, "parentSku": sku}),
                ]
            )
        for identifier in lookup_identifiers:
            probes.extend(
                [
                    (endpoint, {"page": 1, "pageSize": 100, "asin": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "ASIN": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "channelIdentifier": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "channel_identifier": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "search": identifier}),
                    (endpoint, {"page": 1, "pageSize": 100, "q": identifier}),
                ]
            )
        probes.extend(
            [
                (endpoint, {"page": 1, "pageSize": 100, "channel": channel}),
                (endpoint, {"page": 1, "pageSize": 100, "channelName": channel}),
                (endpoint, {"page": 1, "pageSize": 100, "marketplace": channel}),
                (endpoint, {"page": 1, "pageSize": 100, "mapped": "false"}),
                (endpoint, {"page": 1, "pageSize": 100, "unmapped": "true"}),
                (endpoint, {"page": 1, "pageSize": 100, "mappingStatus": "unmapped"}),
                (endpoint, {"page": 1, "pageSize": 100, "productSku": "Undefined"}),
                (endpoint, {"page": 1, "pageSize": 100, "productSKU": "Undefined"}),
                (endpoint, {"page": 1, "pageSize": 100, "includeInactive": "true"}),
                (endpoint, {"page": 1, "pageSize": 100, "includeControlled": "true"}),
                (endpoint, {"page": 1, "pageSize": 100, "includeDiscontinued": "true"}),
            ]
        )
    probe_rows: list[dict[str, Any]] = []
    lookup_rows: list[dict[str, Any]] = []
    fetch_page_rows: list[dict[str, Any]] = []
    normalized_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for endpoint, params in probes:
        key = (endpoint, json.dumps(params, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        status_code = ""
        records: list[dict[str, Any]] = []
        raw = ""
        reason = ""
        try:
            wrapper = client.get_path(endpoint, params=params)
            status_code = int(wrapper.get("_status_code", 0))
            payload = wrapper.get("response", {})
            raw = json.dumps(payload, default=str, ensure_ascii=False)
            records = _extract_records(payload)
            if not records and isinstance(payload, dict) and status_code < 400:
                records = [payload]
            reason = "" if status_code < 400 else f"http_{status_code}"
        except Exception as exc:
            reason = f"exception:{exc}"
        contains_sku = any(term.casefold() in raw.casefold() for term in lookup_skus)
        contains_identifier = any(term.casefold() in raw.casefold() for term in lookup_identifiers)
        sample_keys, detected_columns = _record_shape(records)
        probe_rows.append(
            {
                "endpoint": endpoint,
                "params_json": json.dumps(params, sort_keys=True),
                "status_code": status_code,
                "row_count": len(records),
                "contains_listing_sku": "yes" if contains_sku else "no",
                "contains_channel_identifier": "yes" if contains_identifier else "no",
                "error_or_reason": reason,
                "sample_keys": sample_keys,
                "detected_columns": detected_columns,
            }
        )
        fetch_page_rows.append(
            _listing_fetch_page_row(
                len(fetch_page_rows) + 1,
                int(status_code) if str(status_code).isdigit() else 0,
                [_listing_row(record) for record in records if isinstance(record, dict)],
                reason,
            )
        )
        if not (contains_sku or contains_identifier):
            continue
        for record in records:
            row = _listing_row(record)
            row["source"] = f"overview_probe:{endpoint}"
            normalized_rows.append(row)
            for value in lookup_skus:
                if value.casefold() in json.dumps(record, default=str, ensure_ascii=False).casefold():
                    lookup_rows.append(_listing_lookup_row(value, "overview_search_sku", row, row["source"], ""))
            for value in lookup_identifiers:
                if value.casefold() in json.dumps(record, default=str, ensure_ascii=False).casefold():
                    lookup_rows.append(_listing_lookup_row(value, "overview_search_channel_identifier", row, row["source"], ""))
    if not lookup_rows:
        for value in lookup_skus:
            lookup_rows.append(_overview_missing_lookup_row(value, "overview_search_sku"))
        for value in lookup_identifiers:
            lookup_rows.append(_overview_missing_lookup_row(value, "overview_search_channel_identifier"))
    return (
        pd.DataFrame(probe_rows, columns=OVERVIEW_ENDPOINT_PROBE_COLUMNS),
        pd.DataFrame(lookup_rows, columns=LOOKUP_DEBUG_COLUMNS),
        pd.DataFrame(fetch_page_rows, columns=LISTING_FETCH_PAGE_COLUMNS),
        pd.DataFrame(normalized_rows, columns=LISTING_SOURCE_COLUMNS),
    )


def _record_shape(records: list[dict[str, Any]]) -> tuple[str, str]:
    if not records:
        return "", ""
    sample = records[0]
    if not isinstance(sample, dict):
        return "", ""
    keys = sorted(str(key) for key in sample.keys())
    detected = [key for key in keys if any(token in key.casefold() for token in ["sku", "asin", "channel", "listing", "product", "mapped", "status"])]
    return "|".join(keys[:40]), "|".join(detected[:40])


def _overview_missing_lookup_row(value: str, lookup_type: str) -> dict[str, Any]:
    return {
        "lookup_value": value,
        "lookup_type": lookup_type,
        "found_yes_no": "no",
        "endpoint_source_used": "overview endpoint probes",
        "channel": "",
        "listing_id": "",
        "channel_identifier": "",
        "listing_sku": "",
        "product_sku": "",
        "mapping_status": "",
        "reason_if_missing": "not_found_in_overview_endpoint_probes",
    }


def _channel_matches(row: dict[str, str], channel: str) -> bool:
    if not channel:
        return True
    channel_key = channel.casefold().strip()
    exact_fields = [
        row.get("Channel", ""),
        row.get("Marketplace", ""),
    ]
    if any(str(value).casefold().strip() == channel_key for value in exact_fields):
        return True
    text = " ".join(
        [
            row.get("Channel", ""),
            row.get("Marketplace", ""),
            row.get("ListingTitle", ""),
            row.get("raw_json", ""),
        ]
    ).casefold()
    return channel_key in text


def _listing_fetch_page_row(page: int, status_code: int, rows: list[dict[str, str]], stop_reason: str) -> dict[str, Any]:
    ids = [str(row.get("ListingID", "")).strip() for row in rows if str(row.get("ListingID", "")).strip()]
    counts: dict[str, int] = {}
    for row in rows:
        channel = str(row.get("Channel", "")).strip() or "(blank)"
        marketplace = str(row.get("Marketplace", "")).strip()
        key = channel if not marketplace else f"{channel} / {marketplace}"
        counts[key] = counts.get(key, 0) + 1
    return {
        "page": page,
        "status_code": status_code,
        "row_count": len(rows),
        "first_listing_id": ids[0] if ids else "",
        "last_listing_id": ids[-1] if ids else "",
        "channel_counts": json.dumps(counts, ensure_ascii=False, sort_keys=True),
        "stop_reason": stop_reason,
    }


def _listing_lookup_debug(
    client: StoreFeederApiClient,
    all_listings: list[dict[str, Any]],
    *,
    listing_skus: list[str],
    channel_identifiers: list[str],
    listing_ids: list[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    lookups = []
    lookups.extend(("listing_sku", value) for value in listing_skus if str(value).strip())
    lookups.extend(("channel_identifier", value) for value in channel_identifiers if str(value).strip())
    lookups.extend(("listing_id", value) for value in listing_ids if str(value).strip())
    for lookup_type, value in lookups:
        matches = _listing_lookup_matches(all_listings, lookup_type, value, "/listings paged fetch")
        if lookup_type == "listing_id":
            matches.extend(_listing_detail_lookup(client, value))
        if matches:
            rows.extend(matches)
        else:
            rows.append(
                {
                    "lookup_value": value,
                    "lookup_type": lookup_type,
                    "found_yes_no": "no",
                    "endpoint_source_used": "/listings paged fetch" if lookup_type != "listing_id" else "/listings paged fetch + detail probes",
                    "channel": "",
                    "listing_id": "",
                    "channel_identifier": "",
                    "listing_sku": "",
                    "product_sku": "",
                    "mapping_status": "",
                    "reason_if_missing": "not_found_in_fetched_listing_source",
                }
            )
    return pd.DataFrame(rows, columns=LOOKUP_DEBUG_COLUMNS)


def _listing_lookup_matches(listings: list[dict[str, Any]], lookup_type: str, value: str, source: str) -> list[dict[str, Any]]:
    key = str(value).strip().casefold()
    rows = []
    for row in listings:
        row_text = str(row.get("raw_json", "")).casefold()
        if lookup_type == "listing_sku":
            matched = str(row.get("ListingSKU", "")).strip().casefold() == key or key in row_text
        elif lookup_type == "channel_identifier":
            matched = (
                str(row.get("ASIN", "")).strip().casefold() == key
                or key in row_text
            )
        else:
            matched = str(row.get("ListingID", "")).strip().casefold() == key or key in row_text
        if matched:
            rows.append(_listing_lookup_row(value, lookup_type, row, source, ""))
    return rows


def _listing_detail_lookup(client: StoreFeederApiClient, listing_id: str) -> list[dict[str, Any]]:
    rows = []
    for path in [f"/listings/{listing_id}", f"/channel-listings/{listing_id}", f"/channellistings/{listing_id}"]:
        wrapper = client.get_path(path)
        status = int(wrapper.get("_status_code", 0))
        if status >= 400:
            continue
        records = _extract_records(wrapper.get("response", {}))
        if not records and isinstance(wrapper.get("response"), dict):
            records = [wrapper["response"]]
        for record in records:
            row = _listing_row(record)
            rows.append(_listing_lookup_row(listing_id, "listing_id", row, f"GET {path}", ""))
    return rows


def _listing_lookup_row(value: str, lookup_type: str, row: dict[str, Any], source: str, reason: str) -> dict[str, Any]:
    source_used = str(row.get("source", "")).strip() or source
    return {
        "lookup_value": value,
        "lookup_type": lookup_type,
        "found_yes_no": "yes",
        "endpoint_source_used": source_used,
        "channel": row.get("Channel", ""),
        "listing_id": row.get("ListingID", ""),
        "channel_identifier": row.get("ASIN", "") or _raw_first_text(row.get("raw_json", ""), ["ChannelIdentifier", "Channel Identifier", "ASIN", "asin"]),
        "listing_sku": row.get("ListingSKU", ""),
        "product_sku": row.get("CurrentProductSKU", ""),
        "mapping_status": _mapping_status(row),
        "reason_if_missing": reason,
    }


def _raw_first_text(raw_json: str, keys: list[str]) -> str:
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError):
        return ""
    return _first_text(payload, keys)


def _mapping_status(row: dict[str, Any]) -> str:
    current_product_sku = str(row.get("CurrentProductSKU", "")).strip()
    current_product_id = str(row.get("CurrentProductID", "")).strip()
    raw = str(row.get("raw_json", "")).casefold()
    if current_product_sku or current_product_id:
        return "mapped"
    if "create-map" in raw or "unmapped" in raw or "undefined" in raw:
        return "unmapped"
    return ""


def _listing_source_coverage_summary(
    api_listings: pd.DataFrame,
    overview_listings: pd.DataFrame,
    filtered_listings: pd.DataFrame,
    lookup_debug: pd.DataFrame,
    endpoint_probe_results: pd.DataFrame,
    *,
    overview_probe_results: pd.DataFrame,
    overview_lookup_debug: pd.DataFrame,
) -> pd.DataFrame:
    rows = [
        {"metric": "api_listing_rows", "value": len(api_listings)},
        {"metric": "overview_export_listing_rows", "value": len(overview_listings)},
        {"metric": "combined_listing_rows", "value": len(api_listings) + len(overview_listings)},
        {"metric": "filtered_listing_rows", "value": len(filtered_listings)},
        {"metric": "targeted_lookup_rows", "value": len(lookup_debug)},
        {"metric": "targeted_lookup_found_rows", "value": _count_eq(lookup_debug, "found_yes_no", "yes")},
        {"metric": "endpoint_probe_rows", "value": len(endpoint_probe_results)},
        {"metric": "endpoint_probe_contains_lookup_rows", "value": _count_eq(endpoint_probe_results, "contains_lookup", "yes")},
        {"metric": "overview_endpoint_probe_rows", "value": len(overview_probe_results)},
        {"metric": "overview_endpoint_contains_listing_sku_rows", "value": _count_eq(overview_probe_results, "contains_listing_sku", "yes")},
        {"metric": "overview_endpoint_contains_channel_identifier_rows", "value": _count_eq(overview_probe_results, "contains_channel_identifier", "yes")},
        {"metric": "overview_lookup_rows", "value": len(overview_lookup_debug)},
        {"metric": "overview_lookup_found_rows", "value": _count_eq(overview_lookup_debug, "found_yes_no", "yes")},
    ]
    if not lookup_debug.empty:
        missing = lookup_debug[lookup_debug["found_yes_no"].astype(str).str.casefold().eq("no")]
        rows.append({"metric": "targeted_lookup_missing_rows", "value": len(missing)})
    return pd.DataFrame(rows, columns=COVERAGE_SUMMARY_COLUMNS)


def _unique_listing_channels(listings: pd.DataFrame) -> pd.DataFrame:
    columns = ["Channel", "Marketplace", "count"]
    if listings.empty:
        return pd.DataFrame(columns=columns)
    rows = listings.copy()
    for column in ["Channel", "Marketplace"]:
        if column not in rows.columns:
            rows[column] = ""
        rows[column] = rows[column].fillna("").astype(str).str.strip()
    return rows.groupby(["Channel", "Marketplace"], dropna=False).size().reset_index(name="count").sort_values("count", ascending=False).reset_index(drop=True)


def _listing_channel_search_hits(listings: pd.DataFrame) -> pd.DataFrame:
    if listings.empty:
        return pd.DataFrame(columns=LISTING_COLUMNS + ["matched_terms"])
    terms = ["Custom", "Side", "CustomSide", "Amazon"]
    rows = []
    for _, row in listings.iterrows():
        raw = str(row.get("raw_json", ""))
        matched = [term for term in terms if term.casefold() in raw.casefold()]
        if not matched:
            continue
        out = {column: row.get(column, "") for column in LISTING_COLUMNS}
        out["matched_terms"] = "|".join(matched)
        rows.append(out)
    return pd.DataFrame(rows, columns=LISTING_COLUMNS + ["matched_terms"])

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
