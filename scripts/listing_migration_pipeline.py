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

    for key in ["Items", "items", "Data", "data", "Results", "results", "Listings", "listings", "Products", "products"]:
        value = payload.get(key)
        if isinstance(value, list):
            return value

    return []


def first_value(obj: Any, keys: list[str]) -> str:
    if not isinstance(obj, dict):
        return ""

    for key in keys:
        if key in obj and obj[key] not in [None, ""]:
            return str(obj[key]).strip()

    for value in obj.values():
        if isinstance(value, dict):
            found = first_value(value, keys)
            if found:
                return found

    return ""


def json_text(obj: Any) -> str:
    return json.dumps(obj, default=str, ensure_ascii=False)


def parse_expected_skus(raw: str) -> set[str]:
    return {x.strip().upper() for x in str(raw or "").split(",") if x.strip()}


def supplier_id_maps(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = ["supplier", "SupplierID", "Supplier.Name"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError("supplier ID map missing columns: " + ", ".join(missing))
    ids: dict[str, str] = {}
    names: dict[str, str] = {}
    for _, row in df.iterrows():
        key = str(row["supplier"]).strip().casefold()
        if key:
            ids[key] = str(row["SupplierID"]).strip()
            names[key] = str(row["Supplier.Name"]).strip()
    return ids, names


def build_supplier_stock_index(ralawise_stock_path: Path, uneek_stock_path: Path) -> dict[str, list[dict[str, str]]]:
    from src.stock_mapping import build_supplier_stock_lookup

    ralawise = pd.read_csv(ralawise_stock_path, dtype=str, keep_default_na=False)
    uneek = pd.read_csv(uneek_stock_path, dtype=str, keep_default_na=False)
    stock = build_supplier_stock_lookup(ralawise, uneek)
    index: dict[str, list[dict[str, str]]] = {}
    for _, row in stock.iterrows():
        key = str(row.get("supplier_sku", "")).strip().upper()
        if not key:
            continue
        index.setdefault(key, []).append(
            {
                "supplier": str(row.get("supplier", "")).strip(),
                "supplier_sku": key,
                "supplier_free_stock": str(row.get("supplier_free_stock", "")).strip(),
            }
        )
    return index


COLOR_ALIASES = {
    "BLK": "BLAC",
    "BLACK": "BLAC",
    "NVY": "NAVY",
    "NAV": "NAVY",
    "NAVY": "NAVY",
    "RED": "REDD",
    "REDD": "REDD",
    "FUCH": "FUCH",
    "FUCHSIA": "FUCH",
    "WHI": "WHIT",
    "WHT": "WHIT",
    "WHITE": "WHIT",
    "FORE": "FORE",
    "FOR": "FORE",
    "FOREST": "FORE",
}


def derive_target_sku_from_listing_sku(
    listing_sku: str,
    target_prefix: str,
    target_lookup: dict[str, dict[str, str]],
) -> tuple[str, str]:
    sku = str(listing_sku or "").strip().upper()
    prefix = str(target_prefix or "").strip().upper()

    if sku in target_lookup:
        return sku, "exact_sku_match"

    if not prefix or prefix not in sku:
        return "", "no_family_prefix_in_listing_sku"

    target_suffixes = {}
    for target_sku in target_lookup:
        if target_sku.startswith(prefix):
            suffix = target_sku[len(prefix):]
            target_suffixes[suffix] = target_sku

    tokens = [t for t in re.split(r"[^A-Z0-9]+", sku) if t]

    candidate_tokens = []
    if prefix in tokens:
        idx = tokens.index(prefix)
        candidate_tokens.extend(tokens[idx + 1:])
    else:
        tail = sku.split(prefix, 1)[1]
        candidate_tokens.extend([t for t in re.split(r"[^A-Z0-9]+", tail) if t])

    candidate_tokens.extend(tokens)

    for token in candidate_tokens:
        suffix = COLOR_ALIASES.get(token, token)
        if suffix in target_suffixes:
            return target_suffixes[suffix], f"derived_colour_token:{token}->{suffix}"

    return "", "no_colour_token_match"


def product_row(item: dict[str, Any]) -> dict[str, str]:
    return {
        "ProductID": first_value(item, ["ProductID", "ProductId", "ID", "Id", "id"]),
        "SKU": first_value(item, ["SKU", "Sku", "sku"]),
        "Name": first_value(item, ["Name", "ProductName", "Title", "title"]),
    }


def listing_row(item: dict[str, Any]) -> dict[str, str]:
    return {
        "ListingID": first_value(item, ["ListingID", "ListingId", "listingId", "ID", "Id", "id"]),
        "ListingSKU": first_value(item, ["SKU", "Sku", "sku", "ListingSKU", "ListingSku", "ChannelSKU", "ChannelSku", "SellerSKU", "SellerSku", "ExternalSKU", "ExternalSku"]),
        "Title": first_value(item, ["Title", "title", "Name", "name", "ListingTitle", "ProductName"]),
        "Channel": first_value(item, ["Channel", "channel", "ChannelName", "Integration", "IntegrationName", "Marketplace", "MarketplaceName"]),
        "CurrentProductID": first_value(item, ["ProductID", "ProductId", "MappedProductID", "MappedProductId", "StoreFeederProductID", "StoreFeederProductId"]),
        "CurrentProductSKU": first_value(item, ["ProductSKU", "ProductSku", "MappedProductSKU", "MappedProductSku"]),
        "raw_json": json_text(item),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan StoreFeeder listing migration/remap for a product family.")
    mode = parser.add_mutually_exclusive_group(required=False)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--verify", action="store_true")
    mode.add_argument("--execute", action="store_true")

    parser.add_argument("--build-channel-supplier-setup", action="store_true")
    parser.add_argument("--family-code", required=True)
    parser.add_argument("--target-sku-prefix", required=True)
    parser.add_argument("--channel", default="")
    parser.add_argument("--search", default="")
    parser.add_argument("--expected-skus", default="")
    parser.add_argument("--out-root", type=Path, default=Path("reports/listing_migration"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=500)
    args = parser.parse_args()

    if not (args.plan or args.verify or args.execute or args.build_channel_supplier_setup):
        parser.error("one of --plan, --verify, --execute, or --build-channel-supplier-setup is required")

    if args.execute:
        raise SystemExit("BLOCKED: execute mode is not enabled yet. Listing remap/delete method is not confirmed. Use --plan.")

    load_env(args.env_file)

    from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig())

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / args.family_code / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    expected_skus = parse_expected_skus(args.expected_skus)

    blockers: list[dict[str, str]] = []

    # 1. Scan StoreFeeder products.
    all_products: list[dict[str, str]] = []
    target_products: list[dict[str, str]] = []
    old_product_candidates: list[dict[str, str]] = []

    page = 1
    while page <= args.max_pages:
        result = client.get_products_page(page=page, page_size=args.page_size)
        items = extract_items(result.get("response", {}))

        for item in items:
            row = product_row(item)
            row["raw_json"] = json_text(item)
            all_products.append(row)

            sku_upper = row["SKU"].upper()
            name_lower = row["Name"].casefold()
            search_lower = args.search.casefold().strip()

            if sku_upper.startswith(args.target_sku_prefix.upper()):
                target_products.append(row)
            elif search_lower and search_lower in name_lower:
                old_product_candidates.append(row)

        print(f"products page {page}: scanned {len(items)}, targets {len(target_products)}, old candidates {len(old_product_candidates)}")

        if len(items) < args.page_size:
            break
        page += 1

    target_df = pd.DataFrame(target_products).drop_duplicates(subset=["ProductID", "SKU"]) if target_products else pd.DataFrame(columns=["ProductID", "SKU", "Name", "raw_json"])
    old_products_df = pd.DataFrame(old_product_candidates).drop_duplicates(subset=["ProductID", "SKU"]) if old_product_candidates else pd.DataFrame(columns=["ProductID", "SKU", "Name", "raw_json"])

    write_csv(target_df, out_dir / "01_target_products.csv")
    write_csv(old_products_df, out_dir / "03_candidate_old_products.csv")

    if target_df.empty:
        blockers.append({"stage": "target_products", "reason": "no_target_products_found", "SKU": ""})

    found_target_skus = set(target_df["SKU"].astype(str).str.upper()) if not target_df.empty else set()
    missing_expected = sorted(expected_skus - found_target_skus)

    for sku in missing_expected:
        blockers.append({"stage": "target_products", "reason": "expected_target_sku_missing", "SKU": sku})

    write_csv(pd.DataFrame({"missing_expected_sku": missing_expected}), out_dir / "expected_target_skus_missing.csv")

    # 2. Scan listings.
    listing_candidates: list[dict[str, str]] = []

    page = 1
    while page <= args.max_pages:
        try:
            result = client.get_path("/listings", params={"page": page, "pageSize": args.page_size})
        except Exception as e:
            blockers.append({"stage": "listings", "reason": f"listings_endpoint_error: {repr(e)}", "SKU": ""})
            break

        items = extract_items(result.get("response", {}))

        for item in items:
            row = listing_row(item)
            text = json_text(item).casefold()
            search_match = bool(args.search and args.search.casefold() in text)
            prefix_match = args.target_sku_prefix.casefold() in text
            sku_match = row["ListingSKU"].upper() in found_target_skus if row["ListingSKU"] else False
            channel_match = bool(args.channel and args.channel.casefold() in text)

            is_candidate = search_match or prefix_match or sku_match
            if args.channel:
                is_candidate = is_candidate and channel_match

            if is_candidate:
                row["search_match"] = "yes" if search_match else "no"
                row["prefix_match"] = "yes" if prefix_match else "no"
                row["sku_match"] = "yes" if sku_match else "no"
                row["channel_match"] = "yes" if channel_match else "no"
                listing_candidates.append(row)

        print(f"listings page {page}: scanned {len(items)}, candidates {len(listing_candidates)}")

        if len(items) < args.page_size:
            break
        page += 1

    listings_df = pd.DataFrame(listing_candidates)
    if not listings_df.empty:
        listings_df = listings_df.drop_duplicates(subset=["ListingID", "ListingSKU", "Title"])
    else:
        listings_df = pd.DataFrame(columns=[
            "ListingID", "ListingSKU", "Title", "Channel", "CurrentProductID", "CurrentProductSKU",
            "search_match", "prefix_match", "sku_match", "channel_match", "raw_json"
        ])

    write_csv(listings_df, out_dir / "02_candidate_listings.csv")

    if listings_df.empty:
        blockers.append({"stage": "listings", "reason": "no_candidate_listings_found", "SKU": ""})

    # 3. Build exact SKU mapping manifest.
    target_lookup = {}
    for _, row in target_df.iterrows():
        target_lookup[str(row["SKU"]).strip().upper()] = {
            "TargetProductID": str(row["ProductID"]).strip(),
            "TargetSKU": str(row["SKU"]).strip(),
            "TargetName": str(row["Name"]).strip(),
        }

    mapping_rows: list[dict[str, str]] = []
    for _, listing in listings_df.iterrows():
        listing_sku = str(listing.get("ListingSKU", "")).strip().upper()
        derived_target_sku, confidence = derive_target_sku_from_listing_sku(
            listing_sku,
            args.target_sku_prefix,
            target_lookup,
        )
        target = target_lookup.get(derived_target_sku)

        if target:
            can_map = "yes"
        else:
            can_map = "no"
            blockers.append({"stage": "mapping_manifest", "reason": confidence, "SKU": listing_sku})

        mapping_rows.append({
            "ListingID": listing.get("ListingID", ""),
            "ListingSKU": listing.get("ListingSKU", ""),
            "ListingTitle": listing.get("Title", ""),
            "Channel": listing.get("Channel", ""),
            "CurrentProductID": listing.get("CurrentProductID", ""),
            "CurrentProductSKU": listing.get("CurrentProductSKU", ""),
            "TargetProductID": target["TargetProductID"] if target else "",
            "TargetSKU": target["TargetSKU"] if target else "",
            "TargetName": target["TargetName"] if target else "",
            "confidence": confidence,
            "can_map": can_map,
            "recommended_action": "map_listing_to_target_product" if can_map == "yes" else "manual_review",
        })

    mapping_df = pd.DataFrame(mapping_rows)
    write_csv(mapping_df, out_dir / "04_mapping_manifest.csv")

    channel_setup_needed = pd.DataFrame(
        columns=["ProductID", "SKU", "supplier", "supplier_sku", "supplier_free_stock", "stock_location"]
    )
    channel_setup_blockers = pd.DataFrame(
        columns=["ListingID", "ListingSKU", "CurrentProductID", "TargetSKU", "blocker_reason"]
    )
    channel_stock_targets = pd.DataFrame(
        columns=[
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
        ]
    )

    if args.build_channel_supplier_setup:
        supplier_ids, supplier_names = supplier_id_maps(args.supplier_id_map)
        stock_index = build_supplier_stock_index(args.ralawise_stock, args.uneek_stock)
        setup_rows: list[dict[str, str]] = []
        setup_blockers: list[dict[str, str]] = []
        stock_target_rows: list[dict[str, str]] = []

        for _, row in mapping_df.iterrows():
            listing_sku = str(row.get("ListingSKU", "")).strip()
            current_product_id = str(row.get("CurrentProductID", "")).strip()
            target_sku = str(row.get("TargetSKU", "")).strip().upper()
            reasons: list[str] = []
            matches = stock_index.get(target_sku, [])
            supplier = ""
            supplier_sku = target_sku
            supplier_free_stock = ""
            stock_location = ""
            supplier_id = ""
            supplier_name = ""

            if str(row.get("can_map", "")).strip().casefold() != "yes":
                reasons.append("target_sku_not_derived")
            if not current_product_id:
                reasons.append("missing_current_product_id")
            if not listing_sku:
                reasons.append("missing_listing_sku")
            if not target_sku:
                reasons.append("missing_target_sku")
            if len(matches) == 0:
                reasons.append("target_sku_missing_from_supplier_stock")
            elif len(matches) > 1:
                reasons.append("target_sku_matches_multiple_suppliers")
            else:
                match = matches[0]
                supplier = match["supplier"]
                supplier_sku = match["supplier_sku"]
                supplier_free_stock = match["supplier_free_stock"]
                stock_location = supplier
                supplier_key = supplier.casefold()
                supplier_id = supplier_ids.get(supplier_key, "")
                supplier_name = supplier_names.get(supplier_key, supplier)
                if not supplier_id:
                    reasons.append("missing_supplier_id")
                if not supplier_name:
                    reasons.append("missing_supplier_name")

            if reasons:
                setup_blockers.append(
                    {
                        "ListingID": str(row.get("ListingID", "")),
                        "ListingSKU": listing_sku,
                        "CurrentProductID": current_product_id,
                        "TargetSKU": target_sku,
                        "blocker_reason": "|".join(dict.fromkeys(reasons)),
                    }
                )
                continue

            setup_rows.append(
                {
                    "ProductID": current_product_id,
                    "SKU": listing_sku,
                    "supplier": supplier,
                    "supplier_sku": supplier_sku,
                    "supplier_free_stock": supplier_free_stock,
                    "stock_location": stock_location,
                }
            )
            stock_target_rows.append(
                {
                    "ProductID": current_product_id,
                    "SKU": listing_sku,
                    "supplier": supplier,
                    "SupplierID": supplier_id,
                    "Supplier.Name": supplier_name,
                    "SupplierSKU": supplier_sku,
                    "stock_location": stock_location,
                    "preserve_existing_locations": "yes",
                    "warehouse_safe_mode": "yes",
                    "skip_stock_location_update": "yes",
                }
            )

        channel_setup_needed = pd.DataFrame(
            setup_rows,
            columns=["ProductID", "SKU", "supplier", "supplier_sku", "supplier_free_stock", "stock_location"],
        ).drop_duplicates(subset=["ProductID", "supplier", "supplier_sku"])
        channel_setup_blockers = pd.DataFrame(
            setup_blockers,
            columns=["ListingID", "ListingSKU", "CurrentProductID", "TargetSKU", "blocker_reason"],
        )
        channel_stock_targets = pd.DataFrame(
            stock_target_rows,
            columns=[
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
            ],
        ).drop_duplicates(subset=["ProductID", "SupplierID", "SupplierSKU"])

    write_csv(channel_setup_needed, out_dir / "11_channel_supplier_setup_needed.csv")
    write_csv(channel_setup_blockers, out_dir / "12_channel_supplier_setup_blockers.csv")
    write_csv(channel_stock_targets, out_dir / "13_channel_stock_targets_supplier_only.csv")

    if not mapping_df.empty:
        dupes = mapping_df[mapping_df.duplicated(subset=["ListingSKU"], keep=False)].copy()
    else:
        dupes = pd.DataFrame()

    write_csv(dupes, out_dir / "listing_duplicate_sku_candidates.csv")

    if len(dupes):
        for sku in sorted(set(dupes["ListingSKU"].astype(str))):
            blockers.append({"stage": "mapping_manifest", "reason": "multiple_candidate_listings_for_same_sku", "SKU": sku})

    # 4. Delete candidates AFTER remap only.
    delete_rows = []
    for _, product in old_products_df.iterrows():
        delete_rows.append({
            "ProductID": product.get("ProductID", ""),
            "SKU": product.get("SKU", ""),
            "Name": product.get("Name", ""),
            "safe_to_delete_now": "no",
            "delete_condition": "only_after_all_listings_are_verified_mapped_to_target_products",
            "recommended_action": "manual_review_then_archive_or_delete_old_duplicate_product",
        })

    delete_df = pd.DataFrame(delete_rows)
    write_csv(delete_df, out_dir / "05_delete_candidates_after_remap.csv")

    # 5. Verify whether candidate listings are already mapped to target products.
    verification_rows = []
    for _, row in mapping_df.iterrows():
        current_product_id = str(row.get("CurrentProductID", "")).strip()
        target_product_id = str(row.get("TargetProductID", "")).strip()
        can_map = str(row.get("can_map", "")).strip().casefold() == "yes"
        remap_verified = bool(can_map and current_product_id and target_product_id and current_product_id == target_product_id)

        verification_rows.append({
            "ListingID": row.get("ListingID", ""),
            "ListingSKU": row.get("ListingSKU", ""),
            "CurrentProductID": current_product_id,
            "CurrentProductSKU": row.get("CurrentProductSKU", ""),
            "TargetProductID": target_product_id,
            "TargetSKU": row.get("TargetSKU", ""),
            "confidence": row.get("confidence", ""),
            "can_map": row.get("can_map", ""),
            "remap_verified": "yes" if remap_verified else "no",
            "verification_status": "mapped_to_target" if remap_verified else "still_on_old_product",
        })

    verification_df = pd.DataFrame(verification_rows)
    write_csv(verification_df, out_dir / "09_remap_verification.csv")

    verified_remap_rows = int(verification_df["remap_verified"].astype(str).str.casefold().eq("yes").sum()) if not verification_df.empty else 0
    not_verified_remap_rows = int(verification_df["remap_verified"].astype(str).str.casefold().ne("yes").sum()) if not verification_df.empty else 0
    all_remaps_verified = bool(len(verification_df) > 0 and not_verified_remap_rows == 0)

    safe_delete_rows = []
    for _, row in delete_df.iterrows():
        safe_delete_rows.append({
            "ProductID": row.get("ProductID", ""),
            "SKU": row.get("SKU", ""),
            "Name": row.get("Name", ""),
            "safe_to_delete_now": "yes" if all_remaps_verified else "no",
            "delete_condition": "all_candidate_listings_verified_mapped_to_target_products",
            "verification_rows": len(verification_df),
            "verified_remap_rows": verified_remap_rows,
            "not_verified_remap_rows": not_verified_remap_rows,
            "recommended_action": "archive_or_delete_old_duplicate_product" if all_remaps_verified else "do_not_delete_yet",
        })

    safe_delete_df = pd.DataFrame(safe_delete_rows)
    write_csv(safe_delete_df, out_dir / "10_safe_delete_queue.csv")

    # 6. Endpoint capability probes.
    probes = []
    for path in ["/listings", "/listings/0"]:
        try:
            response = client.options_path(path)
            probes.append({
                "path": path,
                "status": response.get("_status_code", ""),
                "response_json": json_text(response.get("response", response))[:5000],
            })
        except Exception as e:
            probes.append({
                "path": path,
                "status": "error",
                "response_json": repr(e),
            })

    write_csv(pd.DataFrame(probes), out_dir / "06_listing_endpoint_probes.csv")

    blocker_df = pd.DataFrame(blockers)
    write_csv(blocker_df, out_dir / "BLOCKERS.csv")

    ready_for_manual_remap = (
        len(blockers) == 0
        and len(mapping_df) > 0
        and mapping_df["can_map"].astype(str).eq("yes").all()
    )

    ready_for_manual_remap = bool(ready_for_manual_remap)

    mode_name = "channel_supplier_setup" if args.build_channel_supplier_setup else ("verify" if args.verify else "plan")
    summary = pd.DataFrame([
        {"metric": "mode", "value": mode_name},
        {"metric": "family_code", "value": args.family_code},
        {"metric": "target_sku_prefix", "value": args.target_sku_prefix},
        {"metric": "channel", "value": args.channel},
        {"metric": "search", "value": args.search},
        {"metric": "target_products", "value": len(target_df)},
        {"metric": "expected_skus", "value": len(expected_skus)},
        {"metric": "missing_expected_skus", "value": len(missing_expected)},
        {"metric": "candidate_listings", "value": len(listings_df)},
        {"metric": "mapping_manifest_rows", "value": len(mapping_df)},
        {"metric": "mappable_mapping_rows", "value": int(mapping_df["can_map"].astype(str).eq("yes").sum()) if not mapping_df.empty else 0},
        {"metric": "old_product_delete_candidates", "value": len(delete_df)},
        {"metric": "verified_remap_rows", "value": verified_remap_rows},
        {"metric": "not_verified_remap_rows", "value": not_verified_remap_rows},
        {"metric": "safe_delete_candidates", "value": int(safe_delete_df["safe_to_delete_now"].astype(str).str.casefold().eq("yes").sum()) if not safe_delete_df.empty else 0},
        {"metric": "channel_supplier_setup_rows", "value": len(channel_setup_needed)},
        {"metric": "channel_supplier_setup_blockers", "value": len(channel_setup_blockers)},
        {"metric": "channel_supplier_only_stock_targets", "value": len(channel_stock_targets)},
        {"metric": "total_blockers", "value": len(blocker_df)},
        {"metric": "ready_for_manual_remap", "value": "yes" if ready_for_manual_remap else "no"},
        {"metric": "all_remaps_verified", "value": "yes" if all_remaps_verified else "no"},
    ])
    write_csv(summary, out_dir / "SUMMARY.csv")

    brief = [
        f"FAMILY: {args.family_code}",
        f"RUN: {run_id}",
        f"READY_FOR_MANUAL_REMAP: {'yes' if ready_for_manual_remap else 'no'}",
        f"TARGET_PRODUCTS: {len(target_df)}",
        f"CANDIDATE_LISTINGS: {len(listings_df)}",
        f"MAPPING_ROWS: {len(mapping_df)}",
        f"MAPPABLE_MAPPING_ROWS: {int(mapping_df['can_map'].astype(str).eq('yes').sum()) if not mapping_df.empty else 0}",
        f"DELETE_CANDIDATES_AFTER_REMAP: {len(delete_df)}",
        f"VERIFIED_REMAP_ROWS: {verified_remap_rows}",
        f"NOT_VERIFIED_REMAP_ROWS: {not_verified_remap_rows}",
        f"SAFE_DELETE_CANDIDATES: {int(safe_delete_df['safe_to_delete_now'].astype(str).str.casefold().eq('yes').sum()) if not safe_delete_df.empty else 0}",
        f"CHANNEL_SUPPLIER_SETUP_ROWS: {len(channel_setup_needed)}",
        f"CHANNEL_SUPPLIER_SETUP_BLOCKERS: {len(channel_setup_blockers)}",
        f"CHANNEL_SUPPLIER_ONLY_STOCK_TARGETS: {len(channel_stock_targets)}",
        f"TOTAL_BLOCKERS: {len(blocker_df)}",
        f"OUT_DIR: {out_dir}",
        "",
        "Supplier-only setup mode does not append scheduled targets or run live supplier setup.",
        "Paste this file plus SUMMARY.csv, BLOCKERS.csv, 04_mapping_manifest.csv, and channel setup CSVs to ChatGPT if blocked.",
    ]
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(brief), encoding="utf-8")

    operation_packet = {
        "family_code": args.family_code,
        "run_id": run_id,
        "out_dir": str(out_dir),
        "ready_for_manual_remap": ready_for_manual_remap,
        "summary_file": str(out_dir / "SUMMARY.csv"),
        "blockers_file": str(out_dir / "BLOCKERS.csv"),
        "mapping_manifest_file": str(out_dir / "04_mapping_manifest.csv"),
        "delete_candidates_file": str(out_dir / "05_delete_candidates_after_remap.csv"),
        "remap_verification_file": str(out_dir / "09_remap_verification.csv"),
        "safe_delete_queue_file": str(out_dir / "10_safe_delete_queue.csv"),
    }
    (out_dir / "OPERATION_PACKET.json").write_text(json.dumps(operation_packet, indent=2), encoding="utf-8")

    print("\nLISTING MIGRATION PLAN SUMMARY")
    print(summary.to_string(index=False))
    print("\nReports:", out_dir)

    if args.build_channel_supplier_setup:
        print("\nCHANNEL SUPPLIER SETUP REPORTS")
        print("Setup needed:", out_dir / "11_channel_supplier_setup_needed.csv")
        print("Setup blockers:", out_dir / "12_channel_supplier_setup_blockers.csv")
        print("Supplier-only stock targets:", out_dir / "13_channel_stock_targets_supplier_only.csv")
        print("No live supplier setup was run. No scheduled target file was modified.")
        return 0

    if args.verify:
        print("\nREMAP VERIFICATION")
        print("Verification:", out_dir / "09_remap_verification.csv")
        print("Safe delete queue:", out_dir / "10_safe_delete_queue.csv")
        print(f"Verified remap rows: {verified_remap_rows}")
        print(f"Not verified remap rows: {not_verified_remap_rows}")

        if all_remaps_verified:
            print("\nVERIFY PASSED. Old duplicate products in 10_safe_delete_queue.csv are safe to archive/delete.")
            return 0

        print("\nVERIFY BLOCKED. Do not delete old products yet.")
        return 2

    if not ready_for_manual_remap:
        print("\nPLAN BLOCKED OR NEEDS REVIEW.")
        print("Brief:", out_dir / "CHATGPT_BRIEF.txt")
        return 2

    print("\nPLAN READY FOR MANUAL REMAP REVIEW.")
    print("Mapping manifest:", out_dir / "04_mapping_manifest.csv")
    print("Verification:", out_dir / "09_remap_verification.csv")
    print("Safe delete queue:", out_dir / "10_safe_delete_queue.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
