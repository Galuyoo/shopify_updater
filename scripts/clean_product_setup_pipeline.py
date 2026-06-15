from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_mapping import build_supplier_stock_lookup
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

MANIFEST_COLUMNS = [
    "parent_sku",
    "product_name",
    "stock_strategy",
    "inventory_from_supplier",
    "stock_location",
    "supplier_mode",
    "notes",
]

SUPPLIER_SETUP_COLUMNS = ["ProductID", "SKU", "supplier", "supplier_sku", "supplier_free_stock", "stock_location"]

STOCK_TARGET_COLUMNS = [
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
    "allow_stock_location_update",
    "stock_strategy",
]

VARIANT_MATCH_COLUMNS = [
    "parent_sku",
    "product_name",
    "stock_strategy",
    "ProductID",
    "SKU",
    "Name",
    "supplier",
    "supplier_sku",
    "supplier_free_stock",
    "supplier_match_status",
    "candidate_strategy",
    "blocker_reason",
]

RAW_VARIANT_DISCOVERY_COLUMNS = [
    "parent_sku",
    "parent_product_id",
    "discovery_source",
    "variant_id",
    "product_id",
    "sku",
    "name",
    "supplier_sku_candidate",
    "match_status",
    "blocker_reason",
]

BC045_COLOUR_ALIASES = {
    "BLACK": "BLAC",
    "BLK": "BLAC",
    "BK": "BLAC",
    "BLAC": "BLAC",
    "NAVY": "NAVY",
    "NAV": "NAVY",
    "NVY": "NAVY",
    "NV": "NAVY",
    "ROYAL": "ROYA",
    "ROY": "ROYA",
    "ROYA": "ROYA",
    "BOTTLE": "BOTT",
    "BOTT": "BOTT",
    "BOT": "BOTT",
    "GREEN": "BOTT",
    "RED": "REDD",
    "REDD": "REDD",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan clean StoreFeeder product supplier setup and stock target rows.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="Build reports only. No StoreFeeder write endpoints are called.")
    mode.add_argument("--execute", action="store_true", help="Run existing safe supplier setup dry-run after the plan passes.")
    parser.add_argument("--manifest", type=Path, default=Path("data/clean_product_stock_strategy_manifest.csv"))
    parser.add_argument("--out-root", type=Path, default=Path("reports/clean_product_setup"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--targets", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--storefeeder-export", type=Path, default=Path("data/storefeeder_products_latest.xlsx"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=500)
    parser.add_argument("--append-targets", action="store_true", help="During --execute only, append generated stock targets idempotently after backup.")
    parser.add_argument("--allow-partial", action="store_true", help="Allow a reviewed partial plan when blocked rows are excluded from outputs.")
    args = parser.parse_args()
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    if args.max_pages < 1:
        parser.error("--max-pages must be at least 1")
    if args.append_targets and not args.execute:
        parser.error("--append-targets requires --execute")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = _read_csv(args.manifest)
    _require_columns(manifest, MANIFEST_COLUMNS, str(args.manifest))
    manifest = _normalize_manifest(manifest)
    _write_csv(manifest, out_dir / "01_manifest.csv")

    stock_lookup = _build_stock_lookup(args.ralawise_stock, args.uneek_stock)
    supplier_ids, supplier_names = _supplier_id_maps(args.supplier_id_map)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    products = _fetch_products(client, page_size=args.page_size, max_pages=args.max_pages)
    product_index = _build_product_index(products)

    scan_rows: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []
    raw_variant_rows: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []

    for _, manifest_row in manifest.iterrows():
        parent_sku = str(manifest_row["parent_sku"]).strip()
        candidates, product_scan_rows, parent_blockers = _discover_manifest_products(client, manifest_row, product_index)
        scan_rows.extend(product_scan_rows)
        blockers.extend(parent_blockers)

        if not candidates:
            continue

        for candidate in candidates:
            match = _match_variant_to_supplier(manifest_row, candidate, stock_lookup)
            match_rows.append(match)
            raw_variant_rows.append(_raw_variant_discovery_row(manifest_row, candidate, match))
            if match["blocker_reason"]:
                blockers.append(
                    {
                        "stage": "supplier_match",
                        "parent_sku": parent_sku,
                        "SKU": match["SKU"],
                        "reason": match["blocker_reason"],
                    }
                )

    product_scan = pd.DataFrame(scan_rows)
    variant_matches = pd.DataFrame(match_rows, columns=VARIANT_MATCH_COLUMNS)
    raw_variant_discovery = pd.DataFrame(raw_variant_rows, columns=RAW_VARIANT_DISCOVERY_COLUMNS)
    _write_csv(product_scan, out_dir / "02_product_scan.csv")
    _write_csv(variant_matches, out_dir / "03_variant_supplier_matches.csv")

    ready_matches = variant_matches[variant_matches["blocker_reason"].astype(str).str.strip().eq("")].copy()
    supplier_setup = _build_supplier_setup(ready_matches)
    stock_targets, target_blockers = _build_stock_targets(ready_matches, supplier_ids, supplier_names)

    for _, row in target_blockers.iterrows():
        blockers.append(
            {
                "stage": "stock_target",
                "parent_sku": row.get("parent_sku", ""),
                "SKU": row.get("SKU", ""),
                "reason": row.get("blocker_reason", "missing_required_stock_target_field"),
            }
        )

    priority_verify = _build_priority_verification(stock_targets)
    blocker_df = pd.DataFrame(blockers, columns=["stage", "parent_sku", "SKU", "reason"])

    _write_csv(supplier_setup, out_dir / "04_supplier_setup_needed.csv")
    _write_csv(stock_targets, out_dir / "05_stock_update_targets.csv")
    _write_csv(priority_verify, out_dir / "06_priority_warehouse_only_verification.csv")
    _write_csv(raw_variant_discovery, out_dir / "07_raw_variant_discovery.csv")
    _write_csv(blocker_df, out_dir / "BLOCKERS.csv")

    ready_to_execute_full = len(blocker_df) == 0 and len(supplier_setup) > 0
    ready_to_execute_partial = bool(args.allow_partial and len(blocker_df) > 0 and len(supplier_setup) > 0)
    ready_to_execute = ready_to_execute_full or ready_to_execute_partial
    summary = pd.DataFrame(
        [
            {"metric": "mode", "value": "execute" if args.execute else "plan"},
            {"metric": "run_id", "value": run_id},
            {"metric": "manifest_rows", "value": len(manifest)},
            {"metric": "storefeeder_products_scanned", "value": len(products)},
            {"metric": "product_scan_rows", "value": len(product_scan)},
            {"metric": "variant_supplier_match_rows", "value": len(variant_matches)},
            {"metric": "valid_rows", "value": len(ready_matches)},
            {"metric": "blocked_rows", "value": len(blocker_df)},
            {"metric": "supplier_setup_rows", "value": len(supplier_setup)},
            {"metric": "stock_update_target_rows", "value": len(stock_targets)},
            {"metric": "warehouse_only_target_rows", "value": int(stock_targets["stock_strategy"].eq("warehouse_only").sum()) if not stock_targets.empty else 0},
            {"metric": "supplier_synced_target_rows", "value": int(stock_targets["stock_strategy"].eq("supplier_synced_inventory").sum()) if not stock_targets.empty else 0},
            {"metric": "target_blockers", "value": len(target_blockers)},
            {"metric": "total_blockers", "value": len(blocker_df)},
            {"metric": "ready_to_execute_full", "value": "yes" if ready_to_execute_full else "no"},
            {"metric": "ready_to_execute_partial", "value": "yes" if ready_to_execute_partial else "no"},
            {"metric": "ready_to_execute", "value": "yes" if ready_to_execute else "no"},
            {"metric": "blocked_rows_excluded_from_outputs", "value": len(blocker_df)},
            {"metric": "out_dir", "value": str(out_dir)},
        ]
    )
    _write_csv(summary, out_dir / "SUMMARY.csv")
    _write_brief(out_dir, summary, blocker_df)

    print("Clean product setup plan")
    print(summary.to_string(index=False))
    print("Reports:", out_dir)

    if args.plan:
        print("PLAN ONLY. No StoreFeeder write endpoints were called. No target file was appended.")
        return 0 if ready_to_execute or len(blocker_df) == 0 else 2

    if not ready_to_execute:
        print("BLOCKED: execute requested but the plan has blockers.")
        print("Blockers:", out_dir / "BLOCKERS.csv")
        return 2

    dry_run_code = _run_supplier_setup_dry_run(args, out_dir, supplier_setup)
    if dry_run_code != 0:
        print("BLOCKED: supplier setup dry-run failed.")
        print("Log:", out_dir / "supplier_setup_dryrun.log")
        return dry_run_code

    if args.append_targets:
        appended = _append_targets_idempotently(args.targets, stock_targets, out_dir, run_id)
        print(f"Appended target rows: {appended}")

    print("EXECUTE COMPLETE. Supplier setup live writes were not run by this planner.")
    return 0


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
        raise SystemExit(f"{label} missing required columns: {', '.join(missing)}")


def _normalize_manifest(manifest: pd.DataFrame) -> pd.DataFrame:
    rows = manifest[MANIFEST_COLUMNS].copy()
    for column in MANIFEST_COLUMNS:
        rows[column] = rows[column].fillna("").astype(str).str.strip()
    rows = rows[rows["parent_sku"].ne("")].copy()
    rows["stock_strategy"] = rows["stock_strategy"].str.casefold()
    allowed = {"supplier_synced_inventory", "warehouse_only"}
    invalid = sorted(set(rows[~rows["stock_strategy"].isin(allowed)]["stock_strategy"]))
    if invalid:
        raise SystemExit("manifest has unsupported stock_strategy values: " + ", ".join(invalid))
    return rows.reset_index(drop=True)


def _build_stock_lookup(ralawise_path: Path, uneek_path: Path) -> pd.DataFrame:
    ralawise = _read_csv(ralawise_path)
    uneek = _read_csv(uneek_path)
    stock = build_supplier_stock_lookup(ralawise, uneek).copy()
    required = ["supplier", "supplier_sku", "supplier_free_stock"]
    _require_columns(stock, required, "supplier stock lookup")
    stock = stock[required].copy()
    stock["supplier"] = stock["supplier"].fillna("").astype(str).str.strip()
    stock["supplier_sku"] = stock["supplier_sku"].fillna("").astype(str).str.strip().str.upper()
    stock["supplier_free_stock"] = stock["supplier_free_stock"].fillna("").astype(str).str.strip()
    stock = stock[stock["supplier_sku"].ne("")].copy()
    stock["_supplier_sku_key"] = stock["supplier_sku"].str.casefold()
    return stock.reset_index(drop=True)


def _supplier_id_maps(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    df = _read_csv(path)
    _require_columns(df, ["supplier", "SupplierID", "Supplier.Name"], str(path))
    ids: dict[str, str] = {}
    names: dict[str, str] = {}
    for _, row in df.iterrows():
        key = str(row["supplier"]).strip().casefold()
        if not key:
            continue
        ids[key] = str(row["SupplierID"]).strip()
        names[key] = str(row["Supplier.Name"]).strip()
    return ids, names


def _fetch_products(client: StoreFeederApiClient, *, page_size: int, max_pages: int) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        wrapper = client.get_products_page(page=page, page_size=page_size)
        status_code = int(wrapper.get("_status_code", 0))
        if status_code >= 400:
            raise SystemExit(f"StoreFeeder product list request failed {status_code}: {wrapper.get('response')}")
        payload = wrapper.get("response", {})
        items = _extract_records(payload)
        if not items:
            break
        products.extend([item for item in items if isinstance(item, dict)])
        print(f"Fetched product page {page}: {len(items)} rows", flush=True)
        if _is_last_page(payload, page, page_size, len(items)):
            break
    return products


def _build_product_index(products: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    by_sku: dict[str, list[dict[str, Any]]] = {}
    by_parent: dict[str, list[dict[str, Any]]] = {}
    by_id: dict[str, dict[str, Any]] = {}
    for product in products:
        row = _product_row(product)
        rows.append(row)
        sku_key = row["SKU"].casefold()
        parent_key = row["Parent SKU"].casefold()
        if sku_key:
            by_sku.setdefault(sku_key, []).append(row)
        if parent_key:
            by_parent.setdefault(parent_key, []).append(row)
        if row["ProductID"]:
            by_id[row["ProductID"]] = row
    return {"rows": rows, "by_sku": by_sku, "by_parent": by_parent, "by_id": by_id}


def _discover_manifest_products(
    client: StoreFeederApiClient,
    manifest_row: pd.Series,
    product_index: dict[str, Any],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    parent_sku = str(manifest_row["parent_sku"]).strip()
    parent_key = parent_sku.casefold()
    parent_matches = product_index["by_sku"].get(parent_key, [])
    child_matches = list(product_index["by_parent"].get(parent_key, []))

    if _allow_prefix_children(parent_sku):
        prefix = parent_sku.upper() + "-"
        for row in product_index["rows"]:
            if row["SKU"].upper().startswith(prefix):
                child_matches.append(row)

    detail_children: list[dict[str, str]] = []
    for parent in parent_matches:
        product_id = parent.get("ProductID", "")
        if not product_id:
            continue
        detail_wrapper = client.get_product(product_id)
        if int(detail_wrapper.get("_status_code", 0)) >= 400:
            continue
        detail_payload = _first_record(detail_wrapper.get("response", {}))
        detail_children.extend(_child_product_rows(detail_payload, parent_sku, parent.get("ProductID", "")))

    candidates_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for matched_by, rows in [
        ("parent_field", child_matches),
        ("parent_detail_child", detail_children),
    ]:
        for row in rows:
            key = (row.get("ProductID", ""), row.get("SKU", ""))
            if key[1]:
                row = {**row, "matched_by": matched_by}
                candidates_by_key[key] = row

    if not candidates_by_key:
        for parent in parent_matches:
            key = (parent.get("ProductID", ""), parent.get("SKU", ""))
            candidates_by_key[key] = {**parent, "matched_by": "parent_fallback_no_visible_children"}

    scan_rows = []
    for candidate in candidates_by_key.values():
        scan_rows.append(
            {
                "parent_sku": parent_sku,
                "product_name": manifest_row["product_name"],
                "stock_strategy": manifest_row["stock_strategy"],
                "ProductID": candidate.get("ProductID", ""),
                "SKU": candidate.get("SKU", ""),
                "Parent SKU": candidate.get("Parent SKU", ""),
                "Name": candidate.get("Name", ""),
                "matched_by": candidate.get("matched_by", ""),
            }
        )

    blockers = []
    if not parent_matches and not candidates_by_key:
        blockers.append({"stage": "product_scan", "parent_sku": parent_sku, "SKU": "", "reason": "parent_product_not_found"})

    return list(candidates_by_key.values()), scan_rows, blockers


def _allow_prefix_children(parent_sku: str) -> bool:
    text = str(parent_sku).strip()
    return bool(text and not text.isdigit() and "-" in text)


def _product_row(product: dict[str, Any]) -> dict[str, str]:
    return {
        "ProductID": _first_text(product, ["ID", "Id", "ProductID", "ProductId", "productId"]),
        "SKU": _first_text(product, ["SKU", "Sku", "ProductSKU", "ProductSku", "sku"]),
        "Parent SKU": _first_text(product, ["Parent SKU", "ParentSKU", "ParentSku", "ParentProductSKU", "ParentProductSku"]),
        "Name": _first_text(product, ["Name", "ProductName", "Title", "Description"]),
    }


def _child_product_rows(detail: dict[str, Any], parent_sku: str, parent_id: str) -> list[dict[str, str]]:
    rows = []
    for key in ["Variants", "variants", "Children", "children", "ChildProducts", "childProducts", "ProductVariants", "productVariants"]:
        value = detail.get(key)
        if not isinstance(value, list):
            continue
        for index, item in enumerate(value):
            if not isinstance(item, dict):
                continue
            product_node = item.get("Product") if isinstance(item.get("Product"), dict) else item
            row = _product_row(product_node)
            if row["SKU"]:
                row["Parent SKU"] = row["Parent SKU"] or parent_sku
                row["ParentProductID"] = parent_id
                row["VariantID"] = _first_text(item, ["VariantID", "VariantId", "VariationID", "VariationId", "ID", "Id"])
                row["SupplierSKUCandidate"] = _explicit_supplier_sku(product_node) or _explicit_supplier_sku(item)
                row["discovery_source"] = f"detail.{key}[{index}].Product" if product_node is not item else f"detail.{key}[{index}]"
                rows.append(row)
    rows.extend(_recursive_variant_product_rows(detail, parent_sku, parent_id))
    return _dedupe_variant_rows(rows)


def _match_variant_to_supplier(manifest_row: pd.Series, product: dict[str, str], stock_lookup: pd.DataFrame) -> dict[str, Any]:
    sku = str(product.get("SKU", "")).strip().upper()
    parent_sku = str(manifest_row["parent_sku"]).strip().upper()
    if product.get("matched_by") == "parent_fallback_no_visible_children" and sku == parent_sku:
        return _variant_match_row(manifest_row, product, "", "", "", "blocked", "parent_fallback_no_visible_children", "no_child_variant_skus_exposed")

    explicit_supplier_sku = str(product.get("SupplierSKUCandidate", "")).strip().upper()
    if explicit_supplier_sku:
        supplier_sku, strategy = explicit_supplier_sku, "explicit_variant_supplier_sku" if explicit_supplier_sku != sku else "variant_sku_as_supplier_sku"
    else:
        supplier_sku, strategy = _supplier_sku_candidates(parent_sku, sku)
    if not supplier_sku:
        return _variant_match_row(manifest_row, product, "", "", "", "blocked", strategy, "supplier_sku_not_inferred")

    matches = stock_lookup[stock_lookup["_supplier_sku_key"].eq(supplier_sku.casefold())].copy()
    if matches.empty:
        return _variant_match_row(manifest_row, product, "", supplier_sku, "", "blocked", strategy, "supplier_sku_not_found_in_supplier_stock")
    if len(matches) > 1:
        suppliers = sorted(set(matches["supplier"].astype(str).str.strip()))
        return _variant_match_row(
            manifest_row,
            product,
            "|".join(suppliers),
            supplier_sku,
            "",
            "blocked",
            strategy,
            "supplier_sku_matches_multiple_supplier_stock_rows",
        )

    if not str(product.get("ProductID", "")).strip():
        return _variant_match_row(manifest_row, product, "", supplier_sku, "", "blocked", strategy, "variant_has_no_product_id_for_supplier_setup")

    match = matches.iloc[0]
    return _variant_match_row(
        manifest_row,
        product,
        str(match["supplier"]).strip(),
        supplier_sku,
        str(match["supplier_free_stock"]).strip(),
        "matched",
        strategy,
        "",
    )


def _supplier_sku_candidates(parent_sku: str, sku: str) -> tuple[str, str]:
    sku = str(sku).strip().upper()
    parent_sku = str(parent_sku).strip().upper()
    if not sku:
        return "", "blank_sku"
    if parent_sku == "EMB-CSTMINST-BC045":
        candidate = _bc045_supplier_sku(sku)
        if candidate:
            return candidate, "priority_bc045_colour_token"
    return sku, "exact_storefeeder_sku"


def _bc045_supplier_sku(sku: str) -> str:
    prefix = "EMB-CSTMINST-BC045"
    if not sku.startswith(prefix):
        return ""
    tail = sku[len(prefix):].strip("-_")
    tokens = [token for token in tail.replace("_", "-").split("-") if token]
    if not tokens:
        return ""
    candidates = []
    for token in reversed(tokens):
        cleaned = "".join(ch for ch in token.upper() if ch.isalnum())
        if cleaned in {"ONE", "ONESIZE", "SIZE", "OS"}:
            continue
        mapped = BC045_COLOUR_ALIASES.get(cleaned, cleaned)
        if mapped:
            candidates.append("BC045" + mapped)
    return candidates[0] if candidates else ""


def _explicit_supplier_sku(node: Any) -> str:
    if not isinstance(node, dict):
        return ""
    return _first_text(node, ["SupplierSKU", "SupplierSku", "Supplier SKUs", "SupplierSkuCode", "SupplierProductCode"])


def _recursive_variant_product_rows(detail: dict[str, Any], parent_sku: str, parent_id: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            if path and _looks_like_variant_path(path):
                product_node = node.get("Product") if isinstance(node.get("Product"), dict) else node
                row = _product_row(product_node)
                if row["SKU"] and row["SKU"].casefold() != parent_sku.casefold():
                    row["Parent SKU"] = row["Parent SKU"] or parent_sku
                    row["ParentProductID"] = parent_id
                    row["VariantID"] = _first_text(node, ["VariantID", "VariantId", "VariationID", "VariationId", "ID", "Id"])
                    row["SupplierSKUCandidate"] = _explicit_supplier_sku(product_node) or _explicit_supplier_sku(node)
                    row["discovery_source"] = path
                    rows.append(row)
            for key, value in node.items():
                walk(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]")

    walk(detail, "")
    return rows


def _looks_like_variant_path(path: str) -> bool:
    path_l = path.casefold()
    return any(token in path_l for token in ["children", "childproducts", "variants", "productvariants", "productvariations", "variations"])


def _dedupe_variant_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, str]] = []
    for row in rows:
        key = (str(row.get("ProductID", "")), str(row.get("SKU", "")), str(row.get("discovery_source", "")))
        simple_key = (key[0], key[1], "")
        if simple_key in seen:
            continue
        seen.add(simple_key)
        unique.append(row)
    return unique


def _raw_variant_discovery_row(manifest_row: pd.Series, product: dict[str, str], match: dict[str, Any]) -> dict[str, Any]:
    return {
        "parent_sku": manifest_row["parent_sku"],
        "parent_product_id": product.get("ParentProductID", ""),
        "discovery_source": product.get("discovery_source", product.get("matched_by", "")),
        "variant_id": product.get("VariantID", ""),
        "product_id": product.get("ProductID", ""),
        "sku": product.get("SKU", ""),
        "name": product.get("Name", ""),
        "supplier_sku_candidate": product.get("SupplierSKUCandidate", match.get("supplier_sku", "")),
        "match_status": match.get("supplier_match_status", ""),
        "blocker_reason": match.get("blocker_reason", ""),
    }


def _variant_match_row(
    manifest_row: pd.Series,
    product: dict[str, str],
    supplier: str,
    supplier_sku: str,
    supplier_free_stock: str,
    status: str,
    strategy: str,
    blocker_reason: str,
) -> dict[str, Any]:
    return {
        "parent_sku": manifest_row["parent_sku"],
        "product_name": manifest_row["product_name"],
        "stock_strategy": manifest_row["stock_strategy"],
        "ProductID": product.get("ProductID", ""),
        "SKU": product.get("SKU", ""),
        "Name": product.get("Name", ""),
        "supplier": supplier,
        "supplier_sku": supplier_sku,
        "supplier_free_stock": supplier_free_stock,
        "supplier_match_status": status,
        "candidate_strategy": strategy,
        "blocker_reason": blocker_reason,
    }


def _build_supplier_setup(ready_matches: pd.DataFrame) -> pd.DataFrame:
    if ready_matches.empty:
        return pd.DataFrame(columns=SUPPLIER_SETUP_COLUMNS)
    rows = pd.DataFrame(
        {
            "ProductID": ready_matches["ProductID"],
            "SKU": ready_matches["SKU"],
            "supplier": ready_matches["supplier"],
            "supplier_sku": ready_matches["supplier_sku"],
            "supplier_free_stock": ready_matches["supplier_free_stock"],
            "stock_location": ready_matches.apply(_setup_stock_location, axis=1),
        }
    )
    return rows[SUPPLIER_SETUP_COLUMNS].drop_duplicates().reset_index(drop=True)


def _build_stock_targets(
    ready_matches: pd.DataFrame,
    supplier_ids: dict[str, str],
    supplier_names: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for _, row in ready_matches.iterrows():
        strategy = str(row["stock_strategy"]).strip().casefold()
        supplier = str(row["supplier"]).strip()
        supplier_key = supplier.casefold()
        stock_location = supplier if strategy == "supplier_synced_inventory" else "Warehouse Stock"
        rows.append(
            {
                "parent_sku": row["parent_sku"],
                "ProductID": row["ProductID"],
                "SKU": row["SKU"],
                "supplier": supplier,
                "SupplierID": supplier_ids.get(supplier_key, ""),
                "Supplier.Name": supplier_names.get(supplier_key, supplier),
                "SupplierSKU": row["supplier_sku"],
                "stock_location": stock_location,
                "preserve_existing_locations": "yes",
                "warehouse_safe_mode": "yes",
                "skip_stock_location_update": "no" if strategy == "supplier_synced_inventory" else "yes",
                "allow_stock_location_update": "yes" if strategy == "supplier_synced_inventory" else "no",
                "stock_strategy": strategy,
            }
        )
    targets = pd.DataFrame(rows)
    if targets.empty:
        return pd.DataFrame(columns=STOCK_TARGET_COLUMNS), pd.DataFrame(columns=list(STOCK_TARGET_COLUMNS) + ["blocker_reason"])

    blockers = targets[
        targets["ProductID"].astype(str).str.strip().eq("")
        | targets["SKU"].astype(str).str.strip().eq("")
        | targets["supplier"].astype(str).str.strip().eq("")
        | targets["SupplierID"].astype(str).str.strip().eq("")
        | targets["SupplierSKU"].astype(str).str.strip().eq("")
    ].copy()
    if not blockers.empty:
        blockers["blocker_reason"] = blockers.apply(_target_blocker_reason, axis=1)

    targets = targets.drop(blockers.index).drop_duplicates(subset=["ProductID", "SupplierID", "SupplierSKU"]).copy()
    return targets[STOCK_TARGET_COLUMNS].reset_index(drop=True), blockers.reset_index(drop=True)


def _setup_stock_location(row: pd.Series) -> str:
    if str(row.get("stock_strategy", "")).strip().casefold() == "warehouse_only":
        return "Warehouse Stock"
    return str(row.get("supplier", "")).strip()


def _target_blocker_reason(row: pd.Series) -> str:
    reasons = []
    for column in ["ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU"]:
        if not str(row.get(column, "")).strip():
            reasons.append("missing_" + column)
    return "|".join(reasons) or "missing_required_stock_target_field"


def _build_priority_verification(stock_targets: pd.DataFrame) -> pd.DataFrame:
    if stock_targets.empty:
        return pd.DataFrame(columns=list(STOCK_TARGET_COLUMNS) + ["priority_verification_status"])
    priority = stock_targets[stock_targets["stock_strategy"].eq("warehouse_only")].copy()
    if priority.empty:
        return pd.DataFrame(columns=list(STOCK_TARGET_COLUMNS) + ["priority_verification_status"])
    priority["priority_verification_status"] = priority.apply(
        lambda row: "pass_supplier_metadata_only" if str(row["skip_stock_location_update"]).casefold() == "yes" and str(row["allow_stock_location_update"]).casefold() == "no" else "blocked_inventory_update_not_skipped",
        axis=1,
    )
    return priority.reset_index(drop=True)


def _run_supplier_setup_dry_run(args: argparse.Namespace, out_dir: Path, supplier_setup: pd.DataFrame) -> int:
    dry_dir = out_dir / "supplier_setup_dryrun"
    command = [
        sys.executable,
        "scripts/build_storefeeder_supplier_setup.py",
        "--supplier-setup-needed",
        str(out_dir / "04_supplier_setup_needed.csv"),
        "--storefeeder-supplier-id-map",
        str(args.supplier_id_map),
        "--supplier-mapping",
        str(PROJECT_ROOT / "data" / "supplier_mapping.csv"),
        "--storefeeder-export",
        str(args.storefeeder_export),
        "--out-dir",
        str(dry_dir),
        "--limit",
        str(len(supplier_setup)),
    ]
    log_path = out_dir / "supplier_setup_dryrun.log"
    with log_path.open("w", encoding="utf-8") as log:
        result = subprocess.run(command, cwd=PROJECT_ROOT, text=True, stdout=log, stderr=subprocess.STDOUT)
    return result.returncode


def _append_targets_idempotently(target_path: Path, new_targets: pd.DataFrame, out_dir: Path, run_id: str) -> int:
    existing = _read_csv(target_path)
    backup = target_path.with_name(target_path.stem + f".backup_before_clean_setup_{run_id}.csv")
    _write_csv(existing, backup)

    all_columns = list(existing.columns)
    for column in new_targets.columns:
        if column not in all_columns:
            all_columns.append(column)

    key_columns = ["ProductID", "SupplierID", "SupplierSKU"]
    existing_keys = set(zip(existing.get("ProductID", pd.Series(dtype=str)), existing.get("SupplierID", pd.Series(dtype=str)), existing.get("SupplierSKU", pd.Series(dtype=str))))
    append = new_targets.copy()
    append["_key"] = list(zip(append["ProductID"], append["SupplierID"], append["SupplierSKU"]))
    append = append[~append["_key"].isin(existing_keys)].drop(columns=["_key"]).copy()

    combined = pd.concat(
        [existing.reindex(columns=all_columns, fill_value=""), append.reindex(columns=all_columns, fill_value="")],
        ignore_index=True,
    )
    _write_csv(combined, target_path)
    _write_csv(append, out_dir / "targets_appended.csv")
    return len(append)


def _write_brief(out_dir: Path, summary: pd.DataFrame, blockers: pd.DataFrame) -> None:
    values = {str(row["metric"]): str(row["value"]) for _, row in summary.iterrows()}
    lines = [
        "Clean product setup planner brief",
        f"RUN_ID: {values.get('run_id', '')}",
        f"READY_TO_EXECUTE_FULL: {values.get('ready_to_execute_full', '')}",
        f"READY_TO_EXECUTE_PARTIAL: {values.get('ready_to_execute_partial', '')}",
        f"BLOCKED_ROWS_EXCLUDED_FROM_OUTPUTS: {values.get('blocked_rows_excluded_from_outputs', '0')}",
        f"VARIANT_SUPPLIER_MATCH_ROWS: {values.get('variant_supplier_match_rows', '0')}",
        f"VALID_ROWS: {values.get('valid_rows', '0')}",
        f"BLOCKED_ROWS: {values.get('blocked_rows', '0')}",
        f"SUPPLIER_SETUP_ROWS: {values.get('supplier_setup_rows', '0')}",
        f"STOCK_UPDATE_TARGET_ROWS: {values.get('stock_update_target_rows', '0')}",
        f"WAREHOUSE_ONLY_TARGET_ROWS: {values.get('warehouse_only_target_rows', '0')}",
        f"TOTAL_BLOCKERS: {values.get('total_blockers', '0')}",
        f"OUT_DIR: {out_dir}",
        "",
        "Safety: no listing mapping, no product deletion/archive, no StoreFeeder write endpoint in --plan, no target append unless --execute --append-targets.",
    ]
    if not blockers.empty:
        lines.append("")
        lines.append("Top blockers:")
        for _, row in blockers.head(20).iterrows():
            lines.append(f"- {row.get('stage', '')}: {row.get('SKU', '')} {row.get('reason', '')}")
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(lines), encoding="utf-8")


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


def _first_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        records = _extract_records(payload)
        if records:
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


if __name__ == "__main__":
    raise SystemExit(main())


