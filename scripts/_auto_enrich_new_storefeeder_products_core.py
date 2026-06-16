from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import shutil
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.export_storefeeder_products import build_snapshot_rows, fetch_products
from src.stock_mapping import build_supplier_stock_lookup
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv, read_storefeeder_export

PRODUCTION_TARGET_COLUMNS = [
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
    "sellable_stock_location",
]
REPORT_EXTRA_COLUMNS = ["confidence_status", "reason", "supplier_free_stock", "override_source", "override_reason"]
READY_COLUMNS = PRODUCTION_TARGET_COLUMNS + REPORT_EXTRA_COLUMNS
QUARANTINE_COLUMNS = [
    "ProductID",
    "SKU",
    "Parent SKU",
    "Name",
    "Suppliers",
    "Supplier SKUs",
    "classification",
    "quarantine_reason",
    "override_source",
    "override_reason",
]
SKIPPED_COLUMNS = ["ProductID", "SKU", "Parent SKU", "Name", "skip_reason"]
SUMMARY_COLUMNS = ["metric", "value"]
WAREHOUSE_ONLY_SKU_TOKENS = ["EMB-CSTMINST-BC045"]
WAREHOUSE_ONLY_NAME_TOKENS = ["prime", "same day", "same-day", "0 handling", "zero handling", "local"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safely enrich new StoreFeeder products into fast stock-sync target rows.")
    parser.add_argument("--storefeeder-export", type=Path, default=Path("data/storefeeder_products_latest.csv"))
    parser.add_argument("--use-api", action="store_true", help="Read products from StoreFeeder API instead of --storefeeder-export. Read-only.")
    parser.add_argument("--target-file", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--warehouse-only-skus", type=Path)
    parser.add_argument("--supplier-synced-skus", type=Path)
    parser.add_argument("--out-root", type=Path, default=Path("reports/auto_enrich_new_products"))
    parser.add_argument("--out-dir", type=Path, help="Compatibility alias for a fixed report directory; normally use --out-root.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--execute", action="store_true", help="Append ready rows to --target-file. Default is dry-run only.")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be zero or greater")
    if args.page_size < 1:
        parser.error("--page-size must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    products = _load_products(args)
    targets = _read_targets(args.target_file)
    supplier_ids = _load_supplier_ids(args.supplier_id_map)
    supplier_stock = build_supplier_stock_lookup(read_csv(args.ralawise_stock), read_csv(args.uneek_stock))
    warehouse_overrides = _load_overrides(args.warehouse_only_skus)
    supplier_overrides = _load_overrides(args.supplier_synced_skus)

    ready, quarantine, skipped, append_preview, summary = build_enrichment_reports(
        products,
        targets,
        supplier_ids,
        supplier_stock,
        warehouse_overrides,
        supplier_overrides,
    )

    paths = {
        "enrichment_summary": out_dir / "enrichment_summary.csv",
        "new_products_ready": out_dir / "new_products_ready.csv",
        "new_products_quarantine": out_dir / "new_products_quarantine.csv",
        "existing_products_skipped": out_dir / "existing_products_skipped.csv",
        "append_preview": out_dir / "append_preview.csv",
    }
    ready.to_csv(paths["new_products_ready"], index=False)
    quarantine.to_csv(paths["new_products_quarantine"], index=False)
    skipped.to_csv(paths["existing_products_skipped"], index=False)
    append_preview.to_csv(paths["append_preview"], index=False)

    appended = 0
    if args.execute:
        appended = _append_ready_targets(args.target_file, append_preview, out_dir, run_id)
    summary = _replace_summary_metric(summary, "appended_rows", appended)
    summary = _replace_summary_metric(summary, "execute_mode", "yes" if args.execute else "no")
    summary.to_csv(paths["enrichment_summary"], index=False)

    print("StoreFeeder new product auto-enrichment")
    print(summary.to_string(index=False))
    print("\nReports:")
    for path in paths.values():
        print(path)
    if args.execute:
        print(f"\nAppended rows: {appended}")
    else:
        print("\nDry-run only. No target rows were appended. No StoreFeeder write calls or stock updates were made.")
    return 0


def build_enrichment_reports(
    products: pd.DataFrame,
    targets: pd.DataFrame,
    supplier_ids: pd.DataFrame,
    supplier_stock: pd.DataFrame,
    warehouse_overrides: dict[str, str],
    supplier_overrides: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _require_columns(products, ["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"], "StoreFeeder products")
    _require_columns(targets, ["ProductID", "SKU", "SupplierSKU"], "target file")

    products = products.copy()
    for column in ["ID", "SKU", "Parent SKU", "Name", "Suppliers", "Supplier SKUs"]:
        products[column] = products[column].fillna("").astype(str).str.strip()

    existing_product_ids = set(targets["ProductID"].fillna("").astype(str).str.strip())
    existing_skus = {value.casefold() for value in targets["SKU"].fillna("").astype(str).str.strip() if value.strip()}
    parent_skus = {value.casefold() for value in products["Parent SKU"] if str(value).strip()}

    ready_rows: list[dict[str, Any]] = []
    quarantine_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    scanned_variants = 0
    new_candidate_rows = 0

    for _, product in products.iterrows():
        sku = str(product["SKU"]).strip()
        product_id = str(product["ID"]).strip()
        is_parent_with_children = sku.casefold() in parent_skus
        is_variant_or_non_variant = not is_parent_with_children
        if is_variant_or_non_variant:
            scanned_variants += 1

        if product_id in existing_product_ids or sku.casefold() in existing_skus:
            skipped_rows.append(_skipped_row(product, "already_in_stock_update_targets"))
            continue
        if is_parent_with_children:
            skipped_rows.append(_skipped_row(product, "parent_product_has_child_variants"))
            continue

        new_candidate_rows += 1
        override_source, override_reason = _override_for_sku(sku, warehouse_overrides, supplier_overrides)
        classification = _classify_product(product, override_source)

        if not sku:
            quarantine_rows.append(_quarantine_row(product, classification, "missing_sku", override_source, override_reason))
            continue

        candidates = _supplier_candidates(product, supplier_ids, supplier_stock)
        if classification == "warehouse_only":
            if len(candidates) != 1:
                reason = "warehouse_only_missing_supplier_reference_not_appended"
                quarantine_rows.append(_quarantine_row(product, classification, reason, override_source, override_reason))
                continue
            ready_rows.append(_target_row(product, candidates[0], "warehouse_only", override_source, override_reason))
            continue

        if len(candidates) != 1:
            reason = _supplier_quarantine_reason(product, supplier_ids, supplier_stock) if len(candidates) == 0 else "ambiguous_supplier_candidates"
            quarantine_rows.append(_quarantine_row(product, classification, reason, override_source, override_reason))
            continue

        ready_rows.append(_target_row(product, candidates[0], "supplier_synced_inventory", override_source, override_reason))

    ready = pd.DataFrame(ready_rows, columns=READY_COLUMNS)
    quarantine = pd.DataFrame(quarantine_rows, columns=QUARANTINE_COLUMNS)
    skipped = pd.DataFrame(skipped_rows, columns=SKIPPED_COLUMNS)
    append_preview = ready[PRODUCTION_TARGET_COLUMNS].copy() if not ready.empty else pd.DataFrame(columns=PRODUCTION_TARGET_COLUMNS)
    summary = _summary_frame(products, targets, ready, quarantine, scanned_variants, new_candidate_rows)
    return ready, quarantine, skipped, append_preview, summary


def _target_row(product: pd.Series, candidate: dict[str, Any], strategy: str, override_source: str, override_reason: str) -> dict[str, Any]:
    if strategy == "warehouse_only":
        return {
            "ProductID": product["ID"],
            "SKU": product["SKU"],
            "supplier": candidate["supplier"],
            "SupplierID": candidate["SupplierID"],
            "Supplier.Name": candidate["Supplier.Name"],
            "SupplierSKU": candidate["supplier_sku"],
            "stock_location": "Warehouse Stock",
            "preserve_existing_locations": "yes",
            "warehouse_safe_mode": "yes",
            "skip_stock_location_update": "yes",
            "allow_stock_location_update": "no",
            "stock_strategy": "warehouse_only",
            "sellable_stock_location": "",
            "confidence_status": "100_percent_warehouse_only_with_supplier_reference",
            "reason": "warehouse-only product classified confidently; stock locations protected",
            "supplier_free_stock": candidate["supplier_free_stock"],
            "override_source": override_source,
            "override_reason": override_reason,
        }
    return {
        "ProductID": product["ID"],
        "SKU": product["SKU"],
        "supplier": candidate["supplier"],
        "SupplierID": candidate["SupplierID"],
        "Supplier.Name": candidate["Supplier.Name"],
        "SupplierSKU": candidate["supplier_sku"],
        "stock_location": "Warehouse Stock",
        "preserve_existing_locations": "yes",
        "warehouse_safe_mode": "yes",
        "skip_stock_location_update": "no",
        "allow_stock_location_update": "yes",
        "stock_strategy": "supplier_synced_inventory",
        "sellable_stock_location": "Warehouse Stock",
        "confidence_status": "100_percent_supplier_stock_validated",
        "reason": "supplier and Supplier SKU are present on StoreFeeder product and validate uniquely in supplier stock",
        "supplier_free_stock": candidate["supplier_free_stock"],
        "override_source": override_source,
        "override_reason": override_reason,
    }


def _supplier_candidates(product: pd.Series, supplier_ids: pd.DataFrame, supplier_stock: pd.DataFrame) -> list[dict[str, Any]]:
    suppliers = _pipe_values(product.get("Suppliers", ""))
    supplier_skus = _pipe_values(product.get("Supplier SKUs", ""))
    if not suppliers or not supplier_skus or len(suppliers) != len(supplier_skus):
        return []

    candidates: list[dict[str, Any]] = []
    for supplier, supplier_sku in zip(suppliers, supplier_skus):
        supplier = supplier.strip()
        supplier_sku = supplier_sku.strip().upper()
        if not supplier or not supplier_sku:
            continue
        supplier_row = supplier_ids[supplier_ids["_supplier_key"].eq(supplier.casefold())]
        if len(supplier_row) != 1:
            continue
        stock_rows = supplier_stock[
            supplier_stock["supplier"].fillna("").astype(str).str.casefold().eq(supplier.casefold())
            & supplier_stock["supplier_sku"].fillna("").astype(str).str.casefold().eq(supplier_sku.casefold())
        ]
        if len(stock_rows) != 1:
            continue
        row = supplier_row.iloc[0]
        stock = stock_rows.iloc[0]
        candidates.append(
            {
                "supplier": supplier,
                "supplier_sku": supplier_sku,
                "SupplierID": str(row["SupplierID"]).strip(),
                "Supplier.Name": str(row["Supplier.Name"]).strip(),
                "supplier_free_stock": str(stock.get("supplier_free_stock", "")).strip(),
            }
        )
    return candidates


def _supplier_quarantine_reason(product: pd.Series, supplier_ids: pd.DataFrame, supplier_stock: pd.DataFrame) -> str:
    suppliers = _pipe_values(product.get("Suppliers", ""))
    supplier_skus = _pipe_values(product.get("Supplier SKUs", ""))
    if not suppliers:
        return "missing_supplier"
    if not supplier_skus or all(not value.strip() for value in supplier_skus):
        return "missing_supplier_sku"
    if len(suppliers) != len(supplier_skus):
        return "supplier_supplier_sku_alignment_ambiguous"

    reasons: list[str] = []
    for supplier, supplier_sku in zip(suppliers, supplier_skus):
        supplier = supplier.strip()
        supplier_sku = supplier_sku.strip().upper()
        if not supplier:
            reasons.append("missing_supplier")
            continue
        if not supplier_sku:
            reasons.append("missing_supplier_sku")
            continue
        supplier_row = supplier_ids[supplier_ids["_supplier_key"].eq(supplier.casefold())]
        if len(supplier_row) != 1:
            reasons.append("missing_supplier_id_mapping")
            continue
        stock_rows = supplier_stock[
            supplier_stock["supplier"].fillna("").astype(str).str.casefold().eq(supplier.casefold())
            & supplier_stock["supplier_sku"].fillna("").astype(str).str.casefold().eq(supplier_sku.casefold())
        ]
        if len(stock_rows) == 0:
            reasons.append("supplier_sku_not_found")
        elif len(stock_rows) > 1:
            reasons.append("ambiguous_supplier_stock_duplicate")
    return "|".join(sorted(set(reasons))) or "missing_supplier_sku_or_supplier_stock_match"


def _classify_product(product: pd.Series, override_source: str) -> str:
    if override_source == "warehouse_only_override":
        return "warehouse_only"
    if override_source == "supplier_synced_override":
        return "supplier_synced_inventory"
    sku = str(product.get("SKU", "")).strip().casefold()
    name = str(product.get("Name", "")).strip().casefold()
    if any(token.casefold() in sku for token in WAREHOUSE_ONLY_SKU_TOKENS):
        return "warehouse_only"
    if any(token in name for token in WAREHOUSE_ONLY_NAME_TOKENS):
        return "warehouse_only"
    return "supplier_synced_inventory"


def _override_for_sku(sku: str, warehouse_overrides: dict[str, str], supplier_overrides: dict[str, str]) -> tuple[str, str]:
    key = str(sku).strip().casefold()
    if key in warehouse_overrides:
        return "warehouse_only_override", warehouse_overrides[key]
    if key in supplier_overrides:
        return "supplier_synced_override", supplier_overrides[key]
    return "", ""


def _quarantine_row(product: pd.Series, classification: str, reason: str, override_source: str, override_reason: str) -> dict[str, Any]:
    return {
        "ProductID": str(product.get("ID", "")).strip(),
        "SKU": str(product.get("SKU", "")).strip(),
        "Parent SKU": str(product.get("Parent SKU", "")).strip(),
        "Name": str(product.get("Name", "")).strip(),
        "Suppliers": str(product.get("Suppliers", "")).strip(),
        "Supplier SKUs": str(product.get("Supplier SKUs", "")).strip(),
        "classification": classification,
        "quarantine_reason": reason,
        "override_source": override_source,
        "override_reason": override_reason,
    }


def _skipped_row(product: pd.Series, reason: str) -> dict[str, Any]:
    return {
        "ProductID": str(product.get("ID", "")).strip(),
        "SKU": str(product.get("SKU", "")).strip(),
        "Parent SKU": str(product.get("Parent SKU", "")).strip(),
        "Name": str(product.get("Name", "")).strip(),
        "skip_reason": reason,
    }


def _summary_frame(products: pd.DataFrame, targets: pd.DataFrame, ready: pd.DataFrame, quarantine: pd.DataFrame, scanned_variants: int, new_candidate_rows: int) -> pd.DataFrame:
    missing_sku = _reason_count(quarantine, "missing_sku")
    missing_supplier_sku = _reason_count(quarantine, "missing_supplier_sku")
    supplier_sku_not_found = _reason_count(quarantine, "supplier_sku_not_found")
    ambiguous = int(quarantine["quarantine_reason"].astype(str).str.contains("ambiguous", case=False, na=False).sum()) if not quarantine.empty else 0
    return pd.DataFrame(
        [
            {"metric": "scanned_products", "value": len(products)},
            {"metric": "scanned_variants", "value": scanned_variants},
            {"metric": "existing_target_rows", "value": len(targets)},
            {"metric": "new_candidate_rows", "value": new_candidate_rows},
            {"metric": "ready_rows", "value": len(ready)},
            {"metric": "quarantine_rows", "value": len(quarantine)},
            {"metric": "supplier_synced_ready_rows", "value": int(ready["stock_strategy"].eq("supplier_synced_inventory").sum()) if not ready.empty else 0},
            {"metric": "warehouse_only_ready_rows", "value": int(ready["stock_strategy"].eq("warehouse_only").sum()) if not ready.empty else 0},
            {"metric": "missing_sku_rows", "value": missing_sku},
            {"metric": "missing_supplier_sku_rows", "value": missing_supplier_sku},
            {"metric": "supplier_sku_not_found_rows", "value": supplier_sku_not_found},
            {"metric": "ambiguous_rows", "value": ambiguous},
            {"metric": "appended_rows", "value": 0},
            {"metric": "execute_mode", "value": "no"},
        ],
        columns=SUMMARY_COLUMNS,
    )


def _load_products(args: argparse.Namespace) -> pd.DataFrame:
    if args.use_api:
        _load_env_file(args.env_file)
        client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
        products = fetch_products(client, page_size=args.page_size, limit=args.limit)
        return pd.DataFrame(build_snapshot_rows(client, products))
    products = read_storefeeder_export(args.storefeeder_export)
    if args.limit is not None:
        products = products.head(args.limit).copy()
    return products


def _read_targets(path: Path) -> pd.DataFrame:
    targets = read_csv(path)
    for column in PRODUCTION_TARGET_COLUMNS:
        if column not in targets.columns:
            targets[column] = ""
    return targets.copy()


def _load_supplier_ids(path: Path) -> pd.DataFrame:
    supplier_ids = read_csv(path)
    _require_columns(supplier_ids, ["supplier", "SupplierID", "Supplier.Name"], "supplier ID map")
    supplier_ids = supplier_ids.copy()
    for column in ["supplier", "SupplierID", "Supplier.Name"]:
        supplier_ids[column] = supplier_ids[column].fillna("").astype(str).str.strip()
    supplier_ids["_supplier_key"] = supplier_ids["supplier"].str.casefold()
    return supplier_ids


def _load_overrides(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    overrides = read_csv(path)
    _require_columns(overrides, ["SKU", "reason"], str(path))
    result: dict[str, str] = {}
    for _, row in overrides.iterrows():
        sku = str(row.get("SKU", "")).strip()
        if sku:
            result[sku.casefold()] = str(row.get("reason", "")).strip()
    return result


def _append_ready_targets(target_path: Path, append_preview: pd.DataFrame, out_dir: Path, run_id: str) -> int:
    if append_preview.empty:
        return 0
    current = _read_targets(target_path)
    existing_product_ids = set(current["ProductID"].fillna("").astype(str).str.strip())
    existing_skus = {value.casefold() for value in current["SKU"].fillna("").astype(str).str.strip() if value.strip()}
    append = append_preview[
        ~append_preview["ProductID"].astype(str).str.strip().isin(existing_product_ids)
        & ~append_preview["SKU"].astype(str).str.strip().str.casefold().isin(existing_skus)
    ].copy()
    append = append.drop_duplicates(subset=["ProductID", "SKU"], keep="first")
    if append.empty:
        return 0

    target_columns = list(current.columns)
    for column in append.columns:
        if column not in target_columns:
            target_columns.append(column)
            current[column] = ""
    for column in target_columns:
        if column not in append.columns:
            append[column] = ""

    backup = target_path.with_name(target_path.stem + f".backup_before_auto_enrich_{run_id}" + target_path.suffix)
    shutil.copy2(target_path, backup)
    combined = pd.concat([current[target_columns], append[target_columns]], ignore_index=True)
    combined.to_csv(target_path, index=False)
    append[target_columns].to_csv(out_dir / "new_products_appended.csv", index=False)
    return len(append)


def _pipe_values(value: Any) -> list[str]:
    return [part.strip() for part in str(value).split("|")]


def _reason_count(quarantine: pd.DataFrame, reason: str) -> int:
    if quarantine.empty:
        return 0
    return int(quarantine["quarantine_reason"].astype(str).str.contains(reason, regex=False).sum())


def _require_columns(df: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: " + ", ".join(missing))


def _replace_summary_metric(summary: pd.DataFrame, metric: str, value: Any) -> pd.DataFrame:
    summary = summary.copy()
    if metric in set(summary["metric"]):
        summary.loc[summary["metric"].eq(metric), "value"] = value
    else:
        summary = pd.concat([summary, pd.DataFrame([{"metric": metric, "value": value}])], ignore_index=True)
    return summary


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        import os

        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())