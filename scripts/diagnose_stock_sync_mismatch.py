from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.export_storefeeder_products import fetch_products
from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.stock_mapping import build_supplier_stock_lookup

REPORT_ROOT = Path("reports")
SCHEDULED_FAST_ROOT = REPORT_ROOT / "scheduled_fast_stock_sync"
ONBOARDING_ROOT = REPORT_ROOT / "new_product_onboarding_delta"
TARGETS_PATH = Path("data/storefeeder_supplier_stock_update_targets.csv")
RALAWISE_STOCK_PATH = Path("data/RALAWISE_stock_lvl.csv")
UNEEK_STOCK_PATH = Path("data/Uneek_stock_levels.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose StoreFeeder stock sync mismatches without write calls.")
    parser.add_argument("--parent-sku", action="append", default=[])
    parser.add_argument("--sku-prefix", action="append", default=[])
    parser.add_argument("--since-hours", type=int, default=48)
    parser.add_argument("--out-root", type=Path, default=Path("reports/stock_sync_diagnosis"))
    parser.add_argument("--fetch-live-storefeeder", dest="fetch_live_storefeeder", action="store_true", default=True)
    parser.add_argument("--no-live", dest="fetch_live_storefeeder", action="store_false")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prefixes = _clean_list(args.sku_prefix + args.parent_sku)
    if not prefixes:
        raise SystemExit("Provide at least one --sku-prefix or --parent-sku")
    out_dir = args.out_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    _load_env(Path(".env"))

    onboarding_dir = _latest_dir_with_file(ONBOARDING_ROOT, "onboarding_summary.csv")
    latest_scheduled_dir = _latest_child_dir(SCHEDULED_FAST_ROOT)
    fast_dir = _latest_dir_with_file(SCHEDULED_FAST_ROOT, "fast_stock_summary.csv")
    onboarding_summary = _read_metric_csv(onboarding_dir / "onboarding_summary.csv") if onboarding_dir else {}
    fast_summary = _read_metric_csv(fast_dir / "fast_stock_summary.csv") if fast_dir else {}

    targets = _read_csv(TARGETS_PATH)
    relevant_targets = _filter_by_prefix(targets, prefixes, ["SKU", "ParentSKU", "parent_sku"])
    stock_lookup = _load_supplier_stock_lookup()
    target_vs_feed = _build_target_vs_feed(relevant_targets, stock_lookup)
    reports = _load_fast_reports(fast_dir)
    target_vs_report = _build_target_vs_report(relevant_targets, reports)
    stale_report = _build_stale_report(onboarding_dir, fast_dir, latest_scheduled_dir, onboarding_summary, fast_summary, len(targets))
    supplier_failures = _filter_report_by_prefix(reports.get("supplier_failures"), prefixes)
    live_audit = _fetch_live_audit(args, prefixes, relevant_targets) if args.fetch_live_storefeeder else pd.DataFrame()
    family = _build_family_diagnosis(prefixes, relevant_targets, target_vs_feed, target_vs_report, live_audit)
    mismatch = _build_success_but_live_mismatch(target_vs_report, live_audit)
    fixes = _build_recommended_fixes(stale_report, target_vs_report, mismatch, reports)
    summary = _build_summary(prefixes, onboarding_dir, fast_dir, latest_scheduled_dir, onboarding_summary, fast_summary, targets, relevant_targets, stale_report, live_audit, reports)

    _write(out_dir / "diagnosis_summary.csv", summary)
    _write(out_dir / "family_diagnosis.csv", family)
    _write(out_dir / "target_vs_feed.csv", target_vs_feed)
    _write(out_dir / "target_vs_latest_fast_report.csv", target_vs_report)
    _write(out_dir / "live_storefeeder_stock_audit.csv", live_audit)
    _write(out_dir / "stale_or_missing_fast_sync_report.csv", stale_report)
    _write(out_dir / "stock_location_success_but_live_mismatch.csv", mismatch)
    _write(out_dir / "supplier_update_failures_relevant.csv", supplier_failures)
    _write(out_dir / "recommended_fixes.csv", fixes)
    _write_brief(out_dir / "CHATGPT_BRIEF.txt", summary, stale_report, fixes)

    print("StoreFeeder stock sync diagnosis complete")
    print(f"out_dir: {out_dir}")
    for _, row in summary.iterrows():
        print(f"{row['metric']}: {row['value']}")
    print("Read-only only. No StoreFeeder write endpoints were called.")
    return 0


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _clean_list(values: list[str]) -> list[str]:
    seen, cleaned = set(), []
    for value in values:
        text = str(value).strip()
        key = text.upper()
        if text and key not in seen:
            seen.add(key)
            cleaned.append(text)
    return cleaned


def _read_csv(path: Path | None) -> pd.DataFrame:
    if not path or not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _write(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _read_metric_csv(path: Path) -> dict[str, str]:
    df = _read_csv(path)
    if df.empty or "metric" not in df.columns or "value" not in df.columns:
        return {}
    return {str(row["metric"]): str(row["value"]) for _, row in df.iterrows()}


def _latest_child_dir(root: Path) -> Path | None:
    dirs = [p for p in root.iterdir() if p.is_dir()] if root.exists() else []
    return max(dirs, key=lambda p: p.stat().st_mtime) if dirs else None


def _latest_dir_with_file(root: Path, filename: str) -> Path | None:
    dirs = [p for p in root.iterdir() if p.is_dir() and (p / filename).exists()] if root.exists() else []
    return max(dirs, key=lambda p: (p / filename).stat().st_mtime) if dirs else None


def _filter_by_prefix(df: pd.DataFrame, prefixes: list[str], columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column not in df.columns:
            continue
        values = df[column].fillna("").astype(str).str.upper()
        for prefix in prefixes:
            p = prefix.upper()
            mask = mask | values.eq(p) | values.str.startswith(p + "-") | values.str.startswith(p)
    return df[mask].copy().reset_index(drop=True)


def _filter_report_by_prefix(df: pd.DataFrame | None, prefixes: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    return _filter_by_prefix(df, prefixes, ["SKU", "ProductIDType.Value", "ProductID", "supplier_sku", "SupplierSKU"])


def _load_supplier_stock_lookup() -> pd.DataFrame:
    ralawise = _read_csv(RALAWISE_STOCK_PATH)
    uneek = _read_csv(UNEEK_STOCK_PATH)
    return build_supplier_stock_lookup(ralawise if not ralawise.empty else None, uneek if not uneek.empty else None)


def _load_fast_reports(fast_dir: Path | None) -> dict[str, pd.DataFrame]:
    if not fast_dir:
        return {}
    files = {
        "supplier_preview": "fast_stock_payload_preview.csv",
        "location_preview": "fast_stock_location_payload_preview.csv",
        "location_success": "fast_stock_location_update_success.csv",
        "location_failures": "fast_stock_location_update_failures.csv",
        "channel_skips": "fast_stock_channel_safety_skips.csv",
        "supplier_failures": "fast_stock_update_failures.csv",
        "supplier_info_only_preview": "supplier_info_only_payload_preview.csv",
        "zero_preview": "fast_stock_location_zero_payload_preview.csv",
        "zero_success": "fast_stock_location_zero_update_success.csv",
        "zero_failures": "fast_stock_location_zero_update_failures.csv",
    }
    return {key: _read_csv(fast_dir / filename) for key, filename in files.items()}


def _build_target_vs_feed(targets: pd.DataFrame, stock_lookup: pd.DataFrame) -> pd.DataFrame:
    columns = ["SKU", "ProductID", "supplier", "SupplierID", "Supplier.Name", "SupplierSKU", "stock_location", "stock_strategy", "sellable_stock_location", "allow_stock_location_update", "skip_stock_location_update", "feed_match_count", "supplier_free_stock", "diagnosis_category", "reason"]
    rows = []
    for _, row in targets.iterrows():
        supplier = _txt(row.get("supplier"))
        supplier_sku = _txt(row.get("SupplierSKU") or row.get("supplier_sku"))
        if stock_lookup.empty:
            matches = pd.DataFrame()
        else:
            matches = stock_lookup[stock_lookup["supplier"].fillna("").astype(str).str.casefold().eq(supplier.casefold()) & stock_lookup["supplier_sku"].fillna("").astype(str).str.casefold().eq(supplier_sku.casefold())]
        if len(matches) == 1:
            category, reason, free_stock = "feed_match_unique", "supplier SKU found exactly once in supplier feed", _txt(matches.iloc[0].get("supplier_free_stock"))
        elif len(matches) == 0:
            category, reason, free_stock = "supplier_feed_missing", "target supplier SKU is absent from latest supplier feed", ""
        else:
            category, reason, free_stock = "supplier_feed_duplicate", "target supplier SKU matched multiple supplier feed rows", ""
        rows.append({
            "SKU": _txt(row.get("SKU")), "ProductID": _txt(row.get("ProductID")), "supplier": supplier,
            "SupplierID": _txt(row.get("SupplierID")), "Supplier.Name": _txt(row.get("Supplier.Name")), "SupplierSKU": supplier_sku,
            "stock_location": _txt(row.get("stock_location")), "stock_strategy": _txt(row.get("stock_strategy")),
            "sellable_stock_location": _txt(row.get("sellable_stock_location")), "allow_stock_location_update": _txt(row.get("allow_stock_location_update")),
            "skip_stock_location_update": _txt(row.get("skip_stock_location_update")), "feed_match_count": len(matches),
            "supplier_free_stock": free_stock, "diagnosis_category": category, "reason": reason,
        })
    return pd.DataFrame(rows, columns=columns)


def _build_target_vs_report(targets: pd.DataFrame, reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    columns = ["SKU", "ProductID", "SupplierSKU", "stock_strategy", "expected_stock_location", "supplier_payload_rows", "supplier_failure_rows", "stock_location_payload_rows", "stock_location_success_rows", "stock_location_failure_rows", "channel_skip_rows", "zero_payload_rows", "zero_success_rows", "zero_failure_rows", "diagnosis_category", "reason"]
    rows = []
    for _, row in targets.iterrows():
        sku = _txt(row.get("SKU"))
        expected_location = _txt(row.get("sellable_stock_location")) or _txt(row.get("stock_location"))
        supplier_payload_count = _count_matches(reports.get("supplier_preview"), sku, ["SKU", "ProductIDType.Value"])
        supplier_failure_count = _count_matches(reports.get("supplier_failures"), sku, ["SKU", "ProductIDType.Value"])
        location_payload_count = _count_location_matches(reports.get("location_preview"), sku, expected_location)
        location_success_count = _count_location_matches(reports.get("location_success"), sku, expected_location)
        location_failure_count = _count_location_matches(reports.get("location_failures"), sku, expected_location)
        channel_skip_count = _count_matches(reports.get("channel_skips"), sku, ["SKU"])
        zero_payload_count = _count_matches(reports.get("zero_preview"), sku, ["SKU"])
        zero_success_count = _count_matches(reports.get("zero_success"), sku, ["SKU"])
        zero_failure_count = _count_matches(reports.get("zero_failures"), sku, ["SKU"])
        category, reason = "seen_in_latest_fast_report", "supplier and stock-location reports contain expected rows where applicable"
        if supplier_payload_count == 0:
            category, reason = "target_exists_not_in_latest_fast_report", "current target row is absent from latest fast-sync supplier payload preview"
        elif supplier_failure_count > 0:
            category, reason = "supplier_update_failure", "latest fast-sync supplier update failure report contains this SKU"
        elif _is_supplier_info_only(row):
            category, reason = "expected_supplier_info_only_manual_inventory", "supplier-info-only/manual inventory row should update supplier inventory only"
        elif _stock_location_updates_disabled(row):
            category, reason = "protected_manual_inventory_skipped_correctly", "warehouse/manual row has stock-location updates disabled"
        elif location_failure_count > 0:
            category, reason = "stock_location_failure", "latest fast-sync stock-location failure report contains this SKU/location"
        elif location_payload_count == 0:
            category, reason = "stock_location_payload_missing", "supplier-synced target did not appear in stock-location payload for expected sellable location"
        elif location_success_count == 0:
            category, reason = "stock_location_success_missing", "stock-location payload existed but success report has no matching success row"
        rows.append({
            "SKU": sku, "ProductID": _txt(row.get("ProductID")), "SupplierSKU": _txt(row.get("SupplierSKU") or row.get("supplier_sku")),
            "stock_strategy": _txt(row.get("stock_strategy")), "expected_stock_location": expected_location,
            "supplier_payload_rows": supplier_payload_count, "supplier_failure_rows": supplier_failure_count,
            "stock_location_payload_rows": location_payload_count, "stock_location_success_rows": location_success_count,
            "stock_location_failure_rows": location_failure_count, "channel_skip_rows": channel_skip_count,
            "zero_payload_rows": zero_payload_count, "zero_success_rows": zero_success_count, "zero_failure_rows": zero_failure_count,
            "diagnosis_category": category, "reason": reason,
        })
    return pd.DataFrame(rows, columns=columns)


def _build_stale_report(onboarding_dir: Path | None, fast_dir: Path | None, latest_scheduled_dir: Path | None, onboarding_summary: dict[str, str], fast_summary: dict[str, str], current_target_rows: int) -> pd.DataFrame:
    rows = []
    appended = _int_metric(onboarding_summary, "target_rows_appended")
    fast_target_rows = _int_metric(fast_summary, "target_rows")
    if latest_scheduled_dir and fast_dir and latest_scheduled_dir != fast_dir:
        rows.append(_stale_row("latest_scheduled_fast_sync_folder_missing_summary", "latest scheduled fast-sync folder does not contain fast_stock_summary.csv; the job may have failed before reporting", onboarding_dir, fast_dir, latest_scheduled_dir, appended, fast_target_rows, current_target_rows))
    if appended > 0 and fast_target_rows and current_target_rows > fast_target_rows:
        rows.append(_stale_row("enrichment_new_targets_not_followed_by_fast_sync", "onboarding appended target rows, but the latest complete fast-sync report still used fewer target rows", onboarding_dir, fast_dir, latest_scheduled_dir, appended, fast_target_rows, current_target_rows))
    if latest_scheduled_dir and not (latest_scheduled_dir / "fast_stock_summary.csv").exists():
        rows.append(_stale_row("scheduled_task_created_folder_without_report", "scheduled task created an output directory but did not complete/report fast sync results", onboarding_dir, fast_dir, latest_scheduled_dir, appended, fast_target_rows, current_target_rows))
    return pd.DataFrame(rows, columns=["diagnosis_category", "reason", "onboarding_dir", "fast_report_dir", "latest_scheduled_dir", "onboarding_target_rows_appended", "fast_report_target_rows", "current_target_rows"])


def _stale_row(category: str, reason: str, onboarding_dir: Path | None, fast_dir: Path | None, latest_scheduled_dir: Path | None, appended: int, fast_target_rows: int, current_target_rows: int) -> dict[str, Any]:
    return {"diagnosis_category": category, "reason": reason, "onboarding_dir": str(onboarding_dir or ""), "fast_report_dir": str(fast_dir or ""), "latest_scheduled_dir": str(latest_scheduled_dir or ""), "onboarding_target_rows_appended": appended, "fast_report_target_rows": fast_target_rows, "current_target_rows": current_target_rows}


def _fetch_live_audit(args: argparse.Namespace, prefixes: list[str], targets: pd.DataFrame) -> pd.DataFrame:
    columns = ["SKU", "ProductID", "Name", "SupplierInventory", "Inventory", "Suppliers", "Supplier SKUs", "Stock Locations", "Current Inventories", "expected_stock_location", "live_expected_location_quantity", "diagnosis_category", "reason"]
    try:
        client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
        products = fetch_products(client, page_size=args.page_size, limit=None)
    except Exception as exc:
        return pd.DataFrame([{"SKU": "", "ProductID": "", "Name": "", "SupplierInventory": "", "Inventory": "", "Suppliers": "", "Supplier SKUs": "", "Stock Locations": "", "Current Inventories": "", "expected_stock_location": "", "live_expected_location_quantity": "", "diagnosis_category": "live_fetch_failed", "reason": str(exc)}], columns=columns)
    product_rows = []
    for product in products:
        if not isinstance(product, dict):
            continue
        sku = _first_text(product, ["SKU", "Sku", "ProductSKU", "ProductSku"])
        parent = _first_text(product, ["Parent SKU", "ParentSKU", "ParentSku", "ParentProductSKU"])
        if _matches_prefix(sku, prefixes) or _matches_prefix(parent, prefixes):
            product_rows.append(product)
    target_by_sku = {str(row.get("SKU", "")).casefold(): row for _, row in targets.iterrows()}
    rows = []
    for product in product_rows:
        product_id = _first_text(product, ["ID", "Id", "ProductID", "ProductId"])
        detail = product
        if product_id:
            try:
                wrapper = client.get_product(product_id)
                if int(wrapper.get("_status_code", 0)) < 400:
                    detail = {**product, **_first_record(wrapper.get("response", {}))}
            except Exception as exc:
                detail = {**product, "_detail_error": str(exc)}
        sku = _first_text(detail, ["SKU", "Sku", "ProductSKU", "ProductSku"])
        target = target_by_sku.get(sku.casefold())
        expected_location = ""
        if target is not None:
            expected_location = _txt(target.get("sellable_stock_location")) or _txt(target.get("stock_location"))
        stock_locations, _, inventories = _stock_location_pipes(detail)
        location_qty = _pipe_lookup(stock_locations, inventories, expected_location)
        suppliers, supplier_skus = _supplier_pipes(detail)
        category, reason = "live_product_read", "live product detail read successfully"
        if target is None:
            category, reason = "live_product_not_in_target_file", "live product matches requested prefix but is not in target file"
        elif expected_location and location_qty == "":
            category, reason = "expected_location_missing_live", "target expects a sellable location but live product detail does not show it"
        rows.append({
            "SKU": sku, "ProductID": product_id, "Name": _first_text(detail, ["Name", "ProductName", "Title", "Description"]),
            "SupplierInventory": _first_text(detail, ["SupplierInventory", "Supplier Inventory", "SupplierStockLevel"]),
            "Inventory": _first_text(detail, ["Inventory", "CurrentInventory", "TotalInventory", "AvailableInventory", "Stock"]),
            "Suppliers": suppliers, "Supplier SKUs": supplier_skus, "Stock Locations": stock_locations,
            "Current Inventories": inventories, "expected_stock_location": expected_location,
            "live_expected_location_quantity": location_qty, "diagnosis_category": category, "reason": reason,
        })
    return pd.DataFrame(rows, columns=columns)


def _build_family_diagnosis(prefixes: list[str], targets: pd.DataFrame, feed: pd.DataFrame, report: pd.DataFrame, live: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for prefix in prefixes:
        target_rows = _filter_by_prefix(targets, [prefix], ["SKU", "ParentSKU", "parent_sku"])
        feed_rows = _filter_by_prefix(feed, [prefix], ["SKU"])
        report_rows = _filter_by_prefix(report, [prefix], ["SKU"])
        live_rows = _filter_by_prefix(live, [prefix], ["SKU"])
        categories = sorted(set(report_rows.get("diagnosis_category", pd.Series(dtype=str)).astype(str)) | set(feed_rows.get("diagnosis_category", pd.Series(dtype=str)).astype(str)) | set(live_rows.get("diagnosis_category", pd.Series(dtype=str)).astype(str)))
        rows.append({"prefix": prefix, "target_rows": len(target_rows), "feed_rows": len(feed_rows), "latest_fast_report_rows": len(report_rows), "live_rows": len(live_rows), "diagnosis_categories": "|".join([c for c in categories if c])})
    return pd.DataFrame(rows)


def _build_success_but_live_mismatch(report: pd.DataFrame, live: pd.DataFrame) -> pd.DataFrame:
    columns = ["SKU", "ProductID", "diagnosis_category", "reason"]
    if report.empty or live.empty or "diagnosis_category" not in live.columns:
        return pd.DataFrame(columns=columns)
    success = report[pd.to_numeric(report.get("stock_location_success_rows", pd.Series(dtype=str)), errors="coerce").fillna(0).gt(0)].copy()
    live_problem = live[live["diagnosis_category"].eq("expected_location_missing_live")].copy()
    if success.empty or live_problem.empty:
        return pd.DataFrame(columns=columns)
    merged = success.merge(live_problem, on="SKU", how="inner", suffixes=("_report", "_live"))
    if merged.empty:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame({"SKU": merged["SKU"], "ProductID": merged.get("ProductID_live", ""), "diagnosis_category": "stock_location_payload_success_but_live_inventory_mismatch", "reason": "latest fast report shows stock-location success, but live product readback does not show expected location/quantity"}, columns=columns)


def _build_recommended_fixes(stale: pd.DataFrame, report: pd.DataFrame, mismatch: pd.DataFrame, reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    if not stale.empty:
        rows.append({"priority": "high", "fix": "Patch scheduled fast-sync wrapper so failed Python runs return non-zero and always write run.log with the real exit code.", "evidence": "stale_or_missing_fast_sync_report.csv has rows"})
        rows.append({"priority": "high", "fix": "After onboarding appends target rows, require a completed fast sync whose target_rows equals the current target file row count.", "evidence": "latest complete fast-sync report is stale or lower than current target file"})
    if not report.empty and (report.get("diagnosis_category", pd.Series(dtype=str)) == "target_exists_not_in_latest_fast_report").any():
        rows.append({"priority": "high", "fix": "Add stale-report guard to ops checks: current target rows must equal latest fast_stock_summary target_rows.", "evidence": "target_vs_latest_fast_report.csv contains target_exists_not_in_latest_fast_report"})
    if not mismatch.empty:
        rows.append({"priority": "high", "fix": "Add post-update GET verification for sampled live stock-location updates and flag mismatches immediately.", "evidence": "stock_location_success_but_live_mismatch.csv is not empty"})
    location_success = reports.get("location_success", pd.DataFrame())
    if not location_success.empty:
        missing = [col for col in ["ProductID", "SupplierSKU"] if col not in location_success.columns]
        if missing:
            rows.append({"priority": "medium", "fix": "Patch fast_stock_location_update_success/failure reports to preserve ProductID and SupplierSKU from target rows.", "evidence": "fast_stock_location_update_success.csv missing columns: " + ", ".join(missing)})
    if not rows:
        rows.append({"priority": "info", "fix": "No immediate patch identified; inspect live audit and StoreFeeder parent aggregate display.", "evidence": "reports did not find stale, payload, or live mismatch categories"})
    return pd.DataFrame(rows)


def _build_summary(prefixes: list[str], onboarding_dir: Path | None, fast_dir: Path | None, latest_scheduled_dir: Path | None, onboarding_summary: dict[str, str], fast_summary: dict[str, str], targets: pd.DataFrame, relevant_targets: pd.DataFrame, stale_report: pd.DataFrame, live_audit: pd.DataFrame, reports: dict[str, pd.DataFrame]) -> pd.DataFrame:
    location_success = reports.get("location_success", pd.DataFrame())
    rows = [
        ("prefixes", "|".join(prefixes)),
        ("latest_onboarding_report_dir", str(onboarding_dir or "")),
        ("latest_complete_fast_report_dir", str(fast_dir or "")),
        ("latest_scheduled_fast_dir", str(latest_scheduled_dir or "")),
        ("onboarding_target_rows_appended", _int_metric(onboarding_summary, "target_rows_appended")),
        ("fast_report_target_rows", _int_metric(fast_summary, "target_rows")),
        ("current_target_rows", len(targets)),
        ("relevant_target_rows", len(relevant_targets)),
        ("stale_or_missing_fast_sync_issues", len(stale_report)),
        ("live_audit_rows", len(live_audit)),
        ("stock_location_success_has_ProductID", "yes" if "ProductID" in location_success.columns else "no"),
        ("stock_location_success_has_SupplierSKU", "yes" if "SupplierSKU" in location_success.columns else "no"),
    ]
    return pd.DataFrame([{"metric": key, "value": value} for key, value in rows])


def _write_brief(path: Path, summary: pd.DataFrame, stale: pd.DataFrame, fixes: pd.DataFrame) -> None:
    lines = ["STORE FEEDER STOCK SYNC DIAGNOSIS", "", "SUMMARY"]
    for _, row in summary.iterrows():
        lines.append(f"- {row['metric']}: {row['value']}")
    lines.extend(["", "STALE/MISSING FAST SYNC"])
    if stale.empty:
        lines.append("- No stale/missing fast-sync report issue found.")
    else:
        for _, row in stale.iterrows():
            lines.append(f"- {row['diagnosis_category']}: {row['reason']}")
    lines.extend(["", "RECOMMENDED FIXES"])
    for _, row in fixes.iterrows():
        lines.append(f"- [{row['priority']}] {row['fix']} Evidence: {row['evidence']}")
    lines.append("\nRead-only only. No StoreFeeder write endpoints were called.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _count_matches(df: pd.DataFrame | None, sku: str, columns: list[str]) -> int:
    if df is None or df.empty:
        return 0
    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column in df.columns:
            mask = mask | df[column].fillna("").astype(str).str.casefold().eq(sku.casefold())
    return int(mask.sum())


def _count_location_matches(df: pd.DataFrame | None, sku: str, location: str) -> int:
    if df is None or df.empty:
        return 0
    sku_cols = [c for c in ["SKU", "ProductIDType.Value"] if c in df.columns]
    location_cols = [c for c in ["stock_location", "StockLocationID.Value", "stock_location_id_value"] if c in df.columns]
    if not sku_cols:
        return 0
    sku_mask = pd.Series(False, index=df.index)
    for column in sku_cols:
        sku_mask = sku_mask | df[column].fillna("").astype(str).str.casefold().eq(sku.casefold())
    if not location or not location_cols:
        return int(sku_mask.sum())
    location_mask = pd.Series(False, index=df.index)
    for column in location_cols:
        location_mask = location_mask | df[column].fillna("").astype(str).str.casefold().eq(location.casefold())
    return int((sku_mask & location_mask).sum())


def _is_supplier_info_only(row: pd.Series) -> bool:
    values = "|".join(_txt(row.get(col)) for col in ["stock_strategy", "stock_update_mode", "target_type"])
    return "supplier_info_only" in values.casefold()


def _stock_location_updates_disabled(row: pd.Series) -> bool:
    strategy = _txt(row.get("stock_strategy")).casefold()
    skip = _txt(row.get("skip_stock_location_update")).casefold()
    allow = _txt(row.get("allow_stock_location_update")).casefold()
    return strategy == "warehouse_only" or skip in {"yes", "true", "1"} or allow in {"no", "false", "0"}


def _matches_prefix(value: str, prefixes: list[str]) -> bool:
    text = _txt(value).upper()
    if not text:
        return False
    for prefix in prefixes:
        p = prefix.upper()
        if text == p or text.startswith(p + "-") or text.startswith(p):
            return True
    return False


def _txt(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int_metric(metrics: dict[str, str], key: str) -> int:
    try:
        return int(float(metrics.get(key, "0") or 0))
    except (TypeError, ValueError):
        return 0


def _first_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        for key in ["Items", "items", "Products", "products", "Data", "data", "Results", "results", "value", "Value"]:
            value = payload.get(key)
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value[0]
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return {}


def _first_text(payload: Any, keys: list[str]) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        value = _deep_get(payload, key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _deep_get(payload: dict[str, Any], dotted_key: str) -> Any:
    if dotted_key in payload:
        return payload[dotted_key]
    current: Any = payload
    for part in dotted_key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _supplier_pipes(payload: Any) -> tuple[str, str]:
    records = _supplier_records(payload)
    if records:
        names, skus = [], []
        for record in records:
            supplier = record.get("Supplier", {}) if isinstance(record, dict) else {}
            if not isinstance(supplier, dict):
                supplier = {}
            names.append(_first_text(record, ["Supplier.Name", "SupplierName", "Name", "Supplier"]) or _first_text(supplier, ["Name", "SupplierName"]))
            skus.append(_first_text(record, ["SupplierSKU", "SupplierSku", "SKU", "Supplier SKUs"]))
        return _pipe(names), _pipe(skus)
    if isinstance(payload, dict):
        return (_first_text(payload, ["Suppliers", "SupplierNames", "Supplier Names"]), _first_text(payload, ["Supplier SKUs", "SupplierSKUs", "SupplierSku", "SupplierSKU"]))
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
    records = _stock_location_records(payload)
    if records:
        names, types, inventories = [], [], []
        for record in records:
            names.append(_first_text(record, ["StockLocationReference", "Reference", "Name", "StockLocation.Name", "StockLocationReference.Name"]))
            types.append(_first_text(record, ["StockLocationType", "Type", "StockLocation.Type"]))
            inventories.append(_first_text(record, ["CurrentInventory", "Inventory", "Quantity", "Available", "Stock"] ))
        return _pipe(names), _pipe(types), _pipe(inventories)
    if isinstance(payload, dict):
        return (_first_text(payload, ["Stock Locations", "StockLocations", "StockLocationReferences"]), _first_text(payload, ["Stock Location Type", "StockLocationType"]), _first_text(payload, ["Stock Location Current Inventories", "Current Inventories", "CurrentInventories"]))
    return "", "", ""


def _stock_location_records(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    candidates: list[Any] = []
    for key in ["WarehouseInformation", "warehouseInformation"]:
        value = payload.get(key)
        if isinstance(value, dict):
            candidates.append(value)
    candidates.append(payload)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in ["StockLocations", "stockLocations", "Locations", "locations"]:
            value = candidate.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _pipe(values: list[Any]) -> str:
    return "|".join(str(value).strip() for value in values)


def _pipe_lookup(keys_pipe: str, values_pipe: str, wanted_key: str) -> str:
    if not wanted_key:
        return ""
    keys = [part.strip() for part in str(keys_pipe).split("|")]
    values = [part.strip() for part in str(values_pipe).split("|")]
    for index, key in enumerate(keys):
        if key.casefold() == wanted_key.casefold():
            return values[index] if index < len(values) else ""
    return ""


if __name__ == "__main__":
    raise SystemExit(main())
