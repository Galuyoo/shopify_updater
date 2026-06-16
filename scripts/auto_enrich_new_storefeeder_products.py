from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
CORE = Path(__file__).with_name("_auto_enrich_new_storefeeder_products_core.py")
TARGET = ROOT / "data" / "storefeeder_supplier_stock_update_targets.csv"


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [{k: (v if v is not None else "") for k, v in row.items()} for row in reader]
    return fieldnames, rows


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _load_override_file(path: str | None, strategy: str) -> Dict[str, Dict[str, str]]:
    if not path:
        return {}

    p = Path(path)
    if not p.is_absolute():
        p = ROOT / p

    if not p.exists():
        raise FileNotFoundError(f"Override file not found: {p}")

    _, rows = _read_csv(p)
    overrides: Dict[str, Dict[str, str]] = {}

    for row in rows:
        sku = (row.get("SKU") or row.get("sku") or "").strip()
        if not sku:
            continue
        overrides[sku] = {
            "SKU": sku,
            "reason": (row.get("reason") or row.get("Reason") or "").strip(),
            "requested_strategy": strategy,
        }

    return overrides


def _latest_report_dir(out_root: str | None) -> Path | None:
    root = Path(out_root or "reports/auto_enrich_new_products")
    if not root.is_absolute():
        root = ROOT / root

    if not root.exists():
        return None

    dirs = [p for p in root.iterdir() if p.is_dir()]
    if not dirs:
        return None

    return max(dirs, key=lambda p: p.stat().st_mtime)


def _sku_set(rows: List[Dict[str, str]]) -> set[str]:
    values = set()
    for row in rows:
        sku = (row.get("SKU") or row.get("sku") or "").strip()
        if sku:
            values.add(sku)
    return values


def _target_sku_set() -> set[str]:
    _, rows = _read_csv(TARGET)
    values = set()

    for row in rows:
        for key in ("SKU", "sku", "SupplierSKU", "supplier_sku"):
            value = (row.get(key) or "").strip()
            if value:
                values.add(value)

    return values


def _summary_to_dict(rows: List[Dict[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in rows:
        metric = (row.get("metric") or "").strip()
        value = (row.get("value") or "").strip()
        if metric:
            out[metric] = value
    return out


def _dict_to_summary(summary: Dict[str, str]) -> List[Dict[str, str]]:
    return [{"metric": k, "value": v} for k, v in summary.items()]


def _postprocess_reports(args: argparse.Namespace) -> None:
    warehouse_overrides = _load_override_file(args.warehouse_only_skus, "warehouse_only")
    supplier_overrides = _load_override_file(args.supplier_synced_skus, "supplier_synced_inventory")

    # Warehouse-only wins if the same SKU is in both files.
    overrides = dict(supplier_overrides)
    overrides.update(warehouse_overrides)

    if not overrides:
        return

    report_dir = _latest_report_dir(args.out_root)
    if report_dir is None:
        raise RuntimeError("No auto-enrichment report directory found after core script run.")

    ready_path = report_dir / "new_products_ready.csv"
    quarantine_path = report_dir / "new_products_quarantine.csv"
    skipped_path = report_dir / "existing_products_skipped.csv"
    summary_path = report_dir / "enrichment_summary.csv"

    ready_fields, ready_rows = _read_csv(ready_path)
    quarantine_fields, quarantine_rows = _read_csv(quarantine_path)
    skipped_fields, skipped_rows = _read_csv(skipped_path)
    summary_fields, summary_rows = _read_csv(summary_path)

    target_skus = _target_sku_set()
    ready_skus = _sku_set(ready_rows)
    quarantine_skus = _sku_set(quarantine_rows)
    skipped_skus = _sku_set(skipped_rows)

    required_quarantine_fields = [
        "ProductID",
        "SKU",
        "Parent SKU",
        "Name",
        "SupplierSKU",
        "stock_strategy",
        "stock_location",
        "sellable_stock_location",
        "preserve_existing_locations",
        "warehouse_safe_mode",
        "skip_stock_location_update",
        "allow_stock_location_update",
        "quarantine_reason",
        "reason",
        "classification_reason",
    ]

    if not quarantine_fields:
        quarantine_fields = required_quarantine_fields[:]
    else:
        for field in required_quarantine_fields:
            if field not in quarantine_fields:
                quarantine_fields.append(field)

    appended_missing = 0

    for sku, override in overrides.items():
        already_seen = (
            sku in target_skus
            or sku in ready_skus
            or sku in quarantine_skus
            or sku in skipped_skus
        )

        if already_seen:
            continue

        strategy = override["requested_strategy"]

        quarantine_rows.append({
            "ProductID": "",
            "SKU": sku,
            "Parent SKU": "",
            "Name": "Override SKU not found in StoreFeeder product scan",
            "SupplierSKU": "",
            "stock_strategy": strategy,
            "stock_location": "Warehouse Stock" if strategy == "warehouse_only" else "",
            "sellable_stock_location": "",
            "preserve_existing_locations": "yes" if strategy == "warehouse_only" else "",
            "warehouse_safe_mode": "yes" if strategy == "warehouse_only" else "",
            "skip_stock_location_update": "yes" if strategy == "warehouse_only" else "",
            "allow_stock_location_update": "no" if strategy == "warehouse_only" else "",
            "quarantine_reason": "override_sku_not_found_in_storefeeder_product_scan",
            "reason": override.get("reason", ""),
            "classification_reason": "manual_override_not_seen_in_product_scan",
        })

        quarantine_skus.add(sku)
        appended_missing += 1

    _write_csv(quarantine_path, quarantine_fields, quarantine_rows)

    summary = _summary_to_dict(summary_rows)

    summary["quarantine_rows"] = str(len(quarantine_rows))
    summary["override_skus_loaded"] = str(len(overrides))
    summary["override_skus_existing"] = str(sum(1 for sku in overrides if sku in target_skus or sku in skipped_skus))
    summary["override_skus_ready"] = str(sum(1 for sku in overrides if sku in ready_skus))
    summary["override_skus_quarantined"] = str(sum(1 for sku in overrides if sku in quarantine_skus))
    summary["override_skus_not_found"] = str(appended_missing)

    if not summary_fields:
        summary_fields = ["metric", "value"]

    _write_csv(summary_path, summary_fields, _dict_to_summary(summary))

    if appended_missing:
        print()
        print("Override visibility post-process:")
        print(f"  Added {appended_missing} missing override SKU(s) to quarantine.")
        print(f"  Updated: {quarantine_path}")
        print(f"  Updated: {summary_path}")


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--warehouse-only-skus")
    parser.add_argument("--supplier-synced-skus")
    parser.add_argument("--out-root")
    args, _ = parser.parse_known_args()

    result = subprocess.run([sys.executable, str(CORE), *sys.argv[1:]], cwd=str(ROOT))

    if result.returncode == 0:
        _postprocess_reports(args)

    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
