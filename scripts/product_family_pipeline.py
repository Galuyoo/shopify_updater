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


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ["Items", "items", "Data", "data", "Results", "results"]:
        if isinstance(payload.get(key), list):
            return payload[key]
    return []


def pick_col(df: pd.DataFrame, names: list[str]) -> str:
    lower = {c.lower().strip(): c for c in df.columns}
    for name in names:
        key = name.lower().strip()
        if key in lower:
            return lower[key]
    return ""


def brand_name(value: Any) -> str:
    if isinstance(value, dict):
        for key in ["Name", "BrandName", "Brand", "brand"]:
            if value.get(key):
                return str(value[key]).strip()
        return json.dumps(value, default=str)
    return str(value or "").strip()


def infer_supplier(brand: str) -> str:
    return "Uneek" if "uneek" in str(brand).casefold() else "Ralawise"


def parse_expected_skus(raw: str) -> set[str]:
    return {x.strip().upper() for x in raw.split(",") if x.strip()}


def supplier_id_lookup(supplier_ids: pd.DataFrame, supplier: str) -> str:
    id_col = pick_col(supplier_ids, ["SupplierID", "ID", "Id", "supplier_id"])
    if not id_col:
        return ""
    needle = supplier.casefold()
    for _, row in supplier_ids.iterrows():
        text = " ".join(str(x) for x in row.values).casefold()
        if needle in text:
            return str(row.get(id_col, "")).strip()
    return ""


def build_stock_lookup(ralawise_path: Path, uneek_path: Path) -> pd.DataFrame:
    from src.stock_mapping import build_supplier_stock_lookup

    ralawise = read_csv(ralawise_path)
    uneek = read_csv(uneek_path)

    stock = build_supplier_stock_lookup(ralawise, uneek).copy()

    supplier_col = pick_col(stock, ["supplier", "Supplier"])
    sku_col = pick_col(stock, ["supplier_sku", "SupplierSKU", "SKU", "sku"])
    qty_col = pick_col(stock, ["supplier_free_stock", "quantity", "qty", "free", "stock", "available"])

    if not supplier_col or not sku_col or not qty_col:
        raise SystemExit(f"BLOCKED: stock lookup columns not recognised: {list(stock.columns)}")

    out = stock[[supplier_col, sku_col, qty_col]].copy()
    out.columns = ["supplier", "SupplierSKU", "supplier_free_stock"]
    out["supplier"] = out["supplier"].astype(str).str.strip()
    out["SupplierSKU"] = out["SupplierSKU"].astype(str).str.strip().str.upper()
    out["supplier_free_stock"] = out["supplier_free_stock"].astype(str).str.strip()

    dupes = out[out.duplicated(["supplier", "SupplierSKU"], keep=False)]
    if len(dupes):
        out["_duplicate"] = out.duplicated(["supplier", "SupplierSKU"], keep=False)
    else:
        out["_duplicate"] = False

    return out


def run_command(command: list[str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as f:
        p = subprocess.run(command, cwd=PROJECT_ROOT, text=True, stdout=f, stderr=subprocess.STDOUT)
    return p.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="One-command StoreFeeder product family pipeline.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")

    parser.add_argument("--family-code", required=True)
    parser.add_argument("--sku-prefix", required=True)
    parser.add_argument("--expected-skus", default="")
    parser.add_argument("--out-root", type=Path, default=Path("reports/product_family_pipeline"))
    parser.add_argument("--targets", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--supplier-id-map", type=Path, default=Path("data/storefeeder_supplier_ids.csv"))
    parser.add_argument("--supplier-mapping", type=Path, default=Path("data/supplier_mapping.csv"))
    parser.add_argument("--storefeeder-export", type=Path, default=Path("data/storefeeder_products_latest.xlsx"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=500)
    parser.add_argument("--append-targets", action="store_true", help="Append supplier-only targets during execute.")
    parser.add_argument("--run-stock-now", action="store_true", help="Run supplier-only stock update immediately during execute.")
    args = parser.parse_args()

    load_env(args.env_file)

    from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / args.family_code / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig())

    expected = parse_expected_skus(args.expected_skus)

    found = []
    page = 1
    while page <= args.max_pages:
        result = client.get_products_page(page=page, page_size=args.page_size)
        items = extract_items(result.get("response", {}))

        for p in items:
            sku = str(p.get("SKU") or p.get("Sku") or p.get("sku") or "").strip()
            if sku.upper().startswith(args.sku_prefix.upper()):
                found.append({
                    "ProductID": str(p.get("ID") or p.get("Id") or p.get("ProductID") or p.get("ProductId") or "").strip(),
                    "SKU": sku,
                    "Name": str(p.get("Name") or p.get("ProductName") or "").strip(),
                })

        print(f"page {page}: scanned {len(items)}, found {len(found)}")
        if len(items) < args.page_size:
            break
        page += 1

    scan = pd.DataFrame(found)
    if not scan.empty:
        scan = scan.drop_duplicates(["ProductID", "SKU"]).sort_values("SKU")

    write_csv(scan, out_dir / "01_storefeeder_scan.csv")

    blockers = []

    if scan.empty:
        blockers.append({"stage": "scan", "reason": "no_storefeeder_products_found", "SKU": ""})

    found_skus = set(scan["SKU"].astype(str).str.upper()) if not scan.empty else set()
    missing_expected = sorted(expected - found_skus)
    if missing_expected:
        for sku in missing_expected:
            blockers.append({"stage": "expected_skus", "reason": "expected_sku_missing_from_storefeeder", "SKU": sku})

    write_csv(pd.DataFrame({"missing_expected_sku": missing_expected}), out_dir / "expected_sku_missing_from_storefeeder.csv")

    enriched_rows = []
    if not scan.empty:
        for _, row in scan.iterrows():
            detail = client.get_product(row["ProductID"]).get("response", {})
            brand = brand_name(detail.get("Brand"))
            supplier = infer_supplier(brand)

            suppliers = []
            for s in detail.get("Suppliers", []):
                suppliers.append({
                    "supplier_name": s.get("Supplier", {}).get("Name", ""),
                    "supplier_id": s.get("Supplier", {}).get("SupplierID", ""),
                    "supplier_sku": s.get("SupplierSKU", ""),
                    "supplier_stock": s.get("SupplierStockLevel", ""),
                    "priority": s.get("Priority", ""),
                })

            locations = []
            for loc in detail.get("WarehouseInformation", {}).get("StockLocations", []):
                sl = loc.get("StockLocation", {})
                locations.append({
                    "reference": sl.get("StockLocationReference", ""),
                    "available": loc.get("Available", ""),
                    "physical": loc.get("PhysicalStock", ""),
                    "type": sl.get("StockLocationType", ""),
                })

            enriched_rows.append({
                "ProductID": row["ProductID"],
                "SKU": row["SKU"],
                "Name": detail.get("Name", row["Name"]),
                "Brand": brand,
                "supplier": supplier,
                "SupplierSKU": str(row["SKU"]).strip().upper(),
                "existing_suppliers_json": json.dumps(suppliers, default=str),
                "existing_locations_json": json.dumps(locations, default=str),
            })

    enriched = pd.DataFrame(enriched_rows)
    write_csv(enriched, out_dir / "02_product_api_enriched.csv")

    valid = pd.DataFrame()
    mapping_blockers = pd.DataFrame()

    if not enriched.empty:
        stock_lookup = build_stock_lookup(args.ralawise_stock, args.uneek_stock)
        duplicate_stock = stock_lookup[stock_lookup["_duplicate"] == True].copy()
        write_csv(duplicate_stock, out_dir / "stock_feed_duplicate_skus.csv")

        if len(duplicate_stock):
            blockers.append({"stage": "stock_feed", "reason": "duplicate_supplier_sku_in_stock_feed", "SKU": ""})

        merged = enriched.merge(
            stock_lookup.drop(columns=["_duplicate"]),
            on=["supplier", "SupplierSKU"],
            how="left",
        )

        mapping_blockers = merged[
            merged["ProductID"].astype(str).str.strip().eq("")
            | merged["SKU"].astype(str).str.strip().eq("")
            | merged["SupplierSKU"].astype(str).str.strip().eq("")
            | merged["supplier_free_stock"].fillna("").astype(str).str.strip().eq("")
        ].copy()

        if len(mapping_blockers):
            mapping_blockers["blocker_reason"] = "missing_product_or_supplier_stock_match"
            for _, row in mapping_blockers.iterrows():
                blockers.append({
                    "stage": "supplier_mapping",
                    "reason": row["blocker_reason"],
                    "SKU": row.get("SKU", ""),
                })

        valid = merged.drop(mapping_blockers.index).copy()

    write_csv(valid, out_dir / "03_supplier_mapping_validated.csv")
    write_csv(mapping_blockers, out_dir / "04_supplier_mapping_blockers.csv")

    supplier_setup = pd.DataFrame()
    stock_targets = pd.DataFrame()
    target_blockers = pd.DataFrame()

    if not valid.empty:
        supplier_setup = pd.DataFrame({
            "ProductID": valid["ProductID"],
            "SKU": valid["SKU"],
            "supplier": valid["supplier"],
            "supplier_sku": valid["SupplierSKU"],
            "supplier_free_stock": valid["supplier_free_stock"],
            "stock_location": valid["supplier"],
        })

        supplier_ids = read_csv(args.supplier_id_map)

        target_rows = []
        for _, row in valid.iterrows():
            supplier = str(row["supplier"]).strip()
            target_rows.append({
                "ProductID": row["ProductID"],
                "SKU": row["SKU"],
                "supplier": supplier,
                "SupplierID": supplier_id_lookup(supplier_ids, supplier),
                "Supplier.Name": supplier,
                "SupplierSKU": row["SupplierSKU"],
                "stock_location": supplier,
                "preserve_existing_locations": "yes",
                "warehouse_safe_mode": "yes",
                "skip_stock_location_update": "yes",
            })

        stock_targets = pd.DataFrame(target_rows)
        target_blockers = stock_targets[
            stock_targets["ProductID"].astype(str).str.strip().eq("")
            | stock_targets["SKU"].astype(str).str.strip().eq("")
            | stock_targets["SupplierID"].astype(str).str.strip().eq("")
            | stock_targets["SupplierSKU"].astype(str).str.strip().eq("")
        ].copy()

        if len(target_blockers):
            for _, row in target_blockers.iterrows():
                blockers.append({"stage": "stock_target", "reason": "missing_required_target_field", "SKU": row.get("SKU", "")})

    write_csv(supplier_setup, out_dir / "05_supplier_setup_needed.csv")
    write_csv(stock_targets, out_dir / "06_stock_targets_supplier_only.csv")
    write_csv(target_blockers, out_dir / "07_stock_target_blockers.csv")

    blocker_df = pd.DataFrame(blockers)
    write_csv(blocker_df, out_dir / "BLOCKERS.csv")

    ready = len(blockers) == 0 and len(valid) > 0

    summary = pd.DataFrame([
        {"metric": "mode", "value": "execute" if args.execute else "plan"},
        {"metric": "family_code", "value": args.family_code},
        {"metric": "sku_prefix", "value": args.sku_prefix},
        {"metric": "found_products", "value": len(scan)},
        {"metric": "expected_skus", "value": len(expected)},
        {"metric": "missing_expected_skus", "value": len(missing_expected)},
        {"metric": "validated_supplier_rows", "value": len(valid)},
        {"metric": "supplier_mapping_blockers", "value": len(mapping_blockers)},
        {"metric": "stock_target_rows", "value": len(stock_targets)},
        {"metric": "stock_target_blockers", "value": len(target_blockers)},
        {"metric": "total_blockers", "value": len(blockers)},
        {"metric": "ready_to_execute", "value": "yes" if ready else "no"},
    ])
    write_csv(summary, out_dir / "SUMMARY.csv")

    operation_packet = {
        "family_code": args.family_code,
        "sku_prefix": args.sku_prefix,
        "run_id": run_id,
        "out_dir": str(out_dir),
        "ready_to_execute": ready,
        "summary": {row["metric"]: row["value"] for _, row in summary.iterrows()},
        "blockers_file": str(out_dir / "BLOCKERS.csv"),
        "summary_file": str(out_dir / "SUMMARY.csv"),
    }
    (out_dir / "OPERATION_PACKET.json").write_text(json.dumps(operation_packet, indent=2), encoding="utf-8")

    chatgpt_summary = [
        f"FAMILY: {args.family_code}",
        f"RUN: {run_id}",
        f"READY_TO_EXECUTE: {'yes' if ready else 'no'}",
        f"FOUND_PRODUCTS: {len(scan)}",
        f"VALIDATED_SUPPLIER_ROWS: {len(valid)}",
        f"TOTAL_BLOCKERS: {len(blockers)}",
        f"OUT_DIR: {out_dir}",
        "",
        "Paste this file plus SUMMARY.csv/BLOCKERS.csv output to ChatGPT if blocked.",
    ]
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(chatgpt_summary), encoding="utf-8")

    print("\nPIPELINE SUMMARY")
    print(summary.to_string(index=False))
    print("\nReports:", out_dir)

    if not ready:
        print("\nBLOCKED. Paste this to ChatGPT:")
        print(out_dir / "CHATGPT_BRIEF.txt")
        return 2

    if args.plan:
        print("\nPLAN ONLY. No StoreFeeder writes made.")
        return 0

    setup_dir = out_dir / "supplier_setup_live"
    dry_dir = out_dir / "supplier_setup_dryrun"

    dry_cmd = [
        sys.executable,
        "scripts/build_storefeeder_supplier_setup.py",
        "--supplier-setup-needed", str(out_dir / "05_supplier_setup_needed.csv"),
        "--storefeeder-supplier-id-map", str(args.supplier_id_map),
        "--supplier-mapping", str(args.supplier_mapping),
        "--storefeeder-export", str(args.storefeeder_export),
        "--out-dir", str(dry_dir),
        "--limit", str(len(supplier_setup)),
    ]
    dry_code = run_command(dry_cmd, out_dir / "supplier_setup_dryrun.log")

    if dry_code != 0:
        print("BLOCKED: supplier setup dry-run failed.")
        print("Log:", out_dir / "supplier_setup_dryrun.log")
        return 2

    dry_blockers = read_csv(dry_dir / "supplier_setup_blockers.csv")
    if len(dry_blockers):
        print("BLOCKED: supplier setup dry-run produced blockers.")
        print("Blockers:", dry_dir / "supplier_setup_blockers.csv")
        return 2

    live_cmd = dry_cmd.copy()
    live_cmd[live_cmd.index(str(dry_dir))] = str(setup_dir)
    live_cmd.append("--live-supplier-setup")

    live_code = run_command(live_cmd, out_dir / "supplier_setup_live.log")
    if live_code != 0:
        print("BLOCKED: supplier setup live failed.")
        print("Log:", out_dir / "supplier_setup_live.log")
        return 2

    success = read_csv(setup_dir / "supplier_setup_success.csv")
    failures = read_csv(setup_dir / "supplier_setup_failures.csv")
    readback = read_csv(setup_dir / "supplier_setup_readback_verification.csv")

    readback_ok = (
        len(failures) == 0
        and len(success) == len(supplier_setup)
        and readback["readback_verified"].astype(str).str.casefold().eq("true").all()
    )

    if not readback_ok:
        print("BLOCKED: supplier setup readback failed.")
        print("Readback:", setup_dir / "supplier_setup_readback_verification.csv")
        return 2

    if args.append_targets:
        main = read_csv(args.targets)
        backup = args.targets.with_name(
            args.targets.stem + f".backup_before_{args.family_code}_{run_id}.csv"
        )
        write_csv(main, backup)

        existing_keys = set(zip(main["ProductID"], main["SupplierID"], main["SupplierSKU"]))
        stock_targets["_key"] = list(zip(stock_targets["ProductID"], stock_targets["SupplierID"], stock_targets["SupplierSKU"]))
        append = stock_targets[~stock_targets["_key"].isin(existing_keys)].drop(columns=["_key"]).copy()

        all_cols = list(main.columns)
        for col in append.columns:
            if col not in all_cols:
                all_cols.append(col)

        combined = pd.concat(
            [
                main.reindex(columns=all_cols, fill_value=""),
                append.reindex(columns=all_cols, fill_value=""),
            ],
            ignore_index=True,
        )
        write_csv(combined, args.targets)
        write_csv(append, out_dir / "targets_appended.csv")

        print(f"Appended targets: {len(append)}")
        print(f"Target backup: {backup}")

    if args.run_stock_now:
        stock_dir = out_dir / "supplier_only_stock_live"
        stock_cmd = [
            sys.executable,
            "scripts/run_supplier_stock_fast_update.py",
            "--targets", str(out_dir / "06_stock_targets_supplier_only.csv"),
            "--out-dir", str(stock_dir),
            "--api-limit", str(len(stock_targets)),
            "--skip-stock-refresh",
            "--live-stock-update",
        ]
        stock_code = run_command(stock_cmd, out_dir / "supplier_only_stock_live.log")
        if stock_code != 0:
            print("BLOCKED: supplier-only stock update failed.")
            print("Log:", out_dir / "supplier_only_stock_live.log")
            return 2

    print("\nEXECUTE COMPLETE.")
    print("Reports:", out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())