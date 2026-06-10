from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig
from src.storefeeder_stock_export import read_csv, read_storefeeder_export


PREVIEW_COLUMNS = [
    "ProductID",
    "SKU",
    "supplier",
    "SupplierID",
    "Supplier.Name",
    "SupplierSKU",
    "SupplierStockLevel",
    "SupplierCosts",
    "setup_status",
    "reason",
]

BLOCKER_COLUMNS = [
    "ProductID",
    "SKU",
    "supplier",
    "supplier_sku",
    "supplier_free_stock",
    "blocker_reason",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build StoreFeeder product-supplier setup dry-run payloads.")
    parser.add_argument("--supplier-setup-needed", required=True, type=Path)
    parser.add_argument("--storefeeder-supplier-id-map", default=Path("data/storefeeder_supplier_ids.csv"), type=Path)
    parser.add_argument("--supplier-mapping", required=True, type=Path)
    parser.add_argument("--storefeeder-export", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("reports/storefeeder_supplier_setup"), type=Path)
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--limit", "--api-limit", dest="limit", type=int)
    parser.add_argument("--live-supplier-setup", action="store_true")
    parser.add_argument("--supplier-costs", default=0, type=int)
    parser.add_argument("--limit-preview", default=20, type=int)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be zero or greater")
    if args.live_supplier_setup and (args.limit is None or args.limit <= 0):
        parser.error("--live-supplier-setup requires an explicit positive --limit")
    return args


def main() -> int:
    args = parse_args()
    setup_needed = read_csv(args.supplier_setup_needed)
    supplier_id_map = read_csv(args.storefeeder_supplier_id_map)
    supplier_mapping = read_csv(args.supplier_mapping)
    storefeeder_export = read_storefeeder_export(args.storefeeder_export)

    preview, blockers, already_setup = build_supplier_setup_preview(
        setup_needed,
        supplier_id_map,
        supplier_mapping,
        storefeeder_export,
        supplier_costs=args.supplier_costs,
    )
    if args.limit is not None:
        preview = preview.head(args.limit).copy()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "supplier_setup_payload_preview": args.out_dir / "supplier_setup_payload_preview.csv",
        "supplier_setup_dry_run_summary": args.out_dir / "supplier_setup_dry_run_summary.csv",
        "supplier_setup_blockers": args.out_dir / "supplier_setup_blockers.csv",
        "supplier_setup_already_setup": args.out_dir / "supplier_setup_already_setup.csv",
    }
    preview.to_csv(paths["supplier_setup_payload_preview"], index=False)
    blockers.to_csv(paths["supplier_setup_blockers"], index=False)
    already_setup.to_csv(paths["supplier_setup_already_setup"], index=False)
    summary = _summary_frame(setup_needed, preview, blockers, already_setup, live=args.live_supplier_setup)
    summary.to_csv(paths["supplier_setup_dry_run_summary"], index=False)

    print("StoreFeeder supplier setup dry run" if not args.live_supplier_setup else "StoreFeeder supplier setup live test")
    print(summary.to_string(index=False))
    print("\nPayload preview:")
    print(preview.head(args.limit_preview).to_string(index=False))
    print("\nBlockers:")
    print(blockers.head(args.limit_preview).to_string(index=False))
    print("\nWrote reports:")
    for path in paths.values():
        print(path)

    if not args.live_supplier_setup:
        print("\nDry-run only. No StoreFeeder API calls were made.")
        return 0

    if not blockers.empty:
        raise SystemExit("Blocked live supplier setup because blocker rows are present.")
    if preview.empty:
        raise SystemExit("Blocked live supplier setup because preview is empty.")

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    live_paths = _send_supplier_setup_batch(client, preview, args.out_dir)

    print("\nLive supplier setup reports:")
    for path in live_paths.values():
        print(path)
    failure_count = _csv_row_count(live_paths["supplier_setup_failures"])
    if failure_count:
        raise SystemExit(f"Stopped after requested live supplier setup batch because {failure_count} failures were reported.")

    return 0


def build_supplier_setup_preview(
    setup_needed: pd.DataFrame,
    supplier_id_map: pd.DataFrame,
    supplier_mapping: pd.DataFrame,
    storefeeder_export: pd.DataFrame,
    *,
    supplier_costs: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _require_columns(setup_needed, ["SKU", "ProductID", "supplier", "supplier_sku", "supplier_free_stock"], "supplier_setup_needed.csv")
    _require_columns(supplier_id_map, ["supplier", "SupplierID", "Supplier.Name"], "storefeeder_supplier_ids.csv")
    _require_columns(supplier_mapping, ["storefeeder_sku", "supplier", "supplier_sku"], "supplier_mapping.csv")

    rows = setup_needed.copy()
    for column in ["SKU", "ProductID", "supplier", "supplier_sku", "supplier_free_stock"]:
        rows[column] = rows[column].fillna("").astype(str).str.strip()
    rows = rows[rows["SKU"].ne("")].copy()

    id_map = supplier_id_map[["supplier", "SupplierID", "Supplier.Name"]].copy()
    for column in ["supplier", "SupplierID", "Supplier.Name"]:
        id_map[column] = id_map[column].fillna("").astype(str).str.strip()
    id_map["_supplier_key"] = id_map["supplier"].str.casefold()

    rows["_supplier_key"] = rows["supplier"].str.casefold()
    rows = rows.merge(id_map[["_supplier_key", "SupplierID", "Supplier.Name"]], how="left", on="_supplier_key")
    rows["SupplierStockLevel"] = pd.to_numeric(rows["supplier_free_stock"], errors="coerce")
    rows["SupplierID_numeric"] = pd.to_numeric(rows["SupplierID"], errors="coerce")
    rows["blocker_reason"] = rows.apply(_setup_blocker_reason, axis=1)

    already_setup = _already_setup_rows(rows, storefeeder_export)
    if not already_setup.empty:
        already_keys = set(zip(already_setup["ProductID"], already_setup["SupplierID"].astype(str), already_setup["SupplierSKU"].str.upper()))
        rows["_already_key"] = list(zip(rows["ProductID"], rows["SupplierID"].fillna("").astype(str), rows["supplier_sku"].str.upper()))
        rows = rows[~rows["_already_key"].isin(already_keys)].copy()

    rows = rows.drop_duplicates(subset=["ProductID", "SupplierID", "supplier_sku"], keep="first")
    blockers = rows[rows["blocker_reason"].ne("")].copy()
    valid = rows[rows["blocker_reason"].eq("")].copy()

    if valid.empty:
        preview = pd.DataFrame(columns=PREVIEW_COLUMNS)
    else:
        preview = pd.DataFrame(
            {
                "ProductID": valid["ProductID"],
                "SKU": valid["SKU"],
                "supplier": valid["supplier"],
                "SupplierID": valid["SupplierID_numeric"].astype(int),
                "Supplier.Name": valid["Supplier.Name"],
                "SupplierSKU": valid["supplier_sku"].str.upper(),
                "SupplierStockLevel": valid["SupplierStockLevel"].astype(int),
                "SupplierCosts": int(supplier_costs),
                "setup_status": "setup_ready",
                "reason": "supplier mapping validates, product supplier setup is missing",
            },
            columns=PREVIEW_COLUMNS,
        )

    if blockers.empty:
        blocker_report = pd.DataFrame(columns=BLOCKER_COLUMNS)
    else:
        blocker_report = blockers[
            ["ProductID", "SKU", "supplier", "supplier_sku", "supplier_free_stock", "blocker_reason"]
        ].copy()
    return preview.reset_index(drop=True), blocker_report.reset_index(drop=True), already_setup.reset_index(drop=True)


def _setup_blocker_reason(row: pd.Series) -> str:
    reasons = []
    product_id = str(row.get("ProductID", "")).strip()
    supplier_id = str(row.get("SupplierID", "")).strip()
    supplier_name = str(row.get("Supplier.Name", "")).strip()
    if not product_id:
        reasons.append("missing_product_id")
    elif not product_id.isdigit():
        reasons.append("non_integer_product_id")
    if not str(row.get("supplier", "")).strip():
        reasons.append("missing_supplier")
    if not str(row.get("supplier_sku", "")).strip():
        reasons.append("missing_supplier_sku")
    if not supplier_id:
        reasons.append("missing_supplier_id_mapping")
    elif pd.isna(row.get("SupplierID_numeric")) or float(row["SupplierID_numeric"]) != int(row["SupplierID_numeric"]):
        reasons.append("non_integer_supplier_id")
    if not supplier_name:
        reasons.append("missing_supplier_name")
    stock = row.get("SupplierStockLevel")
    if pd.isna(stock):
        reasons.append("missing_supplier_stock")
    elif float(stock) < 0 or float(stock) != int(stock):
        reasons.append("invalid_supplier_stock")
    return "|".join(reasons)


def _already_setup_rows(rows: pd.DataFrame, storefeeder_export: pd.DataFrame) -> pd.DataFrame:
    required = ["ID", "SKU", "Suppliers", "Supplier SKUs"]
    if any(column not in storefeeder_export.columns for column in required):
        return pd.DataFrame(columns=PREVIEW_COLUMNS)

    existing_rows = []
    for _, product in storefeeder_export.iterrows():
        product_id = str(product.get("ID", "")).strip()
        sku = str(product.get("SKU", "")).strip()
        suppliers = _pipe_parts(product.get("Suppliers", ""))
        supplier_skus = _pipe_parts(product.get("Supplier SKUs", ""))
        for index, supplier in enumerate(suppliers):
            supplier_sku = supplier_skus[index] if index < len(supplier_skus) else ""
            if not supplier or not supplier_sku:
                continue
            existing_rows.append(
                {
                    "ProductID": product_id,
                    "SKU": sku,
                    "_supplier_key": supplier.casefold(),
                    "_supplier_sku_key": supplier_sku.upper(),
                }
            )
    if not existing_rows:
        return pd.DataFrame(columns=PREVIEW_COLUMNS)

    existing = pd.DataFrame(existing_rows)
    check = rows.copy()
    check["_supplier_sku_key"] = check["supplier_sku"].str.upper()
    merged = check.merge(existing, how="inner", on=["ProductID", "_supplier_key", "_supplier_sku_key"], suffixes=("", "_export"))
    if merged.empty:
        return pd.DataFrame(columns=PREVIEW_COLUMNS)

    return pd.DataFrame(
        {
            "ProductID": merged["ProductID"],
            "SKU": merged["SKU"],
            "supplier": merged["supplier"],
            "SupplierID": merged["SupplierID"],
            "Supplier.Name": merged["Supplier.Name"],
            "SupplierSKU": merged["supplier_sku"].str.upper(),
            "SupplierStockLevel": pd.to_numeric(merged["SupplierStockLevel"], errors="coerce").fillna(0).astype(int),
            "SupplierCosts": 0,
            "setup_status": "already_setup",
            "reason": "supplier and SupplierSKU already present in StoreFeeder export",
        },
        columns=PREVIEW_COLUMNS,
    )


def _require_columns(df: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: " + ", ".join(missing))


def _pipe_parts(value: object) -> list[str]:
    return [part.strip() for part in str(value).split("|")]


def _summary_frame(
    setup_needed: pd.DataFrame,
    preview: pd.DataFrame,
    blockers: pd.DataFrame,
    already_setup: pd.DataFrame,
    *,
    live: bool,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"metric": "dry_run", "value": "no" if live else "yes"},
            {"metric": "supplier_setup_needed_input_rows", "value": len(setup_needed)},
            {"metric": "supplier_setup_preview_rows", "value": len(preview)},
            {"metric": "supplier_setup_blocker_rows", "value": len(blockers)},
            {"metric": "supplier_already_setup_rows", "value": len(already_setup)},
            {"metric": "live_supplier_setup", "value": "yes" if live else "no"},
        ]
    )

def _send_supplier_setup_batch(client: StoreFeederApiClient, preview: pd.DataFrame, out_dir: Path) -> dict[str, Path]:
    paths = {
        "supplier_setup_success": out_dir / "supplier_setup_success.csv",
        "supplier_setup_failures": out_dir / "supplier_setup_failures.csv",
        "supplier_setup_readback_verification": out_dir / "supplier_setup_readback_verification.csv",
        "supplier_setup_raw_responses": out_dir / "supplier_setup_raw_responses.json",
    }
    report_columns = [
        "batch_index",
        "ProductID",
        "SKU",
        "SupplierID",
        "Supplier.Name",
        "SupplierSKU",
        "status_code",
        "readback_status_code",
        "readback_verified",
    ]
    success_rows = []
    failure_rows = []
    verification_rows = []
    raw_responses = []

    for batch_index, (_, row) in enumerate(preview.iterrows(), start=1):
        item = _setup_item(row)
        product_id = str(row["ProductID"]).strip()
        setup_response = client.create_product_supplier(product_id, item)
        readback = client.get_product_suppliers(product_id)
        verified = _readback_contains_supplier(readback, row)
        report_row = {
            "batch_index": batch_index,
            "ProductID": product_id,
            "SKU": row["SKU"],
            "SupplierID": row["SupplierID"],
            "Supplier.Name": row["Supplier.Name"],
            "SupplierSKU": row["SupplierSKU"],
            "status_code": setup_response["_status_code"],
            "readback_status_code": readback["_status_code"],
            "readback_verified": verified,
        }
        target = success_rows if int(setup_response["_status_code"]) < 400 and int(readback["_status_code"]) < 400 and verified else failure_rows
        target.append(report_row)
        verification_rows.append(report_row)
        raw_responses.append(
            {
                "batch_index": batch_index,
                "ProductID": product_id,
                "SKU": row["SKU"],
                "setup": setup_response,
                "readback": readback,
            }
        )

    pd.DataFrame(success_rows, columns=report_columns).to_csv(paths["supplier_setup_success"], index=False)
    pd.DataFrame(failure_rows, columns=report_columns).to_csv(paths["supplier_setup_failures"], index=False)
    pd.DataFrame(verification_rows, columns=report_columns).to_csv(paths["supplier_setup_readback_verification"], index=False)
    paths["supplier_setup_raw_responses"].write_text(json.dumps(raw_responses, indent=2), encoding="utf-8")
    return paths


def _setup_item(row: pd.Series) -> dict[str, Any]:
    return {
        "Supplier": {
            "SupplierID": int(row["SupplierID"]),
            "Name": str(row["Supplier.Name"]).strip(),
        },
        "SupplierSKU": str(row["SupplierSKU"]).strip(),
        "SupplierStockLevel": int(row["SupplierStockLevel"]),
        "SupplierCosts": int(row["SupplierCosts"]),
    }


def _readback_contains_supplier(readback: dict[str, Any], row: pd.Series) -> bool:
    if int(readback.get("_status_code", 0)) >= 400:
        return False

    payload = readback.get("response")
    if isinstance(payload, dict):
        records = payload.get("value", [])
    elif isinstance(payload, list):
        records = payload
    else:
        records = []

    if isinstance(records, dict):
        records = [records]

    expected_id = str(row["SupplierID"]).strip()
    expected_sku = str(row["SupplierSKU"]).strip().upper()

    for record in records:
        if not isinstance(record, dict):
            continue

        supplier = record.get("Supplier", {})
        if not isinstance(supplier, dict):
            supplier = {}

        supplier_id = str(supplier.get("SupplierID", supplier.get("ID", ""))).strip()
        supplier_sku = str(record.get("SupplierSKU", record.get("SupplierSku", ""))).strip().upper()

        if supplier_id == expected_id and supplier_sku == expected_sku:
            return True

    return False


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(pd.read_csv(path, dtype=str, keep_default_na=False))

if __name__ == "__main__":
    raise SystemExit(main())
