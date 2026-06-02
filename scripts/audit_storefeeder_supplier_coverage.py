from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_stock_export import read_storefeeder_export


KNOWN_SUPPLIERS = ("Ralawise", "Uneek")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit StoreFeeder supplier/Supplier SKU alignment.")
    parser.add_argument("--storefeeder-export", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("reports/storefeeder_supplier_audit"), type=Path)
    parser.add_argument("--verify-product-suppliers-api", action="store_true", help="Allow supplier assignment checks from API readback when export lacks Suppliers. No writes are made.")
    parser.add_argument("--limit-preview", default=20, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    storefeeder_export = read_storefeeder_export(args.storefeeder_export)
    audit = build_supplier_alignment_audit(
        storefeeder_export,
        verify_product_suppliers_api=args.verify_product_suppliers_api,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    path = args.out_dir / "supplier_alignment_audit.csv"
    audit.to_csv(path, index=False)

    print("StoreFeeder supplier alignment audit dry run")
    print(audit["confidence_status"].value_counts(dropna=False).rename_axis("confidence_status").reset_index(name="count").to_string(index=False))
    print("\nPreview:")
    print(audit.head(args.limit_preview).to_string(index=False))
    print(f"\nWrote: {path}")
    print("No StoreFeeder API write calls were made.")
    return 0


def build_supplier_alignment_audit(
    storefeeder_export: pd.DataFrame,
    *,
    verify_product_suppliers_api: bool = False,
) -> pd.DataFrame:
    required = ["ID", "SKU", "Supplier SKUs"]
    missing = [column for column in required if column not in storefeeder_export.columns]
    if missing:
        raise ValueError("StoreFeeder export missing required columns: " + ", ".join(missing))

    has_suppliers_column = "Suppliers" in storefeeder_export.columns
    rows = []
    for _, product in storefeeder_export.iterrows():
        product_id = str(product.get("ID", "")).strip()
        sku = str(product.get("SKU", "")).strip()
        row = {
            "ProductID": product_id,
            "SKU": sku,
            "current_supplier_names": "",
            "current_supplier_skus": "",
            "confidence_status": "",
            "reason": "",
            "allowed_to_update": False,
        }

        if not has_suppliers_column:
            status = "manual_review" if verify_product_suppliers_api else "supplier_assignment_unverified"
            reason = "StoreFeeder export lacks Suppliers column; do not infer suppliers from Stock Locations"
            rows.append({**row, "confidence_status": status, "reason": reason})
            continue

        alignment = align_suppliers_and_supplier_skus(product.get("Suppliers", ""), product.get("Supplier SKUs", ""))
        row["current_supplier_names"] = "|".join(alignment.keys())
        row["current_supplier_skus"] = "|".join(f"{supplier}:{supplier_sku}" for supplier, supplier_sku in alignment.items())

        unknown_suppliers = [supplier for supplier in alignment if supplier not in KNOWN_SUPPLIERS]
        if unknown_suppliers:
            rows.append(
                {
                    **row,
                    "confidence_status": "manual_review",
                    "reason": "unknown supplier assignment: " + "|".join(unknown_suppliers),
                }
            )
            continue

        rows.append({**row, "confidence_status": "supplier_alignment_observed", "reason": "Suppliers and Supplier SKUs aligned by pipe position"})

    return pd.DataFrame(rows)


def align_suppliers_and_supplier_skus(suppliers: Any, supplier_skus: Any) -> dict[str, str]:
    supplier_values = _split_pipe(suppliers)
    sku_values = _split_pipe(supplier_skus)
    if len(sku_values) < len(supplier_values):
        sku_values = sku_values + [""] * (len(supplier_values) - len(sku_values))
    return {
        supplier: sku_values[index].upper() if index < len(sku_values) else ""
        for index, supplier in enumerate(supplier_values)
    }


def _split_pipe(value: Any) -> list[str]:
    text = "" if pd.isna(value) else str(value)
    if text == "":
        return []
    return [part.strip() for part in text.split("|")]


if __name__ == "__main__":
    raise SystemExit(main())
