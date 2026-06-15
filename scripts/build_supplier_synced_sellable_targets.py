from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_SOURCE = Path("data/storefeeder_supplier_stock_update_targets.csv")
DEFAULT_OUT = Path("data/storefeeder_supplier_stock_update_targets_sellable_authority_test.csv")
YES_VALUES = {"yes", "true", "1", "y"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a guarded sellable-inventory authority target file for supplier-synced rows.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--in-place", action="store_true", help="Overwrite --source instead of writing --out.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source = args.source
    out = source if args.in_place else args.out
    targets = pd.read_csv(source, dtype=str, keep_default_na=False)
    if targets.empty:
        raise SystemExit("target file is empty")
    if "stock_strategy" not in targets.columns:
        raise SystemExit("target file missing required stock_strategy column")

    for column in ["sellable_stock_location", "skip_stock_location_update", "allow_stock_location_update"]:
        if column not in targets.columns:
            targets[column] = ""

    stock_strategy = targets["stock_strategy"].fillna("").astype(str).str.strip().str.casefold()
    supplier_synced = stock_strategy.eq("supplier_synced_inventory")
    warehouse_only = stock_strategy.eq("warehouse_only")

    targets.loc[supplier_synced, "sellable_stock_location"] = "Warehouse Stock"
    targets.loc[warehouse_only, "sellable_stock_location"] = ""
    targets.loc[warehouse_only, "skip_stock_location_update"] = "yes"
    targets.loc[warehouse_only, "allow_stock_location_update"] = "no"

    out.parent.mkdir(parents=True, exist_ok=True)
    targets.to_csv(out, index=False)

    print(f"source={source}")
    print(f"out={out}")
    print(f"rows={len(targets)}")
    print("\ncounts_by_stock_strategy:")
    print(targets["stock_strategy"].fillna("").astype(str).str.strip().replace("", "(blank)").value_counts(dropna=False).to_string())
    print("\ncounts_by_sellable_stock_location:")
    print(targets["sellable_stock_location"].fillna("").astype(str).str.strip().replace("", "(blank)").value_counts(dropna=False).to_string())
    protected = targets.loc[warehouse_only]
    protected_bad = protected[
        protected["sellable_stock_location"].astype(str).str.strip().ne("")
        | protected["allow_stock_location_update"].astype(str).str.strip().str.casefold().isin(YES_VALUES)
        | ~protected["skip_stock_location_update"].astype(str).str.strip().str.casefold().isin(YES_VALUES)
    ]
    print(f"\nwarehouse_only_rows={len(protected)}")
    print(f"warehouse_only_protection_violations={len(protected_bad)}")
    if len(protected_bad):
        raise SystemExit("warehouse_only protection validation failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())