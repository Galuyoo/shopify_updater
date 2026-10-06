# tools/audit_stock_coverage.py
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
from stock_sources import build_combined_stock_df

CSV_PATH = r"C:\Users\salah\Desktop\work\shopify_updater\mightymerge.io__fvxtz5wo.csv"
SKU_COL = "Variant SKU"



def main():
    stock_df = build_combined_stock_df(prefer="ralawise", progress=print)
    stock_set = set(stock_df["sku"].astype(str).str.strip().str.upper())

    df = pd.read_csv(CSV_PATH, dtype=str)
    cols = {c.lower(): c for c in df.columns}

    if SKU_COL.lower() not in cols:
        raise RuntimeError(f"CSV missing '{SKU_COL}' column. Found: {list(df.columns)}")

    sku_col = cols[SKU_COL.lower()]
    skus = df[sku_col].astype(str).str.strip().str.upper()
    skus = skus[skus != ""].dropna()

    total = int(len(skus))
    unique = int(skus.nunique())

    missing = sorted(set(skus.unique()) - stock_set)

    print("\n--- COVERAGE ---")
    print(f"rows: {total}")
    print(f"unique SKUs: {unique}")
    print(f"found: {unique - len(missing)}")
    print(f"missing: {len(missing)}")

    if missing:
        print("\n--- MISSING (first 50) ---")
        for s in missing[:50]:
            print(s)


if __name__ == "__main__":
    main()
