import sys
import os
import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# --- load .env from project root ---
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, ".env"))
except Exception:
    # If python-dotenv isn't installed, env vars must already be set in the shell
    pass

from ralawise import get_stock as get_ralawise_stock
from uneek import get_stock as get_uneek_stock


def normalize_stock_df(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = df.copy()

    if "sku" not in df.columns or "free" not in df.columns:
        raise RuntimeError(
            f"{source} DF must have columns ['sku','free'], found: {list(df.columns)}"
        )

    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["free"] = pd.to_numeric(df["free"], errors="coerce").fillna(0).astype(int)
    df["source"] = source
    return df[["sku", "free", "source"]]


def build_combined_stock_df() -> pd.DataFrame:
    ral = normalize_stock_df(get_ralawise_stock(), "ralawise")
    une = normalize_stock_df(get_uneek_stock(), "uneek")

    combined = pd.concat([ral, une], ignore_index=True)

    # Uneek overrides Ralawise if SKU exists in both
    priority = {"ralawise": 0, "uneek": 1}
    combined["__rank"] = combined["source"].map(priority).fillna(0).astype(int)

    combined = (
        combined.sort_values("__rank")
        .groupby("sku", as_index=False)
        .last()
        .drop(columns="__rank")
    )

    return combined


if __name__ == "__main__":
    df = build_combined_stock_df()
    print(df.head())
    print("rows:", len(df))
    print("cols:", list(df.columns))
