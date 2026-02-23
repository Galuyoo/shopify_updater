# stock_sources.py
from __future__ import annotations
import pandas as pd

from ralawise import get_stock as get_ralawise_stock
from uneek import get_stock as get_uneek_stock


def _normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    if "sku" not in df.columns or "free" not in df.columns:
        raise RuntimeError(f"{source} df missing sku/free. cols={list(df.columns)}")

    df["sku"] = df["sku"].astype(str).str.strip().str.upper()
    df["free"] = pd.to_numeric(df["free"], errors="coerce").fillna(0).astype(int)
    df["source"] = source

    # keep only what we need
    return df[["sku", "free", "source"]]


def build_combined_stock_df(*, prefer: str = "ralawise", progress=None) -> pd.DataFrame:
    """
    Returns a combined DF with columns: sku, free, source
    prefer: "ralawise" or "uneek" (which source wins on duplicate SKU)
    """
    ral = _normalize(get_ralawise_stock(progress=progress), "ralawise")
    une = _normalize(get_uneek_stock(progress=progress), "uneek")

    combined = pd.concat([ral, une], ignore_index=True)

    # Resolve duplicates: keep preferred source last (so it wins via keep="last")
    prefer = (prefer or "ralawise").strip().lower()
    if prefer == "uneek":
        combined = pd.concat([ral, une], ignore_index=True)  # uneek last
    else:
        combined = pd.concat([une, ral], ignore_index=True)  # ralawise last

    combined = combined.drop_duplicates(subset=["sku"], keep="last").reset_index(drop=True)

    if progress:
        progress(f"✅ Combined stock: {len(combined)} unique SKUs (prefer={prefer})")
        dup = int(pd.concat([ral, une])["sku"].duplicated(keep=False).sum())
        progress(f"ℹ️ Overlapping SKUs between sources (rows): {dup}")

    return combined


def build_stock_map(*, prefer: str = "ralawise", progress=None) -> dict[str, int]:
    df = build_combined_stock_df(prefer=prefer, progress=progress)
    return dict(zip(df["sku"].tolist(), df["free"].tolist()))
