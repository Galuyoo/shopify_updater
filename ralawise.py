# ralawise.py
from __future__ import annotations

import os
from pathlib import Path
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from urllib.parse import quote

BASE_URL_DEFAULT = "https://share.ralawise.com/api/rest/v1"
FILE_PATH_DEFAULT = "Customer Data/IN064/Stock/Stock_Update.csv"

# --- auto-load .env from project root ---
def _auto_load_env():
    root = Path(__file__).resolve().parent
    env_path = root / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path)
    except Exception:
        return

_auto_load_env()


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


def get_stock(*, progress=None) -> pd.DataFrame:
    """
    1) GET metadata JSON from /files/<path> using basic auth (API_KEY, API_PASSWORD)
    2) Extract meta["download_uri"]
    3) Download CSV from signed URL
    4) Read pandas, normalize columns
    """
    api_key = _env("RALAWISE_API_KEY")
    api_password = _env("RALAWISE_API_PASSWORD")
    base_url = _env("RALAWISE_BASE_URL", BASE_URL_DEFAULT).rstrip("/")
    file_path = _env("RALAWISE_FILE_PATH", FILE_PATH_DEFAULT).lstrip("/")

    encoded_path = quote(file_path, safe="/")
    url = f"{base_url}/files/{encoded_path}"

    if progress:
        progress(f"📡 Ralawise: fetching metadata → {file_path}")

    r = requests.get(url, auth=(api_key, api_password), timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Ralawise metadata failed {r.status_code}: {r.text[:300]}")

    meta = r.json()
    if "download_uri" not in meta:
        raise RuntimeError(f"Ralawise metadata missing download_uri. Keys: {list(meta.keys())}")

    download_url = meta["download_uri"]

    if progress:
        progress("📥 Ralawise: downloading CSV from signed URL…")

    r2 = requests.get(download_url, timeout=120)
    if r2.status_code != 200:
        raise RuntimeError(f"Ralawise CSV download failed {r2.status_code}: {r2.text[:200]}")

    # OPTIONAL local save
    if os.getenv("RALAWISE_SAVE_LOCAL", "false").lower() in ("1", "true", "yes"):
        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        local_filename = f"Stock_Update_{stamp}.csv"
        with open(local_filename, "wb") as f:
            f.write(r2.content)
        if progress:
            progress(f"💾 Saved stock file as {local_filename}")

    df = pd.read_csv(StringIO(r2.text), dtype=str)
    df.columns = df.columns.str.strip().str.lower()

    if "sku" in df.columns and "free" in df.columns:
        df["sku"] = df["sku"].astype(str).str.strip()
        df["free"] = pd.to_numeric(df["free"], errors="coerce").fillna(0).astype(int)

    if df.empty:
        raise RuntimeError("Ralawise stock CSV is empty.")

    if progress:
        progress(f"✅ Ralawise: loaded {len(df)} rows.")

    return df
