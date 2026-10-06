# uneek.py
from __future__ import annotations

import base64
import json
import os
from typing import Optional, Callable

import pandas as pd
import requests

from config import load_env

UNEEK_URL_DEFAULT = "https://dev-vsp-uks-new-website-nav-odata.azurewebsites.net/stockLevel/all"


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v.strip()


def _build_auth_header() -> str:
    """
    Prefer UNEEK_AUTH_B64 if provided (paste from curl),
    otherwise build from UNEEK_USER/UNEEK_PASS.
    """
    b64 = os.getenv("UNEEK_AUTH_B64")
    if b64 and b64.strip():
        b64 = b64.strip()
        # allow user to paste either raw base64 or "Basic <b64>"
        if b64.lower().startswith("basic "):
            return b64
        return f"Basic {b64}"

    user = _env("UNEEK_USER")
    password = _env("UNEEK_PASS")
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def get_stock(*, progress: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    """
    Fetch Uneek stock and return normalized DF with columns:
      - sku (str)
      - free (int)

    Expected columns in payload:
      ProductCode, LiveStock, ...
    """
    load_env()

    url = os.getenv("UNEEK_URL", UNEEK_URL_DEFAULT).strip()
    auth_header = _build_auth_header()

    headers = {
        "accept": "application/json",
        "authorization": auth_header,
    }

    if progress:
        progress(f"📡 Uneek: fetching stock → {url}")

    r = requests.get(url, headers=headers, timeout=120)

    if r.status_code != 200:
        ct = r.headers.get("Content-Type", "")
        body = (r.text or "")[:300]
        raise RuntimeError(
            f"Uneek request failed: HTTP {r.status_code}\n"
            f"Content-Type: {ct}\n"
            f"Body (first 300 chars): {body}"
        )

    # Handle JSON list or JSON string containing JSON list
    try:
        data = r.json()
    except Exception:
        try:
            data = json.loads(r.text)
        except Exception:
            raise RuntimeError(f"Uneek response is not valid JSON. Body (first 300): {(r.text or '')[:300]}")

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            raise RuntimeError(f"Uneek returned JSON string but could not parse it. Body (first 300): {data[:300]}")

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Uneek response shape: {type(data)}")

    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("Uneek returned empty payload.")

    if "ProductCode" not in df.columns or "LiveStock" not in df.columns:
        raise RuntimeError(f"Uneek payload missing expected columns. Found: {list(df.columns)[:60]}")

    out = df[["ProductCode", "LiveStock"]].copy()
    out.columns = ["sku", "free"]

    out["sku"] = out["sku"].astype(str).str.strip().str.upper()
    out["free"] = pd.to_numeric(out["free"], errors="coerce").fillna(0).astype(int)

    if progress:
        progress(f"✅ Uneek: loaded {len(out)} rows.")

    return out
