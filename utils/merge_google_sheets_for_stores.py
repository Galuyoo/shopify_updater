import sys
import io
import os
import re
import json
from pathlib import Path
from typing import List, Optional, Dict, Callable
import pandas as pd
from utils.gsheets_manager import read_sheet_as_dataframe
from utils.utils_io import upload_csv_to_gsheet

import streamlit as st
from google.oauth2 import service_account

service_account_info = st.secrets["google_service_account"]
credentials = service_account.Credentials.from_service_account_info(dict(service_account_info))

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent
SHEET_ID_JSON_PATH = BASE_DIR / "utils" / "sheet_ids.json"
DEFAULT_SHEET_TITLE: Optional[str] = None

# Thresholds
SIZE_WARN_MB = 40
SIZE_ALERT_MB = 50
SIZE_HARD_MB = 60

# Dynamic mapping from sheet_ids.json
def load_sheet_sources() -> Dict[str, List[str]]:
    try:
        with open(SHEET_ID_JSON_PATH) as f:
            return json.load(f)
    except Exception:
        return {}

def save_sheets_to_csvs(force: bool = False, progress: Optional[Callable[[str], None]] = None, stores: Optional[list[str]] = None):
    _log("📥 Exporting Google Sheets to local CSVs...\n", progress)
    sources = load_sheet_sources()
    target_stores = stores or sources.keys()

    for store in target_stores:
        ids = sources.get(store, [])
        for idx, sid in enumerate(ids, start=1):
            out_path = BASE_DIR / f"shopify_inventory_map_{store}_{idx}.csv"
            if out_path.exists() and not force:
                _log(f"⏩ Skipping existing: {out_path.name}", progress)
                continue
            try:
                df = read_sheet_as_dataframe(spreadsheet_id=sid, sheet_title=DEFAULT_SHEET_TITLE)
                df.to_csv(out_path, index=False)
                _log(f"✅ Saved: {out_path.name}", progress)
            except Exception as e:
                _log(f"[ERROR] Failed to download {store}_{idx}: {e}", progress)

def check_and_create_new_sheet_if_necessary(progress: Optional[Callable[[str], None]] = None, stores: Optional[list[str]] = None):
    sources = load_sheet_sources()
    target_stores = stores or sources.keys()

    for store in target_stores:
        pattern = BASE_DIR.glob(f"shopify_inventory_map_{store}_*.csv")
        files = []
        for f in pattern:
            m = re.match(rf"shopify_inventory_map_{store}_(\d+)\.csv", f.name)
            if m:
                files.append((int(m.group(1)), f))

        if not files:
            continue

        files.sort()
        latest_version, latest_file = files[-1]
        size_mb = round(latest_file.stat().st_size / (1024 * 1024), 2)

        if size_mb >= SIZE_HARD_MB:
            _log(f"[HARD] {latest_file.name} is {size_mb} MB → ⛔⛔ Creating new Google Sheet… ⛔⛔", progress)

            try:
                new_sheet_id = upload_csv_to_gsheet(str(latest_file), store)
                _log(f"✅ Created new Google Sheet and uploaded → ID: {new_sheet_id}", progress)
            except Exception as e:
                _log(f"[ERROR] Failed to create/upload new Google Sheet: {e}", progress)

def check_latest_split_file_sizes(progress: Optional[Callable[[str], None]] = None, stores: Optional[list[str]] = None):
    _log("\n📦 Checking latest split CSV file sizes:\n", progress)
    sources = load_sheet_sources()
    target_stores = stores or sources.keys()

    for store in target_stores:
        csv_files = list(BASE_DIR.glob(f"shopify_inventory_map_{store}_*.csv"))
        versions = []
        for f in csv_files:
            m = re.match(rf"shopify_inventory_map_{store}_(\d+)\.csv", f.name)
            if m:
                versions.append((int(m.group(1)), f))

        if not versions:
            continue

        versions.sort()
        _, latest_file = versions[-1]
        size_mb = round(latest_file.stat().st_size / (1024 * 1024), 2)

        if size_mb >= SIZE_HARD_MB:
            level = "[HARD]"
        elif size_mb >= SIZE_ALERT_MB:
            level = "[ALERT]"
        elif size_mb >= SIZE_WARN_MB:
            level = "[WARN]"
        else:
            level = "[OK]"

        _log(f"{level:<8} Store: {store:<10} → {latest_file.name:<40} {size_mb:.2f} MB [LATEST]", progress)

def merge_sheets(progress: Optional[Callable[[str], None]] = None, stores: Optional[list[str]] = None):
    _log("🔗 Merging sheets...\n", progress)
    sources = load_sheet_sources()
    target_stores = stores or sources.keys()

    for store in target_stores:
        dfs = []
        for idx in range(1, 100):  # up to 100 parts
            path = BASE_DIR / f"shopify_inventory_map_{store}_{idx}.csv"
            if path.exists():
                try:
                    dfs.append(pd.read_csv(path, dtype=str))
                except Exception as e:
                    _log(f"[ERROR] Failed to load {path.name}: {e}", progress)
        if dfs:
            out_csv = BASE_DIR / f"shopify_inventory_map_{store}.csv"
            pd.concat(dfs, ignore_index=True).to_csv(out_csv, index=False)
            _log(f"✅ Merged CSV saved → {out_csv.name}", progress)

def run(force_refresh: bool = False, progress=None, stores: Optional[list[str]] = None):
    if stores:
        _log(f"⚡ Running only for stores: {stores}", progress)
    else:
        _log("⚡ Running for all stores", progress)

    target_stores = stores or list(load_sheet_sources().keys())
    save_sheets_to_csvs(force=force_refresh, progress=progress, stores=target_stores)
    check_latest_split_file_sizes(progress, stores=target_stores)
    check_and_create_new_sheet_if_necessary(progress, stores=target_stores)
    merge_sheets(progress, stores=target_stores)

def _log(msg: str, cb: Optional[Callable[[str], None]] = None):
    if cb:
        cb(msg)
    else:
        try:
            print(msg)
        except Exception:
            pass
