import sys
import io
import os
import re
from pathlib import Path
from typing import List, Optional, Dict, Callable
from utils.gsheets_manager import read_sheet_as_dataframe, create_new_sheet_for_store

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_DIR = Path(__file__).resolve().parent
KEYFILE = BASE_DIR / "aqueous-charger-451510-g6-cf5064e00533.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(KEYFILE)

SHEET_SOURCES: Dict[str, List[str]] = {
    "paddy": [
        "1OFD367c_04TfDTaoLpGdy5qeGCOkivMf4xA8r0CPbCc",
        "1Ra0sg9xH8r_nDKLwUANhqb1SCNsepKHKswcnXYhlz4Q",
    ],
    "spoofy": [
        "1TxokufF-Ct9s8P6zjc6quIJrxbmJudYfdJ7R6ij8Z6o",
        "1XiXSIPAORIzK_PT38Kaf3_JySCqvLrPFy4ag1EssGB8",
    ],
}

DEFAULT_SHEET_TITLE: Optional[str] = None
SIZE_WARN_MB = 40
SIZE_ALERT_MB = 50
SIZE_HARD_MB = 60

def _log(msg: str, cb: Optional[Callable[[str], None]] = None):
    print(msg)
    if cb:
        cb(msg)

def save_sheets_to_csvs(force: bool = False, progress: Optional[Callable[[str], None]] = None):
    _log("📥 Exporting ALL Google Sheets to local CSVs...\n", progress)
    for store, sheet_ids in SHEET_SOURCES.items():
        for idx, sid in enumerate(sheet_ids, start=1):
            out_path = BASE_DIR / f"shopify_inventory_map_{store}_{idx}.csv"
            if out_path.exists() and not force:
                _log(f"⏩ Skipping existing: {out_path.name}", progress)
                continue
            try:
                df = read_sheet_as_dataframe(spreadsheet_id=sid, sheet_title=DEFAULT_SHEET_TITLE, credentials_path=str(KEYFILE))
                df.to_csv(out_path, index=False)
                _log(f"✅ Saved: {out_path.name}", progress)
            except Exception as e:
                _log(f"[ERROR] Failed to download {store}_{idx}: {e}", progress)

def check_and_create_new_sheet_if_necessary(progress: Optional[Callable[[str], None]] = None):
    for store in SHEET_SOURCES:
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
            _log(f"[HARD] {latest_file.name} is {size_mb} MB → creating new Google Sheet", progress)
            new_sheet_id = create_new_sheet_for_store(store)
            SHEET_SOURCES[store].append(new_sheet_id)
            _log(f"🆕 Created and added new sheet for {store}: {new_sheet_id}", progress)

def merge_sheets(progress: Optional[Callable[[str], None]] = None):
    import pandas as pd
    _log("🔗 Merging sheets for each store…\n", progress)
    for store, sheet_ids in SHEET_SOURCES.items():
        dfs = []
        for idx in range(1, len(sheet_ids) + 1):
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

def check_latest_split_file_sizes(progress: Optional[Callable[[str], None]] = None):
    _log("\n📦 Checking latest split CSV file sizes:\n", progress)
    csv_files = list(BASE_DIR.glob("shopify_inventory_map_*_*.csv"))
    store_versions = {}

    for f in csv_files:
        match = re.match(r"shopify_inventory_map_(\w+)_([0-9]+)\.csv", f.name)
        if not match:
            continue
        store, version = match.group(1), int(match.group(2))
        store_versions.setdefault(store, []).append((version, f))

    for store, versions in store_versions.items():
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

def run(force_refresh: bool = False, progress: Optional[Callable[[str], None]] = None):
    save_sheets_to_csvs(force=force_refresh, progress=progress)
    check_latest_split_file_sizes(progress)
    check_and_create_new_sheet_if_necessary(progress)
    merge_sheets(progress)
