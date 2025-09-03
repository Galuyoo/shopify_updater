import sys
from pathlib import Path

# Ensure we can import from the main app folder
sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils_io import upload_csv_to_gsheet
from utils.gsheets_manager import create_new_sheet_for_store, get_first_sheet_title
from constants import SIZE_HARD_MB
import json
import os
import toml
from pathlib import Path

# Load .streamlit/secrets.toml manually
secrets_path = Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml"
if secrets_path.exists():
    secrets = toml.load(secrets_path)
    # Optionally patch into global scope if needed
    import streamlit as st
    st.secrets = secrets
else:
    raise RuntimeError(f"Could not find secrets.toml at {secrets_path}")

# rest of your logic here...


STORE = "spoofy"
CSV_FILENAME = Path("utils") / f"shopify_inventory_map_{STORE}_2.csv"
SHEET_JSON = Path("utils/sheet_sources.json")

def get_csv_size_mb(path: Path) -> float:
    return round(path.stat().st_size / (1024 * 1024), 2)

def update_json_file(new_id: str, store: str):
    data = {}
    if SHEET_JSON.exists():
        with open(SHEET_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)

    data.setdefault(store, [])
    if new_id not in data[store]:
        data[store].append(new_id)

    with open(SHEET_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Updated {SHEET_JSON} with new ID: {new_id}")

def main():
    csv_path = Path(CSV_FILENAME)
    if not csv_path.exists():
        print(f"[ERROR] CSV not found at: {csv_path.resolve()}")
        return

    size = get_csv_size_mb(csv_path)
    print(f"📦 {CSV_FILENAME} is {size:.2f} MB")

    if size < SIZE_HARD_MB:
        print(f"✅ File is below the hard limit ({SIZE_HARD_MB} MB). No rotation needed.")
        return

    print("🚨 Over size limit! Creating new Google Sheet and uploading…")

    # 1. Create new Google Sheet
    PARENT_FOLDER = "1q78FNNF4FrjTjYaQvPPfZU-0CYlhPMoC"
    sheet_id = create_new_sheet_for_store(STORE, parent_folder_id=PARENT_FOLDER)

    sheet_tab = get_first_sheet_title(sheet_id)

    # 2. Upload local CSV contents
    upload_csv_to_gsheet(sheet_id, sheet_tab, csv_path)
    print(f"✅ Uploaded CSV to new Google Sheet: {sheet_id} (tab: {sheet_tab})")

    # 3. Add to JSON list
    update_json_file(sheet_id, STORE)

if __name__ == "__main__":
    main()
