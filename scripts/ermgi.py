import pandas as pd
from utils.sheet_config import SHEET_SOURCES
from utils.gsheets_manager import get_services
from utils.sheet_config import SHEET_SOURCES
from utils.gsheets_manager import get_services, get_credentials

def main():
    creds = get_credentials()
    print("Service account:", getattr(creds, "service_account_email", "unknown"))

    _, sheets = get_services()
    for store, ids in SHEET_SOURCES.items():
        print(f"\nStore: {store}")
        for i, sid in enumerate(ids, start=1):
            try:
                meta = sheets.spreadsheets().get(spreadsheetId=sid).execute()
                titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
                print(f"  v{i}: OK  {sid} | tabs: {titles}")
            except Exception as e:
                print(f"  v{i}: FAIL {sid} | {e}")

if __name__ == "__main__":
    main()