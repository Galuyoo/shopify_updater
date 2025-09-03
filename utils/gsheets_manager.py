import os
import json
import time
from typing import List, Optional
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def get_credentials():
    """
    Load Google service account credentials from Streamlit secrets or local fallback.
    """
    # Try to load from st.secrets if available
    try:
        import streamlit as st
        if "google_service_account" in st.secrets:
            return service_account.Credentials.from_service_account_info(
                dict(st.secrets["google_service_account"]),
                scopes=SCOPES
            )
    except Exception:
        pass  # Fall through to manual loading

    # Try local service account JSON file
    cred_path = "utils/google_service_account.json"
    if os.path.exists(cred_path):
        with open(cred_path, "r") as f:
            sa_info = json.load(f)
        return service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)

    # Try environment variable fallback
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        return service_account.Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
        )

    raise RuntimeError("❌ Google service account credentials not found.")

def get_services():
    """
    Returns authenticated Google Drive and Sheets clients.
    """
    creds = get_credentials()
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return drive, sheets

def get_service_account_email():
    """
    Returns the service account email used for authenticating Google API.
    """
    creds = get_credentials()
    return creds.service_account_email

def create_new_sheet_for_store(store_name: str, parent_folder_id: Optional[str] = None) -> str:
    """
    Creates a new Google Sheet for the given store and returns its spreadsheet ID.
    Uses Drive API instead of Sheets API to avoid the previous 403 on create.
    Then transfers ownership to your Gmail so the file uses your 2TB quota.
    """
    import re
    SHEET_JSON = "utils/sheet_sources.json"

    # --- compute next N from the last sheet title ---
    next_n = 1
    if os.path.exists(SHEET_JSON):
        with open(SHEET_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if store_name in data and data[store_name]:
            last_id = data[store_name][-1]
            try:
                _, sheets = get_services()
                info = sheets.spreadsheets().get(
                    spreadsheetId=last_id, fields="properties.title"
                ).execute()
                m = re.search(rf"{store_name}_(\d+)", info["properties"]["title"])
                if m:
                    next_n = int(m.group(1)) + 1
            except Exception as e:
                print(f"[WARN] Could not read last sheet title: {e}")

    title = f"shopify_inventory_map_{store_name}_{next_n}"

    # --- create via Drive API ---
    creds = get_credentials()
    drive = build("drive", "v3", credentials=creds)
    metadata = {
        "name": title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]

    # Create the file first, get its ID
    file_obj = drive.files().create(
        body=metadata,
        fields="id",
        supportsAllDrives=True
    ).execute()
    sheet_id = file_obj["id"]

    # --- transfer ownership to your Gmail so it uses *your* quota ---
    try:
        drive.permissions().create(
            fileId=sheet_id,
            transferOwnership=True,
            supportsAllDrives=True,
            body={
                "type": "user",
                "role": "owner",
                "emailAddress": "salahchouikh2004@gmail.com",
            },
        ).execute()
    except HttpError as e:
        # If ownership transfer isn’t allowed by your account policy, you’ll see it here.
        # In that case the file stays owned by the service account and may hit quota.
        print(f"[WARN] Ownership transfer failed: {e}")

    print(f"✅ Created new Google Sheet: {title} (ID: {sheet_id})")
    return sheet_id



def get_first_sheet_title(spreadsheet_id: str) -> str:
    """
    Gets the title of the first sheet in a spreadsheet.
    """
    _, sheets = get_services()
    info = sheets.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties.title"
    ).execute()
    return info["sheets"][0]["properties"]["title"]

def read_sheet_as_dataframe(spreadsheet_id: str, sheet_title: Optional[str] = None) -> pd.DataFrame:
    """
    Reads a sheet into a pandas DataFrame.
    """
    _, sheets = get_services()
    if sheet_title is None:
        sheet_title = get_first_sheet_title(spreadsheet_id)
    resp = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_title
    ).execute()
    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    width = len(header)
    norm_rows = [row + [""] * (width - len(row)) for row in rows]
    return pd.DataFrame(norm_rows, columns=header)

def merge_sheets_to_dataframe(
    spreadsheet_ids: List[str],
    sheet_title: Optional[str] = None
) -> pd.DataFrame:
    """
    Merges multiple sheets into a unified DataFrame with aligned columns.
    """
    dfs = []
    union_cols = None
    for sid in spreadsheet_ids:
        df = read_sheet_as_dataframe(sid, sheet_title)
        if df.empty:
            continue
        if union_cols is None:
            union_cols = list(df.columns)
        else:
            for c in df.columns:
                if c not in union_cols:
                    union_cols.append(c)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    dfs = [d.reindex(columns=union_cols) for d in dfs]
    return pd.concat(dfs, ignore_index=True)

def merge_sheets_to_csv(
    spreadsheet_ids: List[str],
    out_path: str,
    sheet_title: Optional[str] = None
) -> str:
    """
    Merges multiple sheets and saves the result to a CSV file.
    """
    df = merge_sheets_to_dataframe(spreadsheet_ids, sheet_title)
    if df.empty:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            pass
        return out_path
    df.to_csv(out_path, index=False)
    return out_path
