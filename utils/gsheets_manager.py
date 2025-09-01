import os
from typing import List, Optional, Tuple
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]



def create_new_sheet_for_store(store_name: str) -> str:
    creds = service_account.Credentials.from_service_account_file(
        "utils/aqueous-charger-451510-g6-cf5064e00533.json",
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("sheets", "v4", credentials=creds)
    body = {
        "properties": {
            "title": f"shopify_inventory_map_{store_name}_{int(time.time())}"
        }
    }
    sheet = service.spreadsheets().create(body=body).execute()
    return sheet["spreadsheetId"]

def _get_credentials(credentials_path: Optional[str] = None):
    cred_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path or not os.path.exists(cred_path):
        raise RuntimeError("Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or pass credentials_path.")
    return service_account.Credentials.from_service_account_file(cred_path, scopes=SCOPES)

def get_services(credentials_path: Optional[str] = None):
    creds = _get_credentials(credentials_path)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return drive, sheets

def get_first_sheet_title(spreadsheet_id: str, credentials_path: Optional[str] = None) -> str:
    _, sheets = get_services(credentials_path)
    info = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title").execute()
    return info["sheets"][0]["properties"]["title"]

def read_sheet_as_dataframe(spreadsheet_id: str, sheet_title: Optional[str] = None, credentials_path: Optional[str] = None) -> pd.DataFrame:
    _, sheets = get_services(credentials_path)
    if sheet_title is None:
        sheet_title = get_first_sheet_title(spreadsheet_id, credentials_path)
    rng = f"{sheet_title}"
    resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    width = len(header)
    norm_rows = [row + [""] * (width - len(row)) for row in rows]
    return pd.DataFrame(norm_rows, columns=header)

def merge_sheets_to_dataframe(spreadsheet_ids: List[str], sheet_title: Optional[str] = None, credentials_path: Optional[str] = None) -> pd.DataFrame:
    dfs = []
    union_cols = None
    for sid in spreadsheet_ids:
        df = read_sheet_as_dataframe(sid, sheet_title=sheet_title, credentials_path=credentials_path)
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

def merge_sheets_to_csv(spreadsheet_ids: List[str], out_path: str, sheet_title: Optional[str] = None, credentials_path: Optional[str] = None) -> str:
    df = merge_sheets_to_dataframe(spreadsheet_ids, sheet_title=sheet_title, credentials_path=credentials_path)
    if df.empty:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            pass
        return out_path
    df.to_csv(out_path, index=False)
    return out_path
