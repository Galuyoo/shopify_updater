import os
import csv
import json
import pandas as pd
from pathlib import Path  # ✅ ADD THIS LINE
from typing import Iterable, Dict, List, Optional, Union
from utils.gsheets_manager import get_services, create_new_sheet_for_store


RowLike = Union[Dict[str, object], List[object]]

SHEET_ID_JSON_PATH = "utils/sheet_ids.json"

def ensure_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def get_csv_size_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0

def _normalize_rows(
    rows: Iterable[RowLike],
    columns: Optional[List[str]] = None
) -> List[List[object]]:
    rows = list(rows)
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, dict):
        if columns is None:
            columns = list(first.keys())
        matrix = [[r.get(col, "") for col in columns] for r in rows]  # type: ignore[arg-type]
        return matrix
    else:
        return [list(r) for r in rows]

def save_csv_append(
    path: str,
    rows: Iterable[RowLike],
    columns: Optional[List[str]] = None,
    newline: str = ""
) -> None:
    ensure_dir(path)
    rows = list(rows)
    if not rows:
        return

    file_exists = os.path.exists(path)
    infer_columns: Optional[List[str]] = None
    if isinstance(rows[0], dict):
        infer_columns = columns or list(rows[0].keys())

    matrix = _normalize_rows(rows, columns or infer_columns)

    with open(path, "a", encoding="utf-8", newline=newline) as f:
        writer = csv.writer(f)
        if not file_exists and infer_columns:
            writer.writerow(infer_columns)
        writer.writerows(matrix)

def upload_csv_to_gsheet(csv_path: str, store_name: str) -> str:
    """
    Create a new Google Sheet, upload CSV content into it, and update utils/sheet_ids.json.
    Returns the new Google Sheet ID.
    """
    drive, sheets = get_services()

    # Load the CSV as DataFrame
    df = pd.read_csv(csv_path)

    # Determine new sheet index
    try:
        with open(SHEET_ID_JSON_PATH, "r") as f:
            sheet_map = json.load(f)
    except FileNotFoundError:
        sheet_map = {}

    existing_ids = sheet_map.get(store_name, [])
    new_index = len(existing_ids) + 1

    # Create new sheet file
    title = f"shopify_inventory_map_{store_name}_{new_index}"
    body = {
        "properties": {"title": title}
    }
    new_sheet = sheets.spreadsheets().create(body=body).execute()
    new_sheet_id = new_sheet["spreadsheetId"]

    # Upload the data
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    sheets.spreadsheets().values().update(
        spreadsheetId=new_sheet_id,
        range="Sheet1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    # Append ID to JSON
    sheet_map.setdefault(store_name, []).append(new_sheet_id)
    with open(SHEET_ID_JSON_PATH, "w") as f:
        json.dump(sheet_map, f, indent=2)

    return new_sheet_id


def upload_csv_to_gsheet(
    spreadsheet_id: str,
    sheet_title: str,
    csv_path: Union[str, Path],
):
    """
    Uploads a local CSV file to a given Google Sheet tab.
    Assumes the tab already exists.
    """
    if get_services is None:
        raise RuntimeError("Google Sheets service not available. Ensure utils.gsheets_manager.get_services exists.")
    
    _, sheets = get_services()

    # Read the CSV file content
    with open(csv_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    if not lines:
        print(f"[WARN] Empty CSV: {csv_path}")
        return

    rows = [line.split(",") for line in lines]

    # Upload using batchUpdate
    body = {
        "values": rows
    }
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_title,
        valueInputOption="RAW",
        body=body
    ).execute()
