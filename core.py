import os, time, json, re
from typing import List, Dict, Optional, Callable, Tuple
from datetime import datetime, timedelta
import streamlit as st
from utils.merge_google_sheets_for_stores import run as refresh_csvs
import pandas as pd
import requests
from utils.sheet_config import SHEET_SOURCES
from utils.gsheets_manager import get_services, get_credentials
from utils.merge_google_sheets_for_stores import SIZE_WARN_MB, SIZE_ALERT_MB, SIZE_HARD_MB

from constants import (
    API_VERSION, BATCH_SIZE_DEFAULT, SLEEP_BETWEEN_CALLS, RETRY_429_MAX,
    STOCK_CSV_PATH, size_map, colour_map
)

# ---------------------------
# Utilities
# ---------------------------

def log(msg: str, cb: Optional[Callable[[str], None]] = None):
    if cb:
        try:
            cb(msg)
            return
        except Exception:
            pass
    try:
        print(msg)
    except Exception:
        # last-resort fallback if stdout was messed with
        import sys
        try:
            (sys.__stdout__ or sys.stdout).write(str(msg) + "\n")
        except Exception:
            pass


def _fetch_products_variants(
    endpoint: str,
    headers: Dict[str, str],
    product_types: Optional[List[str]],
    progress: Optional[Callable[[str], None]],
    known_variant_ids: Optional[set] = None,
    days_back: Optional[int] = None
) -> List[Dict]:
    """
    Fetch products + variants, optionally filtered by created_at in the last X days.
    Stops early if known_variant_ids is provided and a known variant is found.
    """
    cutoff_filter = ""
    if days_back is not None and days_back > 0:
        cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        cutoff_filter = f" created_at:>'{cutoff_date}'"

    all_rows: List[Dict] = []
    after_cursor = None
    page_count = 0
    stop_early = False

    if product_types:
        product_filter = " OR ".join([f"product_type:'{ptype}'" for ptype in product_types])
        query_str = f"""
        query($after:String) {{
          products(first: 100, query: "({product_filter}){cutoff_filter} sort:created_at-desc", after: $after) {{
            pageInfo {{ hasNextPage }}
            edges {{
              cursor
              node {{
                id
                variants(first: 100) {{
                  edges {{ node {{ id sku inventoryItem {{ id }} }} }}
                }}
              }}
            }}
          }}
        }}
        """
    else:
        query_str = f"""
        query($after:String) {{
          products(first: 100, query: "{cutoff_filter.strip()}", sortKey:CREATED_AT, reverse:true, after: $after) {{
            pageInfo {{ hasNextPage }}
            edges {{
              cursor
              node {{
                id
                variants(first: 100) {{
                  edges {{ node {{ id sku inventoryItem {{ id }} }} }}
                }}
              }}
            }}
          }}
        }}
        """

    while True:
        data = gql_with_retry(endpoint, headers, query_str, {"after": after_cursor}, progress=progress)
        edges = data["data"]["products"]["edges"]
        page_count += 1

        for e in edges:
            pid = e["node"]["id"]
            for ve in e["node"]["variants"]["edges"]:
                v = ve["node"]
                inv_item = v.get("inventoryItem")
                if not inv_item or not inv_item.get("id"):
                    continue
                vid = v["id"]

                if known_variant_ids and vid in known_variant_ids:
                    stop_early = True
                    if progress:
                        progress(f"🛑 Early stop after {page_count} pages — first known variant {vid} found.")
                    break

                all_rows.append({
                    "product_id": pid,
                    "variant_id": vid,
                    "sku": v["sku"],
                    "inventory_item_id": inv_item["id"]
                })

            if stop_early:
                break

        if stop_early:
            break

        if data["data"]["products"]["pageInfo"]["hasNextPage"]:
            after_cursor = edges[-1]["cursor"]
            time.sleep(0.6)
            if progress:
                progress(f"… fetched {len(all_rows)} variants so far (page {page_count})")
        else:
            break

    return all_rows



def build_headers(access_token: str) -> Dict[str, str]:
    return {"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}


def translate_sku(messy_sku: str) -> Optional[str]:
    if not isinstance(messy_sku, str):
        return None
    m = re.match(r"^(BY102)-([^-]+)-([^-]+)-", messy_sku)
    if not m:
        return None
    base, size_raw, colour_raw = m.groups()
    size = size_map.get(size_raw, "")
    colour = colour_map.get(colour_raw, "")
    if base and size and colour:
        return f"{base}{colour}{size}"
    return None

# --------------------------- Latest Helpers ---------------------------
import glob

def _store_from_map_csv(map_csv_path: str) -> Optional[str]:
    """
    utils/shopify_inventory_map_spoofy.csv -> spoofy
    """
    m = re.search(r"shopify_inventory_map_([a-z0-9_-]+)\.csv$", map_csv_path, re.I)
    return m.group(1) if m else None

def _latest_split_csv_for_store(store: str, base_dir: Optional[str] = None) -> Optional[str]:
    """
    Find utils/shopify_inventory_map_<store>_<N>.csv with the highest N.
    If none exist, return path for _1 (so we can create it).
    """
    base = base_dir or os.path.dirname(os.path.abspath(__file__))
    # Usually your files are under utils/, so anchor there if present
    utils_dir = os.path.join(base, "utils")
    search_dir = utils_dir if os.path.isdir(utils_dir) else base

    pattern = os.path.join(search_dir, f"shopify_inventory_map_{store}_*.csv")
    candidates = []
    for p in glob.glob(pattern):
        m = re.search(rf"shopify_inventory_map_{store}_(\d+)\.csv$", os.path.basename(p), re.I)
        if m:
            candidates.append((int(m.group(1)), p))

    if not candidates:
        # default to _1 in the utils/ folder (or base if utils missing)
        return os.path.join(search_dir, f"shopify_inventory_map_{store}_1.csv")

    candidates.sort(key=lambda t: t[0])
    return candidates[-1][1]

# ===== Rotation + Registry helpers =====
def _sheet_sources_json_path() -> str:
    """
    Locate utils/sheet_config.py then resolve sheet_sources.json next to it.
    This matches your new JSON-based registry.
    """
    import utils.sheet_config as sc
    cfg_dir = os.path.dirname(sc.__file__)
    return os.path.join(cfg_dir, "sheet_sources.json")

def _persist_new_sheet_id(store: str, new_sheet_id: str):
    """
    Append the new sheet ID to sheet_sources.json and update in-memory SHEET_SOURCES.
    """
    json_path = _sheet_sources_json_path()
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}

    data.setdefault(store, [])
    if new_sheet_id not in data[store]:
        data[store].append(new_sheet_id)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    # keep in-memory constant in sync for this process
    if store not in SHEET_SOURCES:
        SHEET_SOURCES[store] = []
    if new_sheet_id not in SHEET_SOURCES[store]:
        SHEET_SOURCES[store].append(new_sheet_id)

def _current_version_for_store(store: str) -> int:
    """
    Version is 1-based. Current/last version = len(SHEET_SOURCES[store]).
    If none exist, treat as version 0 for math (next will be 1).
    """
    return len(SHEET_SOURCES.get(store, []))

def _tab_title(store: str, version: int, with_csv_suffix: bool = True) -> str:
    base = f"shopify_inventory_map_{store}_{version}"
    return f"{base}.csv" if with_csv_suffix else base

def _resolve_or_create_tab_title(sheets, spreadsheet_id: str, desired_base_title: str, columns_count: int = 4) -> str:
    """
    Return matching tab (either 'base' or 'base.csv'). If none, create 'base.csv'.
    """
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = [s['properties']['title'] for s in meta.get('sheets', [])]

    if desired_base_title in titles:
        return desired_base_title

    csv_title = f"{desired_base_title}.csv"
    if csv_title in titles:
        return csv_title

    sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {
            "title": csv_title,
            "gridProperties": {"rowCount": 1000, "columnCount": columns_count}
        }}}]}
    ).execute()
    return csv_title

def _create_new_spreadsheet_with_tab(sheets, store: str, version: int) -> tuple[str, str]:
    """
    Create spreadsheet named 'shopify_inventory_map_<store>_<version>'
    and ensure a tab 'shopify_inventory_map_<store>_<version>.csv' exists.
    Returns (spreadsheet_id, tab_title).
    """
    title = f"shopify_inventory_map_{store}_{version}"
    new_ss = sheets.spreadsheets().create(body={"properties": {"title": title}}).execute()
    ss_id = new_ss.get("spreadsheetId")
    tab_title = _resolve_or_create_tab_title(sheets, ss_id, desired_base_title=title, columns_count=4)
    return ss_id, tab_title

def _rotate_google_targets_if_needed(
    store: str,
    current_split_csv_path: str,
    size_threshold_mb: float = 90.0,
) -> tuple[str, str]:
    """
    Decide which Google Sheet FILE (and tab) to append to.

    If the *local* split CSV size is below threshold:
      → keep appending to the latest registered spreadsheet (vN) and ensure its tab exists.

    If the *local* split CSV size is >= threshold:
      → create a brand-new Google Sheets FILE (vN+1),
         ensure its tab exists, persist its ID to sheet_sources.json,
         and return that new (sheet_id, tab_title) so new rows go there.
    """
    size_mb = _file_size_mb(current_split_csv_path)
    if store not in SHEET_SOURCES or len(SHEET_SOURCES[store]) == 0:
        raise RuntimeError(f"No spreadsheet IDs registered for store '{store}'")

    current_version = _current_version_for_store(store)  # vN
    latest_sheet_id = SHEET_SOURCES[store][-1]
    _, sheets = get_services()

    # Keep writing to current latest file
    if size_mb < size_threshold_mb:
        desired_base_title = _tab_title(store, current_version, with_csv_suffix=False)
        target_tab = _resolve_or_create_tab_title(
            sheets=sheets,
            spreadsheet_id=latest_sheet_id,
            desired_base_title=desired_base_title,
            columns_count=4
        )
        print(f"[rotate] No rotation (size={size_mb:.2f} MB). Using file v{current_version}, tab: {target_tab}")
        return latest_sheet_id, target_tab

    # ROTATE → brand-new FILE
    next_version = current_version + 1
    print(f"[rotate] Threshold hit (size={size_mb:.2f} MB ≥ {size_threshold_mb} MB). Creating new file v{next_version}…")

    new_sheet_id, new_tab_title = _create_new_spreadsheet_with_tab(sheets, store, next_version)
    _persist_new_sheet_id(store, new_sheet_id)
    print(f"[rotate] New spreadsheet created and persisted: v{next_version} id={new_sheet_id}, tab={new_tab_title}")

    # From now on, append to the new file
    return new_sheet_id, new_tab_title


def _append_rows_csv_and_gsheet(
    new_rows: list[dict],
    store: str,
    sheet_ids: dict,
    sheet_json_path: str,
    store_split_prefix: str,
    google_client: gspread.Client,
    progress: Callable[[str], None],
    hard_size_threshold_mb: float,
) -> None:
    """Appends new rows to latest split file and sheet, and creates new one if too big."""
    from utils.utils_io import save_csv_append, get_csv_size_mb, create_new_gsheet_tab

    # Step 1: Determine current latest CSV
    current_index = 1
    while os.path.exists(f"{store_split_prefix}_{current_index}.csv"):
        current_index += 1
    latest_csv = f"{store_split_prefix}_{current_index - 1}.csv"
    latest_size = get_csv_size_mb(latest_csv)
    progress(f"[INFO] Latest CSV for store '{store}' is {latest_csv} ({latest_size:.2f} MB)")

    # Step 2: Check if we need a new file
    if latest_size >= hard_size_threshold_mb:
        progress(f"[HARD] {latest_csv} is {latest_size:.2f} MB → ⛔ Creating a new Google Sheet tab")
        current_index += 1
        new_csv = f"{store_split_prefix}_{current_index}.csv"
        save_csv_append(new_csv, new_rows)
        progress(f"✅ Created new CSV: {new_csv}")

        # Create new sheet tab
        if store in sheet_ids:
            parent_sheet_id = sheet_ids[store]["parent_sheet_id"]
            new_tab_title = os.path.basename(new_csv)
            sheet = google_client.open_by_key(parent_sheet_id)
            sheet.add_worksheet(title=new_tab_title, rows="1000", cols="30")
            sheet_ids[store][f"sheet_{current_index}"] = new_tab_title
            progress(f"✅ Created new tab '{new_tab_title}' in Google Sheet")

            # Save updated JSON map
            with open(sheet_json_path, "w", encoding="utf-8") as f:
                json.dump(sheet_ids, f, indent=2)
            progress("✅ Updated shopify_sheet_ids.json with new sheet tab")

            # Now append rows
            ws = sheet.worksheet(new_tab_title)
            ws.append_rows([list(row.values()) for row in new_rows])
            progress(f"✅ Appended {len(new_rows)} row(s) to new tab '{new_tab_title}'")
        else:
            progress(f"[ERROR] No sheet ID found for store: {store}")
    else:
        # No overflow, just append to existing CSV and sheet
        save_csv_append(latest_csv, new_rows)
        progress(f"✅ Appended {len(new_rows)} row(s) to existing CSV: {latest_csv}")

        # Append to existing Google Sheet tab
        if store in sheet_ids:
            latest_tab = os.path.basename(latest_csv)
            sheet = google_client.open_by_key(sheet_ids[store]["parent_sheet_id"])
            ws = sheet.worksheet(latest_tab)
            ws.append_rows([list(row.values()) for row in new_rows])
            progress(f"✅ Appended {len(new_rows)} row(s) to Google Sheet tab '{latest_tab}'")
        else:
            progress(f"[ERROR] No sheet ID found for store: {store}")


def _file_size_mb(path: str) -> float:
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0
    
def log_file_size_alert(path: str, label: str, progress: Optional[Callable[[str], None]]):
    sz = _file_size_mb(path) #SIZE_WARN_MB, SIZE_ALERT_MB, SIZE_HARD_MB
    if sz >= SIZE_HARD_MB:
        log(f"[HARD] {label} {os.path.basename(path)} = {sz:.2f} MB", progress)
    elif sz >= SIZE_ALERT_MB:
        log(f"[ALERT] {label} {os.path.basename(path)} = {sz:.2f} MB", progress)
    elif sz >= SIZE_WARN_MB:
        log(f"[WARN] {label} {os.path.basename(path)} = {sz:.2f} MB", progress)


# ---- Throttling helpers ----
def _throttle_wait_from_cost(data, default_wait=2.0):
    try:
        cost = data.get("extensions", {}).get("cost", {})
        ts = cost.get("throttleStatus", {})
        avail = float(ts.get("currentlyAvailable", 0))
        restore = float(ts.get("restoreRate", 0))
        if avail <= 0 and restore > 0:
            return max(2.0, 1.5 * (1.0 / restore) * 10)
        return 0.5
    except Exception:
        return default_wait


def gql_with_retry(endpoint: str, headers: Dict[str, str], query: str, variables: Dict = None,
                   max_retries: int = 8, progress: Optional[Callable[[str], None]] = None):
    attempt = 0
    backoff = 1.0
    while True:
        r = requests.post(endpoint, headers=headers, json={"query": query, "variables": variables or {}}, timeout=60)
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"Non-JSON response ({r.status_code}): {r.text[:300]}")
        if "errors" in data:
            throttled = any((e.get("extensions", {}) or {}).get("code") == "THROTTLED" for e in data["errors"])
            if throttled and attempt < max_retries:
                wait = _throttle_wait_from_cost(data, default_wait=backoff)
                if progress:
                    progress(f"⏳ Throttled — waiting {wait:.1f}s before retry {attempt+1}/{max_retries}…")
                time.sleep(wait)
                attempt += 1
                backoff = min(backoff * 2, 10.0)
                continue
            raise RuntimeError(f"GraphQL Errors: {json.dumps(data['errors'], indent=2)}")
        return data


def preflight(endpoint: str, headers: Dict[str, str], progress: Optional[Callable[[str], None]] = None) -> Dict:
    data = gql_with_retry(endpoint, headers, "{ shop { name myshopifyDomain } }", progress=progress)
    shop = data.get("data", {}).get("shop")
    if not shop:
        raise RuntimeError("Preflight failed: no shop object returned")
    return shop


def get_location_gid(endpoint: str, headers: Dict[str, str], location_name: Optional[str]) -> str:
    query = """{ locations(first: 50) { edges { node { id name } } } }"""
    data = gql_with_retry(endpoint, headers, query)
    edges = data["data"]["locations"]["edges"]
    if not edges:
        raise RuntimeError("No locations found on this shop.")
    if location_name:
        for e in edges:
            if e["node"]["name"] == location_name:
                return e["node"]["id"]
        raise RuntimeError(f"Location '{location_name}' not found. Available: {[e['node']['name'] for e in edges]}")
    return edges[0]["node"]["id"]


def get_product_ids_for_types(endpoint: str, headers: Dict[str, str], product_types: List[str]) -> set:
    if not product_types:
        return set()
    found = set()
    query = """
    query($q:String!, $after:String) {
      products(first: 250, query: $q, after: $after) {
        pageInfo { hasNextPage }
        edges { cursor node { id } }
      }
    }"""
    for ptype in product_types:
        after = None
        qstr = f"product_type:'{ptype}'"
        while True:
            data = gql_with_retry(endpoint, headers, query, {"q": qstr, "after": after})
            edges = data["data"]["products"]["edges"]
            for e in edges:
                found.add(e["node"]["id"])
            if data["data"]["products"]["pageInfo"]["hasNextPage"]:
                after = edges[-1]["cursor"]
            else:
                break
    return found


def ensure_mapping(endpoint: str, headers: Dict[str, str], map_csv_path: str,
                   product_types: Optional[List[str]],
                   progress: Optional[Callable[[str], None]] = None,
                   days_back: Optional[int] = None) -> Tuple[int, int]:
    """
    Ensure mapping CSV exists; if missing, build full (no date filter).
    If exists, append only NEW variants (by variant_id) using early stop and optional days_back filter.

    NEW: also append the same new rows to the latest split CSV file for this store:
         utils/shopify_inventory_map_<store>_<N>.csv
         and warn if that file passes 90/95/100 MB.
    """
    cols = ["product_id", "variant_id", "sku", "inventory_item_id"]

    folder = os.path.dirname(map_csv_path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    store = _store_from_map_csv(map_csv_path)
    latest_split_csv = _latest_split_csv_for_store(store) if store else None

    # ---------- Full build ----------
    if not os.path.exists(map_csv_path):
        log(f"🆕 Mapping not found. Building full mapping → {map_csv_path}", progress)
        rows = _fetch_products_variants(
            endpoint, headers, product_types, progress,
            known_variant_ids=None, days_back=None
        )
        pd.DataFrame(rows, columns=cols).to_csv(map_csv_path, index=False)

        if latest_split_csv != map_csv_path:
            csv_path, gsheet_id = get_latest_split_csv_and_gsheet_for_store(store)
            validate_sheet_matches_csv(csv_path= csv_path, sheet_id= gsheet_id)
            log(f"📤 Appending {len(rows)} new rows to Google Sheet ID: {gsheet_id}", progress)

            used_path = _append_rows_csv_and_gsheet(
                rows=rows,  # ✅ FIXED
                columns=cols,
                csv_path=csv_path,
                store=store,
                google_sheet_id=gsheet_id,
                progress=progress,
            )
            log_file_size_alert(used_path, "split file", progress)

        log(f"✅ Mapping created with {len(rows)} variants.", progress)
        return len(rows), len(rows)


    # ---------- Incremental append ----------
    existing = pd.read_csv(map_csv_path, dtype=str)
    known_ids = set(existing["variant_id"].astype(str)) if not existing.empty else set()
    log(f"🔎 Checking for new variants (current count: {len(known_ids)})…", progress)

    new_rows = _fetch_products_variants(
        endpoint, headers, product_types, progress,
        known_variant_ids=known_ids, days_back=days_back
    )

    if new_rows:
        # Append to merged map first
        last_sheet_id = SHEET_SOURCES[store][-1] if store in SHEET_SOURCES else None
        _append_rows_csv_and_gsheet(
            rows=new_rows,
            columns=cols,
            csv_path=latest_split_csv,
            store=store,
            google_sheet_id=last_sheet_id,
            sheet_tab_name=get_sheet_tab_name_from_latest_split_csv(store),
            progress=progress,
        )

        log(f"➕ Added {len(new_rows)} new variants to mapping.", progress)

        # Check if any mismatch with registered sheet/CSV (defensive)
        csv_path, gsheet_id = get_latest_split_csv_and_gsheet_for_store(store)
        validate_sheet_matches_csv(csv_path=csv_path, sheet_id=gsheet_id)
        if csv_path != latest_split_csv or gsheet_id != last_sheet_id:
            _append_rows_csv_and_gsheet(
                rows=new_rows,
                columns=cols,
                csv_path=csv_path,
                store=store,
                google_sheet_id=gsheet_id,
                progress=progress,
            )

        # Always check the latest actual file size (even if append skipped)
        log_file_size_alert(latest_split_csv, "split file", progress)


    else:
        log("✅ No new variants found.", progress)

    total_after = len(known_ids) + len(new_rows)
    return total_after, len(new_rows)


# ---------------------------   
# Other helpers
# ---------------------------

def get_sheet_tab_name_from_latest_split_csv(store: str, base_dir: Optional[str] = None) -> Optional[str]:
    import glob
    base = base_dir or os.path.dirname(os.path.abspath(__file__))
    utils_dir = os.path.join(base, "utils")
    search_dir = utils_dir if os.path.isdir(utils_dir) else base

    pattern = os.path.join(search_dir, f"shopify_inventory_map_{store}_*.csv")
    candidates = []
    for p in glob.glob(pattern):
        m = re.search(rf"shopify_inventory_map_{store}_(\d+)\.csv$", os.path.basename(p), re.I)
        if m:
            candidates.append((int(m.group(1)), p))

    if not candidates:
        raise ValueError(f"No split CSVs found for store '{store}' to infer tab name.")

    candidates.sort(key=lambda t: t[0])
    latest_version = candidates[-1][0]
    return f"shopify_inventory_map_{store}_{latest_version}"


def validate_sheet_matches_csv(csv_path: str, sheet_id: str):
    # Just a sanity check to help during dev
    basename = os.path.basename(csv_path)
    match = re.match(r"shopify_inventory_map_([a-z0-9]+)_(\d+)\.csv", basename)
    if not match:
        return
    store, version = match.group(1), int(match.group(2))
    expected_id = SHEET_SOURCES[store][version - 1]
    if expected_id != sheet_id:
        print(f"[WARN] GSheet mismatch: expected {expected_id}, got {sheet_id}")


def get_latest_split_csv_and_gsheet_for_store(store: str) -> tuple[str, Optional[str]]:
    """
    Returns:
      - path to latest local split CSV for the store
      - Google Sheet ID of last sheet for this store, or None if not found
    """
    latest_csv = _latest_split_csv_for_store(store)
    last_sheet_id = SHEET_SOURCES[store][-1] if store in SHEET_SOURCES else None
    return latest_csv, last_sheet_id

def load_shared_stock_csv(path: str) -> Dict[str, int]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Stock CSV not found: {path}")
    df = pd.read_csv(path, dtype=str)
    cols = {c.lower(): c for c in df.columns}
    if "sku" not in cols or "free" not in cols:
        raise ValueError(f"CSV must contain 'SKU' and 'free' columns. Found: {list(df.columns)}")
    sku_col, free_col = cols["sku"], cols["free"]
    df = df[[sku_col, free_col]].copy()
    df[sku_col] = df[sku_col].astype(str).str.strip()
    df[free_col] = pd.to_numeric(df[free_col], errors="coerce").fillna(0).astype(int)
    return dict(zip(df[sku_col], df[free_col]))


def set_on_hand_quantities(endpoint: str, headers: Dict[str, str], batch_rows,
                           location_gid: str, dry_run: bool,
                           progress: Optional[Callable[[str], None]] = None):
    if dry_run:
        return True, []

    set_quantities = [
        {"inventoryItemId": r["inventoryItemId"], "locationId": location_gid, "quantity": int(r["quantity"])}
        for r in batch_rows
    ]
    mutation = """
    mutation SetOnHand($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        userErrors { field message }
      }
    }"""
    variables = {"input": {"reason": "correction", "setQuantities": set_quantities}}

    backoff = 1.0
    for attempt in range(1, RETRY_429_MAX + 1):
        resp = requests.post(endpoint, headers=headers,
                             json={"query": mutation, "variables": variables}, timeout=60)

        if resp.status_code == 429:
            wait = min(backoff, 10.0)
            if progress:
                progress(f"⏳ HTTP 429 — waiting {wait:.1f}s (attempt {attempt}/{RETRY_429_MAX})…")
            time.sleep(wait)
            backoff = min(backoff * 2, 10.0)
            continue

        try:
            data = resp.json()
        except Exception:
            return False, [{"field": ["network"], "message": f"Non-JSON response {resp.status_code}"}]

        if "errors" in data:
            throttled = any((e.get("extensions", {}) or {}).get("code") == "THROTTLED" for e in data["errors"])
            if throttled and attempt < RETRY_429_MAX:
                wait = _throttle_wait_from_cost(data, default_wait=backoff)
                if progress:
                    progress(f"⏳ GraphQL throttled — waiting {wait:.1f}s (attempt {attempt}/{RETRY_429_MAX})…")
                time.sleep(wait)
                backoff = min(backoff * 2, 10.0)
                continue
            return False, [{"field": ["graphql"], "message": json.dumps(data["errors"])}]

        user_errors = data["data"]["inventorySetOnHandQuantities"].get("userErrors") or []
        return (len(user_errors) == 0), user_errors

    return False, [{"field": ["retry"], "message": "Failed after retries due to throttling."}]


# ---------------------------
# Main workflow
# ---------------------------

def run_update(*, store: str, sku_prefix: Optional[str], product_types: Optional[List[str]],
               location_name: Optional[str], batch_size: int, map_csv: Optional[str],
               stock_csv_path: Optional[str], dry_run: bool, build_map: bool,
               store_profiles: Dict[str, Dict],
               progress: Optional[Callable[[str], None]] = None,
               days_back: int = 7,
               force_refresh_google_sheets: bool = False) -> Tuple[pd.DataFrame, Dict]:

    start_ts = time.time()
    profile = store_profiles[store]
    shop_url = profile["SHOP_URL"]
    access_token = profile["ACCESS_TOKEN"]
    map_csv = map_csv or profile["MAP_CSV"]
    stock_csv_path = stock_csv_path or STOCK_CSV_PATH
    sku_prefix = profile["DEFAULT_SKU_PREFIX"] if sku_prefix is None else sku_prefix
    product_types = product_types if product_types is not None else profile["DEFAULT_PRODUCT_TYPES"]
    location_name = location_name if location_name is not None else profile.get("LOCATION_NAME")

    graphql_endpoint = f"https://{shop_url}/admin/api/{API_VERSION}/graphql.json"
    headers = build_headers(access_token)

    try:
        shop_info = preflight(graphql_endpoint, headers)
        log(f"✅ Connected to {shop_info.get('name')} ({shop_info.get('myshopifyDomain')})", progress)
    except Exception as e:
        msg = str(e)
        if "Invalid API key or access token" in msg or "invalid_token" in msg or "invalid" in msg.lower():
            raise RuntimeError(
                "Shopify rejected the credentials.\n"
                "• Check Admin API token (Develop apps → Your app → Admin API access token)\n"
                "• Ensure scopes include read_products and write_inventory\n"
                "• Confirm SHOP_URL is correct\n"
                "• Remove spaces/newlines from the token"
            )
        raise

    # 🔄 Refresh Google Sheet data (export + merge)
    refresh_csvs(
        force_refresh=force_refresh_google_sheets,
        progress=progress,
        stores=[store]  # 👈 only the current store
    )



    if build_map or not os.path.exists(map_csv):
        total_after, added = ensure_mapping(graphql_endpoint, headers, map_csv, product_types, progress, days_back=days_back)
        if build_map:
            elapsed = round(time.time() - start_ts, 2)
            return pd.DataFrame(), {
                "message": f"Mapping built/updated. Total variants: {total_after}, added: {added}",
                "updated": 0, "dry": 0, "errors": 0, "translated": 0,
                "skipped": 0, "report_filename": "", "elapsed_secs": elapsed
            }

    total_after, added = ensure_mapping(graphql_endpoint, headers, map_csv, product_types, progress, days_back=days_back)
    if added:
        log(f"🔁 Mapping refreshed: +{added} new variants (total {total_after}).", progress)

    stock_map = load_shared_stock_csv(stock_csv_path)
    log(f"📥 Loaded stock rows: {len(stock_map)}", progress)

    inv_map_df = pd.read_csv(map_csv, dtype=str)
    if inv_map_df.empty:
        raise RuntimeError("Mapping CSV is empty. Build mapping first.")

    if product_types:
        allowed = get_product_ids_for_types(graphql_endpoint, headers, product_types)
        before = len(inv_map_df)
        inv_map_df = inv_map_df[inv_map_df["product_id"].isin(allowed)].copy()
        log(f"🔎 Product types {product_types} → {len(inv_map_df)}/{before} variants", progress)

    if sku_prefix:
        before = len(inv_map_df)
        inv_map_df = inv_map_df[inv_map_df["sku"].astype(str).str.startswith(sku_prefix)]
        log(f"🔎 SKU prefix '{sku_prefix}' → {len(inv_map_df)}/{before} variants", progress)

    before = len(inv_map_df)

    def to_lookup_sku(s: str) -> Optional[str]:
        if s in stock_map:
            return s
        t = translate_sku(s)
        return t if t and t in stock_map else None

    inv_map_df["lookup_sku"] = inv_map_df["sku"].astype(str).apply(to_lookup_sku)
    translated_count = int((inv_map_df["lookup_sku"] != inv_map_df["sku"]).sum())
    inv_map_df = inv_map_df[inv_map_df["lookup_sku"].notna()].copy()
    log(f"🔗 Stock matches: {len(inv_map_df)}/{before} (translated: {translated_count})", progress)

    if inv_map_df.empty:
        elapsed = round(time.time() - start_ts, 2)
        return pd.DataFrame(), {
            "updated": 0, "dry": 0, "errors": 0, "translated": translated_count,
            "skipped": before, "report_filename": "", "elapsed_secs": elapsed,
            "message": "Nothing to update after filters."
        }

    location_gid = get_location_gid(graphql_endpoint, headers, location_name)
    log(f"📦 Using location: {location_gid}", progress)

    updates = []
    for _, row in inv_map_df.iterrows():
        key = row["lookup_sku"]
        updates.append({
            "inventoryItemId": row["inventory_item_id"],
            "quantity": int(stock_map[key]),
            "sku": str(row["sku"]),
            "resolved_sku": key,
        })

    total = len(updates)
    log(f"🚚 Updating {total} variants in batches of {batch_size} (dry_run={dry_run})…", progress)

    report_rows = []
    translated_lookup = set(inv_map_df[inv_map_df["lookup_sku"] != inv_map_df["sku"]]["sku"].tolist())

    processed = 0
    for i in range(0, total, batch_size):
        batch = updates[i:i+batch_size]
        ok, user_errors = set_on_hand_quantities(graphql_endpoint, headers, batch, location_gid, dry_run, progress=progress)
        if ok:
            for b in batch:
                report_rows.append({
                    "sku": b["sku"], "resolved_sku": b["resolved_sku"], "new_qty": b["quantity"],
                    "status": "dry-run" if dry_run else "updated",
                    "translated": "yes" if b["sku"] in translated_lookup else "no",
                    "error": ""
                })
        else:
            msg = "; ".join([e.get("message", "") for e in (user_errors or [])])
            for b in batch:
                report_rows.append({
                    "sku": b["sku"], "resolved_sku": b["resolved_sku"], "new_qty": b["quantity"],
                    "status": "error",
                    "translated": "yes" if b["sku"] in translated_lookup else "no",
                    "error": msg
                })
        processed += len(batch)
        log(f"   ✓ {processed}/{total}", progress)
        time.sleep(SLEEP_BETWEEN_CALLS)

    report_df = pd.DataFrame(report_rows)
    updated = int((report_df["status"] == "updated").sum())
    dry = int((report_df["status"] == "dry-run").sum())
    errs = int((report_df["status"] == "error").sum())
    elapsed = round(time.time() - start_ts, 2)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"update_report_{store}_{ts}.csv"
    report_df.to_csv(report_filename, index=False)

    return report_df, {
        "updated": updated,
        "dry": dry,
        "errors": errs,
        "translated": translated_count,
        "skipped": before - len(inv_map_df),
        "report_filename": report_filename,
        "elapsed_secs": elapsed,
    }
