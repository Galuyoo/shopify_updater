# core.py
import os, time, json, re
from typing import List, Dict, Optional, Callable, Tuple
from datetime import datetime, timedelta
import pandas as pd
import requests
from .ralawise import get_stock
from .constants import (
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
    days_back: Optional[int] = None,
    since_iso_utc: Optional[str] = None,
) -> List[Dict]:
    """
    Fetch products + variants.

    ✅ Guaranteed coverage behavior:
    - If since_iso_utc is provided -> use product updated_at window (NO early stop)
    - Else if days_back is provided -> use updated_at window (NO early stop)
    - Else (full scan) -> can early-stop using known_variant_ids if provided

    Why updated_at?
    - Adding a variant updates the product, even if product was created months ago.
    """

    # Determine window
    cutoff_filter = ""
    use_window = False

    if since_iso_utc:
        use_window = True
        cutoff_filter = f" updated_at:>'{since_iso_utc}'"
    elif days_back is not None and days_back > 0:
        use_window = True
        cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        cutoff_filter = f" updated_at:>'{cutoff_date}'"

    # Early stop only allowed when NOT using a window (full scan mode)
    use_early_stop = (known_variant_ids is not None) and (not use_window)

    # Build query string
    q = ""
    if product_types:
        product_filter = " OR ".join([f"product_type:'{ptype}'" for ptype in product_types])
        q = f"({product_filter}){cutoff_filter}"
    else:
        q = cutoff_filter.strip()

    query_str = """
    query($after:String, $q:String!) {
      products(first: 100, query: $q, sortKey: UPDATED_AT, reverse: true, after: $after) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            id
            variants(first: 100) {
              edges { node { id sku inventoryItem { id } } }
            }
          }
        }
      }
    }
    """

    all_rows: List[Dict] = []
    after_cursor = None
    page_count = 0

    while True:
        data = gql_with_retry(
            endpoint,
            headers,
            query_str,
            {"after": after_cursor, "q": q},
            progress=progress
        )

        edges = data["data"]["products"]["edges"]
        page_count += 1

        stop_early = False

        for e in edges:
            pid = e["node"]["id"]
            for ve in e["node"]["variants"]["edges"]:
                v = ve["node"]
                inv_item = v.get("inventoryItem")
                if not inv_item or not inv_item.get("id"):
                    continue

                vid = v["id"]

                if use_early_stop and vid in known_variant_ids:
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

# --- state tracking (per-store) ---

def _safe_store_key(store_key: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "_", str(store_key).strip().lower()) or "store"


def _state_dir_for_map(map_csv_path: str) -> str:
    """
    State lives next to the mapping CSV so each store/project is self-contained.
    Example:
      utils/shopify_inventory_map_spoofy.csv
      utils/_state/spoofy_last_run.json
      utils/_state/spoofy_mapping_cursor.json
    """
    base = os.path.dirname(map_csv_path) or "."
    d = os.path.join(base, "_state")
    os.makedirs(d, exist_ok=True)
    return d


def _last_run_state_path(map_csv_path: str, store_key: str) -> str:
    safe = _safe_store_key(store_key)
    return os.path.join(_state_dir_for_map(map_csv_path), f"{safe}_last_run.json")


def load_last_run_utc(map_csv_path: str, store_key: str) -> Optional[str]:
    p = _last_run_state_path(map_csv_path, store_key)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        v = obj.get("last_run_utc")
        return v if isinstance(v, str) and v.strip() else None
    except Exception:
        return None


def save_last_run_utc(map_csv_path: str, store_key: str, iso_utc: Optional[str] = None) -> None:
    if iso_utc is None:
        iso_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    p = _last_run_state_path(map_csv_path, store_key)
    payload = {"last_run_utc": iso_utc}

    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


def _cursor_state_path(map_csv_path: str, store_key: str) -> str:
    safe = _safe_store_key(store_key)
    return os.path.join(_state_dir_for_map(map_csv_path), f"{safe}_mapping_cursor.json")


def load_cursor(map_csv_path: str, store_key: str) -> Optional[str]:
    p = _cursor_state_path(map_csv_path, store_key)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f) or {}
        return obj.get("after_cursor")
    except Exception:
        return None


def save_cursor(map_csv_path: str, store_key: str, after_cursor: Optional[str]) -> None:
    p = _cursor_state_path(map_csv_path, store_key)
    obj = {"after_cursor": after_cursor, "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}

    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


# --------------------------- helper 
def audit_mapping_coverage(
    endpoint: str,
    headers: Dict[str, str],
    map_csv_path: str,
    product_types: Optional[List[str]] = None,
    pages: int = 5,
    after: Optional[str] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """
    Verifies whether the local mapping CSV contains all variant_ids seen in Shopify
    for the scanned pages. Scans WITHOUT days_back cutoff.
    """
    if not os.path.exists(map_csv_path):
        raise FileNotFoundError(f"Mapping CSV not found: {map_csv_path}")

    df = _safe_read_csv(map_csv_path, dtype=str)
    if df.empty or "variant_id" not in df.columns:
        raise RuntimeError("Mapping CSV empty or missing variant_id column.")

    known = set(df["variant_id"].astype(str))
    scanned_variants = 0
    missing_variants = 0

    # Build query (no cutoff)
    if product_types:
        product_filter = " OR ".join([f"product_type:'{ptype}'" for ptype in product_types])
        query_str = f"""
        query($after:String) {{
          products(first: 100, query: "({product_filter}) sort:created_at-desc", after: $after) {{
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
        query_str = """
        query($after:String) {
          products(first: 100, sortKey:CREATED_AT, reverse:true, after: $after) {
            pageInfo { hasNextPage }
            edges {
              cursor
              node {
                id
                variants(first: 100) {
                  edges { node { id sku inventoryItem { id } } }
                }
              }
            }
          }
        }
        """

    cur = after
    for p in range(1, pages + 1):
        data = gql_with_retry(endpoint, headers, query_str, {"after": cur}, progress=progress)
        edges = data["data"]["products"]["edges"]
        if not edges:
            break

        for e in edges:
            for ve in e["node"]["variants"]["edges"]:
                v = ve["node"]
                inv_item = v.get("inventoryItem")
                if not inv_item or not inv_item.get("id"):
                    continue
                vid = str(v["id"])
                scanned_variants += 1
                if vid not in known:
                    missing_variants += 1

        cur = edges[-1]["cursor"]
        if progress:
            progress(f"🔍 Audit page {p}/{pages}: scanned_variants={scanned_variants}, missing={missing_variants}")

        if not data["data"]["products"]["pageInfo"]["hasNextPage"]:
            break

    return {
        "scanned_variants": scanned_variants,
        "missing_variants": missing_variants,
        "mapping_rows": len(df),
    }


def backfill_missing_variants(
    endpoint: str,
    headers: Dict[str, str],
    map_csv_path: str,
    store_key: str,
    product_types: Optional[List[str]],
    pages: int = 3,
    progress: Optional[Callable[[str], None]] = None,
) -> Tuple[int, Optional[str]]:
    """
    Scan N pages WITHOUT days_back and append only missing variant_ids.
    Returns (added_count, next_cursor).
    """
    cols = ["product_id", "variant_id", "sku", "inventory_item_id"]

    existing = _safe_read_csv(map_csv_path, dtype=str)
    known_ids = set(existing["variant_id"].astype(str)) if (not existing.empty and "variant_id" in existing.columns) else set()

    after_cursor = load_cursor(map_csv_path, store_key)
    added_rows = []

    # Query (no cutoff)
    if product_types:
        product_filter = " OR ".join([f"product_type:'{ptype}'" for ptype in product_types])
        q = f"({product_filter})"
    else:
        q = ""

    query_str = """
    query($after:String, $q:String!) {
      products(first: 100, query: $q, sortKey: CREATED_AT, reverse: true, after: $after) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            id
            variants(first: 100) {
              edges { node { id sku inventoryItem { id } } }
            }
          }
        }
      }
    }
    """

    next_cursor = after_cursor
    for p in range(1, pages + 1):
        data = gql_with_retry(endpoint, headers, query_str, {"after": next_cursor, "q": q}, progress=progress)
        edges = data["data"]["products"]["edges"]
        if not edges:
            next_cursor = None
            break

        page_added = 0
        for e in edges:
            pid = e["node"]["id"]
            for ve in e["node"]["variants"]["edges"]:
                v = ve["node"]
                inv_item = v.get("inventoryItem")
                if not inv_item or not inv_item.get("id"):
                    continue

                vid = str(v["id"])
                if vid in known_ids:
                    continue

                known_ids.add(vid)
                added_rows.append({
                    "product_id": pid,
                    "variant_id": vid,
                    "sku": v.get("sku"),
                    "inventory_item_id": inv_item["id"]
                })
                page_added += 1

        next_cursor = edges[-1]["cursor"]

        if progress:
            progress(f"🩹 Backfill page {p}/{pages}: added={page_added}, total_added={len(added_rows)}")

        if not data["data"]["products"]["pageInfo"]["hasNextPage"]:
            next_cursor = None
            break

    # advance cursor even if no adds (so we keep sweeping the catalog)
    save_cursor(map_csv_path, store_key, next_cursor)

    if not added_rows:
        return 0, next_cursor

    new_df = pd.DataFrame(added_rows, columns=cols)
    combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(subset=["variant_id"], keep="last")
    _atomic_write_csv(combined, map_csv_path)

    return len(new_df), next_cursor

def _parse_iso_utc(s: str) -> Optional[datetime]:
    """
    Accepts 'YYYY-MM-DDTHH:MM:SSZ' or ISO strings with/without trailing Z.
    """
    if not isinstance(s, str) or not s.strip():
        return None
    t = s.strip()
    try:
        if t.endswith("Z"):
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
        # fallback for '2026-01-22T12:34:56' (no Z)
        return datetime.fromisoformat(t.replace("Z", ""))
    except Exception:
        return None

def _iso_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def build_unmatched_sku_report(inv_map_df: pd.DataFrame, stock_map: dict) -> pd.DataFrame:
    """
    inv_map_df must contain: sku, product_id, variant_id, inventory_item_id (and optionally lookup_sku)
    stock_map keys must be normalized SKUs (uppercase/stripped).
    Returns rows that cannot be resolved to stock.
    """
    df = inv_map_df.copy()

    # Normalize sku
    df["sku_norm"] = df["sku"].astype(str).str.strip().str.upper()

    # If lookup_sku exists, normalize it too, else use sku_norm
    if "lookup_sku" in df.columns:
        df["lookup_norm"] = df["lookup_sku"].astype(str).str.strip().str.upper()
    else:
        df["lookup_norm"] = df["sku_norm"]

    stock_keys = set(str(k).strip().upper() for k in stock_map.keys())

    # Unmatched = lookup_norm not in stock_keys
    unmatched = df[~df["lookup_norm"].isin(stock_keys)].copy()
    unmatched["reason"] = "SKU not found in stock feed"


    # Keep only useful columns
    keep = [c for c in ["reason", "sku", "sku_norm", "lookup_sku", "lookup_norm", "product_id", "variant_id", "inventory_item_id"] if c in unmatched.columns]

    unmatched = unmatched[keep].drop_duplicates()

    return unmatched

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

def _atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp, path)


def _safe_read_csv(path: str, *, dtype=str, retries: int = 3, sleep_s: float = 0.5) -> pd.DataFrame:
    """
    Robust CSV read for long-running services.
    If file is temporarily locked/half-written, retry a few times.
    """
    last_err = None
    for _ in range(retries):
        try:
            return pd.read_csv(path, dtype=dtype)
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)

    # Last attempt: try reading with python engine (more forgiving)
    try:
        return pd.read_csv(path, dtype=dtype, engine="python", on_bad_lines="skip")
    except Exception:
        raise RuntimeError(f"Failed to read CSV '{path}': {last_err}")
    
def ensure_mapping_local(
    endpoint: str,
    headers: Dict[str, str],
    map_csv_path: str,
    product_types: Optional[List[str]],
    progress: Optional[Callable[[str], None]] = None,
    days_back: Optional[int] = 7,
    store_key: Optional[str] = None,
    full_build: bool = False,
    backfill_pages_per_run: int = 0,
    safety_overlap_days: int = 1,   # ✅ add this
) -> Tuple[int, int]:


    sk = (store_key or "").strip().lower() or "store"
    cols = ["product_id", "variant_id", "sku", "inventory_item_id"]
    os.makedirs(os.path.dirname(map_csv_path) or ".", exist_ok=True)

    # ---------- Full build ----------
    if full_build or (not os.path.exists(map_csv_path)):
        log(f"🆕 Building mapping (full) → {map_csv_path}", progress)

        rows = _fetch_products_variants(
            endpoint, headers,
            product_types=None if sk == "fullyblessed" else product_types,
            progress=progress,
            known_variant_ids=None,
            days_back=None,
            since_iso_utc=None,
        )

        df = pd.DataFrame(rows, columns=cols).drop_duplicates(subset=["variant_id"], keep="last")
        _atomic_write_csv(df, map_csv_path)

        # mark successful
        save_last_run_utc(map_csv_path, sk)

        log(f"✅ Mapping saved with {len(df)} variants.", progress)
        return len(df), len(df)

    # ---------- Incremental ----------
    existing = _safe_read_csv(map_csv_path, dtype=str)
    known_ids = set(existing["variant_id"].astype(str)) if (not existing.empty and "variant_id" in existing.columns) else set()

    log(f"🔎 Checking for new variants (current count: {len(known_ids)})…", progress)

    since_raw = load_last_run_utc(map_csv_path, sk or "store")

    since_iso = None
    if since_raw:
        dt = _parse_iso_utc(since_raw)
        if dt:
            dt2 = dt - timedelta(days=max(int(safety_overlap_days), 0))
            since_iso = _iso_utc(dt2)

    rows = _fetch_products_variants(
        endpoint, headers,
        product_types=None if sk == "fullyblessed" else product_types,
        progress=progress,
        known_variant_ids=None,                 # no early-stop in incremental
        days_back=None if since_iso else days_back,
        since_iso_utc=since_iso,
    )

    added_count = 0

    if rows:
        fetched = pd.DataFrame(rows, columns=cols).drop_duplicates(subset=["variant_id"], keep="last")
        new_df = fetched[~fetched["variant_id"].astype(str).isin(known_ids)].copy()

        if not new_df.empty:
            combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(subset=["variant_id"], keep="last")
            _atomic_write_csv(combined, map_csv_path)
            existing = combined
            added_count += len(new_df)
            log(f"➕ Added {len(new_df)} new variants (window). Total now {len(existing)}.", progress)
        else:
            log("✅ No new variants found (window).", progress)
    else:
        log("✅ No variants fetched in window.", progress)

    # mark successful window refresh
    save_last_run_utc(map_csv_path, sk)

    # ---------- Self-heal gaps (cursor sweep) ----------
    if backfill_pages_per_run and backfill_pages_per_run > 0:
        healed, _ = backfill_missing_variants(
            endpoint=endpoint,
            headers=headers,
            map_csv_path=map_csv_path,
            store_key=sk,
            product_types=None if sk == "fullyblessed" else product_types,
            pages=int(backfill_pages_per_run),
            progress=progress,
        )
        if healed:
            added_count += healed
            existing = _safe_read_csv(map_csv_path, dtype=str)
            log(f"🩹 Backfill healed +{healed} missing variants. Total now {len(existing)}.", progress)

    return (len(existing), added_count)


# ---------------------------   
# Other helpers
# ---------------------------
def load_shared_stock_csv(path: str) -> Dict[str, int]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Stock CSV not found: {path}")

    df = _safe_read_csv(path, dtype=str)

    cols = {c.lower(): c for c in df.columns}
    if "sku" not in cols or "free" not in cols:
        raise ValueError(f"CSV must contain 'SKU' and 'free' columns. Found: {list(df.columns)}")

    sku_col, free_col = cols["sku"], cols["free"]
    tmp = df[[sku_col, free_col]].copy()
    tmp[sku_col] = tmp[sku_col].astype(str).str.strip()
    tmp[free_col] = pd.to_numeric(tmp[free_col], errors="coerce").fillna(0).astype(int)

    # keep last per SKU (same as DF logic)
    return tmp.groupby(sku_col)[free_col].last().to_dict()



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

def run_update(*,
               store: str,
               sku_prefixes: list[str] | None = None,
               product_types: Optional[List[str]] = None,
               location_name: Optional[str] = None,
               batch_size: int = BATCH_SIZE_DEFAULT,
               map_csv: Optional[str] = None,
               stock_csv_path: Optional[str] = None,   # kept for compatibility, but not used by default
               dry_run: bool = True,
               build_map: bool = False,
               stock_csv_df=None,
               store_profiles: Dict[str, Dict] = None,
               progress: Optional[Callable[[str], None]] = None,
               days_back: int = 7,
               force_refresh_google_sheets: bool = False  # ignored in service-only
               ) -> Tuple[pd.DataFrame, Dict]:

    # --- store/profile resolution (service-safe) ---
    if store_profiles is None:
        raise RuntimeError("store_profiles is required")

    store_in = str(store).strip()
    if store_in in store_profiles:
        store_key = store_in
    else:
        lookup = {k.lower(): k for k in store_profiles.keys()}
        if store_in.lower() in lookup:
            store_key = lookup[store_in.lower()]
        else:
            raise KeyError(f"Unknown store '{store}'. Known: {list(store_profiles.keys())}")

    profile = store_profiles[store_key]
    start_ts = time.time()

    shop_url = profile["SHOP_URL"]
    access_token = profile["ACCESS_TOKEN"]

    # ✅ map_csv can be passed in; otherwise default to profile MAP_CSV
    map_csv = map_csv or profile["MAP_CSV"]

    if not sku_prefixes:
        sku_prefixes = profile.get("DEFAULT_SKU_PREFIXES") or None
    product_types = product_types if product_types is not None else profile.get("DEFAULT_PRODUCT_TYPES") or None
    location_name = location_name if location_name is not None else profile.get("LOCATION_NAME")

    graphql_endpoint = f"https://{shop_url}/admin/api/{API_VERSION}/graphql.json"
    headers = build_headers(access_token)

    shop_info = preflight(graphql_endpoint, headers, progress=progress)
    log(f"✅ Connected to {shop_info.get('name')} ({shop_info.get('myshopifyDomain')})", progress)

    # ---------------------------------------------------------
    # ✅ Mapping behavior (service-friendly + safe)
    #
    # - If build_map=True => do FULL build and return early (manual mode)
    # - Else if mapping file missing => AUTO-create starter mapping safely
    # - Else => normal incremental refresh (backfill pages per run)
    # ---------------------------------------------------------

    # Ensure directory exists for mapping file
    try:
        Path(map_csv).parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    map_exists = os.path.exists(map_csv)

    if build_map:
        # Manual full-build mode
        total_after, added = ensure_mapping_local(
            graphql_endpoint, headers, map_csv,
            product_types=product_types,
            progress=progress,
            days_back=days_back,
            store_key=store_key,
            full_build=True
        )
        elapsed = round(time.time() - start_ts, 2)
        return pd.DataFrame(), {
            "message": f"Mapping built/updated. Total variants: {total_after}, added: {added}",
            "updated": 0, "dry": 0, "errors": 0, "translated": 0,
            "skipped": 0,
            "report_name": "", "report_csv_bytes": b"",
            "unmatched_report_name": "", "unmatched_report_csv_bytes": b"",
            "unmatched_count": 0,
            "failed_report_name": "", "failed_report_csv_bytes": b"",
            "failed_count": 0,
            "elapsed_secs": elapsed
        }

    if not map_exists:
        # ✅ Auto-build starter mapping safely when missing (service mode)
        log(f"🆕 Mapping CSV missing for {store_key}. Auto-creating starter mapping: {map_csv}", progress)

        # Keep this conservative: small backfill and no full build.
        total_after, added = ensure_mapping_local(
            graphql_endpoint, headers, map_csv,
            product_types=product_types,
            progress=progress,
            days_back=days_back,
            store_key=store_key,
            full_build=False,
            backfill_pages_per_run=1
        )

        log(f"🗺️ Starter mapping created: +{added} (total {total_after}).", progress)

        # Re-check (defensive)
        if not os.path.exists(map_csv):
            raise RuntimeError(f"Failed to create mapping CSV at {map_csv}")

    # Decide backfill pages dynamically (works for new stores automatically)
    try:
        existing_rows = len(_safe_read_csv(map_csv, dtype=str))
    except Exception:
        existing_rows = 0

    def _auto_backfill_pages(n_rows: int) -> int:
        if n_rows <= 0:
            return 1
        if n_rows < 200_000:
            return 10
        if n_rows < 800_000:
            return 5
        return 2  # huge stores

    backfill_pages = profile.get("BACKFILL_PAGES_PER_RUN")
    if not backfill_pages:
        backfill_pages = _auto_backfill_pages(existing_rows)

    total_after, added = ensure_mapping_local(
        graphql_endpoint, headers, map_csv,
        product_types=product_types,
        progress=progress,
        days_back=days_back,
        store_key=store_key,
        full_build=False,
        backfill_pages_per_run=int(backfill_pages),
    )

    if added:
        log(f"🔁 Mapping refreshed: +{added} new variants (total {total_after}).", progress)

    # --- STOCK: prefer provided DF, otherwise fetch combined stock DF (ralawise etc.) ---
    if stock_csv_df is None:
        from .stock_sources import build_combined_stock_df
        stock_csv_df = build_combined_stock_df(prefer="ralawise", progress=progress)

    if "sku" not in stock_csv_df.columns or "free" not in stock_csv_df.columns:
        raise RuntimeError(f"stock_csv_df missing required columns. Found: {list(stock_csv_df.columns)}")

    tmp = stock_csv_df[["sku", "free"]].copy()
    tmp["sku"] = tmp["sku"].astype(str).str.strip()
    tmp["free"] = pd.to_numeric(tmp["free"], errors="coerce").fillna(0).astype(int)

    dup_count = int(tmp["sku"].duplicated(keep=False).sum())
    if dup_count:
        log(f"⚠️ Stock feed contains duplicate SKUs: {dup_count} rows (keeping last per SKU).", progress)

    stock_map = tmp.groupby("sku")["free"].last().to_dict()
    log(f"📥 Loaded stock rows: {len(stock_map)}", progress)

    inv_map_df = _safe_read_csv(map_csv, dtype=str)
    log(f"🗺️ Mapping rows loaded: {len(inv_map_df)} from {map_csv}", progress)

    if inv_map_df.empty:
        raise RuntimeError("Mapping CSV is empty. Build mapping first.")

    # product type filtering (optional)
    if product_types:
        allowed = get_product_ids_for_types(graphql_endpoint, headers, product_types)
        before = len(inv_map_df)
        inv_map_df = inv_map_df[inv_map_df["product_id"].isin(allowed)].copy()
        log(f"🔎 Product types {product_types} → {len(inv_map_df)}/{before} variants", progress)

    # SKU prefix filtering (optional)
    if sku_prefixes:
        if isinstance(sku_prefixes, str):
            sku_prefixes = [sku_prefixes]
        prefixes = tuple(p.upper() for p in sku_prefixes if p)

        before = len(inv_map_df)
        inv_map_df["__SKU_U"] = inv_map_df["sku"].astype(str).str.upper()
        inv_map_df = inv_map_df[inv_map_df["__SKU_U"].str.startswith(prefixes)].drop(columns="__SKU_U")
        log(f"🔎 SKU prefixes {sku_prefixes} → {len(inv_map_df)}/{before} variants", progress)

    # -----------------------------
    # Stock matching + unmatched SKU report
    # -----------------------------
    before_all = len(inv_map_df)

    # Normalize stock_map keys ONCE (critical)
    stock_map = {str(k).strip().upper(): int(v) for k, v in stock_map.items()}

    def to_lookup_sku(s: str) -> Optional[str]:
        """
        Returns normalized SKU to lookup in stock_map, or None if no match.
        - Normalizes to UPPER/strip
        - Optionally translates BY102-... -> BY102<colour><size>
        """
        s0 = str(s).strip().upper()

        # If store skips translation, only direct match allowed
        if profile.get("SKIP_TRANSLATION", False):
            return s0 if s0 in stock_map else None

        # Direct match first
        if s0 in stock_map:
            return s0

        # Try translation
        t = translate_sku(s0)
        t0 = t.strip().upper() if t else None
        return t0 if t0 and t0 in stock_map else None

    # Compute lookup SKU for each mapping row
    inv_map_df["lookup_sku"] = inv_map_df["sku"].astype(str).apply(to_lookup_sku)

    # Build unmatched report BEFORE filtering
    unmatched_df = build_unmatched_sku_report(inv_map_df, stock_map)

    # translated count: lookup exists AND differs from normalized original
    translated_mask = (
        inv_map_df["lookup_sku"].notna()
        & (inv_map_df["lookup_sku"].astype(str).str.upper()
           != inv_map_df["sku"].astype(str).str.strip().str.upper())
    )
    translated_count = int(translated_mask.sum())

    # Keep only matched rows for updates
    inv_map_df = inv_map_df[inv_map_df["lookup_sku"].notna()].copy()
    log(f"🔗 Stock matches: {len(inv_map_df)}/{before_all} (translated: {translated_count})", progress)

    # --- In-memory unmatched report bytes (service-friendly) ---
    unmatched_report_name = ""
    unmatched_report_csv_bytes = b""
    if not unmatched_df.empty:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        unmatched_report_name = f"unmatched_skus_{str(store_key).lower()}_{ts}.csv"
        unmatched_report_csv_bytes = unmatched_df.to_csv(index=False).encode("utf-8")
        log(f"⚠️ Unmatched SKUs: {len(unmatched_df)} rows.", progress)
    else:
        log("✅ No unmatched SKUs (everything can be matched to stock feed).", progress)

    if inv_map_df.empty:
        elapsed = round(time.time() - start_ts, 2)
        return pd.DataFrame(), {
            "updated": 0, "dry": 0, "errors": 0, "translated": translated_count,
            "skipped": before_all,
            "report_name": "", "report_csv_bytes": b"",
            "unmatched_report_name": unmatched_report_name,
            "unmatched_report_csv_bytes": unmatched_report_csv_bytes,
            "unmatched_count": int(len(unmatched_df)),
            "failed_report_name": "",
            "failed_report_csv_bytes": b"",
            "failed_count": 0,
            "elapsed_secs": elapsed,
            "message": "Nothing to update after filters."
        }

    location_gid = get_location_gid(graphql_endpoint, headers, location_name)
    log(f"📦 Using location: {location_gid}", progress)

    updates = []
    for _, row in inv_map_df.iterrows():
        key = row["lookup_sku"]  # normalized key or translated key
        updates.append({
            "inventoryItemId": row["inventory_item_id"],
            "quantity": int(stock_map[key]),
            "sku": str(row["sku"]),
            "resolved_sku": key,
        })

    total = len(updates)
    log(f"🚚 Updating {total} variants in batches of {batch_size} (dry_run={dry_run})…", progress)

    report_rows = []
    translated_lookup = set(
        inv_map_df[
            inv_map_df["lookup_sku"].astype(str).str.upper()
            != inv_map_df["sku"].astype(str).str.strip().str.upper()
        ]["sku"].tolist()
    )

    processed = 0
    for i in range(0, total, batch_size):
        batch = updates[i:i + batch_size]
        ok, user_errors = set_on_hand_quantities(
            graphql_endpoint, headers, batch, location_gid, dry_run, progress=progress
        )

        if ok:
            for b in batch:
                report_rows.append({
                    "sku": b["sku"],
                    "resolved_sku": b["resolved_sku"],
                    "new_qty": b["quantity"],
                    "status": "dry-run" if dry_run else "updated",
                    "translated": "yes" if b["sku"] in translated_lookup else "no",
                    "error": ""
                })
        else:
            msg = "; ".join([e.get("message", "") for e in (user_errors or [])])
            for b in batch:
                report_rows.append({
                    "sku": b["sku"],
                    "resolved_sku": b["resolved_sku"],
                    "new_qty": b["quantity"],
                    "status": "error",
                    "translated": "yes" if b["sku"] in translated_lookup else "no",
                    "error": msg
                })

        processed += len(batch)
        log(f"   ✓ {processed}/{total}", progress)
        time.sleep(SLEEP_BETWEEN_CALLS)

    report_df = pd.DataFrame(report_rows)

    failed_df = report_df[report_df["status"] == "error"].copy()
    failed_report_name = ""
    failed_report_csv_bytes = b""
    if not failed_df.empty:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        failed_report_name = f"failed_updates_{str(store_key).lower()}_{ts}.csv"
        failed_report_csv_bytes = failed_df.to_csv(index=False).encode("utf-8")

    updated = int((report_df["status"] == "updated").sum())
    dry = int((report_df["status"] == "dry-run").sum())
    errs = int((report_df["status"] == "error").sum())
    elapsed = round(time.time() - start_ts, 2)

    # --- In-memory report (NO local storage) ---
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_name = f"update_report_{str(store_key).lower()}_{ts}.csv"
    report_csv_bytes = report_df.to_csv(index=False).encode("utf-8")

    return report_df, {
        "updated": updated,
        "dry": dry,
        "errors": errs,
        "translated": translated_count,
        "skipped": before_all - len(inv_map_df),
        "report_name": report_name,
        "report_csv_bytes": report_csv_bytes,
        "unmatched_report_name": unmatched_report_name,
        "unmatched_report_csv_bytes": unmatched_report_csv_bytes,
        "unmatched_count": int(len(unmatched_df)),
        "elapsed_secs": elapsed,
        "failed_report_name": failed_report_name,
        "failed_report_csv_bytes": failed_report_csv_bytes,
        "failed_count": int(len(failed_df)),
    }
