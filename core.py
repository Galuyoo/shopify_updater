import os, time, json, re
from typing import List, Dict, Optional, Callable, Tuple
from datetime import datetime, timedelta

import pandas as pd
import requests

from constants import (
    API_VERSION, BATCH_SIZE_DEFAULT, SLEEP_BETWEEN_CALLS, RETRY_429_MAX,
    STOCK_CSV_PATH, size_map, colour_map
)

# ---------------------------
# Utilities
# ---------------------------

def log(msg: str, cb: Optional[Callable[[str], None]] = None):
    print(msg)
    if cb:
        cb(msg)

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
    Returns (total_variants_after, newly_added_count).
    """
    cols = ["product_id", "variant_id", "sku", "inventory_item_id"]

    folder = os.path.dirname(map_csv_path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    # Full build (ignore days_back)
    if not os.path.exists(map_csv_path):
        log(f"🆕 Mapping not found. Building full mapping → {map_csv_path}", progress)
        rows = _fetch_products_variants(
            endpoint, headers, product_types, progress,
            known_variant_ids=None, days_back=None  # ✅ No date filter
        )
        pd.DataFrame(rows, columns=cols).to_csv(map_csv_path, index=False)
        log(f"✅ Mapping created with {len(rows)} variants.", progress)
        return len(rows), len(rows)

    # Incremental build (use days_back if given)
    existing = pd.read_csv(map_csv_path, dtype=str)
    known_ids = set(existing["variant_id"].astype(str)) if not existing.empty else set()
    log(f"🔎 Checking for new variants (current count: {len(known_ids)})…", progress)

    new_rows = _fetch_products_variants(
        endpoint, headers, product_types, progress,
        known_variant_ids=known_ids, days_back=days_back
    )

    if new_rows:
        pd.DataFrame(new_rows, columns=cols).to_csv(map_csv_path, mode="a", header=False, index=False)
        log(f"➕ Added {len(new_rows)} new variants to mapping.", progress)
    else:
        log("✅ No new variants found.", progress)

    total_after = len(known_ids) + len(new_rows)
    return total_after, len(new_rows)

# ---------------------------
# Other helpers
# ---------------------------

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
               days_back: int = 7) -> Tuple[pd.DataFrame, Dict]:
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
