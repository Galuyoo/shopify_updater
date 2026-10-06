# ---------------------------
# State helpers (NO name collisions)
# ---------------------------
def _state_dir_for_map(map_csv_path: str) -> str:
    base = os.path.dirname(map_csv_path) or "."
    d = os.path.join(base, "_state")
    os.makedirs(d, exist_ok=True)
    return d

def _last_run_state_path(map_csv_path: str, store_key: str) -> str:
    safe = re.sub(r"[^a-z0-9_-]+", "_", str(store_key).strip().lower())
    return os.path.join(_state_dir_for_map(map_csv_path), f"{safe}_last_run.json")

def load_last_run_utc(map_csv_path: str, store_key: str) -> Optional[str]:
    p = _last_run_state_path(map_csv_path, store_key)
    if not os.path.exists(p):
        return None
    try:
        obj = json.loads(open(p, "r", encoding="utf-8").read() or "{}")
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
    safe = re.sub(r"[^a-z0-9_-]+", "_", str(store_key).strip().lower())
    return os.path.join(_state_dir_for_map(map_csv_path), f"{safe}_mapping_cursor.json")

def load_cursor(map_csv_path: str, store_key: str) -> Optional[str]:
    p = _cursor_state_path(map_csv_path, store_key)
    if not os.path.exists(p):
        return None
    try:
        obj = json.loads(open(p, "r", encoding="utf-8").read() or "{}")
        return obj.get("after_cursor")
    except Exception:
        return None

def save_cursor(map_csv_path: str, store_key: str, after_cursor: Optional[str]) -> None:
    p = _cursor_state_path(map_csv_path, store_key)
    obj = {"after_cursor": after_cursor, "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(obj, indent=2))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


# ---------------------------
# Backfill self-heal (cursor-based)
# ---------------------------
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


# ---------------------------
# ✅ Full ensure_mapping_local
# ---------------------------
def ensure_mapping_local(
    endpoint: str,
    headers: Dict[str, str],
    map_csv_path: str,
    product_types: Optional[List[str]],
    progress: Optional[Callable[[str], None]] = None,
    days_back: Optional[int] = 7,
    store_key: Optional[str] = None,
    full_build: bool = False,
    backfill_pages_per_run: int = 0,  # <--- set >0 to self-heal gaps over time
) -> Tuple[int, int]:
    """
    Local-only mapping:
      - one CSV per store (map_csv_path)
      - append only NEW variants by variant_id
      - safe for 24/7

    Incremental strategy:
      1) Fetch products updated since last successful run (updated_at window) OR fallback days_back
      2) Append new variants
      3) Optionally backfill N pages without cutoff (cursor-based) to heal gaps

    Returns: (total_variants_after, added_this_call)
    """
    sk = (store_key or "").strip().lower()
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
        save_last_run_utc(map_csv_path, sk or "store")
        log(f"✅ Mapping saved with {len(df)} variants.", progress)
        return len(df), len(df)

    # ---------- Incremental ----------
    existing = _safe_read_csv(map_csv_path, dtype=str)
    if existing.empty or "variant_id" not in existing.columns:
        known_ids = set()
    else:
        known_ids = set(existing["variant_id"].astype(str))

    log(f"🔎 Checking for new variants (current count: {len(known_ids)})…", progress)

    since = load_last_run_utc(map_csv_path, sk or "store")

    rows = _fetch_products_variants(
        endpoint, headers,
        product_types=None if sk == "fullyblessed" else product_types,
        progress=progress,
        known_variant_ids=None,  # no early-stop in incremental
        days_back=None if since else days_back,
        since_iso_utc=since,
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

    # Always record a successful refresh timestamp (even if 0 added),
    # so "since" keeps moving forward.
    save_last_run_utc(map_csv_path, sk or "store")

    # ---------- Self-heal gaps (cursor sweep) ----------
    if backfill_pages_per_run and backfill_pages_per_run > 0:
        healed, _ = backfill_missing_variants(
            endpoint=endpoint,
            headers=headers,
            map_csv_path=map_csv_path,
            store_key=sk or "store",
            product_types=None if sk == "fullyblessed" else product_types,
            pages=int(backfill_pages_per_run),
            progress=progress,
        )
        if healed:
            added_count += healed
            # reload count (since file changed)
            existing = _safe_read_csv(map_csv_path, dtype=str)
            log(f"🩹 Backfill healed +{healed} missing variants. Total now {len(existing)}.", progress)

    return (len(existing), added_count)
