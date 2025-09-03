# ---- Size check helpers: latest = highest numeric suffix only ----
import re
from pathlib import Path
from merge_google_sheets_for_stores import SIZE_WARN_MB, SIZE_ALERT_MB, SIZE_HARD_MB

# Only match suffixed parts: shopify_inventory_map_<store>_<N>.csv
PART_RE = re.compile(r"^shopify_inventory_map_(?P<store>[a-z0-9_-]+)_(?P<num>\d+)\.csv$", re.IGNORECASE)

WARN_MB = SIZE_WARN_MB
ALERT_MB = SIZE_ALERT_MB
HARD_MB = SIZE_HARD_MB

def file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)

def find_parts_for_store(base_dir: Path, store: str) -> list[Path]:
    parts = []
    for p in base_dir.iterdir():
        if not p.is_file() or p.suffix.lower() != ".csv":
            continue
        m = PART_RE.match(p.name)
        if not m:
            continue
        if m.group("store").lower() == store.lower():
            parts.append(p)
    return parts

def pick_latest_part(parts: list[Path]) -> Path | None:
    """Return the part with the highest numeric suffix, or None if none."""
    if not parts:
        return None
    def keyfn(p: Path):
        m = PART_RE.match(p.name)
        n = int(m.group("num")) if m else -1
        return (n, p.stat().st_mtime)
    return sorted(parts, key=keyfn)[-1]

def check_latest_sizes(stores: list[str], base_dir: Path | None = None) -> None:
    """For each store, consider only suffixed parts (_N). Ignore base combined file."""
    base_dir = base_dir or Path(__file__).resolve().parent

    for store in stores:
        print(f"\nStore: {store}")
        parts = find_parts_for_store(base_dir, store)

        # Also list the base combined (optional info)
        base_combined = base_dir / f"shopify_inventory_map_{store}.csv"
        if base_combined.exists():
            print(f"  (info) found combined file: {base_combined.name}  {file_size_mb(base_combined):.2f} MB")

        if not parts:
            print("  No suffixed parts found (shopify_inventory_map_<store>_<N>.csv). Skipping latest check.")
            continue

        # List all parts with sizes
        parts_sorted = sorted(parts, key=lambda p: int(PART_RE.match(p.name).group('num')))
        for p in parts_sorted:
            num = int(PART_RE.match(p.name).group("num"))
            print(f"  - {p.name:40s}  {file_size_mb(p):8.2f} MB  (part {num})")

        latest = pick_latest_part(parts)
        latest_size = file_size_mb(latest)
        msg = f"Latest part for {store}: {latest.name} -> {latest_size:.2f} MB."

        if latest_size >= HARD_MB:
            print(f"[HARD] {msg} Reached/exceeded 100 MB.")
        elif latest_size >= ALERT_MB:
            print(f"[ALERT] {msg} Over 95 MB. Plan rotation now.")
        elif latest_size >= WARN_MB:
            print(f"[WARN] {msg} Over 90 MB. Approaching limit.")
        else:
            print(f"[OK] {msg}")
            
if __name__ == "__main__":
    # existing run() call that merges
    # run()

    # Then check sizes for the stores you care about (paddy & spoofy here)
    check_latest_sizes(["paddy", "spoofy"])
