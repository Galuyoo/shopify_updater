# store_profiles.py (service-only)
import json
from pathlib import Path
from config import STORE_PROFILES_JSON

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent  # C:\shopify_updater

def _resolve_path(p: Path) -> Path:
    """
    If env provides a relative path (like 'store_profiles.json'),
    resolve it relative to the app folder so it works from any CWD.
    """
    return p if p.is_absolute() else (APP_DIR / p)

def _default_map_csv(store_key: str) -> str:
    # One CSV per store in: C:\shopify_updater\data\maps\
    return str(PROJECT_ROOT / "data" / "maps" / f"shopify_inventory_map_{store_key}.csv")

def load_store_profiles() -> dict:
    profiles_path = _resolve_path(STORE_PROFILES_JSON)

    if not profiles_path.exists():
        raise FileNotFoundError(
            f"store_profiles.json not found.\n"
            f"Resolved path: {profiles_path}\n"
            f"Tip: Either move the file there, or set STORE_PROFILES_JSON to an absolute path in .env."
        )

    raw = json.loads(profiles_path.read_text(encoding="utf-8"))
    out: dict = {}

    for store_name, cfg in raw.items():
        store_key = str(store_name).strip().lower()

        out[store_key] = {
            "SHOP_URL": cfg["SHOP_URL"],
            "ACCESS_TOKEN": cfg["ACCESS_TOKEN"],
            "MAP_CSV": cfg.get("MAP_CSV") or _default_map_csv(store_key),
            "DEFAULT_SKU_PREFIXES": cfg.get("DEFAULT_SKU_PREFIXES") or [],
            "DEFAULT_PRODUCT_TYPES": cfg.get("DEFAULT_PRODUCT_TYPES") or [],
            "LOCATION_NAME": cfg.get("LOCATION_NAME"),
            "SKIP_TRANSLATION": bool(cfg.get("SKIP_TRANSLATION", False)),
            # Optional per-store override
            "BACKFILL_PAGES_PER_RUN": cfg.get("BACKFILL_PAGES_PER_RUN"),
        }

    return out

STORE_PROFILES = load_store_profiles()
