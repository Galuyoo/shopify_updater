# store_profiles.py (service-only)
import json
from pathlib import Path
from config import STORE_PROFILES_JSON

def _project_root() -> Path:
    return Path(__file__).resolve().parent

def _default_map_csv(store_key: str) -> str:
    # One CSV per store inside the repo: ./utils/shopify_inventory_map_<store>.csv
    return str(_project_root() / "utils" / f"shopify_inventory_map_{store_key}.csv")

def load_store_profiles() -> dict:
    raw = json.loads(STORE_PROFILES_JSON.read_text(encoding="utf-8"))
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
        }

    return out

STORE_PROFILES = load_store_profiles()
