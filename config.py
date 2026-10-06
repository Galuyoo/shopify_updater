# config.py
# config.py
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
ROOT = Path(__file__).resolve().parent

def load_env() -> None:
    """
    Load .env from project root in a safe way.
    Works even if script is run from tools/ or elsewhere.
    """
    env_path = ROOT / ".env"
    if not env_path.exists():
        return

    try:
        from dotenv import load_dotenv
        load_dotenv(env_path)
    except Exception:
        # If python-dotenv isn't installed, silently do nothing.
        # (But you should have it in requirements.txt)
        return



def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

# Paths
MAP_BASE_DIR = Path(os.getenv("MAP_BASE_DIR", r"C:\warehouse_data\shopify_updater\maps"))
LOG_DIR = Path(os.getenv("LOG_DIR", r"C:\warehouse_data\shopify_updater\logs"))
STORE_PROFILES_JSON = Path(env("STORE_PROFILES_JSON"))

# Runner defaults
SLEEP_HOURS = float(os.getenv("SLEEP_HOURS", "0.1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "250"))
DAYS_BACK = int(os.getenv("DAYS_BACK", "7"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")
