import os
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRETS_TOML = ROOT / "secrets.toml"
OUT_JSON = ROOT / "store_profiles.json"

def _load_env_file():
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except Exception:
        pass

def main():
    _load_env_file()

    # If secrets.toml exists, use it. Otherwise fallback to env.
    if SECRETS_TOML.exists():
        import tomllib
        data = tomllib.loads(SECRETS_TOML.read_text(encoding="utf-8"))
        stores = data.get("stores") or data  # supports either shape
    else:
        # Minimal fallback: expects STORE_PROFILES_JSON in .env (optional)
        raw = os.getenv("STORE_PROFILES_JSON", "").strip()
        if not raw:
            raise FileNotFoundError(
                f"Missing {SECRETS_TOML} and no STORE_PROFILES_JSON in .env.\n"
                "Fix by either creating secrets.toml or adding STORE_PROFILES_JSON."
            )
        stores = json.loads(raw)

    # Write store_profiles.json
    OUT_JSON.write_text(json.dumps(stores, indent=2), encoding="utf-8")
    print(f"✅ Wrote: {OUT_JSON}")

if __name__ == "__main__":
    main()
