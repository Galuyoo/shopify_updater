import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

product_id = sys.argv[1].strip()
parent_sku = sys.argv[2].strip()

client = StoreFeederApiClient.from_env(StoreFeederApiConfig())
detail = client.get_product(product_id).get("response", {})

out_dir = ROOT / "reports" / "product_family_probe"
out_dir.mkdir(parents=True, exist_ok=True)

raw_path = out_dir / f"product_{product_id}_raw.json"
raw_path.write_text(json.dumps(detail, indent=2, ensure_ascii=False), encoding="utf-8")

print("RAW_JSON:")
print(raw_path)

print()
print("TOP_LEVEL_KEYS:")
for key in sorted(detail.keys()):
    print(key)

INTERESTING_KEYS = [
    "id", "productid", "product id", "sku", "name", "parent", "child",
    "variant", "supplier", "supplier sku", "suppliersku",
    "ean", "mpn", "barcode", "stock", "inventory"
]

def is_interesting_key(key):
    k = str(key).casefold()
    return any(token in k for token in INTERESTING_KEYS)

def compact_value(value):
    if value is None:
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    return text[:180]

def walk(obj, path=""):
    if isinstance(obj, dict):
        if any(is_interesting_key(k) for k in obj.keys()):
            row = {}
            for k, v in obj.items():
                if is_interesting_key(k):
                    row[str(k)] = compact_value(v)
            if row:
                print()
                print("PATH:", path or "<root>")
                for k, v in row.items():
                    print(f"{k}: {v}")
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else str(k)
            walk(v, new_path)

    elif isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj[:5]):
            print()
            print(f"LIST_PATH: {path} count={len(obj)}")
            for idx, item in enumerate(obj[:10]):
                useful = {str(k): compact_value(v) for k, v in item.items() if is_interesting_key(k)}
                if useful:
                    print(f"  ITEM {idx}:")
                    for k, v in useful.items():
                        print(f"    {k}: {v}")
        for i, v in enumerate(obj[:30]):
            walk(v, f"{path}[{i}]")

print()
print("USEFUL_FIELDS:")
walk(detail)
