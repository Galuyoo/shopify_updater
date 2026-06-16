import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load .env
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

client = StoreFeederApiClient.from_env(StoreFeederApiConfig())

products = {
    "AQ011_child_failed": "14156295",
    "known_good_template": "14172175",
}

out_dir = ROOT / "reports" / "product_detail_compare"
out_dir.mkdir(parents=True, exist_ok=True)

def small(obj):
    return json.dumps(obj, indent=2, ensure_ascii=False)[:3000]

for label, product_id in products.items():
    detail = client.get_product(product_id).get("response", {})

    raw_path = out_dir / f"{label}_{product_id}.json"
    raw_path.write_text(json.dumps(detail, indent=2, ensure_ascii=False), encoding="utf-8")

    print()
    print("=" * 100)
    print(label, product_id)
    print("raw:", raw_path)
    print("ProductID:", detail.get("ProductID"))
    print("SKU:", detail.get("SKU"))
    print("ProductType:", detail.get("ProductType"))
    print("Name:", detail.get("Name"))

    print()
    print("SUPPLIERS:")
    print(small(detail.get("Suppliers", [])))

    print()
    print("WAREHOUSE INFORMATION:")
    wh = detail.get("WarehouseInformation", {})
    print("Warehouse:")
    print(small(wh.get("Warehouse", {})))
    print("StockLocations:")
    print(small(wh.get("StockLocations", [])))

    print()
    print("INVENTORY:")
    print(small(detail.get("InventoryInformation", {})))
