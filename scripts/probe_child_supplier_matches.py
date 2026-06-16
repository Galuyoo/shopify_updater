import csv
import json
from pathlib import Path

ROOT = Path(".")
RAW = ROOT / "reports/product_family_probe/product_14156294_raw.json"

SUPPLIERS = [
    ("Ralawise", ROOT / "data/RALAWISE_stock_lvl.csv", "SKU"),
    ("Uneek", ROOT / "data/Uneek_stock_levels.csv", "sku"),
]

detail = json.loads(RAW.read_text(encoding="utf-8"))

children = []
for child in detail.get("Children", []):
    product = child.get("Product") or {}
    product_id = str(product.get("ProductID") or "").strip()
    sku = str(product.get("SKU") or "").strip()
    attrs = child.get("VariantAttributes") or []
    color = ""
    size = ""
    for attr in attrs:
        name = str(attr.get("Name") or "").strip().casefold()
        value = str(attr.get("Value") or "").strip()
        if name == "color":
            color = value
        elif name == "size":
            size = value
    if product_id and sku:
        children.append({
            "ProductID": product_id,
            "SKU": sku,
            "Color": color,
            "Size": size,
        })

supplier_indexes = {}
for supplier, path, sku_col in SUPPLIERS:
    index = {}
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = str(row.get(sku_col, "")).strip()
                if key:
                    index.setdefault(key, []).append(row)
    supplier_indexes[supplier] = index

matches = []
missing = []
multi = []

for child in children:
    sku = child["SKU"]
    found = []

    for supplier, path, sku_col in SUPPLIERS:
        rows = supplier_indexes.get(supplier, {}).get(sku, [])
        if len(rows) == 1:
            row = rows[0]
            found.append({
                **child,
                "supplier": supplier,
                "SupplierSKU": sku,
                "free": str(row.get("free", "")).strip(),
                "match_status": "exact_unique_child_sku_match",
            })
        elif len(rows) > 1:
            multi.append({
                **child,
                "supplier": supplier,
                "SupplierSKU": sku,
                "match_count": str(len(rows)),
                "match_status": "multiple_supplier_rows_for_child_sku",
            })

    if len(found) == 1:
        matches.extend(found)
    elif len(found) == 0 and not any(r["SKU"] == sku for r in multi):
        missing.append({
            **child,
            "SupplierSKU": sku,
            "match_status": "no_supplier_stock_match",
        })
    elif len(found) > 1:
        multi.extend(found)

out = ROOT / "reports/product_family_probe"
out.mkdir(parents=True, exist_ok=True)

def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

write_csv(out / "child_supplier_exact_matches.csv", matches,
          ["ProductID", "SKU", "Color", "Size", "supplier", "SupplierSKU", "free", "match_status"])

write_csv(out / "child_supplier_missing_matches.csv", missing,
          ["ProductID", "SKU", "Color", "Size", "SupplierSKU", "match_status"])

write_csv(out / "child_supplier_multiple_matches.csv", multi,
          ["ProductID", "SKU", "Color", "Size", "supplier", "SupplierSKU", "free", "match_count", "match_status"])

print("children:", len(children))
print("exact_unique_matches:", len(matches))
print("missing_matches:", len(missing))
print("multiple_or_conflict_matches:", len(multi))
print()
print(out / "child_supplier_exact_matches.csv")
print(out / "child_supplier_missing_matches.csv")
print(out / "child_supplier_multiple_matches.csv")
