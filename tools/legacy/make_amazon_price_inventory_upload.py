import argparse
import csv
from pathlib import Path

PRICE_INVENTORY_COLUMNS = [
    "sku",
    "price",
    "minimum-seller-allowed-price",
    "maximum-seller-allowed-price",
    "quantity",
    "handling-time",
    "fulfillment-channel",
]

def latest_dry_run_report():
    files = sorted(
        Path("reports/amazon").glob("amazon_stock_dry_run_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_stock_dry_run_*.csv report found.")
    return files[0]

def detect_delimiter(path):
    sample = path.read_text(encoding="utf-8-sig", errors="ignore")[:4096]
    return "\t" if "\t" in sample else ","

def load_amazon_export(path):
    delimiter = detect_delimiter(path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = {}
        for row in reader:
            sku = (row.get("sku") or "").strip().upper()
            if sku:
                rows[sku] = row
        return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", default=None)
    parser.add_argument("--amazon-export", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--include-warnings", action="store_true")
    parser.add_argument("--only-warnings", action="store_true")
    args = parser.parse_args()

    dry_run_path = Path(args.dry_run) if args.dry_run else latest_dry_run_report()
    amazon_export_path = Path(args.amazon_export)
    output_path = Path(args.output)

    amazon_rows = load_amazon_export(amazon_export_path)

    allowed_statuses = {"ready"}
    if args.include_warnings:
        allowed_statuses.add("warning_duplicate_canonical_sku")
    if args.only_warnings:
        allowed_statuses = {"warning_duplicate_canonical_sku"}

    output_rows = []

    with dry_run_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "").strip()
            if status not in allowed_statuses:
                continue

            amazon_sku = (row.get("amazon_seller_sku") or "").strip()
            qty = (row.get("proposed_amazon_qty") or "").strip()

            if not amazon_sku or qty == "":
                continue

            amazon_source = amazon_rows.get(amazon_sku.upper(), {})
            price = (amazon_source.get("price") or "").strip()

            output_rows.append({
                "sku": amazon_sku,
                "price": price,
                "minimum-seller-allowed-price": "",
                "maximum-seller-allowed-price": "",
                "quantity": str(int(float(qty))),
                "handling-time": "",
                "fulfillment-channel": "",
            })

            if args.limit and args.limit > 0 and len(output_rows) >= args.limit:
                break

    if not output_rows:
        raise RuntimeError("No rows found for selected statuses.")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=PRICE_INVENTORY_COLUMNS,
            delimiter="\t",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Dry-run report used: {dry_run_path}")
    print(f"Amazon export used: {amazon_export_path}")
    print(f"Rows written: {len(output_rows)}")
    print(f"Output: {output_path}")

if __name__ == "__main__":
    main()
