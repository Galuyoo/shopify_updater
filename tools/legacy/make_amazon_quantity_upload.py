import argparse
import csv
from pathlib import Path

def latest_dry_run_report():
    files = sorted(
        Path("reports/amazon").glob("amazon_stock_dry_run_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_stock_dry_run_*.csv report found.")
    return files[0]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", default=None)
    parser.add_argument("--amazon-export", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--include-warnings", action="store_true")
    args = parser.parse_args()

    dry_run_path = Path(args.dry_run) if args.dry_run else latest_dry_run_report()
    amazon_export_path = Path(args.amazon_export)
    output_path = Path(args.output)

    allowed_statuses = {"ready"}
    if args.include_warnings:
        allowed_statuses.add("warning_duplicate_canonical_sku")

    updates = {}
    with dry_run_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "").strip()
            if status not in allowed_statuses:
                continue

            sku = (row.get("amazon_seller_sku") or "").strip()
            qty = (row.get("proposed_amazon_qty") or "").strip()

            if not sku or qty == "":
                continue

            updates[sku.upper()] = str(int(float(qty)))

    if args.limit and args.limit > 0:
        limited = {}
        for sku, qty in updates.items():
            limited[sku] = qty
            if len(limited) >= args.limit:
                break
        updates = limited

    if not updates:
        raise RuntimeError("No updateable rows found.")

    with amazon_export_path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        delimiter = "\t" if "\t" in sample else ","

    with amazon_export_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise RuntimeError("Amazon export has no header row.")

        required = {"sku", "quantity"}
        missing = required - set(fieldnames)
        if missing:
            raise RuntimeError(f"Amazon export missing required columns: {missing}")

        rows = []
        for row in reader:
            sku = (row.get("sku") or "").strip()
            if sku.upper() in updates:
                row["quantity"] = updates[sku.upper()]
                rows.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Dry-run report used: {dry_run_path}")
    print(f"Rows written: {len(rows)}")
    print(f"Output: {output_path}")

if __name__ == "__main__":
    main()
