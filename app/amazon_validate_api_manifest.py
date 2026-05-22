from __future__ import annotations

import argparse
import json
from pathlib import Path


def latest_manifest() -> Path:
    files = sorted(
        Path("reports/amazon").glob("amazon_api_update_manifest_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_api_update_manifest_*.jsonl file found.")
    return files[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--max-quantity", type=int, default=9999)
    args = parser.parse_args()

    manifest_path = Path(args.manifest) if args.manifest else latest_manifest()

    rows = []
    errors = []
    warnings = []
    seen_skus = set()

    with manifest_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            row = json.loads(line)
            rows.append(row)

            amazon_sku = str(row.get("amazon_seller_sku", "")).strip()
            canonical_sku = str(row.get("canonical_sku", "")).strip()
            marketplace_id = str(row.get("marketplace_id", "")).strip()
            quantity = row.get("quantity")

            if not amazon_sku:
                errors.append(f"line {line_no}: missing amazon_seller_sku")

            if not canonical_sku:
                errors.append(f"line {line_no}: missing canonical_sku")

            if not marketplace_id:
                errors.append(f"line {line_no}: missing marketplace_id")

            if amazon_sku.upper() in seen_skus:
                errors.append(f"line {line_no}: duplicate amazon_seller_sku {amazon_sku}")

            seen_skus.add(amazon_sku.upper())

            try:
                qty = int(quantity)
            except Exception:
                errors.append(f"line {line_no}: invalid quantity {quantity}")
                continue

            if qty < 0:
                errors.append(f"line {line_no}: negative quantity {qty}")

            if qty > args.max_quantity:
                warnings.append(f"line {line_no}: high quantity {qty} for {amazon_sku}")

    print("Amazon quantity manifest validation")
    print(f"Manifest: {manifest_path}")
    print(f"Rows: {len(rows)}")
    print(f"Errors: {len(errors)}")
    print(f"Warnings: {len(warnings)}")

    if errors:
        print("")
        print("ERRORS")
        for item in errors[:50]:
            print(item)

    if warnings:
        print("")
        print("WARNINGS")
        for item in warnings[:50]:
            print(item)

    if errors:
        print("")
        print("Result: FAILED")
        return 1

    print("")
    print("Result: PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
