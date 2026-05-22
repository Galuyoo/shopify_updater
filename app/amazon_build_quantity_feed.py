from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


COLUMNS = ["sku", "quantity"]


def latest_manifest() -> Path:
    files = sorted(
        Path("reports/amazon").glob("amazon_api_update_manifest_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_api_update_manifest_*.jsonl found.")
    return files[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    manifest = Path(args.manifest) if args.manifest else latest_manifest()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    rows = []

    with manifest.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            rows.append({
                "sku": row["amazon_seller_sku"],
                "quantity": int(row["quantity"]),
            })

    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Manifest: {manifest}")
    print(f"Rows written: {len(rows)}")
    print(f"Quantity-only file: {output}")
    print("Amazon API called: False")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
