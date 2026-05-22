from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

REVIEWED_SHARED_STOCK_SKUS = {"TC013BLAC", "TC013NAVY"}


def latest_dry_run_report() -> Path:
    files = sorted(
        Path("reports/amazon").glob("amazon_stock_dry_run_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_stock_dry_run_*.csv report found.")
    return files[0]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build dry-run Amazon API update manifest from Amazon stock dry-run CSV."
    )
    parser.add_argument("--dry-run", default=None)
    parser.add_argument("--out-dir", default="reports/amazon")
    parser.add_argument("--include-reviewed-shared-stock", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    dry_run_path = Path(args.dry_run) if args.dry_run else latest_dry_run_report()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = out_dir / f"amazon_api_update_manifest_{timestamp}.jsonl"
    summary_path = out_dir / f"amazon_api_update_manifest_summary_{timestamp}.json"

    rows = []
    skipped = []

    with dry_run_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            status = (row.get("status") or "").strip()
            canonical_sku = (row.get("canonical_sku") or "").strip().upper()
            amazon_seller_sku = (row.get("amazon_seller_sku") or "").strip()
            qty_raw = (row.get("proposed_amazon_qty") or "").strip()
            marketplace_id = (row.get("marketplace_id") or "").strip()
            fulfillment_channel = (row.get("fulfillment_channel") or "").strip()

            allowed = status == "ready"

            if status == "warning_duplicate_canonical_sku":
                allowed = (
                    args.include_reviewed_shared_stock
                    and canonical_sku in REVIEWED_SHARED_STOCK_SKUS
                )

            if not allowed:
                skipped.append({
                    "amazon_seller_sku": amazon_seller_sku,
                    "canonical_sku": canonical_sku,
                    "status": status,
                    "reason": "status_not_allowed_for_api_manifest",
                })
                continue

            if not amazon_seller_sku or qty_raw == "":
                skipped.append({
                    "amazon_seller_sku": amazon_seller_sku,
                    "canonical_sku": canonical_sku,
                    "status": status,
                    "reason": "missing_sku_or_quantity",
                })
                continue

            rows.append({
                "amazon_seller_sku": amazon_seller_sku,
                "canonical_sku": canonical_sku,
                "marketplace_id": marketplace_id,
                "fulfillment_channel": fulfillment_channel,
                "quantity": int(float(qty_raw)),
                "status": status,
                "dry_run_only": True,
            })

            if args.limit and len(rows) >= args.limit:
                break

    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "dry_run_report": str(dry_run_path),
        "manifest": str(manifest_path),
        "rows_ready_for_api": len(rows),
        "rows_skipped": len(skipped),
        "dry_run_only": True,
        "amazon_api_called": False,
    }

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Amazon API update manifest built.")
    print(f"Dry-run report: {dry_run_path}")
    print(f"Manifest rows: {len(rows)} -> {manifest_path}")
    print(f"Skipped rows: {len(skipped)}")
    print(f"Amazon API called: False")
    print(f"Summary: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
