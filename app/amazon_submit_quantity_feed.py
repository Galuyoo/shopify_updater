from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv


CONFIRM_TEXT = "SUBMIT_AMAZON_QUANTITY_FEED"


def env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def latest_quantity_file() -> Path:
    files = sorted(
        Path("reports/amazon").glob("amazon_quantity_*.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_quantity_*.txt file found.")
    return files[0]


def count_rows(path: Path) -> int:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return 0
    return max(len(lines) - 1, 0)


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Submit Amazon quantity feed. Safe skeleton: no live API calls yet."
    )
    parser.add_argument("--file", default=None)
    parser.add_argument("--marketplace-id", default=os.getenv("AMAZON_MARKETPLACE_ID", "A1F83G8C2ARO7P"))
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--out-dir", default="reports/amazon")
    args = parser.parse_args()

    dry_run = env_bool("AMAZON_DRY_RUN", default=True)
    feed_file = Path(args.file) if args.file else latest_quantity_file()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not feed_file.exists():
        raise FileNotFoundError(f"Quantity feed file not found: {feed_file}")

    row_count = count_rows(feed_file)
    file_size = feed_file.stat().st_size

    can_live = (
        not dry_run
        and args.live
        and args.confirm == CONFIRM_TEXT
    )

    print("Amazon quantity feed submitter")
    print(f"File: {feed_file}")
    print(f"Rows: {row_count}")
    print(f"Size bytes: {file_size}")
    print(f"Marketplace ID: {args.marketplace_id}")
    print(f"AMAZON_DRY_RUN: {dry_run}")
    print(f"--live: {args.live}")
    print(f"--confirm valid: {args.confirm == CONFIRM_TEXT}")

    if can_live:
        print("LIVE MODE WOULD BE ENABLED, but API submission is not implemented yet.")
        print("Amazon API called: False")
    else:
        print("Mode: SAFE PREVIEW ONLY")
        print("Amazon API called: False")

    summary = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "quantity_feed_file": str(feed_file),
        "rows": row_count,
        "size_bytes": file_size,
        "marketplace_id": args.marketplace_id,
        "amazon_dry_run": dry_run,
        "live_flag": bool(args.live),
        "confirm_valid": args.confirm == CONFIRM_TEXT,
        "would_live": can_live,
        "amazon_api_called": False,
    }

    summary_path = out_dir / f"amazon_quantity_feed_submit_preview_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Summary: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
