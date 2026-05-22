from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv


def latest_manifest() -> Path:
    files = sorted(
        Path("reports/amazon").glob("amazon_api_update_manifest_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No amazon_api_update_manifest_*.jsonl file found.")
    return files[0]


def env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def load_manifest(path: Path, limit: int = 0) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if limit and len(rows) >= limit:
                break
    return rows


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Prepare Amazon API stock updates from manifest. Safe dry-run sender skeleton."
    )
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    dry_run_env = env_bool("AMAZON_DRY_RUN", default=True)
    manifest_path = Path(args.manifest) if args.manifest else latest_manifest()
    rows = load_manifest(manifest_path, limit=args.limit)

    print("Amazon API sender skeleton")
    print(f"Manifest: {manifest_path}")
    print(f"Rows loaded: {len(rows)}")
    print(f"AMAZON_DRY_RUN: {dry_run_env}")
    print(f"--live flag: {args.live}")

    if dry_run_env or not args.live:
        print("Mode: SAFE PREVIEW ONLY. No Amazon API update calls will be made.")
    else:
        print("Mode: LIVE WOULD BE ENABLED, but real sender is not implemented yet.")
        print("No Amazon API update calls will be made.")

    for row in rows:
        print(
            json.dumps(
                {
                    "amazon_seller_sku": row.get("amazon_seller_sku"),
                    "canonical_sku": row.get("canonical_sku"),
                    "marketplace_id": row.get("marketplace_id"),
                    "fulfillment_channel": row.get("fulfillment_channel"),
                    "quantity": row.get("quantity"),
                    "would_send": False,
                },
                ensure_ascii=False,
            )
        )

    print("Amazon API called: False")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
