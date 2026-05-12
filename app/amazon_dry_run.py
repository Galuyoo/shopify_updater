from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .marketplaces.amazon import AmazonDryRunAdapter, load_listing_map

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Amazon stock dry-run reports.")
    parser.add_argument(
        "--listing-map",
        required=True,
        type=Path,
        help="Path to Amazon listing map CSV.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        type=Path,
        help="Directory where dry-run reports will be written.",
    )
    parser.add_argument(
        "--prefer",
        default="ralawise",
        choices=("ralawise", "uneek"),
        help="Supplier source that wins when duplicate supplier SKUs exist.",
    )
    return parser.parse_args()


def load_supplier_stock(prefer: str):
    from stock_sources import build_combined_stock_df

    return build_combined_stock_df(prefer=prefer, progress=print)


def main() -> int:
    args = parse_args()
    adapter = AmazonDryRunAdapter()

    print(f"Loading supplier stock (prefer={args.prefer})...")
    stock_df = load_supplier_stock(args.prefer)

    print(f"Loading Amazon listing map: {args.listing_map}")
    listing_map_df = load_listing_map(args.listing_map)

    result = adapter.build_dry_run(stock_df, listing_map_df)
    reports, summary = adapter.write_reports(
        result,
        args.out_dir,
        listing_map=args.listing_map,
        prefer=args.prefer,
        stock_rows=len(stock_df),
    )

    print("Amazon dry-run complete.")
    print(
        "Summary: "
        f"would_update={summary.would_update_rows}, "
        f"errors={summary.error_rows}, "
        f"warnings={summary.warning_rows}, "
        f"skipped={summary.skipped_rows}, "
        f"sp_api_called={summary.amazon_sp_api_called}"
    )
    for report in reports:
        print(f"Wrote {report.name} ({report.rows} rows)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
