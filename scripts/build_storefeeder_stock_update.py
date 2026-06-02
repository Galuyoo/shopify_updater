from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_stock_export import (
    build_strict_storefeeder_stock_update,
    build_storefeeder_stock_update,
    write_strict_storefeeder_stock_export,
    write_storefeeder_stock_export,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build StoreFeeder stock-location import files for manual upload.")
    parser.add_argument("--storefeeder-export", type=Path, help="Full StoreFeeder product export after supplier/location mapping")
    parser.add_argument("--supplier-mapping", required=True, type=Path, help="supplier_mapping.csv path")
    parser.add_argument("--ralawise-stock", type=Path, help="Ralawise stock CSV/API-output CSV path")
    parser.add_argument("--uneek-stock", type=Path, help="Uneek stock CSV/API-output CSV path")
    parser.add_argument("--out-dir", default=Path("reports/storefeeder"), type=Path, help="Output report directory")
    parser.add_argument("--buffer", default=0, type=int, help="Subtract this from supplier stock before capping")
    parser.add_argument("--max-stock", default=5, type=int, help="Maximum stock quantity to export")
    parser.add_argument("--missing-as-zero", action="store_true", help="Treat missing supplier stock as 0 instead of quarantining it")
    parser.add_argument("--allow-quarantine-update", action="store_true", help="Allow report/export despite quarantine gate; quarantined rows still never enter API preview")
    parser.add_argument("--max-quarantine-rate", default=0.02, type=float, help="Block live/export if quarantine rate exceeds this")
    parser.add_argument("--max-stock-file-age-hours", default=24.0, type=float, help="Block live/export if supplier stock file is older than this")
    parser.add_argument("--previous-update-ready-count", type=int, help="Previous update_ready count for unusual-count checks")
    parser.add_argument("--allow-unusual-update-count", action="store_true", help="Do not block when update count differs sharply from previous run")
    parser.add_argument("--live-api", action="store_true", help="Validate live API gate and produce API payload preview only; no API call is made")
    parser.add_argument("--export", action="store_true", help="Write ready files and reports. Default is dry-run only")
    parser.add_argument("--legacy-3-column-export", action="store_true", help="Build the old 3-column stock update file instead of strict full import")
    parser.add_argument("--limit-preview", default=20, type=int, help="Preview row count")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.ralawise_stock and not args.uneek_stock:
        raise SystemExit("Provide at least one stock input: --ralawise-stock or --uneek-stock")

    if args.legacy_3_column_export:
        result = build_storefeeder_stock_update(
            args.supplier_mapping,
            args.ralawise_stock,
            args.uneek_stock,
            buffer=args.buffer,
            max_stock=args.max_stock,
            update_missing_as_zero=args.missing_as_zero,
        )
        print("Legacy StoreFeeder 3-column stock update dry run" if not args.export else "Legacy StoreFeeder 3-column stock update build")
        print(result.validation_summary.to_string(index=False))
        print("\nPreview:")
        print(result.stock_update.head(args.limit_preview).to_string(index=False))
        if not args.export:
            print("\nDry-run only. No files were exported and no StoreFeeder API calls were made.")
            return 0
        files = write_storefeeder_stock_export(result, args.out_dir)
        _print_files(files)
        return 0

    if not args.storefeeder_export:
        raise SystemExit("Provide --storefeeder-export for strict full StoreFeeder import workflow, or pass --legacy-3-column-export.")

    previous_ready_count = args.previous_update_ready_count
    if previous_ready_count is None:
        previous_ready_count = _read_previous_update_ready_count(args.out_dir / "validation_summary.csv")

    result = build_strict_storefeeder_stock_update(
        args.storefeeder_export,
        args.supplier_mapping,
        args.ralawise_stock,
        args.uneek_stock,
        buffer=args.buffer,
        max_stock=args.max_stock,
        missing_as_zero=args.missing_as_zero,
        allow_quarantine_update=args.allow_quarantine_update,
        max_quarantine_rate=args.max_quarantine_rate,
        max_stock_file_age_hours=args.max_stock_file_age_hours,
        live_mode=args.live_api,
        allow_unusual_update_count=args.allow_unusual_update_count,
        previous_update_ready_count=previous_ready_count,
    )

    print("Strict StoreFeeder stock update dry run" if not args.export else "Strict StoreFeeder stock update export")
    print(result.validation_summary.to_string(index=False))
    print("\nReady rows preview:")
    print(result.ready_export.head(args.limit_preview).to_string(index=False))
    print("\nQuarantine preview:")
    print(result.quarantine_review.head(args.limit_preview).to_string(index=False))
    print("\nAPI payload preview:")
    print(result.api_payload_preview.head(args.limit_preview).to_string(index=False))

    if not result.live_update_allowed:
        print("\nLIVE/EXPORT GATE BLOCKED:")
        print("|".join(result.blocked_reasons))

    if args.live_api:
        print("\nLive API mode requested, but this command does not call StoreFeeder API. API payload preview contains update_ready rows only.")

    if not args.export:
        print("\nDry-run only. No files were exported and no StoreFeeder API calls were made.")
        return 0

    files = write_strict_storefeeder_stock_export(
        result,
        args.out_dir,
        write_ready_files=result.safety_passed,
    )
    _print_files(files)
    return 0


def _read_previous_update_ready_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        import pandas as pd
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return None
    if "metric" not in df.columns or "value" not in df.columns:
        return None
    matches = df[df["metric"].eq("update_ready_count")]
    if matches.empty:
        return None
    try:
        return int(matches.iloc[0]["value"])
    except Exception:
        return None


def _print_files(files) -> None:
    print("\nWrote files:")
    for path in files.__dict__.values():
        print(path)
    print("\nNo StoreFeeder API calls were made.")


if __name__ == "__main__":
    raise SystemExit(main())

