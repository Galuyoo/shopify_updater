from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_api import (
    ApiPayloadValidationError,
    StoreFeederApiClient,
    StoreFeederApiConfig,
    batch_items,
    build_stock_location_inventory_payload_preview,
    payload_preview_to_storefeeder_items,
    validate_api_batch_size,
)
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
    parser.add_argument("--api-live", action="store_true", help="Send StoreFeeder stock location inventory updates. Default is dry-run with zero API calls")
    parser.add_argument("--allow-unsafe-storefeeder-api-experiment", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--api-limit", type=int, help="Cap API payload rows after validation. Required for guarded live API runs")
    parser.add_argument("--api-batch-size", default=50, type=int, help="StoreFeeder API batch size, maximum 50")
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com", help="StoreFeeder REST API base URL")
    parser.add_argument("--storefeeder-stock-location-id-map", type=Path, help="CSV mapping stock_location to StoreFeeder StockLocationID.IDType and StockLocationID.Value")
    parser.add_argument("--storefeeder-stock-location-id-type", default="StockLocationReference", help="StoreFeeder StockLocationID.IDType for API payloads")
    parser.add_argument("--storefeeder-stock-location-id-value-column", default="stock_location", help="Column to use for StockLocationID.Value in API payloads")
    parser.add_argument("--export", action="store_true", help="Write ready files and reports. Default is dry-run only")
    parser.add_argument("--legacy-3-column-export", action="store_true", help="Build the old 3-column stock update file instead of strict full import")
    parser.add_argument("--limit-preview", default=20, type=int, help="Preview row count")
    args = parser.parse_args()
    try:
        validate_api_batch_size(args.api_batch_size)
    except ValueError as exc:
        parser.error(str(exc))
    if args.api_limit is not None and args.api_limit < 0:
        parser.error("--api-limit must be zero or greater")
    return args


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
        live_mode=args.live_api or args.api_live,
        allow_unusual_update_count=args.allow_unusual_update_count,
        previous_update_ready_count=previous_ready_count,
    )
    stock_location_id_map = _read_stock_location_id_map(args.storefeeder_stock_location_id_map)

    try:
        api_payload_preview = build_stock_location_inventory_payload_preview(
            result.api_payload_preview,
            api_limit=args.api_limit,
            stock_location_id_map=stock_location_id_map,
            stock_location_id_type=args.storefeeder_stock_location_id_type,
            stock_location_id_value_column=args.storefeeder_stock_location_id_value_column,
        )
    except ApiPayloadValidationError as exc:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        invalid_rows_path = args.out_dir / "api_payload_invalid_rows.csv"
        exc.invalid_rows.to_csv(invalid_rows_path, index=False)
        if args.export:
            files = write_strict_storefeeder_stock_export(
                result,
                args.out_dir,
                write_ready_files=result.safety_passed,
            )
            _print_files(files)
        print("\nAPI PAYLOAD SAFETY FAILURE:")
        print(str(exc))
        print(f"Invalid rows report: {invalid_rows_path}")
        print("\nNo StoreFeeder API calls were made.")
        return 2

    print("Strict StoreFeeder stock update dry run" if not args.export else "Strict StoreFeeder stock update export")
    print(result.validation_summary.to_string(index=False))
    print("\nReady rows preview:")
    print(result.ready_export.head(args.limit_preview).to_string(index=False))
    print("\nQuarantine preview:")
    print(result.quarantine_review.head(args.limit_preview).to_string(index=False))
    print("\nAPI payload preview:")
    print(api_payload_preview.head(args.limit_preview).to_string(index=False))

    if not result.live_update_allowed:
        print("\nLIVE/EXPORT GATE BLOCKED:")
        print("|".join(result.blocked_reasons))

    if args.live_api:
        print("\nLive API mode requested, but this command does not call StoreFeeder API. API payload preview contains update_ready rows only.")

    if not args.export:
        _print_api_summary(
            dry_run=True,
            api_live=args.api_live,
            rows_eligible_for_api=len(result.api_payload_preview),
            rows_sent_to_api=0,
            batches_sent=0,
            successful=0,
            failed=0,
            report_paths={"api_payload_preview": "not written without --export"},
        )
        print("\nDry-run only. No files were exported and no StoreFeeder API calls were made.")
        return 0

    files = write_strict_storefeeder_stock_export(
        result,
        args.out_dir,
        write_ready_files=result.safety_passed,
    )
    api_payload_preview.to_csv(files.api_payload_preview, index=False)
    _print_files(files, no_api_calls=not args.api_live)

    if not args.api_live:
        _print_api_summary(
            dry_run=True,
            api_live=False,
            rows_eligible_for_api=len(result.api_payload_preview),
            rows_sent_to_api=0,
            batches_sent=0,
            successful=0,
            failed=0,
            report_paths={"api_payload_preview": str(files.api_payload_preview)},
        )
        return 0

    if not args.allow_unsafe_storefeeder_api_experiment:
        _print_api_summary(
            dry_run=True,
            api_live=True,
            rows_eligible_for_api=len(result.api_payload_preview),
            rows_sent_to_api=0,
            batches_sent=0,
            successful=0,
            failed=0,
            report_paths={"api_payload_preview": str(files.api_payload_preview)},
        )
        raise SystemExit("Blocked: StoreFeeder API stock update semantics are not verified. Live API updates are disabled.")

    if not result.safety_passed:
        raise SystemExit("Blocked StoreFeeder API live update because safety_passed is not True.")
    if len(result.quarantine_review) != 0:
        raise SystemExit("Blocked StoreFeeder API live update because quarantined rows are present.")
    if not result.live_update_allowed:
        raise SystemExit("Blocked StoreFeeder API live update because live_update_allowed is not yes: " + "|".join(result.blocked_reasons))
    if args.api_limit is None:
        raise SystemExit("Blocked StoreFeeder API live update because --api-limit is required for guarded initial runs.")

    items = payload_preview_to_storefeeder_items(api_payload_preview)
    batches = batch_items(items, args.api_batch_size)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    api_report_paths = _send_storefeeder_api_batches(client, batches, args.out_dir)
    _print_api_summary(
        dry_run=False,
        api_live=True,
        rows_eligible_for_api=len(result.api_payload_preview),
        rows_sent_to_api=len(items),
        batches_sent=len(batches),
        successful=_sum_csv_column(api_report_paths["api_update_batches"], "successful"),
        failed=_sum_csv_column(api_report_paths["api_update_batches"], "failed"),
        report_paths={"api_payload_preview": str(files.api_payload_preview), **{k: str(v) for k, v in api_report_paths.items()}},
    )
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


def _read_stock_location_id_map(path: Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _print_files(files, *, no_api_calls: bool = True) -> None:
    print("\nWrote files:")
    for path in files.__dict__.values():
        print(path)
    if no_api_calls:
        print("\nNo StoreFeeder API calls were made.")


def _send_storefeeder_api_batches(client: StoreFeederApiClient, batches: list[list[dict]], out_dir: Path) -> dict[str, Path]:
    paths = {
        "api_update_success": out_dir / "api_update_success.csv",
        "api_update_failures": out_dir / "api_update_failures.csv",
        "api_update_batches": out_dir / "api_update_batches.csv",
        "api_update_raw_responses": out_dir / "api_update_raw_responses.json",
    }
    success_rows = []
    failure_rows = []
    batch_rows = []
    raw_responses = []
    for batch_number, batch in enumerate(batches, start=1):
        result = client.update_stock_location_inventory(batch, batch_number=batch_number)
        batch_rows.append(
            {
                "batch_number": result.batch_number,
                "requested_count": result.requested_count,
                "status_code": result.status_code,
                "total_processed": result.total_processed,
                "successful": result.successful,
                "failed": result.failed,
            }
        )
        raw_responses.append(
            {
                "batch_number": result.batch_number,
                "status_code": result.status_code,
                "response": result.response_json,
            }
        )
        target_rows = failure_rows if result.status_code >= 400 or result.failed else success_rows
        for item in batch:
            target_rows.append(_api_item_report_row(batch_number, item, result.status_code))

    pd.DataFrame(success_rows).to_csv(paths["api_update_success"], index=False)
    pd.DataFrame(failure_rows).to_csv(paths["api_update_failures"], index=False)
    pd.DataFrame(batch_rows).to_csv(paths["api_update_batches"], index=False)
    paths["api_update_raw_responses"].write_text(json.dumps(raw_responses, indent=2), encoding="utf-8")
    return paths


def _api_item_report_row(batch_number: int, item: dict, status_code: int) -> dict[str, object]:
    return {
        "batch_number": batch_number,
        "status_code": status_code,
        "SKU": item["ProductIDType"]["Value"],
        "stock_location_id_type": item["StockLocationID"]["IDType"],
        "stock_location_id_value": item["StockLocationID"]["Value"],
        "adjustment_type": item["AdjustmentType"],
        "adjustment_amount": item["AdjustmentAmount"],
    }


def _sum_csv_column(path: Path, column: str) -> int:
    if not path.exists():
        return 0
    df = pd.read_csv(path)
    if column not in df.columns:
        return 0
    return int(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def _print_api_summary(
    *,
    dry_run: bool,
    api_live: bool,
    rows_eligible_for_api: int,
    rows_sent_to_api: int,
    batches_sent: int,
    successful: int,
    failed: int,
    report_paths: dict[str, str],
) -> None:
    print("\nStoreFeeder API summary:")
    print(f"dry_run: {'yes' if dry_run else 'no'}")
    print(f"api_live: {'yes' if api_live else 'no'}")
    print(f"rows_eligible_for_api: {rows_eligible_for_api}")
    print(f"rows_sent_to_api: {rows_sent_to_api}")
    print(f"batches_sent: {batches_sent}")
    print(f"successful: {successful}")
    print(f"failed: {failed}")
    print("report_paths:")
    for name, path in report_paths.items():
        print(f"  {name}: {path}")


if __name__ == "__main__":
    raise SystemExit(main())

