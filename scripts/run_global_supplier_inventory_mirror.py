from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_supplier_stock_fast_update import (
    _active_targets,
    _inventory_values,
    _load_env_file,
    _location_id,
    _number,
    _records_from_api_response,
    _stock_locations,
    build_fast_payload_preview,
)
from src.storefeeder_api import (
    StoreFeederApiClient,
    StoreFeederApiConfig,
    batch_items,
    fetch_storefeeder_access_token,
    payload_preview_to_storefeeder_items,
    supplier_payload_preview_to_items,
)
from src.storefeeder_response_helpers import _response_error, _response_failed, _truthy
from src.storefeeder_stock_export import read_csv
from src.storefeeder_available_inventory_mirror import (
    InventoryLedgerError,
    build_physical_stock_mirror_plan,
    deduction_signature,
)


WAREHOUSE_ID = "162454"
FULL_CONFIRMATION = "LIVE GLOBAL STOREFEEDER INVENTORY MIRROR"
CANARY_CONFIRMATION = "LIVE GLOBAL STOREFEEDER INVENTORY MIRROR CANARY 10"
TRANSIENT_STATUSES = {401, 408, 409, 429, 500, 502, 503, 504}

STATE_COLUMNS = [
    "ProductID", "SKU", "ProductType", "Archived", "Inventory", "AvailableInventory",
    "WarehouseCount", "WarehousePhysical", "WarehouseAvailable", "WarehouseAllocated",
    "WarehousePendingOut", "TotalAllocated", "TotalPendingOut", "NegativeLedger",
    "LedgerError", "DeductionSignature", "OtherNonzero", "OtherLocationsJSON",
    "StockLocationsJSON",
]
EVALUATION_COLUMNS = [
    "ProductID", "SKU", "supplier", "SupplierID", "SupplierSKU", "ExpectedQuantity",
    "AppliedSupplierStockLevel", "Inventory", "AvailableInventory", "WarehousePhysical",
    "TargetAdjustmentAmount", "TargetWarehousePhysical", "DeductionSignature",
    "SupplierWriteRequired", "InventoryWriteRequired", "Result", "Reason",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Global diff-only StoreFeeder supplier and dropship physical-stock mirror reconciliation."
    )
    parser.add_argument("--targets", type=Path, default=Path("data/storefeeder_supplier_stock_update_targets.csv"))
    parser.add_argument("--catalogue", type=Path, default=Path("reports/cleaned_catalogue/cleaned_storefeeder_catalogue.csv"))
    parser.add_argument("--ralawise-stock", type=Path, default=Path("data/RALAWISE_stock_lvl.csv"))
    parser.add_argument("--uneek-stock", type=Path, default=Path("data/Uneek_stock_levels.csv"))
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--resume-run-dir", type=Path)
    parser.add_argument("--product-ids-file", type=Path)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--api-batch-size", type=int, default=50)
    parser.add_argument("--skip-feed-refresh", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--api-base-url", default="https://rest.storefeeder.com")
    args = parser.parse_args()
    if not 1 <= args.page_size <= 100:
        parser.error("--page-size must be between 1 and 100")
    if not 1 <= args.api_batch_size <= 50:
        parser.error("--api-batch-size must be between 1 and 50")
    if args.live:
        expected = CANARY_CONFIRMATION if args.product_ids_file else FULL_CONFIRMATION
        if args.confirm != expected:
            parser.error(f"live mode requires --confirm {expected!r}")
    return args


def main() -> int:
    args = parse_args()
    _load_env_file(ROOT / ".env")
    run_dir = resolve_run_dir(args)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_status(run_dir, "STARTED", live=args.live)

    if not args.skip_feed_refresh:
        refresh_feeds(run_dir, args)

    targets = _active_targets(read_csv(args.targets))
    selected_ids = load_selected_ids(args.product_ids_file)
    if selected_ids:
        targets = targets[targets["ProductID"].astype(str).isin(selected_ids)].copy()
        missing = selected_ids - set(targets["ProductID"].astype(str))
        if missing:
            raise RuntimeError(f"selected ProductIDs missing from full targets: {sorted(missing)}")

    expected, invalid = build_expected(targets, args)
    expected.to_csv(run_dir / "expected_targets.csv", index=False)
    invalid.to_csv(run_dir / "invalid_rows.csv", index=False)

    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.api_base_url))
    live_state = scan_live_products(
        client,
        set(expected["ProductID"].astype(str)),
        run_dir,
        page_size=args.page_size,
        prefix="preflight",
        detail_mode=bool(selected_ids),
    )
    baseline, baseline_source = load_supplier_baseline(run_dir)
    evaluation = evaluate(expected, live_state, baseline)
    write_evaluation_reports(run_dir, evaluation)
    summary = build_summary(evaluation, invalid, baseline_source, live=args.live)
    write_json(run_dir / "preflight_summary.json", summary)
    print(json.dumps(summary, indent=2), flush=True)

    if not args.live:
        write_status(run_dir, "DRY_RUN_COMPLETE", live=False, **summary)
        print(f"Dry run complete. No StoreFeeder writes were made. Reports: {run_dir}", flush=True)
        return 0

    actionable = evaluation[evaluation["Result"].ne("QUARANTINED")].copy()
    supplier_writes = actionable[actionable["SupplierWriteRequired"].eq("yes")].copy()
    inventory_writes = actionable[actionable["InventoryWriteRequired"].eq("yes")].copy()

    supplier_success, supplier_failures = send_supplier_differences(
        client, supplier_writes, run_dir, args.api_batch_size
    )
    supplier_failures.to_csv(run_dir / "supplier_write_unresolved_failures.csv", index=False)
    if not supplier_failures.empty:
        write_status(run_dir, "FAILED_SUPPLIER_WRITES", live=True, failures=len(supplier_failures))
        raise RuntimeError(f"supplier writes failed for {len(supplier_failures)} products")

    inventory_success, inventory_failures = send_inventory_differences(
        client, inventory_writes, run_dir, args.api_batch_size
    )
    inventory_failures.to_csv(run_dir / "inventory_write_unresolved_failures.csv", index=False)
    if not inventory_failures.empty:
        write_status(run_dir, "FAILED_INVENTORY_WRITES", live=True, failures=len(inventory_failures))
        raise RuntimeError(f"inventory writes failed for {len(inventory_failures)} products")

    convergence_success, convergence_failures = converge_inventory_differences(
        client, inventory_writes, run_dir
    )
    if not convergence_failures.empty:
        write_status(
            run_dir,
            "FAILED_INVENTORY_CONVERGENCE",
            live=True,
            failures=len(convergence_failures),
        )
        raise RuntimeError(
            f"inventory convergence failed for {len(convergence_failures)} products"
        )

    verify_state = scan_live_products(
        client,
        set(actionable["ProductID"].astype(str)),
        run_dir,
        page_size=args.page_size,
        prefix="verification",
        detail_mode=bool(selected_ids),
    )
    verification = verify(actionable, verify_state)
    if selected_ids:
        verification = verify_canary_suppliers(client, verification)
    verification.to_csv(run_dir / "verification_results.csv", index=False)
    failures = verification[verification["Result"].ne("PASS")].copy()
    failures.to_csv(run_dir / "verification_failures.csv", index=False)

    applied = update_applied_supplier_state(baseline, actionable, supplier_success)
    applied.to_csv(run_dir / "supplier_applied_state.csv", index=False)
    final_summary = build_summary(evaluation, invalid, baseline_source, live=True)
    final_summary.update({
        "supplier_writes_successful": len(supplier_success),
        "supplier_write_failures_unresolved": len(supplier_failures),
        "inventory_writes_successful": len(inventory_success),
        "inventory_convergence_writes": len(convergence_success),
        "inventory_write_failures_unresolved": len(inventory_failures),
        "verification_passed": int(verification["Result"].eq("PASS").sum()),
        "verification_failed": len(failures),
        "listing_writes": 0,
        "stock_location_association_writes": 0,
    })
    write_json(run_dir / "final_summary.json", final_summary)
    if not failures.empty:
        write_status(run_dir, "FAILED_VERIFICATION", live=True, **final_summary)
        raise RuntimeError(f"global mirror verification failed for {len(failures)} products")
    write_status(run_dir, "COMPLETE", live=True, **final_summary)
    print(json.dumps(final_summary, indent=2), flush=True)
    print(f"Global mirror complete. Reports: {run_dir}", flush=True)
    return 0


def resolve_run_dir(args: argparse.Namespace) -> Path:
    if args.resume_run_dir:
        return args.resume_run_dir
    return args.out_dir or Path("reports/global_supplier_inventory_mirror") / datetime.now().strftime("%Y%m%d_%H%M%S")


def refresh_feeds(run_dir: Path, args: argparse.Namespace) -> None:
    command = [
        sys.executable, "scripts/refresh_supplier_stock_files.py",
        "--ralawise-out", str(args.ralawise_stock),
        "--uneek-out", str(args.uneek_stock),
    ]
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    (run_dir / "supplier_feed_refresh.log").write_text(
        completed.stdout + "\nSTDERR:\n" + completed.stderr, encoding="utf-8"
    )
    if completed.returncode:
        raise RuntimeError("supplier feed refresh failed")


def build_expected(targets: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    ralawise = read_csv(args.ralawise_stock)
    uneek = read_csv(args.uneek_stock)
    from src.stock_mapping import build_supplier_stock_lookup

    lookup = build_supplier_stock_lookup(ralawise, uneek)
    preview, _, _, invalid, _, _ = build_fast_payload_preview(
        targets,
        lookup,
        buffer=0,
        max_stock=2_147_483_647,
        missing_as_zero=False,
        zero_other_locations_for_supplier_synced=False,
    )
    columns = [
        "ProductID", "SKU", "supplier", "Supplier.SupplierID", "Supplier.Name",
        "SupplierSKU", "SupplierStockLevel", "SupplierCosts", "confidence_status",
    ]
    expected = preview[columns].rename(columns={"Supplier.SupplierID": "SupplierID"}).copy()
    expected["ProductID"] = expected["ProductID"].astype(str)
    expected["ExpectedQuantity"] = pd.to_numeric(expected["SupplierStockLevel"], errors="raise").astype(int)
    return expected, invalid


def load_selected_ids(path: Path | None) -> set[str]:
    if not path:
        return set()
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if "ProductID" not in frame:
        raise ValueError("product ID selection file requires ProductID")
    values = {value.strip() for value in frame["ProductID"] if value.strip()}
    if not values or any(not value.isdigit() for value in values):
        raise ValueError("selection contains missing or non-numeric ProductID")
    return values


def scan_live_products(
    client: StoreFeederApiClient,
    target_ids: set[str],
    run_dir: Path,
    *,
    page_size: int,
    prefix: str,
    detail_mode: bool,
) -> pd.DataFrame:
    state_path = run_dir / f"{prefix}_live_product_state.csv"
    progress_path = run_dir / f"{prefix}_scan_progress.csv"
    raw_path = run_dir / f"{prefix}_raw_products.jsonl"
    if state_path.exists():
        existing = pd.read_csv(state_path, dtype=str, keep_default_na=False)
    else:
        existing = pd.DataFrame(columns=STATE_COLUMNS)
    completed_ids = set(existing.get("ProductID", pd.Series(dtype=str)).astype(str))

    if detail_mode:
        remaining = sorted(target_ids - completed_ids, key=int)
        for index, product_id in enumerate(remaining, start=1):
            wrapper = api_with_retry(client, lambda pid=product_id: client.get_product(pid))
            require_success(f"product {product_id}", wrapper)
            append_jsonl(raw_path, {"ProductID": product_id, "response": wrapper.get("response")})
            append_csv(state_path, [flatten_product(wrapper["response"])], STATE_COLUMNS)
            append_csv(progress_path, [{"unit": product_id, "records": 1, "completed_at": datetime.now().isoformat()}])
            print(f"{prefix}: product {index}/{len(remaining)} ProductID={product_id}", flush=True)
    else:
        completed_pages = set()
        if progress_path.exists():
            progress = pd.read_csv(progress_path, dtype=str, keep_default_na=False)
            completed_pages = {int(value) for value in progress.get("unit", []) if str(value).isdigit()}
        page = max(completed_pages, default=0) + 1
        while True:
            wrapper = api_with_retry(
                client,
                lambda current=page: client.get_products_page(
                    page=current, page_size=page_size, include_stock_locations=True
                ),
            )
            require_success(f"products page {page}", wrapper)
            response = wrapper.get("response", {})
            records = _records_from_api_response(response)
            append_jsonl(raw_path, {"page": page, "response": response})
            selected = [flatten_product(item) for item in records if product_id_of(item) in target_ids]
            append_csv(state_path, selected, STATE_COLUMNS)
            append_csv(progress_path, [{"unit": page, "records": len(records), "completed_at": datetime.now().isoformat()}])
            if page == 1 or page % 25 == 0:
                print(f"{prefix}: fetched page {page}; API products={page * page_size}", flush=True)
            if len(records) < page_size:
                break
            page += 1

    state = pd.read_csv(state_path, dtype=str, keep_default_na=False).drop_duplicates("ProductID", keep="last")
    missing = target_ids - set(state["ProductID"].astype(str))
    if missing:
        missing_frame = pd.DataFrame({"ProductID": sorted(missing, key=int), "Reason": "missing_from_live_products"})
        missing_frame.to_csv(run_dir / f"{prefix}_missing_products.csv", index=False)
    return state


def flatten_product(product: dict[str, Any]) -> dict[str, Any]:
    locations = _stock_locations(product)
    warehouse = [item for item in locations if _location_id(item) == WAREHOUSE_ID]
    wh = warehouse[0] if len(warehouse) == 1 else {}
    inventory, available = _inventory_values(product)
    other = [item for item in locations if _location_id(item) != WAREHOUSE_ID]
    fields = ("Available", "Allocated", "PendingOut", "PhysicalStock")
    other_nonzero = any(int(_number(item.get("PhysicalStock"))) != 0 for item in other)
    negative = any(
        int(_number(item.get(field))) < 0
        for item in locations
        for field in ("Allocated", "PendingOut", "PhysicalStock")
    )
    ledger_error = ""
    try:
        build_physical_stock_mirror_plan(
            target_warehouse_physical=int(_number(wh.get("PhysicalStock"))) if wh else 0,
            current_available=int(_number(available)),
            locations=locations,
            warehouse_stock_location_id=WAREHOUSE_ID,
        )
    except InventoryLedgerError as exc:
        ledger_error = str(exc)
    info = product.get("InventoryInformation") if isinstance(product.get("InventoryInformation"), dict) else {}
    return {
        "ProductID": product_id_of(product),
        "SKU": str(product.get("SKU") or "").strip(),
        "ProductType": str(product.get("ProductType") or "").strip(),
        "Archived": "true" if _truthy(product.get("Archived")) else "false",
        "Inventory": int(_number(inventory)),
        "AvailableInventory": int(_number(available)),
        "WarehouseCount": len(warehouse),
        "WarehousePhysical": int(_number(wh.get("PhysicalStock"))) if wh else "",
        "WarehouseAvailable": int(_number(wh.get("Available"))) if wh else "",
        "WarehouseAllocated": int(_number(wh.get("Allocated"))) if wh else "",
        "WarehousePendingOut": int(_number(wh.get("PendingOut"))) if wh else "",
        "TotalAllocated": sum(int(_number(item.get("Allocated"))) for item in locations),
        "TotalPendingOut": sum(int(_number(item.get("PendingOut"))) for item in locations),
        "NegativeLedger": "yes" if negative else "no",
        "LedgerError": ledger_error,
        "DeductionSignature": deduction_signature(locations, WAREHOUSE_ID),
        "OtherNonzero": "yes" if other_nonzero else "no",
        "OtherLocationsJSON": json.dumps(other, ensure_ascii=True, separators=(",", ":")),
        "StockLocationsJSON": json.dumps(locations, ensure_ascii=True, separators=(",", ":")),
    }


def product_id_of(product: dict[str, Any]) -> str:
    for key in ("ProductID", "ProductId", "ID", "Id"):
        value = product.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def load_supplier_baseline(run_dir: Path) -> tuple[pd.DataFrame, str]:
    global_candidates: list[Path] = []
    for root in (
        Path("reports/global_supplier_inventory_mirror"),
        Path("reports/scheduled_daily_inventory_mirror"),
    ):
        if root.exists():
            global_candidates.extend(
                path for path in root.glob("*/supplier_applied_state.csv")
                if run_dir not in path.parents
            )
    if global_candidates:
        source = max(global_candidates, key=lambda path: path.stat().st_mtime)
        baseline = pd.read_csv(source, dtype=str, keep_default_na=False)
    else:
        source = latest_clean_full_supplier_success()
        baseline = pd.read_csv(source, dtype=str, keep_default_na=False)

    baseline = baseline.rename(columns={"SupplierStockLevel": "AppliedSupplierStockLevel"})
    needed = ["ProductID", "SupplierID", "SupplierSKU", "AppliedSupplierStockLevel"]
    baseline = baseline[needed].copy()
    source_mtime = source.stat().st_mtime
    overlays = list(Path("reports/scheduled_priority_supplier_sync").glob("*/fast_stock_update_success.csv"))
    for overlay in sorted((p for p in overlays if p.stat().st_mtime > source_mtime), key=lambda p: p.stat().st_mtime):
        frame = pd.read_csv(overlay, dtype=str, keep_default_na=False)
        if frame.empty:
            continue
        frame = frame.rename(columns={"SupplierStockLevel": "AppliedSupplierStockLevel"})
        baseline = pd.concat([baseline, frame[needed]], ignore_index=True).drop_duplicates("ProductID", keep="last")
    return baseline.drop_duplicates("ProductID", keep="last"), str(source)


def latest_clean_full_supplier_success() -> Path:
    candidates = []
    for run_dir in Path("reports/scheduled_fast_stock_sync").glob("*"):
        success = run_dir / "fast_stock_update_success.csv"
        failure = run_dir / "fast_stock_update_failures.csv"
        exit_status = run_dir / "wrapper_exit_status.csv"
        if not success.exists() or not exit_status.exists():
            continue
        status = pd.read_csv(exit_status, dtype=str, keep_default_na=False)
        if status.empty or status.iloc[0].get("python_process_exit_code", status.iloc[0].get("python_exit_code", "")) not in {"", "0"}:
            continue
        if failure.exists() and len(pd.read_csv(failure, dtype=str, keep_default_na=False)):
            continue
        candidates.append(success)
    if not candidates:
        raise RuntimeError("no clean full supplier success baseline is available")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def evaluate(expected: pd.DataFrame, state: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    rows = expected.merge(state, on="ProductID", how="left", suffixes=("", "_live"))
    rows = rows.merge(baseline, on="ProductID", how="left", suffixes=("", "_applied"))
    output = []
    for row in rows.to_dict("records"):
        expected_qty = int(row["ExpectedQuantity"])
        reasons = []
        if not str(row.get("SKU_live", "")):
            reasons.append("missing_live_product")
        elif str(row.get("SKU_live", "")) != str(row.get("SKU", "")):
            reasons.append("live_sku_mismatch")
        if "parent" in str(row.get("ProductType", "")).casefold():
            reasons.append("parent_product")
        if str(row.get("Archived", "")).casefold() == "true":
            reasons.append("archived_product")
        if int_value(row.get("WarehouseCount")) != 1:
            reasons.append("warehouse_stock_missing_or_duplicate")
        if str(row.get("NegativeLedger", "")).casefold() == "yes":
            reasons.append("negative_ledger_state")
        if str(row.get("LedgerError", "")).strip():
            reasons.append(str(row["LedgerError"]))
        if str(row.get("OtherNonzero", "")).casefold() == "yes":
            reasons.append("unexplained_non_warehouse_stock")
        applied_id = str(row.get("SupplierID_applied", row.get("SupplierID_y", row.get("SupplierID", "")))).strip()
        applied_sku = str(row.get("SupplierSKU_applied", "")).strip()
        if applied_id and applied_id != str(row["SupplierID"]):
            reasons.append("applied_supplier_id_mismatch")
        if applied_sku and applied_sku != str(row["SupplierSKU"]):
            reasons.append("applied_supplier_sku_mismatch")
        applied_stock = row.get("AppliedSupplierStockLevel", "")
        supplier_write = "yes" if str(applied_stock).strip() == "" or int_value(applied_stock) != expected_qty else "no"
        target_adjustment = ""
        target_physical = ""
        if not reasons:
            try:
                plan = build_physical_stock_mirror_plan(
                    target_warehouse_physical=expected_qty,
                    current_available=int_value(row.get("AvailableInventory")),
                    locations=json.loads(str(row.get("StockLocationsJSON", "[]")) or "[]"),
                    warehouse_stock_location_id=WAREHOUSE_ID,
                )
            except (InventoryLedgerError, json.JSONDecodeError) as exc:
                reasons.append(str(exc))
            else:
                target_adjustment = plan.target_adjustment_amount
                target_physical = plan.target_warehouse_physical
        inventory_write = (
            "yes" if int_value(row.get("WarehousePhysical")) != expected_qty else "no"
        )
        result = "QUARANTINED" if reasons else (
            "ALREADY_COMPLIANT" if supplier_write == inventory_write == "no" else "READY"
        )
        output.append({
            "ProductID": str(row["ProductID"]), "SKU": str(row["SKU"]),
            "supplier": str(row["supplier"]), "SupplierID": str(row["SupplierID"]),
            "SupplierSKU": str(row["SupplierSKU"]), "ExpectedQuantity": expected_qty,
            "AppliedSupplierStockLevel": applied_stock, "Inventory": row.get("Inventory", ""),
            "AvailableInventory": row.get("AvailableInventory", ""),
            "WarehousePhysical": row.get("WarehousePhysical", ""),
            "TargetAdjustmentAmount": target_adjustment,
            "TargetWarehousePhysical": target_physical,
            "DeductionSignature": row.get("DeductionSignature", ""),
            "SupplierWriteRequired": supplier_write, "InventoryWriteRequired": inventory_write,
            "Result": result, "Reason": "|".join(reasons),
        })
    return pd.DataFrame(output, columns=EVALUATION_COLUMNS)


def write_evaluation_reports(run_dir: Path, evaluation: pd.DataFrame) -> None:
    evaluation.to_csv(run_dir / "full_mirror_evaluation.csv", index=False)
    evaluation[evaluation["Result"].eq("ALREADY_COMPLIANT")].to_csv(run_dir / "already_compliant.csv", index=False)
    evaluation[evaluation["Result"].eq("QUARANTINED")].to_csv(run_dir / "quarantined.csv", index=False)
    evaluation[(evaluation["Result"].ne("QUARANTINED")) & evaluation["SupplierWriteRequired"].eq("yes")].to_csv(
        run_dir / "supplier_writes_proposed.csv", index=False
    )
    evaluation[(evaluation["Result"].ne("QUARANTINED")) & evaluation["InventoryWriteRequired"].eq("yes")].to_csv(
        run_dir / "inventory_writes_proposed.csv", index=False
    )


def build_summary(evaluation: pd.DataFrame, invalid: pd.DataFrame, baseline_source: str, *, live: bool) -> dict[str, Any]:
    safe = evaluation[evaluation["Result"].ne("QUARANTINED")]
    return {
        "mode": "live" if live else "dry_run",
        "eligible_products": len(evaluation),
        "already_compliant": int(evaluation["Result"].eq("ALREADY_COMPLIANT").sum()),
        "proposed_supplier_writes": int((safe["SupplierWriteRequired"] == "yes").sum()),
        "proposed_inventory_writes": int((safe["InventoryWriteRequired"] == "yes").sum()),
        "quarantined": int(evaluation["Result"].eq("QUARANTINED").sum()),
        "invalid_rows": len(invalid),
        "supplier_baseline": baseline_source,
        "listing_writes": 0,
        "stock_location_association_writes": 0,
    }


def supplier_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ProductIDType": {"IDType": "ID", "Value": str(row["ProductID"])},
        "Supplier": {"SupplierID": int(row["SupplierID"]), "Name": str(row["supplier"])},
        "SupplierSKU": str(row["SupplierSKU"]),
        "SupplierStockLevel": int(row["ExpectedQuantity"]),
        "SupplierCosts": 0,
    }


def inventory_item(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ProductIDType": {"IDType": "ID", "Value": str(row["ProductID"])},
        "AdjustmentType": "AbsoluteAdjustment",
        "AdjustmentAmount": int(row["TargetAdjustmentAmount"]),
        "StockLocationID": {"IDType": "ID", "Value": WAREHOUSE_ID},
        "Reason": "Global dropship mirror: Warehouse PhysicalStock equals SupplierStockLevel",
    }


def send_supplier_differences(client: StoreFeederApiClient, rows: pd.DataFrame, run_dir: Path, batch_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    return send_batches(client, rows, run_dir, "supplier", batch_size)


def send_inventory_differences(client: StoreFeederApiClient, rows: pd.DataFrame, run_dir: Path, batch_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    return send_batches(client, rows, run_dir, "inventory", batch_size)


def send_batches(client: StoreFeederApiClient, rows: pd.DataFrame, run_dir: Path, kind: str, batch_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    success_path = run_dir / f"{kind}_write_successes.csv"
    failure_path = run_dir / f"{kind}_write_failures.csv"
    raw_path = run_dir / f"{kind}_write_raw.jsonl"
    completed = set()
    if success_path.exists():
        completed = set(pd.read_csv(success_path, dtype=str, keep_default_na=False)["ProductID"])
    pending = rows[~rows["ProductID"].astype(str).isin(completed)].copy()
    records = pending.to_dict("records")
    for batch_number, group in enumerate(batch_items(records, batch_size), start=1):
        items = [supplier_item(row) if kind == "supplier" else inventory_item(row) for row in group]
        result = None
        for attempt in range(1, 4):
            if kind == "supplier":
                result = client.update_product_supplier_inventory_cost(items, batch_number=batch_number)
            else:
                result = client.update_stock_location_inventory(items, batch_number=batch_number)
            append_jsonl(raw_path, {
                "batch_number": batch_number, "attempt": attempt,
                "status_code": result.status_code, "response": result.response_json,
            })
            if result.status_code == 401:
                token = fetch_storefeeder_access_token(client.config)
                client.session.headers.update({"Authorization": f"Bearer {token}"})
            if result.status_code not in TRANSIENT_STATUSES or attempt == 3:
                break
            time.sleep(min(30, 5 * attempt))
        assert result is not None
        success_flags, errors = response_outcomes(result.status_code, result.response_json, len(group))
        success_rows, failure_rows = [], []
        for source, succeeded, error in zip(group, success_flags, errors):
            report = {
                "batch_number": batch_number, "status_code": result.status_code,
                "ProductID": source["ProductID"], "SKU": source["SKU"],
                "SupplierID": source["SupplierID"], "SupplierSKU": source["SupplierSKU"],
                "ExpectedQuantity": source["ExpectedQuantity"], "success": "yes" if succeeded else "no",
                "error": error,
            }
            (success_rows if succeeded else failure_rows).append(report)
        append_csv(success_path, success_rows)
        append_csv(failure_path, failure_rows)
        print(f"{kind} writes: batch {batch_number}; completed={len(completed) + min(len(records), batch_number * batch_size)}/{len(rows)}", flush=True)
        if failure_rows:
            break
    success = pd.read_csv(success_path, dtype=str, keep_default_na=False) if success_path.exists() else pd.DataFrame()
    failures = pd.read_csv(failure_path, dtype=str, keep_default_na=False) if failure_path.exists() else pd.DataFrame()
    if not failures.empty and not success.empty:
        failures = failures[~failures["ProductID"].astype(str).isin(set(success["ProductID"].astype(str)))].copy()
    return success, failures


def converge_inventory_differences(
    client: StoreFeederApiClient,
    rows: pd.DataFrame,
    run_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    success_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    raw_path = run_dir / "inventory_convergence_raw.jsonl"
    for row in rows.to_dict("records"):
        product_id = str(row["ProductID"])
        expected = int(row["ExpectedQuantity"])
        extra_writes = 0
        try:
            while True:
                wrapper = api_with_retry(
                    client, lambda pid=product_id: client.get_product(pid)
                )
                require_success(f"convergence product {product_id}", wrapper)
                product = wrapper.get("response", {})
                _, available = _inventory_values(product)
                locations = _stock_locations(product)
                signature = deduction_signature(locations, WAREHOUSE_ID)
                warehouse = [
                    location for location in locations
                    if _location_id(location) == WAREHOUSE_ID
                ]
                warehouse_physical = (
                    int(_number(warehouse[0].get("PhysicalStock")))
                    if len(warehouse) == 1 else None
                )

                # Allocation/pending-out ledgers can legitimately change while the
                # multi-hour reconciliation is running. If physical warehouse stock
                # has already converged to the supplier target, that is the authority
                # this job controls and the row is complete; do not fail only because
                # an order changed the deduction ledger after the preflight snapshot.
                if warehouse_physical == expected:
                    success_rows.append({
                        "ProductID": product_id,
                        "SKU": row["SKU"],
                        "ExpectedQuantity": expected,
                        "ExtraWrites": extra_writes,
                        "AvailableInventory": int(_number(available)),
                        "WarehousePhysical": warehouse_physical,
                        "Result": "PASS",
                    })
                    break

                if signature != str(row["DeductionSignature"]):
                    raise RuntimeError("ledger_deductions_changed_during_mirror")

                plan = build_physical_stock_mirror_plan(
                    target_warehouse_physical=expected,
                    current_available=int(_number(available)),
                    locations=locations,
                    warehouse_stock_location_id=WAREHOUSE_ID,
                )
                if extra_writes >= 2:
                    raise RuntimeError(
                        f"bounded_convergence_exhausted: physical={warehouse_physical}; target={expected}"
                    )
                item = inventory_item({
                    **row,
                    "TargetAdjustmentAmount": plan.target_adjustment_amount,
                })
                result = client.update_stock_location_inventory(
                    [item], batch_number=950 + extra_writes
                )
                append_jsonl(raw_path, {
                    "ProductID": product_id,
                    "attempt": extra_writes + 2,
                    "current_available": int(_number(available)),
                    "current_warehouse_physical": warehouse_physical,
                    "target_adjustment_amount": plan.target_adjustment_amount,
                    "predicted_warehouse_physical": plan.target_warehouse_physical,
                    "status_code": result.status_code,
                    "response": result.response_json,
                })
                if result.status_code >= 400 or result.failed or result.successful != 1:
                    raise RuntimeError(
                        f"bounded_convergence_write_failed: {result.response_json}"
                    )
                extra_writes += 1
                time.sleep(2)
        except Exception as exc:
            failure_rows.append({
                "ProductID": product_id,
                "SKU": row.get("SKU", ""),
                "ExpectedQuantity": expected,
                "ExtraWrites": extra_writes,
                "Result": "FAIL",
                "Reason": str(exc),
            })
    success = pd.DataFrame(success_rows)
    failures = pd.DataFrame(failure_rows)
    success.to_csv(run_dir / "inventory_convergence_successes.csv", index=False)
    failures.to_csv(run_dir / "inventory_convergence_failures.csv", index=False)
    return success, failures


def response_outcomes(status: int, response: dict[str, Any], count: int) -> tuple[list[bool], list[str]]:
    responses = response.get("Responses") if isinstance(response, dict) else None
    if isinstance(responses, list) and len(responses) == count:
        flags = [_truthy(item.get("Success")) if isinstance(item, dict) else False for item in responses]
        errors = [_response_error(item) if isinstance(item, dict) and not ok else "" for item, ok in zip(responses, flags)]
        return flags, errors
    success = 200 <= status < 300 and not _response_failed(response)
    return [success] * count, ["" if success else _response_error(response)] * count


def verify(actionable: pd.DataFrame, state: pd.DataFrame) -> pd.DataFrame:
    rows = actionable.merge(state, on="ProductID", how="left", suffixes=("", "_after"))
    output = []
    for row in rows.to_dict("records"):
        expected = int(row["ExpectedQuantity"])
        reasons = []
        if int_value(row.get("WarehousePhysical_after")) != expected:
            reasons.append("warehouse_physical_mismatch")
        # TotalAllocated/TotalPendingOut exist only on the post-write state frame,
        # so pandas does not suffix them during the merge. Using *_after here
        # silently treated real allocations as zero and caused false verification
        # failures whenever Unspecified carried allocated stock.
        expected_available = (
            expected
            - int_value(row.get("TotalAllocated"))
            - int_value(row.get("TotalPendingOut"))
        )
        if int_value(row.get("AvailableInventory_after")) != expected_available:
            reasons.append("available_inventory_ledger_mismatch")
        if str(row.get("LedgerError_after", "")).strip():
            reasons.append(str(row["LedgerError_after"]))
        output.append({
            "ProductID": row["ProductID"], "SKU": row["SKU"], "ExpectedQuantity": expected,
            "Inventory": row.get("Inventory_after", ""), "AvailableInventory": row.get("AvailableInventory_after", ""),
            "WarehousePhysical": row.get("WarehousePhysical_after", ""),
            "SupplierVerification": "successful_write_or_applied_checkpoint",
            "Result": "PASS" if not reasons else "FAIL", "Reason": "|".join(reasons),
        })
    return pd.DataFrame(output)


def verify_canary_suppliers(client: StoreFeederApiClient, verification: pd.DataFrame) -> pd.DataFrame:
    for index, row in verification.iterrows():
        wrapper = api_with_retry(client, lambda pid=str(row["ProductID"]): client.get_product_suppliers(pid))
        require_success(f"supplier verification {row['ProductID']}", wrapper)
        suppliers = _records_from_api_response(wrapper.get("response", {}))
        expected = int(row["ExpectedQuantity"])
        if len(suppliers) != 1 or int(_number(suppliers[0].get("SupplierStockLevel"))) != expected:
            verification.at[index, "Result"] = "FAIL"
            verification.at[index, "Reason"] = append_reason(verification.at[index, "Reason"], "supplier_verification_failed")
        else:
            verification.at[index, "SupplierVerification"] = "live_GET_exact"
    return verification


def update_applied_supplier_state(baseline: pd.DataFrame, actionable: pd.DataFrame, successes: pd.DataFrame) -> pd.DataFrame:
    output = baseline.copy()
    if not successes.empty:
        success_ids = set(successes["ProductID"].astype(str))
        updated = actionable[actionable["ProductID"].astype(str).isin(success_ids)][
            ["ProductID", "SupplierID", "SupplierSKU", "ExpectedQuantity"]
        ].rename(columns={"ExpectedQuantity": "AppliedSupplierStockLevel"})
        output = pd.concat([output, updated], ignore_index=True).drop_duplicates("ProductID", keep="last")
    return output


def api_with_retry(client: StoreFeederApiClient, call: Any, attempts: int = 6) -> dict[str, Any]:
    last = {}
    for attempt in range(1, attempts + 1):
        last = call()
        status = int(last.get("_status_code") or 0)
        if 200 <= status < 300:
            return last
        if status == 401:
            token = fetch_storefeeder_access_token(client.config)
            client.session.headers.update({"Authorization": f"Bearer {token}"})
        if status not in TRANSIENT_STATUSES or attempt == attempts:
            return last
        time.sleep(min(60, 5 * attempt))
    return last


def require_success(action: str, wrapper: dict[str, Any]) -> None:
    status = int(wrapper.get("_status_code") or 0)
    if not 200 <= status < 300:
        raise RuntimeError(f"{action} failed HTTP {status}: {wrapper.get('response')}")


def int_value(value: Any) -> int:
    return int(_number(value))


def append_reason(existing: Any, reason: str) -> str:
    return reason if not str(existing).strip() else f"{str(existing).strip()}|{reason}"


def append_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows)
    if columns:
        frame = frame.reindex(columns=columns, fill_value="")
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def write_status(run_dir: Path, status: str, *, live: bool, **details: Any) -> None:
    write_json(run_dir / "run_status.json", {
        "status": status, "live": live, "updated_at": datetime.now().isoformat(), **details,
    })


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise
