from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from .stock_mapping import (
    FullStoreFeederStockResult,
    StockMappingResult,
    StrictStockSafetyConfig,
    StrictStoreFeederStockResult,
    SupplierStockFileInfo,
    build_full_storefeeder_stock_import,
    build_stock_mapping_result,
    build_strict_storefeeder_stock_import,
    build_supplier_stock_lookup,
)
from .stock_rules import StockRuleConfig


@dataclass(frozen=True)
class StoreFeederStockExportFiles:
    stock_update_csv: Path
    stock_update_xlsx: Path
    mapped_stock_updates: Path
    missing_supplier_skus: Path
    zero_stock_skus: Path
    unmapped_storefeeder_skus: Path
    duplicate_storefeeder_skus: Path
    validation_summary: Path


@dataclass(frozen=True)
class FullStoreFeederStockExportFiles:
    full_import_xlsx: Path
    updated_stock_rows: Path
    skipped_rows: Path
    missing_supplier_skus: Path
    zero_stock_rows: Path
    validation_summary: Path


@dataclass(frozen=True)
class StrictStoreFeederStockExportFiles:
    stock_updates_ready_csv: Path
    stock_updates_ready_xlsx: Path
    quarantine_review: Path
    zero_stock_updates: Path
    missing_supplier_skus: Path
    skipped_not_in_supplier_mapping: Path
    mapping_skus_missing_from_storefeeder_export: Path
    validation_summary: Path
    api_payload_preview: Path


def read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="latin1")


def read_storefeeder_export(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv(path)
    if suffix == ".xlsx":
        return pd.read_excel(path, dtype=str, keep_default_na=False, engine="openpyxl")
    if suffix == ".xls":
        tables = pd.read_html(path, keep_default_na=False)
        if not tables:
            raise ValueError("No table was found in the StoreFeeder .xls file.")
        return tables[0].fillna("").astype(str)
    raise ValueError("StoreFeeder export must be .csv, .xls, or .xlsx")




def _stock_file_info(supplier: str, path: Path | None, max_age_hours: float) -> SupplierStockFileInfo | None:
    if path is None:
        return None
    age_hours = None
    stale = False
    if path.exists():
        age_seconds = datetime.now().timestamp() - path.stat().st_mtime
        age_hours = age_seconds / 3600
        stale = age_hours > max_age_hours
    return SupplierStockFileInfo(supplier=supplier, path=str(path), age_hours=age_hours, stale=stale)


def build_strict_storefeeder_stock_update(
    storefeeder_export_path: Path,
    supplier_mapping_path: Path,
    ralawise_stock_path: Path | None = None,
    uneek_stock_path: Path | None = None,
    *,
    buffer: int = 0,
    max_stock: int = 5,
    missing_as_zero: bool = False,
    allow_quarantine_update: bool = False,
    max_quarantine_rate: float = 0.02,
    max_stock_file_age_hours: float = 24.0,
    live_mode: bool = False,
    allow_unusual_update_count: bool = False,
    previous_update_ready_count: int | None = None,
) -> StrictStoreFeederStockResult:
    storefeeder_export = read_storefeeder_export(storefeeder_export_path)
    supplier_mapping = read_csv(supplier_mapping_path)
    ralawise_stock = read_csv(ralawise_stock_path) if ralawise_stock_path else None
    uneek_stock = read_csv(uneek_stock_path) if uneek_stock_path else None
    file_infos = [
        info for info in [
            _stock_file_info("Ralawise", ralawise_stock_path, max_stock_file_age_hours),
            _stock_file_info("Uneek", uneek_stock_path, max_stock_file_age_hours),
        ]
        if info is not None
    ]
    config = StrictStockSafetyConfig(
        buffer=buffer,
        max_stock=max_stock,
        missing_as_zero=missing_as_zero,
        allow_quarantine_update=allow_quarantine_update,
        max_quarantine_rate=max_quarantine_rate,
        max_stock_file_age_hours=max_stock_file_age_hours,
        live_mode=live_mode,
        allow_unusual_update_count=allow_unusual_update_count,
        previous_update_ready_count=previous_update_ready_count,
    )
    return build_strict_storefeeder_stock_import(
        storefeeder_export,
        supplier_mapping,
        ralawise_stock,
        uneek_stock,
        config,
        stock_file_infos=file_infos,
    )

def build_storefeeder_stock_update(
    supplier_mapping_path: Path,
    ralawise_stock_path: Path | None = None,
    uneek_stock_path: Path | None = None,
    *,
    buffer: int = 0,
    max_stock: int = 5,
    update_missing_as_zero: bool = False,
) -> StockMappingResult:
    supplier_mapping = read_csv(supplier_mapping_path)
    ralawise_stock = read_csv(ralawise_stock_path) if ralawise_stock_path else None
    uneek_stock = read_csv(uneek_stock_path) if uneek_stock_path else None
    supplier_stock = build_supplier_stock_lookup(ralawise_stock, uneek_stock)
    config = StockRuleConfig(buffer=buffer, max_stock=max_stock, update_missing_as_zero=update_missing_as_zero)
    return build_stock_mapping_result(supplier_mapping, supplier_stock, config)


def build_full_storefeeder_stock_update(
    storefeeder_export_path: Path,
    supplier_mapping_path: Path,
    ralawise_stock_path: Path | None = None,
    uneek_stock_path: Path | None = None,
    *,
    buffer: int = 0,
    max_stock: int = 5,
    update_missing_as_zero: bool = False,
    update_supplier_location_fields: bool = False,
) -> FullStoreFeederStockResult:
    storefeeder_export = read_storefeeder_export(storefeeder_export_path)
    supplier_mapping = read_csv(supplier_mapping_path)
    ralawise_stock = read_csv(ralawise_stock_path) if ralawise_stock_path else None
    uneek_stock = read_csv(uneek_stock_path) if uneek_stock_path else None
    supplier_stock = build_supplier_stock_lookup(ralawise_stock, uneek_stock)
    config = StockRuleConfig(buffer=buffer, max_stock=max_stock, update_missing_as_zero=update_missing_as_zero)
    return build_full_storefeeder_stock_import(
        storefeeder_export,
        supplier_mapping,
        supplier_stock,
        config,
        update_supplier_location_fields=update_supplier_location_fields,
    )


def write_storefeeder_stock_export(
    result: StockMappingResult,
    out_dir: Path,
    *,
    timestamp: str | None = None,
) -> StoreFeederStockExportFiles:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")

    files = StoreFeederStockExportFiles(
        stock_update_csv=out_dir / f"storefeeder_stock_update_{stamp}.csv",
        stock_update_xlsx=out_dir / f"storefeeder_stock_update_{stamp}.xlsx",
        mapped_stock_updates=out_dir / f"storefeeder_mapped_stock_updates_{stamp}.csv",
        missing_supplier_skus=out_dir / f"storefeeder_missing_supplier_skus_{stamp}.csv",
        zero_stock_skus=out_dir / f"storefeeder_zero_stock_skus_{stamp}.csv",
        unmapped_storefeeder_skus=out_dir / f"storefeeder_unmapped_storefeeder_skus_{stamp}.csv",
        duplicate_storefeeder_skus=out_dir / f"storefeeder_duplicate_storefeeder_skus_{stamp}.csv",
        validation_summary=out_dir / f"storefeeder_stock_validation_summary_{stamp}.csv",
    )

    result.stock_update.to_csv(files.stock_update_csv, index=False)
    with pd.ExcelWriter(files.stock_update_xlsx, engine="openpyxl") as writer:
        result.stock_update.to_excel(writer, index=False, sheet_name="StoreFeeder Stock Update")

    result.mapped_stock_updates.to_csv(files.mapped_stock_updates, index=False)
    result.missing_supplier_skus.to_csv(files.missing_supplier_skus, index=False)
    result.zero_stock_skus.to_csv(files.zero_stock_skus, index=False)
    result.unmapped_storefeeder_skus.to_csv(files.unmapped_storefeeder_skus, index=False)
    result.duplicate_storefeeder_skus.to_csv(files.duplicate_storefeeder_skus, index=False)
    result.validation_summary.to_csv(files.validation_summary, index=False)
    return files


def write_full_storefeeder_stock_export(
    result: FullStoreFeederStockResult,
    out_dir: Path,
    *,
    timestamp: str | None = None,
) -> FullStoreFeederStockExportFiles:
    if not result.safety_passed:
        raise RuntimeError(
            "Blocked full StoreFeeder export because unexpected columns changed: "
            + ", ".join(result.unexpected_changed_columns)
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    files = FullStoreFeederStockExportFiles(
        full_import_xlsx=out_dir / f"storefeeder_full_stock_import_{stamp}.xlsx",
        updated_stock_rows=out_dir / f"storefeeder_updated_stock_rows_{stamp}.csv",
        skipped_rows=out_dir / f"storefeeder_skipped_stock_rows_{stamp}.csv",
        missing_supplier_skus=out_dir / f"storefeeder_missing_supplier_skus_{stamp}.csv",
        zero_stock_rows=out_dir / f"storefeeder_zero_stock_rows_{stamp}.csv",
        validation_summary=out_dir / f"storefeeder_full_stock_validation_summary_{stamp}.csv",
    )

    with pd.ExcelWriter(files.full_import_xlsx, engine="openpyxl") as writer:
        result.updated_export.to_excel(writer, index=False, sheet_name="StoreFeeder Import")
    result.updated_stock_rows.to_csv(files.updated_stock_rows, index=False)
    result.skipped_rows.to_csv(files.skipped_rows, index=False)
    result.missing_supplier_skus.to_csv(files.missing_supplier_skus, index=False)
    result.zero_stock_skus.to_csv(files.zero_stock_rows, index=False)
    result.validation_summary.to_csv(files.validation_summary, index=False)
    return files


def write_strict_storefeeder_stock_export(
    result: StrictStoreFeederStockResult,
    out_dir: Path,
    *,
    write_ready_files: bool = True,
) -> StrictStoreFeederStockExportFiles:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = StrictStoreFeederStockExportFiles(
        stock_updates_ready_csv=out_dir / "storefeeder_stock_updates_ready.csv",
        stock_updates_ready_xlsx=out_dir / "storefeeder_stock_updates_ready.xlsx",
        quarantine_review=out_dir / "quarantine_review.csv",
        zero_stock_updates=out_dir / "zero_stock_updates.csv",
        missing_supplier_skus=out_dir / "missing_supplier_skus.csv",
        skipped_not_in_supplier_mapping=out_dir / "skipped_not_in_supplier_mapping.csv",
        mapping_skus_missing_from_storefeeder_export=out_dir / "mapping_skus_missing_from_storefeeder_export.csv",
        validation_summary=out_dir / "validation_summary.csv",
        api_payload_preview=out_dir / "api_payload_preview.csv",
    )

    if write_ready_files:
        if not result.safety_passed:
            raise RuntimeError(
                "Blocked ready stock export because unexpected columns changed: "
                + "|".join(result.blocked_reasons)
            )
        result.ready_export.to_csv(files.stock_updates_ready_csv, index=False)
        with pd.ExcelWriter(files.stock_updates_ready_xlsx, engine="openpyxl") as writer:
            result.ready_export.to_excel(writer, index=False, sheet_name="StoreFeeder Stock Updates Ready")
        result.api_payload_preview.to_csv(files.api_payload_preview, index=False)

    result.quarantine_review.to_csv(files.quarantine_review, index=False)
    result.zero_stock_updates.to_csv(files.zero_stock_updates, index=False)
    result.missing_supplier_skus.to_csv(files.missing_supplier_skus, index=False)
    result.skipped_not_in_supplier_mapping.to_csv(files.skipped_not_in_supplier_mapping, index=False)
    result.mapping_skus_missing_from_storefeeder_export.to_csv(files.mapping_skus_missing_from_storefeeder_export, index=False)
    result.validation_summary.to_csv(files.validation_summary, index=False)

    if not write_ready_files:
        result.api_payload_preview.to_csv(files.api_payload_preview, index=False)
    return files

