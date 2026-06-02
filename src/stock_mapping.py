from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .stock_rules import StockRuleConfig, calculate_safe_stock, coerce_non_negative_int


REQUIRED_MAPPING_COLUMNS = ["storefeeder_sku", "supplier", "supplier_sku", "stock_location"]
MAPPED_REPORT_COLUMNS = [
    "storefeeder_sku",
    "supplier",
    "supplier_sku",
    "stock_location",
    "supplier_free_stock",
    "safe_stock",
]
MISSING_REPORT_COLUMNS = ["storefeeder_sku", "supplier", "supplier_sku", "stock_location"]
ZERO_REPORT_COLUMNS = MAPPED_REPORT_COLUMNS
DUPLICATE_REPORT_COLUMNS = ["storefeeder_sku", "mapping_rows"]
UNMAPPED_STOREFEEDER_COLUMNS = ["storefeeder_sku", "reason"]
SUMMARY_COLUMNS = ["metric", "value"]

FULL_UPDATE_REQUIRED_COLUMNS = ["SKU", "Stock Locations", "Stock Location Current Inventories"]
FULL_UPDATED_REPORT_COLUMNS = [
    "SKU",
    "supplier",
    "supplier_sku",
    "stock_location",
    "supplier_free_stock",
    "safe_stock",
    "old_Stock Location Current Inventories",
    "new_Stock Location Current Inventories",
]
SKIPPED_REPORT_COLUMNS = ["SKU", "supplier", "supplier_sku", "stock_location", "reason"]
ALLOWED_FULL_EXPORT_CHANGED_COLUMNS = ["Stock Location Current Inventories"]


@dataclass(frozen=True)
class FullStoreFeederStockResult:
    updated_export: pd.DataFrame
    updated_stock_rows: pd.DataFrame
    skipped_rows: pd.DataFrame
    missing_supplier_skus: pd.DataFrame
    zero_stock_skus: pd.DataFrame
    validation_summary: pd.DataFrame
    safety_passed: bool
    unexpected_changed_columns: list[str]


@dataclass(frozen=True)
class StockMappingResult:
    stock_update: pd.DataFrame
    mapped_stock_updates: pd.DataFrame
    missing_supplier_skus: pd.DataFrame
    zero_stock_skus: pd.DataFrame
    unmapped_storefeeder_skus: pd.DataFrame
    duplicate_storefeeder_skus: pd.DataFrame
    validation_summary: pd.DataFrame


def normalize_supplier_mapping(mapping_df: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in REQUIRED_MAPPING_COLUMNS if column not in mapping_df.columns]
    if missing:
        raise ValueError("supplier_mapping.csv missing required columns: " + ", ".join(missing))

    normalized = mapping_df[REQUIRED_MAPPING_COLUMNS].copy()
    for column in REQUIRED_MAPPING_COLUMNS:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()

    return normalized[normalized["storefeeder_sku"].ne("")]


def normalize_ralawise_stock(stock_df: pd.DataFrame | None) -> pd.DataFrame:
    if stock_df is None or stock_df.empty:
        return _empty_stock_frame()
    if "SKU" not in stock_df.columns or "free" not in stock_df.columns:
        raise ValueError("Ralawise stock CSV must contain SKU and free columns.")

    normalized = stock_df[["SKU", "free"]].copy()
    normalized.columns = ["supplier_sku", "supplier_free_stock"]
    normalized["supplier"] = "Ralawise"
    return _finalize_stock_frame(normalized)


def normalize_uneek_stock(stock_df: pd.DataFrame | None) -> pd.DataFrame:
    if stock_df is None or stock_df.empty:
        return _empty_stock_frame()

    sku_col = _first_existing_column(stock_df, ["ItemNo", "ProductCode", "SKU", "sku"])
    stock_col = _first_existing_column(stock_df, ["Stock", "StockLevel", "LiveStock", "free"])
    if not sku_col or not stock_col:
        raise ValueError("Uneek stock CSV/API output must contain an item SKU column and a stock column.")

    normalized = stock_df[[sku_col, stock_col]].copy()
    normalized.columns = ["supplier_sku", "supplier_free_stock"]
    normalized["supplier"] = "Uneek"
    return _finalize_stock_frame(normalized)


def build_supplier_stock_lookup(
    ralawise_stock: pd.DataFrame | None = None,
    uneek_stock: pd.DataFrame | None = None,
) -> pd.DataFrame:
    stock = pd.concat(
        [normalize_ralawise_stock(ralawise_stock), normalize_uneek_stock(uneek_stock)],
        ignore_index=True,
    )
    if stock.empty:
        return _empty_stock_frame()
    return stock.drop_duplicates(subset=["supplier", "supplier_sku"], keep="last").reset_index(drop=True)


def build_stock_mapping_result(
    supplier_mapping_df: pd.DataFrame,
    supplier_stock_df: pd.DataFrame,
    config: StockRuleConfig,
) -> StockMappingResult:
    mapping = normalize_supplier_mapping(supplier_mapping_df)
    stock = supplier_stock_df.copy()
    if stock.empty:
        stock = _empty_stock_frame()

    merged = mapping.merge(stock, how="left", on=["supplier", "supplier_sku"])
    missing_mask = merged["supplier_free_stock"].isna()

    missing_supplier_skus = merged.loc[missing_mask, REQUIRED_MAPPING_COLUMNS].copy()
    rows = []
    zero_rows = []

    for _, row in merged.iterrows():
        safe_stock = calculate_safe_stock(row.get("supplier_free_stock"), config)
        if safe_stock is None:
            continue

        mapped_row = {
            "storefeeder_sku": row["storefeeder_sku"],
            "supplier": row["supplier"],
            "supplier_sku": row["supplier_sku"],
            "stock_location": row["stock_location"],
            "supplier_free_stock": "" if pd.isna(row.get("supplier_free_stock")) else coerce_non_negative_int(row.get("supplier_free_stock")),
            "safe_stock": safe_stock,
        }
        rows.append(mapped_row)
        if safe_stock == 0:
            zero_rows.append(mapped_row)

    mapped_stock_updates = pd.DataFrame(rows, columns=MAPPED_REPORT_COLUMNS)
    zero_stock_skus = pd.DataFrame(zero_rows, columns=ZERO_REPORT_COLUMNS)
    stock_update = _build_storefeeder_stock_update(mapped_stock_updates)
    update_skus = set(stock_update["SKU"]) if not stock_update.empty else set()
    mapped_skus = set(mapping["storefeeder_sku"])
    unmapped_storefeeder_skus = pd.DataFrame(
        [
            {"storefeeder_sku": sku, "reason": "no_exportable_supplier_stock"}
            for sku in sorted(mapped_skus - update_skus)
        ],
        columns=UNMAPPED_STOREFEEDER_COLUMNS,
    )

    duplicate_storefeeder_skus = (
        mapping.groupby("storefeeder_sku", as_index=False)
        .size()
        .rename(columns={"size": "mapping_rows"})
    )
    duplicate_storefeeder_skus = duplicate_storefeeder_skus[
        duplicate_storefeeder_skus["mapping_rows"] > 1
    ].reset_index(drop=True)

    validation_summary = pd.DataFrame(
        [
            {"metric": "mapping_rows", "value": int(len(mapping))},
            {"metric": "supplier_stock_rows", "value": int(len(stock))},
            {"metric": "storefeeder_skus_in_mapping", "value": int(mapping["storefeeder_sku"].nunique())},
            {"metric": "stock_update_rows", "value": int(len(stock_update))},
            {"metric": "missing_supplier_sku_rows", "value": int(len(missing_supplier_skus))},
            {"metric": "zero_stock_rows", "value": int(len(zero_stock_skus))},
            {"metric": "unmapped_storefeeder_skus", "value": int(len(unmapped_storefeeder_skus))},
            {"metric": "duplicate_storefeeder_skus", "value": int(len(duplicate_storefeeder_skus))},
            {"metric": "buffer", "value": int(config.buffer)},
            {"metric": "max_stock", "value": int(config.max_stock)},
            {"metric": "update_missing_as_zero", "value": bool(config.update_missing_as_zero)},
        ],
        columns=SUMMARY_COLUMNS,
    )

    return StockMappingResult(
        stock_update=stock_update,
        mapped_stock_updates=mapped_stock_updates,
        missing_supplier_skus=missing_supplier_skus.reset_index(drop=True),
        zero_stock_skus=zero_stock_skus.reset_index(drop=True),
        unmapped_storefeeder_skus=unmapped_storefeeder_skus,
        duplicate_storefeeder_skus=duplicate_storefeeder_skus,
        validation_summary=validation_summary,
    )


def _build_storefeeder_stock_update(mapped_stock_updates: pd.DataFrame) -> pd.DataFrame:
    if mapped_stock_updates.empty:
        return pd.DataFrame(columns=["SKU", "Stock Locations", "Stock Location Current Inventories"])

    grouped = (
        mapped_stock_updates.groupby("storefeeder_sku", sort=False)
        .agg(
            **{
                "Stock Locations": ("stock_location", _pipe_join),
                "Stock Location Current Inventories": ("safe_stock", _pipe_join),
            }
        )
        .reset_index()
        .rename(columns={"storefeeder_sku": "SKU"})
    )
    return grouped[["SKU", "Stock Locations", "Stock Location Current Inventories"]]


def _pipe_join(values: pd.Series) -> str:
    return "|".join(str(value).strip() for value in values)


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lookup = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    return None


def _empty_stock_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["supplier", "supplier_sku", "supplier_free_stock"])


def _finalize_stock_frame(df: pd.DataFrame) -> pd.DataFrame:
    finalized = df[["supplier", "supplier_sku", "supplier_free_stock"]].copy()
    finalized["supplier"] = finalized["supplier"].fillna("").astype(str).str.strip()
    finalized["supplier_sku"] = finalized["supplier_sku"].fillna("").astype(str).str.strip().str.upper()
    finalized["supplier_free_stock"] = finalized["supplier_free_stock"].map(coerce_non_negative_int)
    return finalized[finalized["supplier_sku"].ne("")].reset_index(drop=True)


def build_full_storefeeder_stock_import(
    storefeeder_export_df: pd.DataFrame,
    supplier_mapping_df: pd.DataFrame,
    supplier_stock_df: pd.DataFrame,
    config: StockRuleConfig,
    *,
    update_supplier_location_fields: bool = False,
) -> FullStoreFeederStockResult:
    if update_supplier_location_fields:
        raise NotImplementedError("Updating Suppliers, Supplier SKUs, or Stock Locations is intentionally disabled in this builder.")

    missing_export_columns = [column for column in FULL_UPDATE_REQUIRED_COLUMNS if column not in storefeeder_export_df.columns]
    if missing_export_columns:
        raise ValueError("StoreFeeder export missing required columns: " + ", ".join(missing_export_columns))

    original = storefeeder_export_df.copy()
    updated = storefeeder_export_df.copy()
    mapping = normalize_supplier_mapping(supplier_mapping_df)
    stock = supplier_stock_df.copy() if supplier_stock_df is not None else _empty_stock_frame()
    if stock.empty:
        stock = _empty_stock_frame()

    merged = mapping.merge(stock, how="left", on=["supplier", "supplier_sku"])
    storefeeder_skus = set(updated["SKU"].fillna("").astype(str).str.strip())
    updated_index_by_sku = {
        str(row["SKU"]).strip(): index
        for index, row in updated.iterrows()
        if str(row["SKU"]).strip()
    }

    updated_rows: list[dict[str, object]] = []
    skipped_rows: list[dict[str, str]] = []
    missing_rows: list[dict[str, str]] = []
    zero_rows: list[dict[str, object]] = []

    for _, row in merged.iterrows():
        storefeeder_sku = str(row["storefeeder_sku"]).strip()
        supplier = str(row["supplier"]).strip()
        supplier_sku = str(row["supplier_sku"]).strip()
        stock_location = str(row["stock_location"]).strip()

        if storefeeder_sku not in storefeeder_skus:
            skipped_rows.append(_skipped_row(storefeeder_sku, supplier, supplier_sku, stock_location, "storefeeder_sku_not_in_export"))
            continue

        if pd.isna(row.get("supplier_free_stock")):
            missing_rows.append(
                {
                    "storefeeder_sku": storefeeder_sku,
                    "supplier": supplier,
                    "supplier_sku": supplier_sku,
                    "stock_location": stock_location,
                }
            )

        safe_stock = calculate_safe_stock(row.get("supplier_free_stock"), config)
        if safe_stock is None:
            skipped_rows.append(_skipped_row(storefeeder_sku, supplier, supplier_sku, stock_location, "missing_supplier_stock"))
            continue

        export_index = updated_index_by_sku[storefeeder_sku]
        locations = _split_pipe_values(updated.at[export_index, "Stock Locations"])
        inventories = _split_pipe_values(updated.at[export_index, "Stock Location Current Inventories"])
        inventories = _pad_inventory_values(inventories, len(locations))

        location_index = _find_location_index(locations, stock_location)
        if location_index is None:
            skipped_rows.append(_skipped_row(storefeeder_sku, supplier, supplier_sku, stock_location, "stock_location_not_in_storefeeder_export"))
            continue

        old_inventory_text = updated.at[export_index, "Stock Location Current Inventories"]
        inventories[location_index] = str(int(safe_stock))
        new_inventory_text = "|".join(inventories)
        updated.at[export_index, "Stock Location Current Inventories"] = new_inventory_text

        report_row = {
            "SKU": storefeeder_sku,
            "supplier": supplier,
            "supplier_sku": supplier_sku,
            "stock_location": stock_location,
            "supplier_free_stock": "" if pd.isna(row.get("supplier_free_stock")) else coerce_non_negative_int(row.get("supplier_free_stock")),
            "safe_stock": int(safe_stock),
            "old_Stock Location Current Inventories": str(old_inventory_text),
            "new_Stock Location Current Inventories": new_inventory_text,
        }
        updated_rows.append(report_row)
        if int(safe_stock) == 0:
            zero_rows.append(report_row)

    updated_stock_rows = pd.DataFrame(updated_rows, columns=FULL_UPDATED_REPORT_COLUMNS)
    skipped_report = pd.DataFrame(skipped_rows, columns=SKIPPED_REPORT_COLUMNS)
    missing_supplier_skus = pd.DataFrame(missing_rows, columns=MISSING_REPORT_COLUMNS).drop_duplicates(ignore_index=True)
    zero_stock_skus = pd.DataFrame(zero_rows, columns=FULL_UPDATED_REPORT_COLUMNS)
    safety_passed, unexpected_changed_columns = _validate_full_export_safety(original, updated)
    changed_row_count = int(
        original["Stock Location Current Inventories"].fillna("").astype(str).ne(
            updated["Stock Location Current Inventories"].fillna("").astype(str)
        ).sum()
    )

    validation_summary = pd.DataFrame(
        [
            {"metric": "storefeeder_export_rows", "value": int(len(original))},
            {"metric": "mapping_rows", "value": int(len(mapping))},
            {"metric": "supplier_stock_rows", "value": int(len(stock))},
            {"metric": "updated_stock_rows", "value": int(len(updated_stock_rows))},
            {"metric": "changed_storefeeder_rows", "value": changed_row_count},
            {"metric": "skipped_rows", "value": int(len(skipped_report))},
            {"metric": "missing_supplier_sku_rows", "value": int(len(missing_supplier_skus))},
            {"metric": "zero_stock_rows", "value": int(len(zero_stock_skus))},
            {"metric": "buffer", "value": int(config.buffer)},
            {"metric": "max_stock", "value": int(config.max_stock)},
            {"metric": "update_missing_as_zero", "value": bool(config.update_missing_as_zero)},
            {"metric": "safety_passed", "value": bool(safety_passed)},
            {"metric": "unexpected_changed_columns", "value": "|".join(unexpected_changed_columns)},
        ],
        columns=SUMMARY_COLUMNS,
    )

    return FullStoreFeederStockResult(
        updated_export=updated,
        updated_stock_rows=updated_stock_rows,
        skipped_rows=skipped_report,
        missing_supplier_skus=missing_supplier_skus,
        zero_stock_skus=zero_stock_skus,
        validation_summary=validation_summary,
        safety_passed=safety_passed,
        unexpected_changed_columns=unexpected_changed_columns,
    )


def _split_pipe_values(value) -> list[str]:
    text = "" if pd.isna(value) else str(value)
    if text == "":
        return []
    return [part.strip() for part in text.split("|")]


def _pad_inventory_values(values: list[str], target_length: int) -> list[str]:
    padded = list(values)
    while len(padded) < target_length:
        padded.append("0")
    return padded[:target_length]


def _find_location_index(locations: list[str], stock_location: str) -> int | None:
    wanted = stock_location.strip().casefold()
    for index, location in enumerate(locations):
        if location.strip().casefold() == wanted:
            return index
    return None


def _skipped_row(storefeeder_sku: str, supplier: str, supplier_sku: str, stock_location: str, reason: str) -> dict[str, str]:
    return {
        "SKU": storefeeder_sku,
        "supplier": supplier,
        "supplier_sku": supplier_sku,
        "stock_location": stock_location,
        "reason": reason,
    }


def _validate_full_export_safety(original: pd.DataFrame, updated: pd.DataFrame) -> tuple[bool, list[str]]:
    if list(original.columns) != list(updated.columns):
        return False, ["column_order_or_columns_changed"]

    unexpected = []
    for column in original.columns:
        left = original[column].fillna("").astype(str).reset_index(drop=True)
        right = updated[column].fillna("").astype(str).reset_index(drop=True)
        if not left.equals(right) and column not in ALLOWED_FULL_EXPORT_CHANGED_COLUMNS:
            unexpected.append(column)
    return len(unexpected) == 0, unexpected


VALIDATION_CATEGORIES = [
    "missing_supplier_mapping",
    "missing_supplier_sku_in_stock_feed",
    "duplicate_storefeeder_sku",
    "duplicate_supplier_mapping_conflict",
    "invalid_supplier_name",
    "stock_location_supplier_mismatch",
    "invalid_stock_quantity",
    "negative_stock_quantity",
    "colour_size_validation_failed",
    "ambiguous_match",
    "stale_supplier_stock_file",
    "discontinued_supplier_sku",
    "unknown_error",
]
VALID_SUPPLIERS = {"RALAWISE", "UNEEK"}
STRICT_STATUS_UPDATE_READY = "update_ready"
STRICT_STATUS_QUARANTINED = "quarantined"
STRICT_STATUS_SKIPPED = "skipped"
STRICT_ALLOWED_CHANGED_COLUMNS = ["Stock Location Current Inventories"]
STRICT_REPORT_STATUS_COLUMN = "confidence_status"


@dataclass(frozen=True)
class SupplierStockFileInfo:
    supplier: str
    path: str
    age_hours: float | None
    stale: bool


@dataclass(frozen=True)
class StrictStockSafetyConfig:
    buffer: int = 0
    max_stock: int = 5
    missing_as_zero: bool = False
    allow_quarantine_update: bool = False
    max_quarantine_rate: float = 0.02
    max_stock_file_age_hours: float = 24.0
    live_mode: bool = False
    allow_unusual_update_count: bool = False
    previous_update_ready_count: int | None = None
    low_update_count_ratio: float = 0.5
    high_update_count_ratio: float = 1.5


@dataclass(frozen=True)
class StrictStoreFeederStockResult:
    ready_export: pd.DataFrame
    quarantine_review: pd.DataFrame
    zero_stock_updates: pd.DataFrame
    missing_supplier_skus: pd.DataFrame
    skipped_not_in_supplier_mapping: pd.DataFrame
    mapping_skus_missing_from_storefeeder_export: pd.DataFrame
    validation_summary: pd.DataFrame
    api_payload_preview: pd.DataFrame
    safety_passed: bool
    live_update_allowed: bool
    blocked_reasons: list[str]


def build_strict_storefeeder_stock_import(
    storefeeder_export_df: pd.DataFrame,
    supplier_mapping_df: pd.DataFrame,
    ralawise_stock_df: pd.DataFrame | None,
    uneek_stock_df: pd.DataFrame | None,
    config: StrictStockSafetyConfig,
    *,
    stock_file_infos: list[SupplierStockFileInfo] | None = None,
) -> StrictStoreFeederStockResult:
    missing_export_columns = [column for column in FULL_UPDATE_REQUIRED_COLUMNS if column not in storefeeder_export_df.columns]
    if missing_export_columns:
        raise ValueError("StoreFeeder export missing required columns: " + ", ".join(missing_export_columns))

    original = storefeeder_export_df.copy()
    mapping = normalize_supplier_mapping(supplier_mapping_df)
    stock = _build_strict_supplier_stock_lookup(ralawise_stock_df, uneek_stock_df)
    stock_file_infos = stock_file_infos or []
    stale_suppliers = {info.supplier.casefold() for info in stock_file_infos if info.stale}

    quarantine_rows: list[dict[str, object]] = []
    ready_rows: list[dict[str, object]] = []
    zero_rows: list[dict[str, object]] = []
    missing_rows: list[dict[str, object]] = []
    skipped_not_in_mapping_rows: list[dict[str, object]] = []
    api_rows: list[dict[str, object]] = []

    export_skus = original["SKU"].fillna("").astype(str).str.strip()
    export_sku_counts = export_skus.value_counts()
    mapping_counts = mapping["storefeeder_sku"].value_counts()
    mapping_conflicts = _duplicate_mapping_conflicts(mapping)
    export_sku_set = {sku for sku in export_skus if sku}

    mapping_by_sku = {sku: group.copy() for sku, group in mapping.groupby("storefeeder_sku", sort=False)}
    mapping_skus_missing_rows = [
        {
            "storefeeder_sku": str(row["storefeeder_sku"]),
            "supplier": str(row["supplier"]),
            "supplier_sku": str(row["supplier_sku"]),
            "stock_location": str(row["stock_location"]),
            "reason": "Mapping SKU was not found in StoreFeeder export",
        }
        for _, row in mapping.iterrows()
        if str(row["storefeeder_sku"]) not in export_sku_set
    ]
    update_candidate_product_skus = set(mapping_by_sku).intersection(export_sku_set)
    update_candidate_mapping_rows = int(mapping["storefeeder_sku"].isin(export_sku_set).sum())
    stock_by_key = {
        (str(row["supplier"]).casefold(), str(row["supplier_sku"]).casefold()): row
        for _, row in stock.iterrows()
    }

    for export_index, product in original.iterrows():
        sku = str(product.get("SKU", "")).strip()
        product_quarantine = []
        if not sku:
            skipped_not_in_mapping_rows.append(_skipped_row("", "", "", "", "Blank StoreFeeder SKU is not in supplier_mapping.csv"))
            continue

        if sku not in mapping_by_sku:
            skipped_not_in_mapping_rows.append(_skipped_row(sku, "", "", "", "StoreFeeder SKU is not in supplier_mapping.csv"))
            continue

        if export_sku_counts.get(sku, 0) > 1:
            product_quarantine.append(_quarantine_row(product, sku, "", "", "", "duplicate_storefeeder_sku", "SKU appears more than once in StoreFeeder export"))

        if mapping_counts.get(sku, 0) > 1 and sku in mapping_conflicts:
            product_quarantine.append(_quarantine_row(product, sku, "", "", "", "duplicate_supplier_mapping_conflict", mapping_conflicts[sku]))

        product_update = product.copy()
        locations = _split_pipe_values(product_update["Stock Locations"])
        inventories = _pad_inventory_values(_split_pipe_values(product_update["Stock Location Current Inventories"]), len(locations))
        sku_ready_rows = []

        for _, map_row in mapping_by_sku[sku].iterrows():
            supplier = str(map_row["supplier"]).strip()
            supplier_sku = str(map_row["supplier_sku"]).strip()
            stock_location = str(map_row["stock_location"]).strip()
            supplier_key = supplier.casefold()

            if supplier.upper() not in VALID_SUPPLIERS:
                product_quarantine.append(_quarantine_row(product, sku, supplier, supplier_sku, stock_location, "invalid_supplier_name", "Supplier must be Ralawise or Uneek"))
                continue

            if supplier_key in stale_suppliers:
                product_quarantine.append(_quarantine_row(product, sku, supplier, supplier_sku, stock_location, "stale_supplier_stock_file", "Supplier stock file is older than configured limit"))
                continue

            location_index = _find_location_index(locations, stock_location)
            if location_index is None:
                product_quarantine.append(_quarantine_row(product, sku, supplier, supplier_sku, stock_location, "stock_location_supplier_mismatch", "Mapping stock_location is not present in StoreFeeder Stock Locations"))
                continue

            stock_row = stock_by_key.get((supplier_key, supplier_sku.casefold()))
            if stock_row is None:
                missing = _quarantine_row(product, sku, supplier, supplier_sku, stock_location, "missing_supplier_sku_in_stock_feed", "Supplier SKU not found in stock feed")
                missing_rows.append(missing)
                if not config.missing_as_zero:
                    product_quarantine.append(missing)
                    continue
                supplier_qty = 0
            else:
                if bool(stock_row["discontinued"]):
                    product_quarantine.append(_quarantine_row(product, sku, supplier, supplier_sku, stock_location, "discontinued_supplier_sku", "Supplier SKU is discontinued"))
                    continue
                if not bool(stock_row["quantity_valid"]):
                    product_quarantine.append(_quarantine_row(product, sku, supplier, supplier_sku, stock_location, "invalid_stock_quantity", "Supplier stock quantity is not numeric"))
                    continue
                if bool(stock_row["quantity_negative"]):
                    product_quarantine.append(_quarantine_row(product, sku, supplier, supplier_sku, stock_location, "negative_stock_quantity", "Supplier stock quantity is negative"))
                    continue
                supplier_qty = int(stock_row["supplier_free_stock"])

            safe_stock = min(max(supplier_qty - config.buffer, 0), config.max_stock)
            inventories[location_index] = str(int(safe_stock))
            ready_detail = {
                "SKU": sku,
                "supplier": supplier,
                "supplier_sku": supplier_sku,
                "stock_location": stock_location,
                "supplier_free_stock": supplier_qty,
                "safe_stock": int(safe_stock),
                "validation_category": "",
                STRICT_REPORT_STATUS_COLUMN: STRICT_STATUS_UPDATE_READY,
            }
            sku_ready_rows.append(ready_detail)
            if safe_stock == 0:
                zero_rows.append(ready_detail)

        if product_quarantine:
            quarantine_rows.extend(product_quarantine)
            if config.allow_quarantine_update:
                quarantine_note = "allow_quarantine_update is set, but quarantined rows remain excluded from live API payload preview"
                for detail in sku_ready_rows:
                    detail["quarantine_override_note"] = quarantine_note
            else:
                continue

        if sku_ready_rows:
            product_update["Stock Location Current Inventories"] = "|".join(inventories)
            product_update[STRICT_REPORT_STATUS_COLUMN] = STRICT_STATUS_UPDATE_READY
            ready_rows.append(product_update.to_dict())
            ready_rows[-1]["validation_category"] = ""
            ready_rows[-1]["ready_detail_count"] = len(sku_ready_rows)
            ready_rows[-1]["details"] = "|".join(
                f"{row['supplier']}:{row['supplier_sku']}:{row['stock_location']}={row['safe_stock']}" for row in sku_ready_rows
            )
            api_rows.extend(_api_rows_for_sku(product_update, sku_ready_rows))

    ready_export = _ready_export_dataframe(ready_rows, original.columns)
    quarantine_review = pd.DataFrame(quarantine_rows, columns=_quarantine_columns())
    zero_stock_updates = pd.DataFrame(zero_rows, columns=_detail_columns())
    missing_supplier_skus = pd.DataFrame(missing_rows, columns=_quarantine_columns())
    skipped_not_in_supplier_mapping = pd.DataFrame(skipped_not_in_mapping_rows, columns=SKIPPED_REPORT_COLUMNS)
    mapping_skus_missing_from_storefeeder_export = pd.DataFrame(
        mapping_skus_missing_rows,
        columns=REQUIRED_MAPPING_COLUMNS + ["reason"],
    )
    api_payload_preview = pd.DataFrame(
        api_rows,
        columns=["ProductID", "SKU", "supplier", "supplier_sku", "stock_location", "quantity", STRICT_REPORT_STATUS_COLUMN],
    )

    safety_passed, unexpected_columns = _validate_ready_export_safety(original, ready_export)
    blocked_reasons = _final_blocked_reasons(
        update_candidate_mapping_rows=update_candidate_mapping_rows,
        ready_count=len(ready_export),
        quarantined_count=len(quarantine_review),
        duplicate_conflict_count=int((quarantine_review["validation_category"].isin(["duplicate_storefeeder_sku", "duplicate_supplier_mapping_conflict"])).sum()) if not quarantine_review.empty else 0,
        stale_file_count=len(stale_suppliers),
        safety_passed=safety_passed,
        unexpected_columns=unexpected_columns,
        config=config,
    )
    live_update_allowed = len(blocked_reasons) == 0

    validation_summary = _strict_validation_summary(
        total_rows=len(original),
        update_candidate_mapping_rows=update_candidate_mapping_rows,
        update_candidate_product_skus_count=len(update_candidate_product_skus),
        ready_count=len(ready_export),
        quarantined_count=len(quarantine_review),
        skipped_not_in_mapping_count=len(skipped_not_in_supplier_mapping),
        mapping_missing_from_export_count=len(mapping_skus_missing_from_storefeeder_export),
        zero_count=len(zero_stock_updates),
        missing_count=len(missing_supplier_skus),
        duplicate_conflict_count=int((quarantine_review["validation_category"].isin(["duplicate_storefeeder_sku", "duplicate_supplier_mapping_conflict"])).sum()) if not quarantine_review.empty else 0,
        live_update_allowed=live_update_allowed,
        blocked_reasons=blocked_reasons,
        config=config,
        safety_passed=safety_passed,
        unexpected_columns=unexpected_columns,
    )

    return StrictStoreFeederStockResult(
        ready_export=ready_export,
        quarantine_review=quarantine_review,
        zero_stock_updates=zero_stock_updates,
        missing_supplier_skus=missing_supplier_skus,
        skipped_not_in_supplier_mapping=skipped_not_in_supplier_mapping,
        mapping_skus_missing_from_storefeeder_export=mapping_skus_missing_from_storefeeder_export,
        validation_summary=validation_summary,
        api_payload_preview=api_payload_preview,
        safety_passed=safety_passed,
        live_update_allowed=live_update_allowed,
        blocked_reasons=blocked_reasons,
    )


def _strict_supplier_frame(supplier: str, stock_df: pd.DataFrame | None) -> pd.DataFrame:
    if stock_df is None or stock_df.empty:
        return _strict_empty_stock_frame()
    if supplier == "Ralawise":
        sku_col = _first_existing_column(stock_df, ["SKU", "sku"])
        qty_col = _first_existing_column(stock_df, ["free", "Free", "Stock", "StockLevel", "LiveStock"])
        discontinued_col = _first_existing_column(stock_df, ["DiscontinuedStatus", "discontinued", "Discontinued"])
    else:
        sku_col = _first_existing_column(stock_df, ["ItemNo", "ProductCode", "SKU", "sku"])
        qty_col = _first_existing_column(stock_df, ["Stock", "StockLevel", "LiveStock", "free"])
        discontinued_col = _first_existing_column(stock_df, ["DiscontinuedStatus", "discontinued", "Discontinued"])
    if not sku_col or not qty_col:
        return _strict_empty_stock_frame()
    out = pd.DataFrame({
        "supplier": supplier,
        "supplier_sku": stock_df[sku_col].fillna("").astype(str).str.strip().str.upper(),
        "raw_stock_quantity": stock_df[qty_col].fillna("").astype(str).str.strip(),
        "raw_discontinued_status": stock_df[discontinued_col].fillna("").astype(str).str.strip() if discontinued_col else "",
    })
    numeric = pd.to_numeric(out["raw_stock_quantity"], errors="coerce")
    out["quantity_valid"] = numeric.notna()
    out["quantity_negative"] = numeric.lt(0).fillna(False)
    out["supplier_free_stock"] = numeric.fillna(0).astype(int)
    out["discontinued"] = out["raw_discontinued_status"].astype(str).str.strip().ne("")
    return out[out["supplier_sku"].ne("")].reset_index(drop=True)


def _build_strict_supplier_stock_lookup(ralawise_stock_df: pd.DataFrame | None, uneek_stock_df: pd.DataFrame | None) -> pd.DataFrame:
    stock = pd.concat([
        _strict_supplier_frame("Ralawise", ralawise_stock_df),
        _strict_supplier_frame("Uneek", uneek_stock_df),
    ], ignore_index=True)
    if stock.empty:
        return _strict_empty_stock_frame()
    return stock.drop_duplicates(subset=["supplier", "supplier_sku"], keep="last").reset_index(drop=True)


def _strict_empty_stock_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "supplier", "supplier_sku", "raw_stock_quantity", "raw_discontinued_status",
        "quantity_valid", "quantity_negative", "supplier_free_stock", "discontinued",
    ])


def _duplicate_mapping_conflicts(mapping: pd.DataFrame) -> dict[str, str]:
    conflicts: dict[str, str] = {}
    for sku, group in mapping.groupby("storefeeder_sku"):
        if len(group) <= 1:
            continue
        duplicated_locations = group["stock_location"].duplicated(keep=False).any()
        duplicated_supplier_skus = group[["supplier", "supplier_sku", "stock_location"]].duplicated(keep=False).any()
        if duplicated_locations or duplicated_supplier_skus:
            conflicts[sku] = "Duplicate mapping rows conflict on supplier SKU or stock location"
    return conflicts


def _quarantine_row(product, sku: str, supplier: str, supplier_sku: str, stock_location: str, category: str, reason: str) -> dict[str, object]:
    return {
        "SKU": sku,
        "Name": str(product.get("Name", "")),
        "supplier": supplier,
        "supplier_sku": supplier_sku,
        "stock_location": stock_location,
        "validation_category": category if category in VALIDATION_CATEGORIES else "unknown_error",
        STRICT_REPORT_STATUS_COLUMN: STRICT_STATUS_QUARANTINED,
        "reason": reason,
    }


def _quarantine_columns() -> list[str]:
    return ["SKU", "Name", "supplier", "supplier_sku", "stock_location", "validation_category", STRICT_REPORT_STATUS_COLUMN, "reason"]


def _detail_columns() -> list[str]:
    return ["SKU", "supplier", "supplier_sku", "stock_location", "supplier_free_stock", "safe_stock", "validation_category", STRICT_REPORT_STATUS_COLUMN]


def _ready_export_dataframe(rows: list[dict[str, object]], original_columns: pd.Index) -> pd.DataFrame:
    extra_columns = [STRICT_REPORT_STATUS_COLUMN, "validation_category", "ready_detail_count", "details"]
    columns = list(original_columns) + extra_columns
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows).reindex(columns=columns).fillna("")


def _api_rows_for_sku(product_update: pd.Series, ready_details: list[dict[str, object]]) -> list[dict[str, object]]:
    rows = []
    for detail in ready_details:
        rows.append({
            "ProductID": product_update.get("ID", ""),
            "SKU": product_update["SKU"],
            "supplier": detail["supplier"],
            "supplier_sku": detail["supplier_sku"],
            "stock_location": detail["stock_location"],
            "quantity": detail["safe_stock"],
            STRICT_REPORT_STATUS_COLUMN: STRICT_STATUS_UPDATE_READY,
        })
    return rows


def _validate_ready_export_safety(original: pd.DataFrame, ready_export: pd.DataFrame) -> tuple[bool, list[str]]:
    unexpected = []
    original_lookup = original.set_index("SKU", drop=False)
    original_columns = list(original.columns)
    for _, row in ready_export.iterrows():
        sku = row["SKU"]
        if sku not in original_lookup.index:
            unexpected.append("SKU")
            continue
        original_row = original_lookup.loc[sku]
        if isinstance(original_row, pd.DataFrame):
            original_row = original_row.iloc[0]
        for column in original_columns:
            if column == "Stock Location Current Inventories":
                continue
            if str(original_row[column]) != str(row[column]):
                unexpected.append(column)
    return len(unexpected) == 0, sorted(set(unexpected))


def _final_blocked_reasons(
    *,
    update_candidate_mapping_rows: int,
    ready_count: int,
    quarantined_count: int,
    duplicate_conflict_count: int,
    stale_file_count: int,
    safety_passed: bool,
    unexpected_columns: list[str],
    config: StrictStockSafetyConfig,
) -> list[str]:
    reasons = []
    quarantine_rate = (quarantined_count / update_candidate_mapping_rows) if update_candidate_mapping_rows else 0.0
    if not safety_passed:
        reasons.append("unexpected_changed_columns:" + "|".join(unexpected_columns))
    if quarantine_rate > config.max_quarantine_rate and not config.allow_quarantine_update:
        reasons.append(f"quarantine_rate_exceeds_limit:{quarantine_rate:.4f}>{config.max_quarantine_rate:.4f}")
    if stale_file_count > 0:
        reasons.append("stale_supplier_stock_file")
    if duplicate_conflict_count > 0:
        reasons.append("duplicate_or_conflict_rows_present")
    if config.previous_update_ready_count is not None and config.previous_update_ready_count > 0 and not config.allow_unusual_update_count:
        low = config.previous_update_ready_count * config.low_update_count_ratio
        high = config.previous_update_ready_count * config.high_update_count_ratio
        if ready_count < low or ready_count > high:
            reasons.append(f"unusual_update_count:{ready_count};previous={config.previous_update_ready_count}")
    return reasons


def _strict_validation_summary(
    *,
    total_rows: int,
    update_candidate_mapping_rows: int,
    update_candidate_product_skus_count: int,
    ready_count: int,
    quarantined_count: int,
    skipped_not_in_mapping_count: int,
    mapping_missing_from_export_count: int,
    zero_count: int,
    missing_count: int,
    duplicate_conflict_count: int,
    live_update_allowed: bool,
    blocked_reasons: list[str],
    config: StrictStockSafetyConfig,
    safety_passed: bool,
    unexpected_columns: list[str],
) -> pd.DataFrame:
    quarantine_rate = (quarantined_count / update_candidate_mapping_rows) if update_candidate_mapping_rows else 0.0
    rows = [
        {"metric": "total_rows_read", "value": int(total_rows)},
        {"metric": "update_candidate_mapping_rows", "value": int(update_candidate_mapping_rows)},
        {"metric": "update_candidate_product_skus", "value": int(update_candidate_product_skus_count)},
        {"metric": "update_ready_count", "value": int(ready_count)},
        {"metric": "quarantined_count", "value": int(quarantined_count)},
        {"metric": "skipped_not_in_supplier_mapping_count", "value": int(skipped_not_in_mapping_count)},
        {"metric": "mapping_skus_missing_from_storefeeder_export_count", "value": int(mapping_missing_from_export_count)},
        {"metric": "zero_stock_count", "value": int(zero_count)},
        {"metric": "missing_stock_count", "value": int(missing_count)},
        {"metric": "duplicate_conflict_count", "value": int(duplicate_conflict_count)},
        {"metric": "quarantine_rate", "value": f"{quarantine_rate:.6f}"},
        {"metric": "max_quarantine_rate", "value": config.max_quarantine_rate},
        {"metric": "buffer", "value": config.buffer},
        {"metric": "max_stock", "value": config.max_stock},
        {"metric": "missing_as_zero", "value": bool(config.missing_as_zero)},
        {"metric": "allow_quarantine_update", "value": bool(config.allow_quarantine_update)},
        {"metric": "safety_passed", "value": bool(safety_passed)},
        {"metric": "unexpected_changed_columns", "value": "|".join(unexpected_columns)},
        {"metric": "live_update_allowed", "value": "yes" if live_update_allowed else "no"},
        {"metric": "blocked_reasons", "value": "|".join(blocked_reasons)},
    ]
    summary = pd.DataFrame(rows, columns=SUMMARY_COLUMNS)
    summary[STRICT_REPORT_STATUS_COLUMN] = STRICT_STATUS_SKIPPED
    return summary
