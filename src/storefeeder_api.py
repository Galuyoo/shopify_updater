from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import os
import time
from typing import Any

import pandas as pd
import requests

from .stock_mapping import STRICT_REPORT_STATUS_COLUMN, STRICT_STATUS_UPDATE_READY


MAX_STOREFEEDER_BATCH_SIZE = 50
STOREFEEDER_STOCK_LOCATION_INVENTORY_PATH = "/products/stocklocationinventory"
STOREFEEDER_SUPPLIER_INVENTORY_COST_PATH = "/products/productsuppliers/inventory-cost"


@dataclass(frozen=True)
class StoreFeederApiConfig:
    base_url: str = "https://rest.storefeeder.com"
    max_requests_per_second: int = 3
    max_requests_per_minute: int = 60
    timeout_seconds: int = 60


@dataclass(frozen=True)
class StoreFeederBatchResult:
    batch_number: int
    requested_count: int
    status_code: int
    total_processed: int
    successful: int
    failed: int
    response_json: dict[str, Any]


class ApiPayloadValidationError(ValueError):
    def __init__(self, message: str, invalid_rows: pd.DataFrame) -> None:
        super().__init__(message)
        self.invalid_rows = invalid_rows


def build_stock_location_inventory_payload_preview(
    api_payload_preview: pd.DataFrame,
    *,
    api_limit: int | None = None,
    stock_location_id_map: pd.DataFrame | None = None,
    stock_location_id_type: str = "StockLocationReference",
    stock_location_id_value_column: str = "stock_location",
    reason: str = "Supplier stock sync",
) -> pd.DataFrame:
    """Build the exact rows that are allowed to become StoreFeeder API payload items."""
    required = ["SKU", "stock_location", "quantity", STRICT_REPORT_STATUS_COLUMN]
    if stock_location_id_map is None and stock_location_id_value_column not in required:
        required.append(stock_location_id_value_column)
    missing = [column for column in required if column not in api_payload_preview.columns]
    if missing:
        raise ValueError("API payload preview missing required columns: " + ", ".join(missing))
    if api_limit is not None and api_limit < 0:
        raise ValueError("--api-limit must be zero or greater")
    if not stock_location_id_type.strip():
        raise ValueError("--storefeeder-stock-location-id-type cannot be blank")

    rows = api_payload_preview.copy()
    rows[STRICT_REPORT_STATUS_COLUMN] = rows[STRICT_REPORT_STATUS_COLUMN].fillna("").astype(str).str.strip()
    rows = rows[rows[STRICT_REPORT_STATUS_COLUMN].eq(STRICT_STATUS_UPDATE_READY)].copy()
    update_ready_count_before_limit = len(rows)
    rows["_source_row_number"] = rows.index + 2
    rows["_SKU_text"] = rows["SKU"].fillna("").astype(str).str.strip()
    rows["_stock_location_text"] = rows["stock_location"].fillna("").astype(str).str.strip()
    if stock_location_id_map is None:
        rows["_stock_location_id_type_text"] = stock_location_id_type.strip()
        rows["_stock_location_id_value_text"] = rows[stock_location_id_value_column].fillna("").astype(str).str.strip()
    else:
        rows = _merge_stock_location_id_map(rows, stock_location_id_map)
    rows["_quantity_numeric"] = pd.to_numeric(rows["quantity"], errors="coerce")
    rows["_invalid_reason"] = rows.apply(_invalid_payload_reason, axis=1)

    invalid_rows = rows[rows["_invalid_reason"].ne("")].copy()
    if not invalid_rows.empty:
        report_columns = ["_source_row_number", "SKU", "stock_location", "quantity", STRICT_REPORT_STATUS_COLUMN, "_invalid_reason"]
        if stock_location_id_value_column != "stock_location":
            report_columns.insert(3, stock_location_id_value_column)
        invalid_report = invalid_rows[report_columns].rename(
            columns={"_source_row_number": "source_row_number", "_invalid_reason": "invalid_reason"}
        )
        raise ApiPayloadValidationError("StoreFeeder API payload validation failed; invalid update_ready rows found", invalid_report)

    valid_rows_before_limit = int(rows["_invalid_reason"].eq("").sum())
    if update_ready_count_before_limit != valid_rows_before_limit:
        raise ApiPayloadValidationError(
            f"StoreFeeder API row-count assertion failed: update_ready={update_ready_count_before_limit}, valid={valid_rows_before_limit}",
            pd.DataFrame(columns=["source_row_number", "SKU", "stock_location", "quantity", STRICT_REPORT_STATUS_COLUMN, "invalid_reason"]),
        )

    rows["quantity"] = rows["_quantity_numeric"].astype(int)
    if api_limit is not None:
        rows = rows.head(api_limit)

    preview = pd.DataFrame(
        {
            "SKU": rows["_SKU_text"],
            "stock_location": rows["_stock_location_text"],
            "quantity": rows["quantity"],
            STRICT_REPORT_STATUS_COLUMN: rows[STRICT_REPORT_STATUS_COLUMN],
            "ProductIDType.IDType": "SKU",
            "ProductIDType.Value": rows["_SKU_text"],
            "AdjustmentType": "AbsoluteAdjustment",
            "AdjustmentAmount": rows["quantity"],
            "StockLocationID.IDType": rows["_stock_location_id_type_text"],
            "StockLocationID.Value": rows["_stock_location_id_value_text"],
            "Reason": reason,
        }
    )
    return preview.reset_index(drop=True)


def build_supplier_inventory_cost_payload_preview(
    api_payload_preview: pd.DataFrame,
    supplier_id_map: pd.DataFrame,
    *,
    api_limit: int | None = None,
    supplier_costs: int = 0,
) -> pd.DataFrame:
    required = ["ProductID", "SKU", "supplier", "supplier_sku", "quantity", STRICT_REPORT_STATUS_COLUMN]
    missing = [column for column in required if column not in api_payload_preview.columns]
    if missing:
        raise ValueError("Supplier API payload preview missing required columns: " + ", ".join(missing))
    if api_limit is not None and api_limit < 0:
        raise ValueError("--api-limit must be zero or greater")

    rows = api_payload_preview.copy()
    rows[STRICT_REPORT_STATUS_COLUMN] = rows[STRICT_REPORT_STATUS_COLUMN].fillna("").astype(str).str.strip()
    rows = rows[rows[STRICT_REPORT_STATUS_COLUMN].eq(STRICT_STATUS_UPDATE_READY)].copy()
    update_ready_count_before_limit = len(rows)
    rows["_source_row_number"] = rows.index + 2
    rows["_ProductID_text"] = rows["ProductID"].fillna("").astype(str).str.strip()
    rows["_SKU_text"] = rows["SKU"].fillna("").astype(str).str.strip()
    rows["_supplier_text"] = rows["supplier"].fillna("").astype(str).str.strip()
    rows["_supplier_sku_text"] = rows["supplier_sku"].fillna("").astype(str).str.strip()
    rows = _merge_supplier_id_map(rows, supplier_id_map)
    rows["_quantity_numeric"] = pd.to_numeric(rows["quantity"], errors="coerce")
    rows["_supplier_id_numeric"] = pd.to_numeric(rows["_SupplierID_text"], errors="coerce")
    rows["_invalid_reason"] = rows.apply(_invalid_supplier_payload_reason, axis=1)

    invalid_rows = rows[rows["_invalid_reason"].ne("")].copy()
    if not invalid_rows.empty:
        invalid_report = invalid_rows[
            [
                "_source_row_number",
                "ProductID",
                "SKU",
                "supplier",
                "supplier_sku",
                "quantity",
                STRICT_REPORT_STATUS_COLUMN,
                "_invalid_reason",
            ]
        ].rename(columns={"_source_row_number": "source_row_number", "_invalid_reason": "invalid_reason"})
        raise ApiPayloadValidationError("StoreFeeder supplier API payload validation failed; invalid update_ready rows found", invalid_report)

    valid_rows_before_limit = int(rows["_invalid_reason"].eq("").sum())
    if update_ready_count_before_limit != valid_rows_before_limit:
        raise ApiPayloadValidationError(
            f"StoreFeeder supplier API row-count assertion failed: update_ready={update_ready_count_before_limit}, valid={valid_rows_before_limit}",
            pd.DataFrame(columns=["source_row_number", "ProductID", "SKU", "supplier", "supplier_sku", "quantity", STRICT_REPORT_STATUS_COLUMN, "invalid_reason"]),
        )

    rows["SupplierStockLevel"] = rows["_quantity_numeric"].astype(int)
    rows["Supplier.SupplierID"] = rows["_supplier_id_numeric"].astype(int)
    if api_limit is not None:
        rows = rows.head(api_limit)

    preview = pd.DataFrame(
        {
            "ProductID": rows["_ProductID_text"],
            "SKU": rows["_SKU_text"],
            "supplier": rows["_supplier_text"],
            "supplier_sku": rows["_supplier_sku_text"],
            "quantity": rows["SupplierStockLevel"],
            STRICT_REPORT_STATUS_COLUMN: rows[STRICT_REPORT_STATUS_COLUMN],
            "ProductIDType.IDType": "ID",
            "ProductIDType.Value": rows["_ProductID_text"],
            "Supplier.SupplierID": rows["Supplier.SupplierID"],
            "Supplier.Name": rows["_SupplierName_text"],
            "SupplierSKU": rows["_supplier_sku_text"],
            "SupplierStockLevel": rows["SupplierStockLevel"],
            "SupplierCosts": int(supplier_costs),
        }
    )
    return preview.reset_index(drop=True)


def supplier_payload_preview_to_items(preview: pd.DataFrame) -> list[dict[str, Any]]:
    required = [
        "ProductIDType.IDType",
        "ProductIDType.Value",
        "Supplier.SupplierID",
        "Supplier.Name",
        "SupplierSKU",
        "SupplierStockLevel",
        "SupplierCosts",
        STRICT_REPORT_STATUS_COLUMN,
    ]
    missing = [column for column in required if column not in preview.columns]
    if missing:
        raise ValueError("Supplier API payload preview missing required columns: " + ", ".join(missing))
    disallowed = preview[~preview[STRICT_REPORT_STATUS_COLUMN].astype(str).str.strip().eq(STRICT_STATUS_UPDATE_READY)]
    if not disallowed.empty:
        raise ValueError("Supplier API payload contains rows that are not update_ready")

    items: list[dict[str, Any]] = []
    for _, row in preview.iterrows():
        items.append(
            {
                "ProductIDType": {
                    "IDType": str(row["ProductIDType.IDType"]).strip(),
                    "Value": str(row["ProductIDType.Value"]).strip(),
                },
                "Supplier": {
                    "SupplierID": int(row["Supplier.SupplierID"]),
                    "Name": str(row["Supplier.Name"]).strip(),
                },
                "SupplierSKU": str(row["SupplierSKU"]).strip(),
                "SupplierStockLevel": int(row["SupplierStockLevel"]),
                "SupplierCosts": int(row["SupplierCosts"]),
            }
        )
    return items


def normalize_supplier_id_map(supplier_id_map: pd.DataFrame) -> pd.DataFrame:
    required = ["supplier", "SupplierID", "Supplier.Name"]
    missing = [column for column in required if column not in supplier_id_map.columns]
    if missing:
        raise ValueError("StoreFeeder supplier ID map missing required columns: " + ", ".join(missing))

    normalized = supplier_id_map[required].copy()
    for column in required:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized = normalized[normalized["supplier"].ne("")].copy()
    normalized["_supplier_key"] = normalized["supplier"].str.casefold()
    duplicate_keys = normalized[normalized["_supplier_key"].duplicated(keep=False)]["_supplier_key"].unique()
    if len(duplicate_keys) > 0:
        raise ValueError("StoreFeeder supplier ID map has duplicate supplier rows: " + ", ".join(sorted(duplicate_keys)))
    return normalized


def _merge_supplier_id_map(rows: pd.DataFrame, supplier_id_map: pd.DataFrame) -> pd.DataFrame:
    normalized_map = normalize_supplier_id_map(supplier_id_map)
    mapped = rows.copy()
    mapped["_supplier_key"] = mapped["_supplier_text"].str.casefold()
    mapped = mapped.merge(
        normalized_map[["_supplier_key", "SupplierID", "Supplier.Name"]],
        how="left",
        on="_supplier_key",
    )
    mapped["_SupplierID_text"] = mapped["SupplierID"].fillna("").astype(str).str.strip()
    mapped["_SupplierName_text"] = mapped["Supplier.Name"].fillna("").astype(str).str.strip()
    mapped = mapped.drop(columns=["SupplierID", "Supplier.Name", "_supplier_key"])
    return mapped


def _invalid_supplier_payload_reason(row: pd.Series) -> str:
    reasons = []
    if not row["_ProductID_text"]:
        reasons.append("missing_storefeeder_product_id")
    elif not str(row["_ProductID_text"]).isdigit():
        reasons.append("non_integer_storefeeder_product_id")
    if not row["_SKU_text"]:
        reasons.append("blank_SKU")
    if not row["_supplier_text"]:
        reasons.append("missing_supplier_mapping")
    if not row["_supplier_sku_text"]:
        reasons.append("missing_supplier_sku_mapping")
    if not row["_SupplierID_text"]:
        reasons.append("missing_supplier_id_mapping")
    elif pd.isna(row["_supplier_id_numeric"]) or float(row["_supplier_id_numeric"]) != int(row["_supplier_id_numeric"]):
        reasons.append("non_integer_supplier_id")
    if not row["_SupplierName_text"]:
        reasons.append("missing_supplier_name_mapping")
    quantity = row["_quantity_numeric"]
    if pd.isna(quantity):
        reasons.append("missing_or_non_numeric_quantity")
    else:
        if float(quantity) < 0:
            reasons.append("negative_quantity")
        if float(quantity) != int(quantity):
            reasons.append("non_integer_quantity")
    return "|".join(reasons)


def normalize_stock_location_id_map(stock_location_id_map: pd.DataFrame) -> pd.DataFrame:
    required = ["stock_location", "StockLocationID.IDType", "StockLocationID.Value"]
    missing = [column for column in required if column not in stock_location_id_map.columns]
    if missing:
        raise ValueError("StoreFeeder stock location ID map missing required columns: " + ", ".join(missing))

    normalized = stock_location_id_map[required].copy()
    for column in required:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized = normalized[normalized["stock_location"].ne("")].copy()
    normalized["_stock_location_key"] = normalized["stock_location"].str.casefold()
    duplicate_keys = normalized[normalized["_stock_location_key"].duplicated(keep=False)]["_stock_location_key"].unique()
    if len(duplicate_keys) > 0:
        raise ValueError("StoreFeeder stock location ID map has duplicate stock_location rows: " + ", ".join(sorted(duplicate_keys)))
    return normalized


def _merge_stock_location_id_map(rows: pd.DataFrame, stock_location_id_map: pd.DataFrame) -> pd.DataFrame:
    normalized_map = normalize_stock_location_id_map(stock_location_id_map)
    mapped = rows.copy()
    mapped["_stock_location_key"] = mapped["_stock_location_text"].str.casefold()
    mapped = mapped.merge(
        normalized_map[["_stock_location_key", "StockLocationID.IDType", "StockLocationID.Value"]],
        how="left",
        on="_stock_location_key",
    )
    mapped["_stock_location_id_type_text"] = mapped["StockLocationID.IDType"].fillna("").astype(str).str.strip()
    mapped["_stock_location_id_value_text"] = mapped["StockLocationID.Value"].fillna("").astype(str).str.strip()
    mapped = mapped.drop(columns=["StockLocationID.IDType", "StockLocationID.Value", "_stock_location_key"])
    return mapped


def _invalid_payload_reason(row: pd.Series) -> str:
    reasons = []
    if not row["_SKU_text"]:
        reasons.append("blank_SKU")
    if not row["_stock_location_text"]:
        reasons.append("blank_stock_location")
    if not row["_stock_location_id_type_text"]:
        reasons.append("blank_stock_location_id_type")
    if not row["_stock_location_id_value_text"]:
        reasons.append("missing_stock_location_id_mapping")
    quantity = row["_quantity_numeric"]
    if pd.isna(quantity):
        reasons.append("missing_or_non_numeric_quantity")
    else:
        if float(quantity) < 0:
            reasons.append("negative_quantity")
        if float(quantity) != int(quantity):
            reasons.append("non_integer_quantity")
    return "|".join(reasons)


def validate_api_batch_size(batch_size: int) -> int:
    if batch_size < 1:
        raise ValueError("--api-batch-size must be at least 1")
    if batch_size > MAX_STOREFEEDER_BATCH_SIZE:
        raise ValueError("--api-batch-size cannot exceed 50 for StoreFeeder stock location inventory updates")
    return batch_size


def payload_preview_to_storefeeder_items(preview: pd.DataFrame) -> list[dict[str, Any]]:
    required = [
        "ProductIDType.IDType",
        "ProductIDType.Value",
        "AdjustmentType",
        "AdjustmentAmount",
        "StockLocationID.IDType",
        "StockLocationID.Value",
        "Reason",
        STRICT_REPORT_STATUS_COLUMN,
    ]
    missing = [column for column in required if column not in preview.columns]
    if missing:
        raise ValueError("API payload preview missing required columns: " + ", ".join(missing))
    disallowed = preview[~preview[STRICT_REPORT_STATUS_COLUMN].astype(str).str.strip().eq(STRICT_STATUS_UPDATE_READY)]
    if not disallowed.empty:
        raise ValueError("API payload contains rows that are not update_ready")

    items: list[dict[str, Any]] = []
    for _, row in preview.iterrows():
        items.append(
            {
                "ProductIDType": {
                    "IDType": str(row["ProductIDType.IDType"]).strip(),
                    "Value": str(row["ProductIDType.Value"]).strip(),
                },
                "AdjustmentType": "AbsoluteAdjustment",
                "AdjustmentAmount": int(row["AdjustmentAmount"]),
                "StockLocationID": {
                    "IDType": str(row["StockLocationID.IDType"]).strip(),
                    "Value": str(row["StockLocationID.Value"]).strip(),
                },
                "Reason": str(row["Reason"]).strip(),
            }
        )
    return items


def batch_items(items: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    batch_size = validate_api_batch_size(batch_size)
    return [items[index:index + batch_size] for index in range(0, len(items), batch_size)]


class StoreFeederRateLimiter:
    def __init__(self, *, max_per_second: int = 3, max_per_minute: int = 60) -> None:
        self.max_per_second = max_per_second
        self.max_per_minute = max_per_minute
        self._second_window: deque[float] = deque()
        self._minute_window: deque[float] = deque()

    def wait(self) -> None:
        while True:
            now = time.monotonic()
            self._drop_old(self._second_window, now - 1.0)
            self._drop_old(self._minute_window, now - 60.0)
            waits = []
            if len(self._second_window) >= self.max_per_second:
                waits.append(1.0 - (now - self._second_window[0]))
            if len(self._minute_window) >= self.max_per_minute:
                waits.append(60.0 - (now - self._minute_window[0]))
            if not waits:
                self._second_window.append(now)
                self._minute_window.append(now)
                return
            time.sleep(max(max(waits), 0.01))

    @staticmethod
    def _drop_old(window: deque[float], cutoff: float) -> None:
        while window and window[0] <= cutoff:
            window.popleft()


class StoreFeederApiClient:
    def __init__(self, config: StoreFeederApiConfig, access_token: str, *, session: requests.Session | None = None) -> None:
        if not access_token.strip():
            raise ValueError("StoreFeeder access token is required")
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {access_token.strip()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self.rate_limiter = StoreFeederRateLimiter(
            max_per_second=config.max_requests_per_second,
            max_per_minute=config.max_requests_per_minute,
        )

    @classmethod
    def from_env(cls, config: StoreFeederApiConfig) -> "StoreFeederApiClient":
        access_token = os.getenv("STOREFEEDER_ACCESS_TOKEN", "").strip()
        if not access_token:
            access_token = fetch_storefeeder_access_token(config)
        return cls(config, access_token)

    def update_stock_location_inventory(self, batch: list[dict[str, Any]], *, batch_number: int) -> StoreFeederBatchResult:
        if len(batch) > MAX_STOREFEEDER_BATCH_SIZE:
            raise ValueError("StoreFeeder API batch exceeds 50 products")
        self.rate_limiter.wait()
        url = self.config.base_url.rstrip("/") + STOREFEEDER_STOCK_LOCATION_INVENTORY_PATH
        response = self.session.put(url, json=batch, timeout=self.config.timeout_seconds)
        response_json = _response_json(response)
        if response.status_code == 409:
            time.sleep(10)
            self.rate_limiter.wait()
            response = self.session.put(url, json=batch, timeout=self.config.timeout_seconds)
            response_json = _response_json(response)

        total_processed = _int_from_response(response_json, "TotalItemsProcessed", default=len(batch))
        successful = _int_from_response(response_json, "Successful", default=0)
        failed = _int_from_response(response_json, "Failed", default=len(batch) if response.status_code >= 400 else 0)
        if response.status_code >= 400 and failed == 0:
            failed = len(batch)
        return StoreFeederBatchResult(
            batch_number=batch_number,
            requested_count=len(batch),
            status_code=response.status_code,
            total_processed=total_processed,
            successful=successful,
            failed=failed,
            response_json=response_json,
        )

    def update_product_supplier_inventory_cost(self, batch: list[dict[str, Any]], *, batch_number: int) -> StoreFeederBatchResult:
        if len(batch) > MAX_STOREFEEDER_BATCH_SIZE:
            raise ValueError("StoreFeeder API batch exceeds 50 products")
        self.rate_limiter.wait()
        url = self.config.base_url.rstrip("/") + STOREFEEDER_SUPPLIER_INVENTORY_COST_PATH
        response = self.session.patch(url, json=batch, timeout=self.config.timeout_seconds)
        response_json = _response_json(response)
        if response.status_code == 409:
            time.sleep(10)
            self.rate_limiter.wait()
            response = self.session.patch(url, json=batch, timeout=self.config.timeout_seconds)
            response_json = _response_json(response)

        total_processed = _int_from_response(response_json, "TotalItemsProcessed", default=len(batch))
        successful = _int_from_response(response_json, "Successful", default=0)
        failed = _int_from_response(response_json, "Failed", default=len(batch) if response.status_code >= 400 else 0)
        if response.status_code >= 400 and failed == 0:
            failed = len(batch)
        return StoreFeederBatchResult(
            batch_number=batch_number,
            requested_count=len(batch),
            status_code=response.status_code,
            total_processed=total_processed,
            successful=successful,
            failed=failed,
            response_json=response_json,
        )


def fetch_storefeeder_access_token(config: StoreFeederApiConfig) -> str:
    username = os.getenv("STOREFEEDER_API_USERNAME", "").strip()
    password = os.getenv("STOREFEEDER_API_PASSWORD", "").strip()
    api_key = os.getenv("STOREFEEDER_API_KEY", "").strip()
    missing = [
        name for name, value in [
            ("STOREFEEDER_API_USERNAME", username),
            ("STOREFEEDER_API_PASSWORD", password),
            ("STOREFEEDER_API_KEY", api_key),
        ]
        if not value
    ]
    if missing:
        raise RuntimeError("Missing StoreFeeder API credential env vars: " + ", ".join(missing))

    url = config.base_url.rstrip("/") + "/Token"
    response = requests.post(
        url,
        data={
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": api_key,
        },
        headers={"Accept": "application/json"},
        timeout=config.timeout_seconds,
    )
    payload = _response_json(response)
    if response.status_code >= 400:
        raise RuntimeError(f"StoreFeeder token request failed {response.status_code}: {_safe_error_text(payload)}")
    access_token = str(payload.get("access_token", "")).strip()
    if not access_token:
        raise RuntimeError("StoreFeeder token response did not include access_token")
    return access_token


def _response_json(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {"raw_text": response.text[:1000]}
    if isinstance(payload, dict):
        return payload
    return {"value": payload}


def _int_from_response(payload: dict[str, Any], key: str, *, default: int) -> int:
    try:
        return int(payload.get(key, default))
    except (TypeError, ValueError):
        return default


def _safe_error_text(payload: dict[str, Any]) -> str:
    text = str(payload)
    return text[:500]
