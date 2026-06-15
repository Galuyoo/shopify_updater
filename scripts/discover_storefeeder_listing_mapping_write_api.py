from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storefeeder_api import StoreFeederApiClient, StoreFeederApiConfig

BASE_ENDPOINTS = [
    "/listings",
    "/channel/listings",
    "/channel-listings",
    "/channellistings",
    "/amazon/listings",
    "/channels",
    "/marketplaces",
]
DETAIL_PATTERNS = [
    "/listings/{listing_id}",
    "/channel-listings/{listing_id}",
    "/channellistings/{listing_id}",
]
MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only discovery for StoreFeeder listing remap write capability.")
    parser.add_argument("--out-root", type=Path, default=Path("reports/listing_mapping_write_api_discovery"))
    parser.add_argument("--storefeeder-api-base-url", default="https://rest.storefeeder.com")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--page-size", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_env_file(args.env_file)
    client = StoreFeederApiClient.from_env(StoreFeederApiConfig(base_url=args.storefeeder_api_base_url))
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    raw: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    sample_listing_ids: list[str] = []

    for path in BASE_ENDPOINTS:
        for method in ["GET", "OPTIONS"]:
            result = _probe(client, method, path, params={"page": 1, "pageSize": args.page_size} if method == "GET" else None)
            raw[f"{method} {path}"] = result
            rows.append(_probe_row(method, path, result))
            if method == "GET" and int(result.get("_status_code", 0)) < 400:
                for item in _extract_records(result.get("response", {})):
                    listing_id = _first_text(item, ["ListingID", "ListingId", "ID", "Id", "id"])
                    if listing_id and listing_id not in sample_listing_ids:
                        sample_listing_ids.append(listing_id)
                    if len(sample_listing_ids) >= 3:
                        break

    for listing_id in sample_listing_ids[:3]:
        for pattern in DETAIL_PATTERNS:
            path = pattern.format(listing_id=listing_id)
            for method in ["GET", "OPTIONS"]:
                result = _probe(client, method, path)
                raw[f"{method} {path}"] = result
                rows.append(_probe_row(method, path, result))

    discovery = pd.DataFrame(rows)
    discovery.to_csv(out_dir / "listing_mapping_write_api_discovery.csv", index=False)
    (out_dir / "listing_endpoint_probe_raw.json").write_text(json.dumps(raw, indent=2, default=str), encoding="utf-8")

    exposes_mutating = discovery["allow_methods"].astype(str).apply(_allow_has_mutating).any() if not discovery.empty else False
    found_listing_rows = bool(sample_listing_ids)
    remap_confirmed = False
    summary_lines = [
        "StoreFeeder listing mapping write API discovery",
        f"RUN_ID: {run_id}",
        f"FOUND_LISTING_ROWS: {'yes' if found_listing_rows else 'no'}",
        f"SAMPLE_LISTING_IDS: {', '.join(sample_listing_ids[:3])}",
        f"OPTIONS_EXPOSES_MUTATING_METHODS: {'yes' if exposes_mutating else 'no'}",
        f"LISTING_REMAP_WRITE_ENDPOINT_CONFIRMED: {'yes' if remap_confirmed else 'no'}",
        "CONFIRMED_ENDPOINT: ",
        "CONFIRMED_METHOD: ",
        "REQUIRED_PAYLOAD_FIELDS: unknown",
        "",
        "Safety: this script used only GET and OPTIONS. No POST/PUT/PATCH/DELETE was called.",
        "Result: listing remap remains unconfirmed until StoreFeeder documents or safely proves a write endpoint/payload.",
    ]
    (out_dir / "CHATGPT_BRIEF.txt").write_text("\n".join(summary_lines), encoding="utf-8")
    pd.DataFrame([
        {"metric": "found_listing_rows", "value": "yes" if found_listing_rows else "no"},
        {"metric": "options_exposes_mutating_methods", "value": "yes" if exposes_mutating else "no"},
        {"metric": "listing_remap_write_endpoint_confirmed", "value": "no"},
        {"metric": "out_dir", "value": str(out_dir)},
    ]).to_csv(out_dir / "SUMMARY.csv", index=False)

    print("Listing write API discovery")
    print("Reports:", out_dir)
    print("Listing remap write endpoint confirmed: no")
    print("Read-only only. No POST/PUT/PATCH/DELETE was called.")
    return 0


def _probe(client: StoreFeederApiClient, method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        if method == "GET":
            return client.get_path(path, params=params)
        if method == "OPTIONS":
            return client.options_path(path)
    except Exception as exc:  # noqa: BLE001 - discovery must report failures, not crash early.
        return {"_status_code": "exception", "allow": "", "response": repr(exc)}
    raise ValueError(method)


def _probe_row(method: str, path: str, result: dict[str, Any]) -> dict[str, Any]:
    response = result.get("response", {})
    items = _extract_records(response)
    return {
        "method": method,
        "path": path,
        "status_code": result.get("_status_code", ""),
        "allow_methods": result.get("allow", ""),
        "item_count": len(items),
        "has_product_id_field": _payload_has_key(response, ["ProductID", "ProductId", "MappedProductID", "StoreFeederProductID"]),
        "has_listing_id_field": _payload_has_key(response, ["ListingID", "ListingId", "listingId", "ID", "Id"]),
        "response_preview": json.dumps(response, default=str, ensure_ascii=False)[:1000],
    }


def _allow_has_mutating(value: str) -> bool:
    methods = {part.strip().upper() for part in str(value).replace(",", " ").split() if part.strip()}
    return bool(methods & MUTATING_METHODS)


def _payload_has_key(payload: Any, keys: list[str]) -> str:
    if isinstance(payload, dict):
        if any(key in payload for key in keys):
            return "yes"
        for value in payload.values():
            found = _payload_has_key(value, keys)
            if found == "yes":
                return "yes"
    elif isinstance(payload, list):
        for item in payload:
            found = _payload_has_key(item, keys)
            if found == "yes":
                return "yes"
    return "no"


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ["Items", "items", "Data", "data", "Results", "results", "Listings", "listings", "Products", "products", "value", "Value"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _first_text(payload: Any, keys: list[str]) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in keys:
        if key in payload and payload[key] not in [None, ""]:
            value = payload[key]
            if not isinstance(value, (dict, list, tuple)):
                return str(value).strip()
    for value in payload.values():
        if isinstance(value, dict):
            found = _first_text(value, keys)
            if found:
                return found
    return ""


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    raise SystemExit(main())
