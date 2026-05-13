from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .marketplaces.amazon import build_amazon_dry_run, build_stock_lookup

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

PRICE_INVENTORY_COLUMNS = [
    "sku",
    "price",
    "minimum-seller-allowed-price",
    "maximum-seller-allowed-price",
    "quantity",
    "handling-time",
    "fulfillment-channel",
]

LISTING_MAP_COLUMNS = [
    "canonical_sku",
    "amazon_seller_sku",
    "asin",
    "marketplace_id",
    "fulfillment_channel",
    "enabled",
]

REVIEWED_SHARED_STOCK_SKUS = {"TC013BLAC", "TC013NAVY"}
PARENT_ONLY_SKUS = {"PERSOO-UC921"}
DEFAULT_LISTING_MAP = PROJECT_ROOT / "data" / "amazon_listing_map.csv"
DEFAULT_MARKETPLACE_ID = "A1F83G8C2ARO7P"
DEFAULT_FULFILLMENT_CHANNEL = "FBM"


@dataclass(frozen=True)
class BuildUploadSummary:
    amazon_export: str
    listing_map: str
    out_dir: str
    prefer: str
    marketplace_id: str
    fulfillment_channel: str
    include_reviewed_shared_stock: bool
    amazon_export_rows: int
    listing_map_rows: int
    map_unmatched_rows: int
    parent_excluded_rows: int
    dry_run_rows: int
    ready_rows: int
    warning_rows: int
    reviewed_shared_stock_rows_included: int
    upload_rows: int
    amazon_sp_api_called: bool
    dry_run_report: str
    upload_file: str
    map_unmatched_report: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Amazon listing map, dry-run report, and Price/Inventory upload file."
    )
    parser.add_argument("--amazon-export", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--prefer", default="ralawise", choices=("ralawise", "uneek"))
    parser.add_argument("--include-reviewed-shared-stock", action="store_true")
    parser.add_argument("--listing-map", default=DEFAULT_LISTING_MAP, type=Path)
    parser.add_argument("--marketplace-id", default=DEFAULT_MARKETPLACE_ID)
    parser.add_argument("--fulfillment-channel", default=DEFAULT_FULFILLMENT_CHANNEL)
    return parser.parse_args()


def detect_delimiter(path: Path) -> str:
    sample = path.read_text(encoding="utf-8-sig", errors="ignore")[:4096]
    return "\t" if "\t" in sample else ","


def normalize_header(value: str) -> str:
    return str(value).strip().lower()


def load_amazon_export(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Amazon export not found: {path}")

    delimiter = detect_delimiter(path)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            raise RuntimeError("Amazon export has no header row.")

        field_lookup = {normalize_header(name): name for name in reader.fieldnames}
        required = {"sku", "asin", "price", "quantity"}
        missing = required - set(field_lookup)

        if missing:
            raise RuntimeError(f"Amazon export missing required columns: {sorted(missing)}")

        rows = []
        for raw in reader:
            rows.append(
                {
                    "sku": (raw.get(field_lookup["sku"]) or "").strip(),
                    "asin": (raw.get(field_lookup["asin"]) or "").strip(),
                    "price": (raw.get(field_lookup["price"]) or "").strip(),
                    "quantity": (raw.get(field_lookup["quantity"]) or "").strip(),
                    "marketplace_id": (
                        raw.get(field_lookup.get("marketplace_id", ""))
                        or raw.get(field_lookup.get("marketplace-id", ""))
                        or ""
                    ).strip(),
                    "fulfillment_channel": (
                        raw.get(field_lookup.get("fulfillment_channel", ""))
                        or raw.get(field_lookup.get("fulfillment-channel", ""))
                        or ""
                    ).strip(),
                }
            )

    return pd.DataFrame(rows)


def convert_size_token(token: str) -> str:
    token = str(token).strip().upper()
    return {
        "2X": "2XL",
        "3X": "3XL",
        "4X": "4XL",
        "2XL": "2XL",
        "3XL": "3XL",
        "4XL": "4XL",
        "LG": "LR",
        "L": "LR",
        "MD": "MD",
        "M": "MD",
        "SM": "SM",
        "S": "SM",
        "XL": "XL",
        "XS": "XS",
    }.get(token, token)


def strip_known_prefixes(sku: str) -> str:
    sku = str(sku).strip().upper()
    sku = re.sub(r"^MAN-AMZ\d+-", "", sku)

    for prefix in ("PERSOO-", "PERSOFR-", "PERSO-", "PERS-", "KCD7TK-"):
        if sku.startswith(prefix):
            return sku[len(prefix):]

    return sku


def transform_canonical_sku(amazon_seller_sku: str) -> tuple[str, str]:
    sku = str(amazon_seller_sku).strip().upper()

    if not sku:
        return "", "missing_amazon_seller_sku"

    if sku in PARENT_ONLY_SKUS:
        return "", "excluded_parent_sku"

    # TC013 design SKUs: D1/D2/D3 are design variants, not stock variants.
    # R237X uses normal garment size suffixes, not UC/Uneek LR-MD-SM suffixes.
    # Examples:
    # KCD7TK-R237X-BKBK-L -> R237XBKBKL
    # KCD7TK-R237X-BKBK-M -> R237XBKBKM
    # KCD7TK-R237X-NYNYE-2XL -> R237XNYYE2XL
    r237x_match = re.match(r"^KCD7TK-R237X-([A-Z0-9]+)-(2XL|3XL|4XL|L|M|S|XL|XS)$", sku)
    if r237x_match:
        colour_code, size_token = r237x_match.groups()
        if colour_code == "NYNYE":
            colour_code = "NYYE"
        return f"R237X{colour_code}{size_token}", "r237x"
    tc_match = re.match(r"^MAN-AMZ\d+-TC013-(BLK|NVY)-D[123]$", sku)
    if tc_match:
        colour = {"BLK": "BLAC", "NVY": "NAVY"}[tc_match.group(1)]
        return f"TC013{colour}", "tc013_shared_stock"

    # Proven manual rules.
    special_rules = [
        (r"^PERSO-UC504YL-(.+)$", "504YW", "uc504_yellow_to_yw"),
        (r"^PERSO-UC620BG-(.+)$", "620BK", "uc620_bg_to_bk"),
        (r"^PERSOO-UC921CG-(.+)$", "921CY", "uc921_cg_to_cy"),
        (r"^PERSOO-UC921NV-(.+)$", "921NY", "uc921_nv_to_ny"),
    ]

    for pattern, canonical_base, rule_name in special_rules:
        match = re.match(pattern, sku)
        if match:
            return f"{canonical_base}{convert_size_token(match.group(1))}", rule_name

    # Generic PERSO/PERSOO UC products.
    # Examples:
    # PERSO-UC101BG-2X -> 101BG2XL
    # PERSOO-UC106NY-LG -> 106NYLR
    # PERSOO-UC921AQ-2X -> 921AQ2XL
    uc_match = re.match(r"^(?:PERSO|PERSOO)-UC(\d+)([A-Z]+)-(.+)$", sku)
    if uc_match:
        product_code, colour_code, size_token = uc_match.groups()
        return f"{product_code}{colour_code}{convert_size_token(size_token)}", "generic_uc_product"

    # Compact PERS-612 SKUs.
    # Examples:
    # PERS-612BK2X -> 612BK2XL
    # PERS-612BKLG -> 612BKLR
    compact_612 = re.match(r"^PERS-612([A-Z]{2})(2X|3X|4X|LG|MD|SM|XL|XS)$", sku)
    if compact_612:
        colour_code, size_token = compact_612.groups()
        return f"612{colour_code}{convert_size_token(size_token)}", "compact_612"

    # PERSOFR one-size caps.
    # Example:
    # PERSOFR-BC15C-BKBR-ONE -> BC15CBKBR
    one_size = re.match(r"^PERSOFR-([A-Z0-9]+)-([A-Z0-9]+)-ONE$", sku)
    if one_size:
        product_code, colour_code = one_size.groups()
        return f"{product_code}{colour_code}", "one_size"

    # Generic fallback:
    # remove known prefix, remove trailing -ONE, convert final size token if present,
    # then remove hyphens.
    stripped = strip_known_prefixes(sku)

    if stripped.endswith("-ONE"):
        stripped = stripped[:-4]
        return stripped.replace("-", ""), "generic_one_size"

    if "-" in stripped:
        base, maybe_size = stripped.rsplit("-", 1)
        converted_size = convert_size_token(maybe_size)
        known_size_tokens = {
            "2X", "3X", "4X", "2XL", "3XL", "4XL",
            "LG", "L", "MD", "M", "SM", "S", "XL", "XS",
        }

        if maybe_size.upper() in known_size_tokens:
            return f"{base.replace('-', '')}{converted_size}", "generic_size_suffix"

    return stripped.replace("-", ""), "generic_remove_prefix_hyphens"


def stock_keys(stock_df: pd.DataFrame) -> set[str]:
    stock_lookup = build_stock_lookup(stock_df)
    return set(stock_lookup["canonical_sku_norm"].astype(str).str.strip().str.upper())


def build_listing_map_from_export(
    export_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    *,
    marketplace_id: str,
    fulfillment_channel: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    keys = stock_keys(stock_df)
    map_rows = []
    unmatched_rows = []

    for _, row in export_df.iterrows():
        amazon_sku = str(row.get("sku", "")).strip()
        canonical_sku, rule_used = transform_canonical_sku(amazon_sku)
        canonical_norm = canonical_sku.strip().upper()

        row_marketplace_id = str(row.get("marketplace_id", "")).strip() or marketplace_id
        row_fulfillment_channel = str(row.get("fulfillment_channel", "")).strip() or fulfillment_channel

        base_row = {
            "amazon_seller_sku": amazon_sku,
            "canonical_sku": canonical_sku,
            "asin": str(row.get("asin", "")).strip(),
            "price": str(row.get("price", "")).strip(),
            "marketplace_id": row_marketplace_id,
            "fulfillment_channel": row_fulfillment_channel,
            "rule_used": rule_used,
            "reason": "",
        }

        if rule_used == "excluded_parent_sku":
            unmatched_rows.append({**base_row, "reason": "excluded_parent_sku"})
            continue

        if not canonical_norm:
            unmatched_rows.append({**base_row, "reason": rule_used})
            continue

        if canonical_norm not in keys:
            unmatched_rows.append(
                {**base_row, "reason": "canonical_sku_not_found_in_supplier_stock"}
            )
            continue

        map_rows.append(
            {
                "canonical_sku": canonical_sku,
                "amazon_seller_sku": amazon_sku,
                "asin": str(row.get("asin", "")).strip(),
                "marketplace_id": row_marketplace_id,
                "fulfillment_channel": row_fulfillment_channel,
                "enabled": "true",
            }
        )

    return (
        pd.DataFrame(map_rows, columns=LISTING_MAP_COLUMNS),
        pd.DataFrame(
            unmatched_rows,
            columns=[
                "amazon_seller_sku",
                "canonical_sku",
                "asin",
                "price",
                "marketplace_id",
                "fulfillment_channel",
                "rule_used",
                "reason",
            ],
        ),
    )


def price_lookup(export_df: pd.DataFrame) -> dict[str, str]:
    prices = {}
    for _, row in export_df.iterrows():
        sku = str(row.get("sku", "")).strip().upper()
        if sku:
            prices[sku] = str(row.get("price", "")).strip()
    return prices


def upload_statuses(include_reviewed_shared_stock: bool) -> set[str]:
    statuses = {"ready"}
    if include_reviewed_shared_stock:
        statuses.add("warning_duplicate_canonical_sku")
    return statuses


def build_price_inventory_upload(
    dry_run_df: pd.DataFrame,
    export_df: pd.DataFrame,
    *,
    include_reviewed_shared_stock: bool,
) -> tuple[pd.DataFrame, int]:
    prices = price_lookup(export_df)
    allowed = upload_statuses(include_reviewed_shared_stock)

    output_rows = []
    reviewed_warning_rows = 0

    for _, row in dry_run_df.iterrows():
        status = str(row.get("status", "")).strip()
        canonical_sku = str(row.get("canonical_sku", "")).strip().upper()

        if status not in allowed:
            continue

        if status == "warning_duplicate_canonical_sku":
            if canonical_sku not in REVIEWED_SHARED_STOCK_SKUS:
                continue
            reviewed_warning_rows += 1

        amazon_sku = str(row.get("amazon_seller_sku", "")).strip()
        qty = row.get("proposed_amazon_qty")

        if not amazon_sku or pd.isna(qty) or str(qty).strip() == "":
            continue

        output_rows.append(
            {
                "sku": amazon_sku,
                "price": prices.get(amazon_sku.upper(), ""),
                "minimum-seller-allowed-price": "",
                "maximum-seller-allowed-price": "",
                "quantity": str(int(float(qty))),
                "handling-time": "",
                "fulfillment-channel": "",
            }
        )

    return pd.DataFrame(output_rows, columns=PRICE_INVENTORY_COLUMNS), reviewed_warning_rows


def write_price_inventory_upload(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep="\t", index=False, quoting=csv.QUOTE_MINIMAL)


def write_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_supplier_stock(prefer: str) -> pd.DataFrame:
    from stock_sources import build_combined_stock_df

    return build_combined_stock_df(prefer=prefer, progress=print)


def parent_excluded_count(map_unmatched_df: pd.DataFrame) -> int:
    if map_unmatched_df.empty:
        return 0
    return int((map_unmatched_df["reason"] == "excluded_parent_sku").sum())


def count_statuses(dry_run_df: pd.DataFrame, statuses: Iterable[str]) -> int:
    return int(dry_run_df["status"].astype(str).isin(set(statuses)).sum())


def main() -> int:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Loading Amazon export: {args.amazon_export}")
    export_df = load_amazon_export(args.amazon_export)

    print(f"Loading supplier stock (prefer={args.prefer})...")
    stock_df = load_supplier_stock(args.prefer)

    listing_map_df, map_unmatched_df = build_listing_map_from_export(
        export_df,
        stock_df,
        marketplace_id=args.marketplace_id,
        fulfillment_channel=args.fulfillment_channel,
    )

    args.listing_map.parent.mkdir(parents=True, exist_ok=True)
    listing_map_df.to_csv(args.listing_map, index=False)

    dry_run_result = build_amazon_dry_run(stock_df, listing_map_df)
    dry_run_df = dry_run_result["report"]

    args.out_dir.mkdir(parents=True, exist_ok=True)

    dry_run_path = args.out_dir / f"amazon_stock_dry_run_{timestamp}.csv"
    upload_path = args.out_dir / f"amazon_price_inventory_ALL_{timestamp}.txt"
    map_unmatched_path = args.out_dir / f"amazon_map_unmatched_{timestamp}.csv"
    summary_path = args.out_dir / f"amazon_build_upload_summary_{timestamp}.json"

    dry_run_df.to_csv(dry_run_path, index=False)
    map_unmatched_df.to_csv(map_unmatched_path, index=False)

    upload_df, reviewed_warning_rows = build_price_inventory_upload(
        dry_run_df,
        export_df,
        include_reviewed_shared_stock=args.include_reviewed_shared_stock,
    )

    write_price_inventory_upload(upload_df, upload_path)

    warning_rows = int(dry_run_df["status"].astype(str).str.startswith("warning_").sum())

    summary = BuildUploadSummary(
        amazon_export=str(args.amazon_export),
        listing_map=str(args.listing_map),
        out_dir=str(args.out_dir),
        prefer=args.prefer,
        marketplace_id=args.marketplace_id,
        fulfillment_channel=args.fulfillment_channel,
        include_reviewed_shared_stock=bool(args.include_reviewed_shared_stock),
        amazon_export_rows=int(len(export_df)),
        listing_map_rows=int(len(listing_map_df)),
        map_unmatched_rows=int(len(map_unmatched_df)),
        parent_excluded_rows=parent_excluded_count(map_unmatched_df),
        dry_run_rows=int(len(dry_run_df)),
        ready_rows=count_statuses(dry_run_df, {"ready"}),
        warning_rows=warning_rows,
        reviewed_shared_stock_rows_included=int(reviewed_warning_rows),
        upload_rows=int(len(upload_df)),
        amazon_sp_api_called=False,
        dry_run_report=str(dry_run_path),
        upload_file=str(upload_path),
        map_unmatched_report=str(map_unmatched_path),
    )

    write_json(asdict(summary), summary_path)

    print("Amazon build/upload files complete.")
    print(f"Amazon export rows: {summary.amazon_export_rows}")
    print(f"Listing map rows: {summary.listing_map_rows} -> {args.listing_map}")
    print(f"Parent excluded rows: {summary.parent_excluded_rows}")
    print(f"Map unmatched rows: {summary.map_unmatched_rows} -> {map_unmatched_path}")
    print(f"Dry-run rows: {summary.dry_run_rows} -> {dry_run_path}")
    print(f"Ready rows: {summary.ready_rows}")
    print(f"Warning rows: {summary.warning_rows}")
    print(f"Reviewed shared-stock warning rows included: {summary.reviewed_shared_stock_rows_included}")
    print(f"Upload rows: {summary.upload_rows} -> {upload_path}")
    print(f"Amazon API calls: {summary.amazon_sp_api_called}")
    print(f"Summary: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
