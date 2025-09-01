# cli.py
import argparse
from secrets import STORE_PROFILES
from constants import BATCH_SIZE_DEFAULT
from core import run_update

def main():
    parser = argparse.ArgumentParser(description="Shopify Stock Updater")
    parser.add_argument("--store", required=True, choices=STORE_PROFILES.keys())
    parser.add_argument("--sku-prefix", default=None)
    parser.add_argument("--product-type", action="append")
    parser.add_argument("--location-name", default=None)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT)
    parser.add_argument("--map-csv", default=None)
    parser.add_argument("--stock-csv", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--build-map", action="store_true")
    args = parser.parse_args()

    report_df, summary = run_update(
        store=args.store,
        sku_prefix=args.sku_prefix,
        product_types=args.product_type,
        location_name=args.location_name,
        batch_size=args.batch_size,
        map_csv=args.map_csv,
        stock_csv_path=args.stock_csv,
        dry_run=args.dry_run,
        build_map=args.build_map,
        store_profiles=STORE_PROFILES
    )

    print(summary)

if __name__ == "__main__":
    main()
