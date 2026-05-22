# Amazon Upload Review Checklist

Before uploading amazon_price_inventory_ALL_*.txt to Seller Central, review the generated reports.

## Required checks

- Open the latest amazon_stock_dry_run_*.csv.
- Confirm unmatched rows are expected.
- Confirm parent-only SKUs are excluded.
- Confirm warning rows are reviewed shared-stock SKUs only.
- Confirm upload row count matches expectation.
- Confirm prices are not blank unless intentionally blank.
- Confirm quantities look reasonable.
- Confirm amazon_sp_api_called is false in the summary JSON.
- Upload manually through Seller Central only after review.

## Files to review

reports/amazon/amazon_stock_dry_run_*.csv
reports/amazon/amazon_map_unmatched_*.csv
reports/amazon/amazon_build_upload_summary_*.json
reports/amazon/amazon_price_inventory_ALL_*.txt

## Safety rule

The Amazon flat-file upload must be treated as a reviewed output, not an automatic live stock update.

Do not upload the generated .txt file if:

- unmatched SKU count is unexpected
- warning count is unexpected
- parent SKUs appear in the upload file
- upload row count is much higher or lower than expected
- supplier stock feed failed or looks incomplete
