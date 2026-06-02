![CI](https://github.com/Galuyoo/shopify_updater/actions/workflows/ci.yml/badge.svg)

# Multi-Source Inventory Reconciliation Engine

A headless, multi-tenant inventory synchronization engine designed to
reconcile heterogeneous supplier stock feeds into distributed e-commerce
storefronts.

This system was independently engineered and deployed within a warehouse
operational environment to provide continuous, automated stock
synchronization across multiple Shopify stores.

------------------------------------------------------------------------

## 🚀 Problem Context

The warehouse operated multiple storefronts dependent on two separate
suppliers.

Technical challenges included:

-   Inconsistent SKU naming conventions across suppliers
-   Mismatched SKU namespaces between suppliers and storefronts
-   Tens of thousands of product variants per store
-   Risk of destructive or incorrect stock overwrites
-   Manual reconciliation bottlenecks
-   Need for continuous 24/7 execution

This project was designed to solve these problems through architectural
decoupling and automation.

------------------------------------------------------------------------

## 🧠 Core Technical Innovations

### 1️⃣ SKU Namespace Decoupling & Translation Layer

A custom reconciliation layer was built to:

-   Establish a canonical internal SKU namespace
-   Translate supplier-specific SKUs into internal identifiers
-   Normalize and validate SKU mappings
-   Isolate unmatched identifiers
-   Prevent incorrect stock propagation

This allows supplier systems and storefront systems to evolve
independently while maintaining synchronization integrity.

------------------------------------------------------------------------

### 2️⃣ Single-Fetch Multi-Store Cycle Architecture

Instead of querying suppliers per store, the engine:

-   Fetches supplier stock once per execution cycle
-   Reuses normalized data across all stores
-   Minimizes API calls and rate-limit pressure
-   Enables scalable multi-tenant synchronization

This design significantly improves efficiency and resilience.

------------------------------------------------------------------------

### 3️⃣ Autonomous 24/7 Execution Model

The system was operationalized as a continuously running service:

-   Headless execution (no UI dependency)
-   Scheduled cycle-based execution
-   Per-cycle reconciliation reporting
-   Structured diagnostic attachments
-   Store-level isolation to prevent cross-impact

The engine runs autonomously within warehouse infrastructure and
provides operational visibility through automated email reporting.

------------------------------------------------------------------------

## 🏗 High-Level Architecture

Supplier A API ----\
\
--\> Stock Ingestion Layer / Supplier B API ----/

            ↓

SKU Translation / Normalization Layer

            ↓

Reconciliation Engine - mapping validation - difference detection -
safety checks

            ↓

Batch Update Executor (Per-Store Isolation)

            ↓

Reporting & Notification Layer

------------------------------------------------------------------------

## 🔒 Safety & Reliability Features

-   Validation before stock overwrite
-   Isolation between storefront executions
-   Unmatched SKU reporting
-   Failed update tracking
-   Structured cycle summaries
-   Error containment per store

------------------------------------------------------------------------

## 📊 Operational Characteristics

-   Processes tens of thousands of variants per store
-   Designed for continuous 24/7 execution
-   Multi-tenant store isolation
-   Supplier API-efficient architecture
-   Structured reporting per execution cycle

------------------------------------------------------------------------

## 📂 Documentation

-   docs/INNOVATION.md -- Detailed technical innovation analysis
-   docs/ARCHITECTURE.md -- System architecture and design principles
-   docs/IMPACT.md -- Operational and engineering impact

------------------------------------------------------------------------

## 🛠 Technology Stack

-   Python
-   Shopify Admin API
-   Supplier API integrations
-   Pandas (data reconciliation layer)
-   Windows Task Scheduler (deployment orchestration)

------------------------------------------------------------------------

## ⚙ Execution Example

*/30 * \* \* \* python service_runner.py

Runs the reconciliation cycle every 30 minutes.

------------------------------------------------------------------------

## 📌 Engineering Scope

This repository demonstrates:

-   Cross-system namespace reconciliation design
-   Multi-source stock ingestion architecture
-   Multi-tenant execution isolation
-   API-efficient synchronization strategy
-   Real-world autonomous deployment

This is infrastructure-level automation engineering rather than a simple
update script.

------------------------------------------------------------------------

## Amazon Stock Dry Run

The first Amazon phase is report-only. It reads the existing supplier stock
feeds, reads an Amazon listing map CSV, matches `canonical_sku` to
`amazon_seller_sku`, and writes dry-run reports. It does not call Amazon
SP-API and does not require Amazon credentials.

Run from the project root:

```bash
python -m app.amazon_dry_run --listing-map data/amazon_listing_map.csv --out-dir reports/amazon --prefer ralawise
```

The real listing map is local-only and should not be committed:

```text
data/amazon_listing_map.csv
```

Use this committed template for the required columns:

```text
data/amazon_listing_map.example.csv
```

Required columns:

```text
canonical_sku,amazon_seller_sku,asin,marketplace_id,fulfillment_channel,enabled
```

Generated reports:

-   `amazon_stock_dry_run_YYYYMMDD_HHMMSS.csv`
-   `amazon_unmatched_skus_YYYYMMDD_HHMMSS.csv`
-   `amazon_duplicate_amazon_seller_sku_YYYYMMDD_HHMMSS.csv`
-   `amazon_duplicate_canonical_sku_YYYYMMDD_HHMMSS.csv`
-   `amazon_stock_dry_run_summary_YYYYMMDD_HHMMSS.json`

Duplicate `amazon_seller_sku` rows are errors. Duplicate `canonical_sku`
rows are warnings because one supplier SKU may intentionally feed multiple
Amazon listings.

## Amazon Price/Inventory Upload Build

Place the raw Seller Central export here:

```text
data/amazon_export_raw.txt
```

The export must include at least these columns:

```text
sku, asin, price, quantity
```

Build the listing map, dry-run report, and official Price/Inventory upload
file from the project root:

```bash
python -m app.amazon_build_upload --amazon-export data/amazon_export_raw.txt --out-dir reports/amazon --prefer ralawise --include-reviewed-shared-stock
```

The final Seller Central upload file is created in:

```text
reports/amazon/amazon_price_inventory_ALL_YYYYMMDD_HHMMSS.txt
```

Upload that `.txt` manually through Seller Central Price/Inventory upload.
This command does not call Amazon SP-API.

Do not commit real Amazon exports, generated listing maps, dry-run reports,
or upload files:

```text
data/amazon_export_raw.txt
data/amazon_listing_map.csv
reports/amazon/
```

## StoreFeeder Stock Update Export

This tool builds strict, warehouse-safe StoreFeeder stock-location update files for manual upload/testing. It does not call the StoreFeeder API and does not update StoreFeeder directly.

Inputs:

- latest full StoreFeeder product export after supplier/location mapping
- `supplier_mapping.csv`
- Ralawise stock CSV
- Uneek stock CSV, if available

Required mapping CSV columns:

```text
storefeeder_sku,supplier,supplier_sku,stock_location
```

Ralawise stock CSV columns:

```text
SKU,free,DiscontinuedStatus
```

Uneek stock CSV/API-output can use `ItemNo` plus `Stock`/`StockLevel`, or normalized `sku,free` output.

Default mode is dry-run. It prints the validation gate, ready-row preview, quarantine preview, and API payload preview without writing files:

```powershell
python scripts/build_storefeeder_stock_update.py --storefeeder-export data/storefeeder_products.xlsx --supplier-mapping data/supplier_mapping.csv --ralawise-stock data/RALAWISE_stock_lvl.csv --uneek-stock data/Uneek_stock_levels.csv --out-dir reports/storefeeder --buffer 0 --max-stock 5
```

Add `--export` to write files. The main CSV/XLSX outputs include only `update_ready` rows. Quarantined rows are never included in the API payload preview.

Stock rule:

```text
final_stock = min(max(supplier_stock - buffer, 0), max_stock)
```

Defaults:

- `buffer = 0`
- `max_stock = 5`
- `--missing-as-zero` is false by default
- `--max-quarantine-rate = 0.02`
- `--max-stock-file-age-hours = 24`

Strict safety behavior:

- preserves StoreFeeder columns and original column order in ready exports
- updates only `Stock Location Current Inventories`
- treats `supplier_mapping.csv` as the update universe; StoreFeeder rows outside the mapping are skipped, not quarantined
- keeps `Suppliers`, `Supplier SKUs`, and `Stock Locations` exactly as they are in the StoreFeeder export
- aligns pipe-separated inventory values to the matching existing stock location
- blocks ready export if any original column other than `Stock Location Current Inventories` changes
- blocks the live/API gate when quarantine rate is too high against mapped update candidates, stock files are stale, duplicate/conflict rows exist, or update count is unusual compared with a previous run

Quarantine categories include:

```text
missing_supplier_mapping
missing_supplier_sku_in_stock_feed
duplicate_storefeeder_sku
duplicate_supplier_mapping_conflict
invalid_supplier_name
stock_location_supplier_mismatch
invalid_stock_quantity
negative_stock_quantity
colour_size_validation_failed
ambiguous_match
stale_supplier_stock_file
discontinued_supplier_sku
unknown_error
```

Each output includes a `confidence_status` column with one of:

```text
update_ready
quarantined
skipped
```

Generated files:

- `storefeeder_stock_updates_ready.csv`
- `storefeeder_stock_updates_ready.xlsx`
- `quarantine_review.csv`
- `zero_stock_updates.csv`
- `missing_supplier_skus.csv`
- `skipped_not_in_supplier_mapping.csv`
- `mapping_skus_missing_from_storefeeder_export.csv`
- `validation_summary.csv`
- `api_payload_preview.csv`

Useful flags:

- `--missing-as-zero`: treat missing supplier stock as zero instead of quarantining it
- `--allow-quarantine-update`: diagnostic override for report/export gates; quarantined rows still do not enter API payload preview
- `--max-quarantine-rate 0.02`: block when quarantine rate is above the configured threshold
- `--max-stock-file-age-hours 24`: block when supplier stock files are stale
- `--previous-update-ready-count N`: block unusually low/high update counts compared with a known previous run
- `--allow-unusual-update-count`: do not block on unusual update count
- `--live-api`: runs the live gate and produces API payload preview only; no StoreFeeder API call is made

Example alignment:

```text
Stock Locations = Ralawise|Uneek
Stock Location Current Inventories = 0|0
```

Updating Uneek to 5 produces:

```text
Stock Location Current Inventories = 0|5
```

The old 3-column export is still available only for debugging:

```powershell
python scripts/build_storefeeder_stock_update.py --legacy-3-column-export --supplier-mapping data/supplier_mapping.csv --ralawise-stock data/RALAWISE_stock_lvl.csv --out-dir reports/storefeeder
```


