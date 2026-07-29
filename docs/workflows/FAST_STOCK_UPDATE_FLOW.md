# Fast Supplier Stock Update Flow

## Status

This document records the current structure of `scripts/run_supplier_stock_fast_update.py` before architectural extraction.

It describes existing behaviour. It does not propose new mapping rules or stock policies.

The protected baseline is:

- Tag: `mapping-baseline-2026-07-29`
- Baseline commit: `335eebbb3933851421a2104c5292d43123d85a0f`

## Production entry point

```text
scripts/run_supplier_stock_fast_update.py
```

The script remains the production-facing orchestrator until extracted components have characterization tests and output-equivalence checks.

## Current high-level flow

```text
CLI and configuration
        |
        v
Optional supplier-feed refresh
        |
        v
Load active target manifest
        |
        v
Load live StoreFeeder products
        |
        v
Reconcile runtime Product IDs
        |
        v
Load Ralawise and Uneek stock
        |
        v
Build supplier stock lookup
        |
        v
Build stock payload preview
        |
        +-----------------------------+
        |                             |
        v                             v
Supplier quantity updates      Stock-location safety planning
        |                             |
        v                             v
Supplier API batches           Stock-location API batches
        |                             |
        +--------------+--------------+
                       |
                       v
              Response reporting
                       |
                       v
             Summary and stale guards
                       |
                       v
            Optional live verification
```

## Authoritative dependencies

The entry point currently imports active logic from:

- `src.stock_mapping`
- `src.stock_rules`
- `src.storefeeder_api`
- `src.storefeeder_stock_export`
- `scripts.export_storefeeder_products`

Historical, backup, `before`, `pre`, and step-copy scripts are not architectural authorities.

## Subsystems

### 1. CLI and orchestration

Functions:

- `parse_args`
- `main`
- `_out_dir_from_argv`

Responsibilities:

- Parse command-line options.
- Create the run output directory.
- Load environment configuration.
- Coordinate all workflow stages.
- Decide between preview and live execution.
- Write top-level run artifacts.

`main` currently spans lines 171-389 and is the central orchestration boundary.

### 2. Product-ID reconciliation

Functions:

- `_load_live_products_for_reconciliation`
- `_reconcile_runtime_product_ids`
- `_product_id_quarantine_row`
- `_backup_and_write_targets`

Responsibilities:

- Load current StoreFeeder product ID and SKU pairs.
- Match target rows to current runtime Product IDs.
- Quarantine ambiguous or missing matches.
- Update the target manifest only when explicitly permitted.

This subsystem must preserve SKU matching and quarantine behaviour exactly during extraction.

### 3. Target and supplier payload preparation

Functions:

- `_active_targets`
- `build_fast_payload_preview`
- `_invalid_target_reason`
- `_supplier_info_only_payload_preview`

Responsibilities:

- Select active target rows.
- Join targets to supplier quantities.
- Apply current stock calculations.
- Enforce stock-strategy behaviour.
- Produce valid payload rows and skipped-row diagnostics.
- Build supplier-information-only update previews.

`build_fast_payload_preview` spans lines 514-675 and is the main business-rule concentration in the entry-point script.

### 4. Stock-location safety

Functions:

- `_default_clear_stock_locations`
- `_stock_location_payload_preview`
- `_stock_location_payload_row`
- `_zero_other_locations_preview`
- `_zero_candidate_locations`
- `_contains_casefold`
- `_zero_location_skip_row`
- `_zero_preview_to_stock_location_payload`
- `_pipe_values`

Responsibilities:

- Determine the authoritative supplier location.
- Protect warehouse-only and manual-inventory lanes.
- Identify non-authoritative locations eligible for zeroing.
- Prevent Warehouse Stock from being changed without the required strategy and permission.
- Produce stock-location update and skip previews.

This subsystem contains safety policy and must not be mixed with general API transport during the first extraction.

### 5. API batching and response normalization

Functions:

- `_send_fast_stock_batches`
- `_append_supplier_response_rows`
- `_send_fast_stock_location_batches`
- `_supplier_item_report_row`
- `_append_stock_location_response_rows`
- `_stock_location_item_report_row`
- `_stock_location_metadata`
- `_stock_location_item_metadata`
- `_response_failed`
- `_response_error`

Responsibilities:

- Divide supplier and stock-location updates into batches.
- Submit batches through the StoreFeeder client.
- Normalise item-level success and failure responses.
- Retain metadata needed to connect responses to preview rows.
- Write request and response reports.

Transport extraction must preserve batch sizes, payload shapes, retry behaviour, and report columns.

### 6. Reporting and runtime guards

Functions:

- `_live_report_counts`
- `_summary_frame`
- `_stale_fast_sync_guard`
- `_latest_report_file`
- `_file_mtime`
- `_channel_decorated_count`
- `_supplier_info_only_target_count`
- `_explicit_stock_location_allowed_count`
- `_skip_reason_count`
- `_append_reason`
- `_csv_count`
- `_supplier_info_only_update_count`

Responsibilities:

- Calculate run metrics.
- Build the summary report.
- Detect stale or unexpectedly reduced target manifests.
- Count protected strategies and explicit permissions.
- Derive update counts from generated reports.
- Add diagnostic reasons without discarding existing reasons.

### 7. Live verification

Functions:

- `_verify_live_sample`
- `_live_stock_location_quantity`

Responsibilities:

- Read a controlled sample back from StoreFeeder.
- Compare expected and observed stock-location quantities.
- Produce verification and mismatch reports.

Live verification remains optional and must stay separated from payload construction.

### 8. Shared runtime utilities

Functions:

- `_first_text`
- `_truthy`
- `_require_columns`
- `_run`
- `_load_env_file`
- `_write_exception_artifacts`

Responsibilities:

- Normalise common scalar values.
- Validate tabular inputs.
- Execute controlled subprocesses.
- Load environment variables.
- Preserve failure artifacts.

## Active stock strategies

### `supplier_synced_inventory`

Supplier stock is authoritative for the permitted supplier location.

The workflow may also zero eligible non-authoritative locations when explicit stock-location permission is present.

### `warehouse_only`

Warehouse inventory remains authoritative.

Supplier stock-location updates and automated zeroing are protected from modifying this lane.

### `supplier_info_only_manual_inventory`

Supplier metadata may be updated while manual inventory remains authoritative.

Automated inventory and stock-location changes are skipped for this lane.

## Active suppliers

- Ralawise
- Uneek

## Safety-sensitive areas

1. Runtime Product ID reconciliation.
2. Invalid-target quarantine reasons.
3. Supplier-safe-stock calculation.
4. Strategy-specific inventory behaviour.
5. Stock-location keep and clear decisions.
6. Warehouse Stock protection.
7. Explicit stock-location permission.
8. API payload shapes.
9. Item-level response interpretation.
10. Stale-target guard thresholds.
11. Live verification comparisons.

## Extraction order

1. Pure response-normalisation helpers.
2. Pure reporting-count helpers.
3. Product-ID reconciliation.
4. API batching and response reporting.
5. Stock-location safety planning.
6. Supplier payload preparation.
7. Reduction of `main` to orchestration only.

Payload preparation and stock-location safety are intentionally late because they contain the highest concentration of business and safety rules.

## Change rule

For every extraction:

1. Add characterization tests against the current implementation.
2. Save representative before-output fixtures.
3. Move functions without changing their decisions.
4. Compare generated DataFrames, CSV columns, row counts, skip reasons, and payload dictionaries.
5. Commit architectural movement separately from rule changes.
6. Do not copy logic from historical scripts.

