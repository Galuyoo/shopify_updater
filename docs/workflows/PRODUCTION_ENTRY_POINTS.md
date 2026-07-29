# Approved Production Entry Points

These files are treated as operational entry points and must not be moved or substantially refactored without validation.

| Entry point | Purpose | Current status |
|---|---|---|
| `app/service_runner.py` | Existing scheduled service runner | Production |
| `scripts/run_supplier_stock_fast_update.py` | Supplier-driven stock update | Production candidate under validation |
| `scripts/run_stock_sync_self_heal.py` | Stock synchronisation repair workflow | Approved workflow |
| `scripts/run_new_product_onboarding_delta.py` | New-product onboarding workflow | Approved workflow |
| `scripts/clean_product_setup_pipeline.py` | Clean product setup process | Approved workflow |
| `scripts/product_family_pipeline.py` | Product-family processing | Approved workflow |
| `scripts/clean_product_setup_pipeline.py` | Clean product setup process | Approved workflow |
| `scripts/product_family_pipeline.py` | Product-family processing | Approved workflow |
| `scripts/warehouse_stock_pipeline.py` | Warehouse inventory processing | Approved workflow |

## Change rule

Before changing an approved entry point:

1. Record current command-line behaviour.
2. Run its dry-run or validation process.
3. Save representative output.
4. Add or update tests.
5. Make one isolated change.
6. Compare output against the baseline.
