# Mapping Source of Truth

## Status

This document records the authoritative mapping implementation before repository refactoring.

Older scripts, backups and historical copies must not be used to redefine current mapping behaviour.

## Authoritative files

The following files must be reviewed as the current mapping implementation:

- `scripts/listing_mapping_planner.py`
- `scripts/run_supplier_stock_fast_update.py`
- `src/stock_mapping.py`
- `src/stock_rules.py`

Additional files may be added only after their role has been verified from current production imports and behaviour.

## Historical files

The following are not mapping authorities:

- Files containing `.backup`
- Files containing `before_`
- Files containing `pre_`
- Files containing `step<number>_backup`
- Files under `.history/`
- Files under `backups/`
- Files under `archive/`
- Superseded parser and suffix patch scripts

These files may document previous attempts, but their assumptions must not be copied into production code.

## Refactoring rule

During refactoring:

1. Preserve current outputs.
2. Preserve current mapping precedence.
3. Preserve current supplier and warehouse lane behaviour.
4. Do not merge rules from older scripts.
5. Do not expand matching logic without a separate reviewed change.
6. Extract code without rewriting its decisions.
7. Compare outputs before and after every extraction.

## Mapping policy

The current catalogue lanes are:

### supplier_managed

- Supplier retained
- Supplier SKU retained
- Supplier stock retained
- Inventory forced to zero

### parent

- Inventory zero
- Supplier fields blank
- Supplier stock zero

### warehouse_only

- Inventory preserved
- Supplier fields blank
- Supplier stock zero

## Active suppliers

Only these suppliers are active:

- Ralawise
- Uneek

## Change control

Any mapping-rule change must be separated from architectural refactoring and committed independently.
