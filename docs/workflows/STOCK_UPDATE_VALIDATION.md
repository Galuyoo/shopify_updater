# Stock Updater Validation

## Objective

Verify that the stock updater calculates and publishes the correct stock for every relevant StoreFeeder product.

The first validation pass must be read-only or dry-run.

## Catalogue baseline

| Classification | Expected rows |
|---|---:|
| Supplier-managed | 94,738 |
| Ralawise | 89,207 |
| Uneek | 5,531 |
| Warehouse-only | 6,356 |
| Parent | 4,220 |
| Total | 105,314 |

## Expected behaviour

### Supplier-managed

- Supplier must be Ralawise or Uneek.
- Supplier SKU must exist.
- Supplier stock may be refreshed.
- Unsupported suppliers must be reported.

### Warehouse-only

- Supplier must remain blank.
- Supplier SKU must remain blank.
- Supplier stock must remain zero.
- Warehouse inventory must remain authoritative.

### Parent

- Inventory must remain zero.
- Supplier must remain blank.
- Supplier SKU must remain blank.
- Supplier stock must remain zero.

## Validation checklist

### Entry point

- [ ] Identify the actual production command.
- [ ] Review app/service_runner.py.
- [ ] Review scripts/run_supplier_stock_fast_update.py.
- [ ] Identify wrappers and scheduled tasks.

### Inputs

- [ ] Identify the Ralawise input.
- [ ] Identify the Uneek input.
- [ ] Identify the StoreFeeder catalogue input.
- [ ] Identify the warehouse inventory input.
- [ ] Identify supplier mapping inputs.

### Coverage

- [ ] Count matched supplier-managed rows.
- [ ] Count unmatched supplier-managed rows.
- [ ] Count duplicate supplier SKUs.
- [ ] Count invalid mappings.
- [ ] Count warehouse-only rows touched.
- [ ] Count parent rows touched.

### StoreFeeder writes

- [ ] Identify the write method.
- [ ] Document fields written.
- [ ] Confirm dry-run support.
- [ ] Confirm success logging.
- [ ] Confirm failure logging.
- [ ] Document retry behaviour.

## Pass conditions

The updater is validated only when:

1. Every catalogue classification is accounted for.
2. All written fields are documented.
3. Warehouse-only rows are protected.
4. Parent rows are protected.
5. Unmatched supplier rows are reported.
6. Failed updates are reported.
7. Dry-run output is reproducible.
8. Representative live records have been checked.
