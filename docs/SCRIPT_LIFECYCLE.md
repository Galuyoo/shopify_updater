# Script Lifecycle

## Production code

Production entry points live in `app/` or in explicitly approved workflow scripts.

Reusable business logic lives in `src/`.

Production code must not import from:

- `scripts/experiments/`
- `scripts/one_off/`
- `scripts/diagnostics/`
- `scripts/validation/`
- `archive/`
- `.history/`
- `backups/`

## Script categories

### experiments

Temporary investigations and prototypes. These may be deleted or archived.

### one_off

Scripts created for a single repair, import, reconciliation or investigation.

### diagnostics

Read-only tools used to inspect system state.

### validation

Read-only checks, previews and dry runs.

### maintenance

Repeatable operational repair or administrative tools.

### migrations

Versioned scripts that perform controlled data or configuration transitions.

### workflows

Approved repeatable business processes.

## Promotion rule

A temporary script may become production code only after:

1. Its reusable logic is moved into `src/`.
2. Tests are added.
3. Configuration is externalised.
4. Dry-run behaviour exists where writes are involved.
5. The script is documented as an approved entry point.

## Safety rule

Experiments and one-off scripts must never be imported by production code.
