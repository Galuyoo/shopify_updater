# Repository Working Guide

## Current approach

The repository currently contains operational code, development utilities, historical experiments and generated reports.

A full refactor is intentionally postponed until the stock updater has been validated.

## New script locations

scripts/supplier/
scripts/catalogue/
scripts/stock/
scripts/mapping/
scripts/channels/
scripts/maintenance/
scripts/diagnostics/

## Rules

- Do not create *.backup.py files.
- Use Git commits instead of backup scripts.
- New investigation scripts belong in scripts/diagnostics.
- Generated outputs belong in reports.
- Production data belongs in data.
- New production scripts should not be placed in the repository root.
- Default all new write operations to dry-run.

## Preferred naming

build_<thing>.py
validate_<thing>.py
sync_<thing>.py
export_<thing>.py
diagnose_<thing>.py

## Never commit

- API keys
- passwords
- customer data
- supplier credentials
- StoreFeeder tokens
