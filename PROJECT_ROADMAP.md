# Project Roadmap

## Purpose

This repository contains an operational catalogue and inventory integration system for a multi-channel print and embroidery business.

The project connects supplier feeds, internal warehouse inventory, StoreFeeder products and external sales channels.

## Workflow

Supplier feeds
-> Supplier normalisation
-> StoreFeeder catalogue reconciliation
-> Inventory classification
-> Stock calculation and validation
-> Channel listing mapping
-> Priority-based channel updates

## Current phases

### Phase 1 - Supplier ingestion

Status: Complete

Supported suppliers:

- Ralawise
- Uneek

### Phase 2 - Catalogue reconciliation

Status: Complete for the current catalogue import

| Classification | Rows |
|---|---:|
| Ralawise supplier-managed | 89,207 |
| Uneek supplier-managed | 5,531 |
| Parent | 4,220 |
| Warehouse-only | 6,356 |
| Total | 105,314 |

Only Ralawise and Uneek are valid external suppliers.

### Phase 3 - Stock updater validation

Status: In progress

Goal:

Confirm that every sellable SKU receives the correct stock while warehouse-only and parent rows remain protected.

Completion conditions:

- all supplier-managed rows are accounted for
- unmatched supplier rows are reported
- warehouse-only inventory is preserved
- parent rows remain unavailable
- failures are logged
- dry-run output is reproducible

### Phase 4 - Channel mapping

Status: Planned

Planned channels include:

- Amazon
- Shopify
- eBay
- other StoreFeeder-connected channels

### Phase 5 - Product update priority

Status: Planned

Proposed levels:

- Critical
- High
- Standard
- Low
- Disabled

### Phase 6 - Stock update policies

Status: Planned

| Policy | Behaviour |
|---|---|
| Inventory only | Use warehouse inventory |
| Supplier inventory only | Use supplier availability |
| Inventory plus supplier | Combine approved quantities |
| Warehouse only | Preserve warehouse inventory |
| Parent | Always unavailable |
| Disabled | Never publish stock |
| Manual review | Do not update automatically |

## Immediate next task

Validate the stock updater against all 105,314 catalogue rows.

## Working rules

- do not place new scripts in the repository root
- use Git instead of creating backup script copies
- place diagnostics under scripts/diagnostics
- place generated output under reports
- place operational data under data
- default write operations to dry-run mode
- never commit credentials or commercially sensitive source data
