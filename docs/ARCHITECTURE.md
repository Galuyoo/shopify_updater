# System Architecture

## High-Level Architecture

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

Reporting & Email Notification Layer

------------------------------------------------------------------------

## Execution Flow

1.  Service cycle begins.
2.  Supplier stock data is fetched once.
3.  Data is normalized into canonical SKU format.
4.  Each store runs in an isolated reconciliation context.
5.  Differences are detected and validated.
6.  Batch updates are executed.
7.  Structured reports are generated and emailed.
8.  System sleeps until next scheduled cycle.

------------------------------------------------------------------------

## Design Principles

-   Decoupling of external supplier identifiers from internal
    identifiers
-   API efficiency through single-fetch strategy
-   Isolation between storefront execution contexts
-   Prevention of destructive bulk updates
-   Observability via structured reporting

------------------------------------------------------------------------

## Operational Model

The engine runs continuously within warehouse infrastructure using
scheduled execution.

Each cycle produces:

-   Update summary
-   Error diagnostics
-   Per-store reconciliation reports
-   Attachments for unmatched or failed updates

This ensures traceability and operational visibility.
