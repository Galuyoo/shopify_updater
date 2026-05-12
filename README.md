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
