# 🚀 Shopify Inventory Updater — Headless 24/7 Service

A fully autonomous, production-grade Shopify inventory synchronization system.

This version (`main` branch) represents the evolution of the project from a Streamlit UI tool into a continuously running backend service that updates inventory across multiple Shopify stores using supplier stock feeds.

---

## 🧠 System Evolution

This repository contains two architectural generations:

- **main** → Headless 24/7 autonomous inventory synchronization service (production system)
- **legacy-streamlit-ui** → Original Streamlit-based Google Sheets uploader

The system was refactored from a manual UI-driven workflow into a scheduled infrastructure service designed for continuous operation.

---

## ⚙️ What It Does

- 🔄 Runs 24/7 via Task Scheduler / cron
- 📡 Fetches live supplier stock feeds (Ralawise, Uneek)
- 🗺 Maintains local inventory mapping per store
- 🔍 Detects new Shopify variants automatically
- 🔗 Matches SKUs with translation logic when required
- 📦 Updates inventory via Shopify GraphQL API in controlled batches
- 📊 Generates per-cycle update reports
- 📧 Emails success, unmatched SKUs, and failed mutation reports
- 🛑 Prevents unsafe full rebuilds for large stores

---

## 🏗 Architecture Overview
app/
    │
    ├── service_runner.py # 24/7 scheduler entrypoint
    ├── core.py # Main orchestration logic
    ├── stock_sources.py # Supplier stock aggregation
    ├── ralawise.py # Ralawise integration
    ├── uneek.py # Uneek integration
    ├── store_profiles.py # Per-store configuration loader
    ├── utils/
             └── emailer.py # Automated reporting system
             

---

## 🔁 Runtime Flow

1. Scheduler triggers `service_runner.py`
2. Stock feeds fetched once per cycle
3. For each store:
   - Mapping refreshed incrementally
   - SKU matching performed
   - GraphQL inventory updates batched
   - Unmatched + failure reports generated
4. Email summary sent
5. Sleep → next cycle

---

## 📊 Scale

Designed to handle:

- Tens of thousands of variants per store
- Multi-store architecture
- Batched GraphQL updates
- Continuous incremental mapping growth
- Translation-based SKU normalization

---

## 🛡 Safety Mechanisms

- Prevents accidental full rebuild on large stores
- Controlled batch sizes
- Duplicate SKU detection
- Unmatched SKU reporting
- Error isolation per store
- Graceful stock feed fallback

---

## 🔐 Configuration

Each store is defined in:
app/store_profiles.json 

Secrets are never committed. Use:

- `.env`
- `store_profiles.json` (ignored by git)
- `store_profiles.example.json` (template)

---

## 🚀 Running the Service

### Windows (Task Scheduler)
python app/service_runner.py

### Linux (cron)
*/30**** python /path/to/service_runner.py

---

## 🧩 Dependencies
pandas
requests
python-dotenv

install:
pip install -r requirements.txt

---

## 📸 24/7 Service Evidence

The system runs continuously on a dedicated machine and emails per-cycle reports.

(See `/docs/` for runtime logs.)

---

## 📈 Why This Matters

This project demonstrates:

- Architectural refactoring from UI tool → backend infrastructure
- Multi-store system design
- External supplier API integration
- Automated data reconciliation
- Production-safe deployment practices
- Version-controlled evolution

---

## 📌 Legacy Version

To view the original Streamlit UI implementation:

Switch to branch:
legacy-streamlit-ui

---
