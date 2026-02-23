
# 🛒 Shopify Inventory Updater

A modular tool to automatically upload and rotate large Shopify CSV exports into Google Sheets — organized by store, split by size, and tracked across versions.

---

## 📦 Features

- 🔍 Detects large CSVs and splits them automatically
- 📤 Uploads CSVs to Google Sheets (auto or manual)
- 🧠 Maintains mapping in `sheet_sources.json`
- ✅ Supports both local and Streamlit environments
- 🧪 CLI test runner: `test_sheet_rotation.py`
- 🔒 Secure auth with `.streamlit/secrets.toml` or fallback JSON

---

## 🧰 Project Structure

```
shopify_updater/
│
├── app.py                      # Streamlit app
├── cli.py                      # Optional CLI interface
├── constants.py                # Size limits and global config
├── core.py                     # Core logic (if used)
├── store_profiles.py           # Store configuration (if used)
│
├── .streamlit/
│   └── secrets.toml            # [google_service_account] credentials
│
├── scripts/
│   └── ermgi.py                # External scripts or helpers
│
├── utils/
│   ├── google_service_account.json   # Optional local fallback auth
│   ├── gsheets_manager.py            # All Sheets/Drive logic
│   ├── utils_io.py                   # CSV uploaders
│   ├── ui_utils.py                   # Streamlit utilities
│   ├── sheet_sources.json            # Mapping of store → list of sheet IDs
│   ├── sheet_config.py               # Extra sheet options (if used)
│   └── test_sheet_rotation.py        # Local test entrypoint
```

---

## 🔐 Authentication Setup

### 1. Preferred: `.streamlit/secrets.toml`

```toml
[google_service_account]
type = "service_account"
project_id = "…"
private_key_id = "…"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "your-service-account@project.iam.gserviceaccount.com"
client_id = "…"
```

✅ Used automatically in Streamlit.
✅ Manually loaded in CLI via `toml.load()`.

---

### 2. Optional: `google_service_account.json`

```bash
utils/google_service_account.json
```

Used when not running inside Streamlit and `secrets.toml` is missing.

---

## 🚀 Usage

### ▶️ 1. CLI Test Upload

```bash
python utils/test_sheet_rotation.py
```

Behavior:
- Checks size of `shopify_inventory_map_<store>_<N>.csv`
- If oversized: creates new Google Sheet, uploads, updates JSON

### 💻 2. In Streamlit (`app.py`)

Import and use:
```python
from utils.utils_io import upload_csv_to_gsheet
from utils.gsheets_manager import get_first_sheet_title
```

Sheet selection:
```python
# Use latest known Google Sheet
with open("utils/sheet_sources.json") as f:
    sheet_ids = json.load(f)[store_name]
    latest_id = sheet_ids[-1]
```

---

## ✍️ Manual Sheet Management

If Drive quota prevents automatic creation:

1. Create a Google Sheet manually
2. Copy its ID
3. Add it to `utils/sheet_sources.json`:

```json
{
  "spoofy": [
    "1st_sheet_id",
    "2nd_sheet_id",
    "your_new_sheet_id"
  ]
}
```

4. Rerun your app

---

## ⚠️ Known Limitations

| Limitation               | Status        | Notes                            |
|--------------------------|---------------|----------------------------------|
| Drive quota issues       | ⚠️ In manual mode | Use personal Google account or clear service account Drive
| Folder auto-move         | ❌ Skipped     | Folder ID can be manually added
| Ownership transfer       | ❌ Skipped     | Optional, not reliable via API
| Sheet ID registry growth | ✅ Handled     | Appends to `sheet_sources.json`

---

## 📦 Dependencies

```
streamlit
pandas
google-api-python-client
google-auth
google-auth-oauthlib
toml
```

Install via:
```bash
pip install -r requirements.txt
```

---

## ✅ Ready to Deploy

This setup works in:
- ✅ Local CLI
- ✅ Streamlit Cloud
- ✅ Scheduled automation (later)

---

## 📬 Questions?

Reach out if you need help integrating Google Drive, S3, or switching to BigQuery.

---
