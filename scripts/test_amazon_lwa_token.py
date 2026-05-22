from __future__ import annotations

import os
import requests
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("AMAZON_LWA_APP_ID", "").strip()
client_secret = os.getenv("AMAZON_LWA_CLIENT_SECRET", "").strip()
refresh_token = os.getenv("AMAZON_REFRESH_TOKEN", "").strip()

missing = [
    name for name, value in {
        "AMAZON_LWA_APP_ID": client_id,
        "AMAZON_LWA_CLIENT_SECRET": client_secret,
        "AMAZON_REFRESH_TOKEN": refresh_token,
    }.items()
    if not value
]

if missing:
    raise RuntimeError(f"Missing env vars: {missing}")

response = requests.post(
    "https://api.amazon.com/auth/o2/token",
    data={
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    },
    timeout=30,
)

print(f"HTTP status: {response.status_code}")

payload = response.json()

if not response.ok:
    print("FAILED")
    print(payload)
    raise SystemExit(1)

access_token = payload.get("access_token", "")

print("LWA token exchange OK")
print(f"token_type: {payload.get('token_type')}")
print(f"expires_in: {payload.get('expires_in')}")
print(f"access_token_length: {len(access_token)}")
print("Token received but not printed.")
