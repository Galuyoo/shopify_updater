from __future__ import annotations

import os
import requests
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("AMAZON_LWA_APP_ID", "").strip()
client_secret = os.getenv("AMAZON_LWA_CLIENT_SECRET", "").strip()
refresh_token = os.getenv("AMAZON_REFRESH_TOKEN", "").strip()

token_res = requests.post(
    "https://api.amazon.com/auth/o2/token",
    data={
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    },
    timeout=30,
)

print("LWA status:", token_res.status_code)
token_res.raise_for_status()

access_token = token_res.json()["access_token"]

res = requests.get(
    "https://sandbox.sellingpartnerapi-eu.amazon.com/sellers/v1/marketplaceParticipations",
    headers={
        "x-amz-access-token": access_token,
        "user-agent": "GaluyooAmazonStockUpdater/0.1",
    },
    timeout=30,
)

print("SP-API status:", res.status_code)
print(res.text[:1000])
