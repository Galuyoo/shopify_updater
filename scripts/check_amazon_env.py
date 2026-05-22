from dotenv import load_dotenv
import os

load_dotenv()

keys = [
    "AMAZON_LWA_APP_ID",
    "AMAZON_LWA_CLIENT_SECRET",
    "AMAZON_REFRESH_TOKEN",
    "AMAZON_SELLER_ID",
    "AMAZON_MARKETPLACE_ID",
    "AMAZON_REGION",
    "AMAZON_DRY_RUN",
]

for key in keys:
    value = os.getenv(key, "").strip()
    if value:
        print(f"{key}: OK length={len(value)}")
    else:
        print(f"{key}: MISSING")
