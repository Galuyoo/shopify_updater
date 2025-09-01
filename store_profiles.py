import streamlit as st

STORE_PROFILES = {}

for store, creds in st.secrets["store_profiles"].items():
    STORE_PROFILES[store] = {
        "SHOP_URL": creds["SHOP_URL"],
        "ACCESS_TOKEN": creds["ACCESS_TOKEN"],
        "MAP_CSV": f"utils/shopify_inventory_map_{store}.csv",
        "DEFAULT_SKU_PREFIX": "",
        "DEFAULT_PRODUCT_TYPES": [],
        "LOCATION_NAME": None
    }
