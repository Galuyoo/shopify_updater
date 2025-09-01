# app.py
import streamlit as st
import pandas as pd
import tempfile

from constants import BATCH_SIZE_DEFAULT
from core import run_update
from store_profiles import STORE_PROFILES

st.set_page_config(page_title="Shopify Stock Updater", page_icon="📦", layout="centered")
st.title("📦 Shopify Stock Updater")

with st.sidebar:
    st.header("Run options")
    store = st.selectbox("Store", list(STORE_PROFILES.keys()), index=1)
    sku_prefix = st.text_input("SKU prefix (optional)", "")
    product_types_in = st.text_input("Product types (comma-separated, optional)", "")
    location_name = st.text_input("Location name (optional)", "")
    batch_size = st.number_input("Batch size", min_value=1, max_value=250, value=BATCH_SIZE_DEFAULT, step=1)
    dry_run = st.checkbox("Dry run", value=True)
    build_map = st.checkbox("Build/refresh mapping before run", value=True)
    map_csv_override = st.text_input("Mapping CSV override (optional)", "")
    stock_csv_override = st.text_input("Stock CSV override (optional)", "")
    stock_csv_upload = st.file_uploader("Upload Stock CSV", type=["csv"])

run_btn = st.button("Run update", type="primary")

log_box = st.empty()
log_lines = []

def push(msg: str):
    log_lines.append(msg)
    log_box.text("\n".join(log_lines[-40:]))

if run_btn:
    # Build filters first
    pts = [s.strip() for s in product_types_in.split(",") if s.strip()] if product_types_in else None

    # Persist uploaded CSV to a temp file (if provided)
    tmp_stock_path = None
    if stock_csv_upload is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(stock_csv_upload.read())
            tmp_stock_path = tmp.name

    with st.spinner("Working…"):
        try:
            report_df, summary = run_update(
                store=store,
                sku_prefix=sku_prefix or None,
                product_types=pts,
                location_name=location_name or None,
                batch_size=int(batch_size),
                map_csv=map_csv_override or None,
                stock_csv_path=tmp_stock_path or (stock_csv_override or None),
                dry_run=dry_run,
                build_map=build_map,
                store_profiles=STORE_PROFILES,
                progress=push
            )
        except FileNotFoundError as e:
            st.error(str(e))
            st.stop()

    st.success("Done.")
    st.subheader("Summary")
    st.json(summary)

    if report_df is not None and not report_df.empty:
        st.subheader("Report preview")
        st.dataframe(report_df.head(200))
        csv = report_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download full report CSV",
            data=csv,
            file_name=summary.get("report_filename", "report.csv"),
            mime="text/csv"
        )
