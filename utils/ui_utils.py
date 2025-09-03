# ui_utils.py
import streamlit as st
import re

SECTIONS = {
    "connection": "Connection",
    "export": "Importing Google Sheets",
    "split_check": "Check Split Sizes",
    "merge": "Merge Sheets",
    "mapping": "Mapping Variants",
    "update": "Stock Update",
}

MAX_LINES_PER_SECTION = 400

def setup_log_state():
    if "log_lines" not in st.session_state:
        st.session_state.log_lines = {k: [] for k in SECTIONS}
    if "seen" not in st.session_state:
        st.session_state.seen = {k: set() for k in SECTIONS}
    if "summaries" not in st.session_state:
        st.session_state.summaries = {k: "⏳ Pending" for k in SECTIONS}
    if "current_section" not in st.session_state:
        st.session_state.current_section = "connection"
    if "placeholders" not in st.session_state:
        st.session_state.placeholders = {
            k: {"header": st.empty(), "body": st.empty()} for k in SECTIONS
        }

def _render_section(k: str):
    title = SECTIONS[k]
    summary = st.session_state.summaries[k]
    lines = st.session_state.log_lines[k]

    st.session_state.placeholders[k]["header"].markdown(f"**{title} — {summary}**")

    with st.session_state.placeholders[k]["body"].expander(title, expanded=False):
        if lines:
            st.code("\n".join(lines), language="text")
        else:
            st.caption("No logs yet…")

def render_all_sections():
    for _k in SECTIONS:
        _render_section(_k)

def set_section(section_key: str, summary: str | None = None):
    if section_key in SECTIONS:
        st.session_state.current_section = section_key
        if summary is not None:
            st.session_state.summaries[section_key] = summary
        _render_section(section_key)

def reset_ui_state():
    for k in SECTIONS:
        st.session_state.log_lines[k] = []
        st.session_state.seen[k] = set()
        st.session_state.summaries[k] = "⏳ Pending"
    st.session_state.current_section = "connection"
    st.session_state.placeholders = {
        k: {"header": st.empty(), "body": st.empty()} for k in SECTIONS
    }
    render_all_sections()
    for k in ["out_summary", "out_report", "status_placeholder"]:
        if k in st.session_state:
            st.session_state[k].empty()

def make_push_with_status(status_placeholder):
    def push(msg: str):
        if not isinstance(msg, str):
            return

        lower = msg.lower().strip()

        # Status messages
        if "connected to" in lower:
            set_section("connection", "✅ Connected")
            status_placeholder.info("🔌 Connecting to Shopify…")
        elif "exporting google sheets" in lower:
            set_section("export", "⏳ Exporting")
            status_placeholder.info("📥 Exporting from Google Sheets…")
        elif lower.startswith("✅ saved"):
            set_section("export", "✅ Downloaded")
        elif "checking latest split csv" in lower:
            set_section("split_check", "⏳ Checking")
            status_placeholder.info("📦 Checking split CSV sizes…")
        elif "merging sheets" in lower:
            set_section("merge", "⏳ Merging")
            status_placeholder.info("🔗 Merging split sheets…")
        elif "merged csv saved" in lower:
            set_section("merge", "✅ Merged")
        elif "checking for new variants" in lower:
            set_section("mapping", "⏳ Scanning")
            status_placeholder.info("🔎 Checking for new variants…")
        elif "no new variants" in lower:
            set_section("mapping", "✅ No new variants")
        elif "added" in lower and "new variants" in lower:
            set_section("mapping", "✅ Added")
        elif "updating" in lower and "batches" in lower:
            set_section("update", "⏳ Updating")
            status_placeholder.info("🚀 Updating stock in batches…")

        # Severity info
        if "[hard]" in lower or "[alert]" in lower or "[warn]" in lower:
            set_section("split_check")
            m = re.search(r"\[(HARD|ALERT|WARN)\].*?→.*?([\d\.]+)\s*MB", msg, re.IGNORECASE)
            if m:
                level = m.group(1).upper()
                size = m.group(2)
                badge = {"HARD": "🚨", "ALERT": "⚠️", "WARN": "⚠️"}[level]
                st.session_state.summaries["split_check"] = f"{badge} {level} — {size} MB"
                _render_section("split_check")
        if "call salah" in lower or "create a new sheet" in lower:
            st.session_state.summaries["split_check"] = "🚨🚨🚨 — Copy the section below to Salah"
            _render_section("split_check")
        if " [latest]" in lower and "mb" in lower and "split" in lower:
            if st.session_state.summaries["split_check"].startswith("⏳"):
                st.session_state.summaries["split_check"] = "✅ Checked"
                _render_section("split_check")

        # Append log to current section
        k = st.session_state.current_section
        if msg not in st.session_state.seen[k]:
            st.session_state.seen[k].add(msg)
            st.session_state.log_lines[k].append(msg)
            if len(st.session_state.log_lines[k]) > MAX_LINES_PER_SECTION:
                st.session_state.log_lines[k] = st.session_state.log_lines[k][-MAX_LINES_PER_SECTION:]
            _render_section(k)
    return push
