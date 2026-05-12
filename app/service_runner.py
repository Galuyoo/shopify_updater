# service_runner.py
import time
import traceback
import os
from datetime import datetime
from pathlib import Path

from core import run_update
from store_profiles import STORE_PROFILES
from config import SLEEP_HOURS, BATCH_SIZE, DAYS_BACK, DRY_RUN, LOG_DIR
from utils.emailer import send_email, maybe_gzip_csv
from stock_sources import build_combined_stock_df  # ✅ NEW

IGNORE_STORES = {"paddy", "spoofy","fullyblessed"}  # or {"store1", "store2"}


# --- simple file logger (append) ---
def _log_line(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = LOG_DIR / f"service_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # don't crash the service because logging failed
        pass


def _progress_cb(msg: str):
    """
    Prevent console spam. Only log important lines.
    """
    keep = (
        "✅ Connected" in msg
        or "🆕" in msg                 # mapping auto-create / build messages
        or "🗺️" in msg
        or "🔎 Checking for new variants" in msg
        or "🔁 Mapping refreshed" in msg
        or "➕ Added" in msg
        or "✅ No new variants" in msg
        or "📡 Ralawise" in msg
        or "📥 Loaded stock rows" in msg
        or "🔗 Stock matches" in msg
        or "📦 Using location" in msg
        or "🚚 Updating" in msg
        or "⚠️" in msg
        or "❌" in msg
    )

    # Drop the noisy "✓ 50/xxxx" progress spam:
    if msg.strip().startswith("✓") or msg.strip().startswith("   ✓"):
        return
    if keep:
        _log_line(msg)


def run_forever():
    stores = [k for k in STORE_PROFILES.keys() if k not in IGNORE_STORES]

    _log_line(f"🧠 Stores loaded: {stores}")
    _log_line(
        f"⚙️  Loop config: SLEEP_HOURS={SLEEP_HOURS}, BATCH_SIZE={BATCH_SIZE}, "
        f"DAYS_BACK={DAYS_BACK}, DRY_RUN={DRY_RUN}"
    )

    cycle = 0
    while True:
        cycle += 1
        _log_line(f"🔁 ===== Cycle {cycle} start =====")

        cycle_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        attachments = []
        lines = []

        # ✅ Fetch stock ONCE per cycle
        _cycle_stock_df = None
        try:
            _progress_cb("📦 Fetching stock feeds (once per cycle)…")
            _cycle_stock_df = build_combined_stock_df(prefer="ralawise", progress=_progress_cb)
            _progress_cb(f"✅ Stock feeds ready for cycle (rows={len(_cycle_stock_df)})")
        except Exception as e:
            _log_line(f"❌ Stock fetch failed for cycle {cycle}: {type(e).__name__}: {e}")
            _log_line(traceback.format_exc())
            # Fallback: run_update will fetch stock internally (old behavior)

        for store_key in stores:
            store_started = time.time()
            _log_line(f"🏪 Store: {store_key}")

            try:
                # ✅ Real mapping path comes from profile (matches your store_profiles.py)
                map_csv = STORE_PROFILES[store_key]["MAP_CSV"]
                try:
                    Path(map_csv).parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass

                if not os.path.exists(map_csv):
                    _progress_cb(f"🆕 Mapping missing for {store_key} → will auto-create: {map_csv}")
                else:
                    _progress_cb(f"🗺️ Mapping found for {store_key}: {map_csv}")

                df, summary = run_update(
                    store=store_key,
                    store_profiles=STORE_PROFILES,
                    dry_run=DRY_RUN,
                    build_map=False,  # ✅ keep service safe; run_update handles missing map
                    batch_size=BATCH_SIZE,
                    days_back=DAYS_BACK,
                    progress=_progress_cb,
                    stock_csv_df=_cycle_stock_df,
                    # Optional but explicit (good practice):
                    map_csv=map_csv,
                )

                # Always include store in cycle email, even when 0 updates
                lines.append(
                    f"{store_key}: OK | updated={summary.get('updated', 0)} dry={summary.get('dry', 0)} "
                    f"errors={summary.get('errors', 0)} translated={summary.get('translated', 0)} "
                    f"skipped={summary.get('skipped', 0)} elapsed={summary.get('elapsed_secs', 0)}s"
                )

                # Attach CSV (gzip if big)
                rname = summary.get("report_name")
                rbytes = summary.get("report_csv_bytes")
                if rname and rbytes:
                    attachments.append(maybe_gzip_csv(rname, rbytes))

                # Attach unmatched + failed for flairmerchandise only
                if store_key.lower() == "flairmerchandise":
                    uname = summary.get("unmatched_report_name")
                    ubytes = summary.get("unmatched_report_csv_bytes")
                    if uname and ubytes:
                        attachments.append(
                            maybe_gzip_csv(uname.replace("unmatched_skus_", "unupdated_variants_"), ubytes)
                        )

                    fname = summary.get("failed_report_name")
                    fbytes = summary.get("failed_report_csv_bytes")
                    if fname and fbytes:
                        attachments.append(maybe_gzip_csv(fname, fbytes))

                _log_line(
                    f"✅ Done {store_key}: updated={summary.get('updated')} dry={summary.get('dry')} "
                    f"errors={summary.get('errors')} translated={summary.get('translated')} "
                    f"skipped={summary.get('skipped')} elapsed={summary.get('elapsed_secs')}s "
                    f"report={summary.get('report_name')}"
                )

            except Exception as e:
                # ✅ IMPORTANT: include failures in the email summary so a store never "disappears"
                err_line = f"{store_key}: FAILED | {type(e).__name__}: {e}"
                lines.append(err_line)

                _log_line(f"❌ Store failed: {store_key} | {type(e).__name__}: {e}")
                _log_line(traceback.format_exc())
                continue

            finally:
                # (Optional) if you want, you can append runtime here, but we keep it simple.
                _ = time.time() - store_started

        subject = f"[Shopify Updater] Cycle {cycle} complete — {cycle_ts}"
        body = "Cycle summary:\n\n" + "\n".join(lines) + "\n"

        # ✅ Send one email per cycle ALWAYS (attachments optional)
        try:
            send_email(subject, body, attachments)
            if attachments:
                _log_line(f"📧 Email sent (attachments={len(attachments)}).")
            else:
                _log_line("📧 Email sent (no attachments).")
        except Exception as e:
            _log_line(f"❌ Email failed: {type(e).__name__}: {e}")
            _log_line(traceback.format_exc())

        _log_line(f"🛌 Cycle {cycle} complete. Sleeping {SLEEP_HOURS} hours…")
        time.sleep(max(1, int(SLEEP_HOURS * 3600)))


if __name__ == "__main__":
    run_forever()