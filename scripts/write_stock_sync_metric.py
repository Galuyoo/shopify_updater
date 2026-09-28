from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def coerce(value: str):
    text = str(value or "").strip()

    if text.lower() in {"yes", "true"}:
        return True
    if text.lower() in {"no", "false"}:
        return False

    try:
        return int(text)
    except ValueError:
        pass

    try:
        return float(text)
    except ValueError:
        return text


def duration_ms(start: str, end: str) -> int | None:
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        return int((end_dt - start_dt).total_seconds() * 1000)
    except Exception:
        return None


parser = argparse.ArgumentParser()
parser.add_argument("--run-dir", required=True)
parser.add_argument("--job-type", required=True)
args = parser.parse_args()

run_dir = Path(args.run_dir).resolve()
run_id = run_dir.name

summary_rows = read_rows(run_dir / "fast_stock_summary.csv")
summary = {
    str(row.get("metric", "") or ""): coerce(
        str(row.get("value", "") or "")
    )
    for row in summary_rows
    if row.get("metric")
}

exit_rows = read_rows(run_dir / "wrapper_exit_status.csv")
exit_status = exit_rows[0] if exit_rows else {}

environment = {}
env_path = run_dir / "wrapper_environment.txt"

if env_path.exists():
    for line in env_path.read_text(
        encoding="utf-8-sig",
        errors="replace",
    ).splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            environment[key.strip()] = value.strip()

ps_code = str(
    exit_status.get("powershell_exit_code", "") or ""
).strip()

python_code = str(
    exit_status.get("python_process_exit_code", "")
    or exit_status.get("python_exit_code", "")
    or ""
).strip()

lock_acquired = str(
    exit_status.get("lock_acquired", "") or ""
).strip().lower() == "true"

if not lock_acquired and not python_code and not summary:
    status = "skipped"
    event_type = "stock_sync_run_skipped"
elif ps_code == "0" and python_code in {"", "0"}:
    status = "success"
    event_type = "stock_sync_run_completed"
else:
    status = "failure"
    event_type = "stock_sync_run_failed"

start_timestamp = str(
    exit_status.get("start_timestamp", "") or ""
)
end_timestamp = str(
    exit_status.get("end_timestamp", "") or ""
)

event = {
    "event_id": str(
        uuid5(
            NAMESPACE_URL,
            f"shopify_updater:{args.job_type}:{run_id}",
        )
    ),
    "schema_version": 1,
    "app_name": "shopify_updater",
    "event_type": event_type,
    "status": status,
    "job_type": args.job_type,
    "run_id": run_id,
    "occurred_at": end_timestamp or start_timestamp,
    "recorded_at_utc": datetime.now(
        timezone.utc
    ).isoformat(),
    "duration_ms": duration_ms(
        start_timestamp,
        end_timestamp,
    ),
    "git_branch": environment.get("git_branch", ""),
    "git_commit": environment.get("git_commit", ""),
    "powershell_exit_code": coerce(ps_code),
    "python_process_exit_code": coerce(python_code),
    "failure_stage": str(
        exit_status.get("failure_stage", "") or ""
    ),
    "lock_acquired": lock_acquired,
    "metrics": summary,
}

output = (
    Path.cwd()
    / "reports"
    / "production_metrics"
    / "events.jsonl"
)

output.parent.mkdir(parents=True, exist_ok=True)

event_key = (
    event["job_type"],
    event["run_id"],
)

if output.exists():
    for line in output.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():
        try:
            existing = json.loads(line)
        except Exception:
            continue

        if (
            existing.get("job_type"),
            existing.get("run_id"),
        ) == event_key:
            print(
                f"Metrics already recorded for "
                f"{args.job_type}/{run_id}"
            )
            raise SystemExit(0)

with output.open("a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )

print(
    f"Recorded {event_type}: "
    f"{args.job_type}/{run_id}"
)
