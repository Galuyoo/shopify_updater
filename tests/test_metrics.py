import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "app"))

from metrics import classify_error, log_event


def test_log_event_writes_jsonl_without_sensitive_payload(monkeypatch, tmp_path):
    path = tmp_path / "events.jsonl"
    monkeypatch.setenv("METRICS_PATH", str(path))
    monkeypatch.setenv("APP_VERSION", "test-sha")

    assert log_event(
        "inventory_reconciliation_completed",
        cycle_id="cycle-1",
        workflow_id="cycle-1:store",
        store="store",
        variants_count=10,
        updates_succeeded=3,
    )

    row = json.loads(path.read_text(encoding="utf-8").strip())
    assert row["event_type"] == "inventory_reconciliation_completed"
    assert row["app_version"] == "test-sha"
    assert row["variants_count"] == 10
    assert row["updates_succeeded"] == 3
    assert row["schema_version"] == 1


def test_rate_limit_error_category():
    assert classify_error("HTTP 429 Too Many Requests") == "rate_limit"
