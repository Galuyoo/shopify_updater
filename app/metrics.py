from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

SCHEMA_VERSION = 1
APP_NAME = "shopify_updater"


def _metrics_path() -> Path:
    configured = os.getenv("METRICS_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[1] / "data" / "metrics" / "events.jsonl"


def app_version() -> str:
    return (
        os.getenv("APP_VERSION")
        or os.getenv("GIT_COMMIT")
        or os.getenv("COMMIT_SHA")
        or os.getenv("SOURCE_VERSION")
        or "unversioned"
    )


def classify_error(value: Any) -> str:
    text = str(value or "").lower()
    if "429" in text or "rate limit" in text:
        return "rate_limit"
    if "auth" in text or "credential" in text or "token" in text:
        return "authentication_error"
    if "timeout" in text or "connection" in text or "network" in text:
        return "network_error"
    if "valid" in text or "missing" in text or "column" in text:
        return "validation_error"
    return "internal_error"


def _safe_error(value: Any) -> str:
    text = str(value or "")
    lowered = text.lower()
    if any(marker in lowered for marker in ("access_token", "authorization:", "password=", "refresh_token")):
        return "[redacted sensitive error]"
    return text[:1000]


def log_event(
    event_type: str,
    *,
    status: str = "success",
    workflow_id: str = "",
    cycle_id: str = "",
    store: str = "",
    duration_ms: int | None = None,
    products_count: int | None = None,
    variants_count: int | None = None,
    discrepancies_count: int | None = None,
    updates_attempted: int | None = None,
    updates_succeeded: int | None = None,
    updates_failed: int | None = None,
    retry_count: int | None = None,
    error: Any = "",
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Append one best-effort operational event. Never raises to business logic."""
    try:
        event = {
            "event_id": str(uuid4()),
            "occurred_at_utc": datetime.now(timezone.utc).isoformat(),
            "schema_version": SCHEMA_VERSION,
            "app_name": APP_NAME,
            "app_version": app_version(),
            "environment": os.getenv("APP_ENVIRONMENT", ""),
            "event_type": str(event_type),
            "status": str(status),
            "workflow_id": str(workflow_id or ""),
            "cycle_id": str(cycle_id or ""),
            "store": str(store or ""),
            "duration_ms": duration_ms,
            "products_count": products_count,
            "variants_count": variants_count,
            "discrepancies_count": discrepancies_count,
            "updates_attempted": updates_attempted,
            "updates_succeeded": updates_succeeded,
            "updates_failed": updates_failed,
            "retry_count": retry_count,
            "error_category": classify_error(error) if error else "",
            "error_message": _safe_error(error) if error else "",
            "metadata": metadata or {},
        }
        path = _metrics_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        return True
    except Exception:
        return False
