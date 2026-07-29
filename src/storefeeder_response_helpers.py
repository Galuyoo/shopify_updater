from __future__ import annotations

from typing import Any


def _response_failed(response_json: dict[str, Any]) -> bool:
    try:
        return int(response_json.get("Failed", 0)) > 0
    except (TypeError, ValueError):
        return False


def _response_error(response_json: dict[str, Any]) -> str:
    for key in ["Error", "Errors", "Message", "ExceptionMessage", "raw_text"]:
        value = response_json.get(key)
        if value:
            return str(value)
    return ""


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in ["true", "1", "yes", "y", "success"]
