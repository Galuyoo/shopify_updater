from __future__ import annotations

import pytest

from src.storefeeder_response_helpers import (
    _response_error,
    _response_failed,
    _truthy,
)


@pytest.mark.parametrize(
    ("response_json", "expected"),
    [
        ({}, False),
        ({"Failed": 0}, False),
        ({"Failed": 1}, True),
        ({"Failed": 2}, True),
        ({"Failed": "1"}, True),
        ({"Failed": "0"}, False),
        ({"Failed": None}, False),
        ({"Failed": "invalid"}, False),
        ({"Failed": -1}, False),
    ],
)
def test_response_failed(response_json: dict[str, object], expected: bool) -> None:
    assert _response_failed(response_json) is expected


@pytest.mark.parametrize(
    ("response_json", "expected"),
    [
        ({}, ""),
        ({"Error": "error value"}, "error value"),
        ({"Errors": ["first", "second"]}, "['first', 'second']"),
        ({"Message": "message value"}, "message value"),
        ({"ExceptionMessage": "exception value"}, "exception value"),
        ({"raw_text": "raw response"}, "raw response"),
        ({"Error": "", "Message": "fallback message"}, "fallback message"),
        ({"Error": 0, "Message": "fallback message"}, "fallback message"),
        (
            {
                "Error": "highest priority",
                "Message": "lower priority",
                "raw_text": "lowest priority",
            },
            "highest priority",
        ),
    ],
)
def test_response_error(response_json: dict[str, object], expected: str) -> None:
    assert _response_error(response_json) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("TRUE", True),
        ("  true  ", True),
        ("1", True),
        (1, True),
        ("yes", True),
        ("Y", True),
        ("success", True),
        ("false", False),
        ("0", False),
        (0, False),
        ("no", False),
        ("", False),
        (None, False),
    ],
)
def test_truthy(value: object, expected: bool) -> None:
    assert _truthy(value) is expected
