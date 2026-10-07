from __future__ import annotations

from src import storefeeder_api as module


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


def test_token_fetch_retries_transient_server_error(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_post(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 3:
            return FakeResponse(
                500,
                {
                    "error": "invalid_grant",
                    "error_description": "Internal server error",
                },
            )
        return FakeResponse(200, {"access_token": "fresh-token"})

    monkeypatch.setenv("STOREFEEDER_API_USERNAME", "user")
    monkeypatch.setenv("STOREFEEDER_API_PASSWORD", "pass")
    monkeypatch.setenv("STOREFEEDER_API_KEY", "key")
    monkeypatch.setattr(module.requests, "post", fake_post)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    token = module.fetch_storefeeder_access_token(
        module.StoreFeederApiConfig(base_url="https://example.test")
    )

    assert token == "fresh-token"
    assert calls["count"] == 3
