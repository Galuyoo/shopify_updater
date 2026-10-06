from __future__ import annotations

import requests

from scripts import export_storefeeder_products as module


class FakeClient:
    def __init__(self) -> None:
        self.calls = 0

    def get_products_page(self, *, page: int, page_size: int):
        self.calls += 1
        if self.calls < 3:
            return {"_status_code": 429, "response": {"error": "rate limited"}}
        return {
            "_status_code": 200,
            "response": {"Items": [{"ID": 1}], "TotalPages": 1},
        }


def test_product_page_fetch_retries_transient_http_status(monkeypatch) -> None:
    client = FakeClient()
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    products = module.fetch_products(client, page_size=100, limit=None)

    assert client.calls == 3
    assert products == [{"ID": 1}]


class TransportFlakyClient:
    def __init__(self) -> None:
        self.calls = 0

    def get_products_page(self, *, page: int, page_size: int):
        self.calls += 1
        if self.calls < 3:
            raise requests.ConnectionError("temporary connection failure")
        return {
            "_status_code": 200,
            "response": {"Items": [{"ID": 2}], "TotalPages": 1},
        }


def test_product_page_fetch_retries_transport_errors(monkeypatch) -> None:
    client = TransportFlakyClient()
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    products = module.fetch_products(client, page_size=100, limit=None)

    assert client.calls == 3
    assert products == [{"ID": 2}]
