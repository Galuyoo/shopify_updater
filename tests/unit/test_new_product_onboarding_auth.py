from __future__ import annotations

from types import SimpleNamespace

from scripts import run_new_product_onboarding_delta as module


class FakeSession:
    def __init__(self) -> None:
        self.headers = {}


class FakeClient:
    def __init__(self) -> None:
        self.config = SimpleNamespace()
        self.session = FakeSession()
        self.calls = 0

    def get_product_suppliers(self, product_id: str):
        self.calls += 1
        if self.calls == 1:
            return {
                "_status_code": 401,
                "response": {"Message": "Authorization has been denied"},
            }
        return {
            "_status_code": 200,
            "response": {
                "Suppliers": [
                    {
                        "Supplier": {"SupplierID": 4763},
                        "SupplierSKU": "712BK2XL",
                    }
                ]
            },
        }


def test_product_has_supplier_refreshes_auth_after_401(monkeypatch) -> None:
    client = FakeClient()
    candidate = {
        "SupplierID": "4763",
        "supplier_sku": "712BK2XL",
    }
    monkeypatch.setattr(module, "fetch_storefeeder_access_token", lambda config: "fresh-token")

    result = module._product_has_supplier(client, "14109744", candidate)

    assert result is True
    assert client.calls == 2
    assert client.session.headers["Authorization"] == "Bearer fresh-token"
