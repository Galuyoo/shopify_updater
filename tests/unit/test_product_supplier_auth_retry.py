from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from scripts import promote_exact_supplier_matches as module


class FakeSession:
    def __init__(self) -> None:
        self.headers = {}


class FakeClient:
    def __init__(self) -> None:
        self.config = SimpleNamespace()
        self.session = FakeSession()
        self.create_calls = 0
        self.readback_calls = 0

    def create_product_supplier(self, product_id: str, item: dict):
        self.create_calls += 1
        if self.create_calls == 1:
            return {"_status_code": 401, "response": {"Message": "Authorization denied"}}
        return {"_status_code": 200, "response": {"ok": True}}

    def get_product_suppliers(self, product_id: str):
        self.readback_calls += 1
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


def test_create_supplier_refreshes_auth_after_401(monkeypatch) -> None:
    client = FakeClient()
    monkeypatch.setattr(module, "fetch_storefeeder_access_token", lambda config: "fresh-token")

    success_rows = []
    failure_rows = []
    ok = module._create_and_verify_product_supplier(
        client=client,
        product_id="14109744",
        product=pd.Series({"SKU": "712BK2XL"}),
        candidate={
            "supplier": "Uneek",
            "SupplierID": "4763",
            "Supplier.Name": "Uneek",
            "supplier_sku": "712BK2XL",
            "supplier_free_stock": 10,
        },
        supplier_costs=0,
        success_rows=success_rows,
        failure_rows=failure_rows,
    )

    assert ok is True
    assert client.create_calls == 2
    assert client.session.headers["Authorization"] == "Bearer fresh-token"
    assert len(success_rows) == 1
    assert failure_rows == []
