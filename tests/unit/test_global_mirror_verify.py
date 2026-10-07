from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "run_global_supplier_inventory_mirror",
    ROOT / "scripts" / "run_global_supplier_inventory_mirror.py",
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_verify_uses_unsuffixed_post_state_allocation_fields() -> None:
    actionable = pd.DataFrame(
        [
            {
                "ProductID": "14161728",
                "SKU": "BC045ABLU",
                "ExpectedQuantity": 292,
                "AvailableInventory": 0,
                "WarehousePhysical": 0,
            }
        ]
    )
    state = pd.DataFrame(
        [
            {
                "ProductID": "14161728",
                "SKU": "BC045ABLU",
                "AvailableInventory": 290,
                "WarehousePhysical": 292,
                "TotalAllocated": 2,
                "TotalPendingOut": 0,
                "LedgerError": "",
            }
        ]
    )

    result = MODULE.verify(actionable, state)

    assert result.iloc[0]["Result"] == "PASS"
    assert result.iloc[0]["Reason"] == ""


class NoWriteClient:
    def update_stock_location_inventory(self, batch, *, batch_number):
        raise AssertionError("no inventory write should occur once physical stock is already converged")


def test_convergence_accepts_changed_ledger_when_physical_target_already_met(tmp_path, monkeypatch) -> None:
    actionable = pd.DataFrame(
        [
            {
                "ProductID": "14216078",
                "SKU": "301MGMD",
                "ExpectedQuantity": 375,
                "DeductionSignature": "preflight-signature",
                "TargetAdjustmentAmount": 0,
            }
        ]
    )

    monkeypatch.setattr(
        MODULE,
        "api_with_retry",
        lambda client, call: {"_status_code": 200, "response": {"ID": 14216078}},
    )
    monkeypatch.setattr(MODULE, "_inventory_values", lambda product: (374, 374))
    monkeypatch.setattr(
        MODULE,
        "_stock_locations",
        lambda product: [
            {
                "StockLocation": {"StockLocationID": int(MODULE.WAREHOUSE_ID)},
                "PhysicalStock": 375,
                "Available": 375,
                "Allocated": 0,
                "PendingOut": 0,
            },
            {
                "StockLocation": {"StockLocationID": 158745},
                "PhysicalStock": 0,
                "Available": -1,
                "Allocated": 1,
                "PendingOut": 0,
            },
        ],
    )
    monkeypatch.setattr(MODULE, "deduction_signature", lambda locations, warehouse_id: "changed-signature")

    success, failures = MODULE.converge_inventory_differences(
        NoWriteClient(),
        actionable,
        tmp_path,
    )

    assert len(success) == 1
    assert failures.empty
    assert success.iloc[0]["WarehousePhysical"] == 375
