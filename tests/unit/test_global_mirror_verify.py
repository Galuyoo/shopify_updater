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
