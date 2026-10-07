from __future__ import annotations

import unittest

from src.storefeeder_available_inventory_mirror import (
    InventoryLedgerError,
    build_available_inventory_mirror_plan,
    build_physical_stock_mirror_plan,
)


def location(location_id: str, physical: int, allocated: int = 0, pending: int = 0):
    return {
        "StockLocation": {"StockLocationID": int(location_id)},
        "PhysicalStock": physical,
        "Allocated": allocated,
        "PendingOut": pending,
        "Available": physical - allocated - pending,
    }


class AvailableInventoryMirrorPlanTests(unittest.TestCase):
    def test_allocations_are_added_to_target_physical(self):
        plan = build_available_inventory_mirror_plan(
            target_available=328,
            current_available=378,
            locations=[
                location("162454", 380, allocated=1),
                location("158745", 0, allocated=1),
            ],
            warehouse_stock_location_id="162454",
        )
        self.assertEqual(plan.target_warehouse_physical, 330)
        self.assertEqual(plan.target_adjustment_amount, 329)
        self.assertEqual(plan.total_allocated, 2)

    def test_pending_out_is_a_deterministic_deduction(self):
        plan = build_available_inventory_mirror_plan(
            target_available=15,
            current_available=7,
            locations=[location("162454", 10, allocated=1, pending=2)],
            warehouse_stock_location_id="162454",
        )
        self.assertEqual(plan.target_warehouse_physical, 18)
        self.assertEqual(plan.target_adjustment_amount, 15)

    def test_consistent_negative_available_is_allowed(self):
        plan = build_available_inventory_mirror_plan(
            target_available=15,
            current_available=0,
            locations=[
                location("162454", 1, allocated=1),
                location("158745", 0),
            ],
            warehouse_stock_location_id="162454",
        )
        self.assertEqual(plan.target_warehouse_physical, 16)
        self.assertEqual(plan.target_adjustment_amount, 15)

    def test_inconsistent_location_ledger_is_rejected(self):
        bad = location("162454", 10, allocated=1)
        bad["Available"] = 10
        with self.assertRaisesRegex(InventoryLedgerError, "location_ledger_mismatch"):
            build_available_inventory_mirror_plan(
                target_available=5,
                current_available=10,
                locations=[bad],
                warehouse_stock_location_id="162454",
            )

    def test_non_warehouse_physical_stock_is_rejected(self):
        with self.assertRaisesRegex(
            InventoryLedgerError, "unexplained_non_warehouse_physical_stock"
        ):
            build_available_inventory_mirror_plan(
                target_available=5,
                current_available=6,
                locations=[location("162454", 5), location("158745", 1)],
                warehouse_stock_location_id="162454",
            )

    def test_dropship_physical_target_does_not_replace_allocated_availability(self):
        plan = build_physical_stock_mirror_plan(
            target_warehouse_physical=328,
            current_available=328,
            locations=[
                location("162454", 330, allocated=1),
                location("158745", 0, allocated=1),
            ],
            warehouse_stock_location_id="162454",
        )
        self.assertEqual(plan.target_adjustment_amount, 327)
        self.assertEqual(plan.target_warehouse_physical, 328)
        self.assertEqual(plan.expected_product_available, 326)

    def test_dropship_physical_target_can_have_negative_location_available(self):
        plan = build_physical_stock_mirror_plan(
            target_warehouse_physical=0,
            current_available=0,
            locations=[location("162454", 1, allocated=1)],
            warehouse_stock_location_id="162454",
        )
        self.assertEqual(plan.target_adjustment_amount, -1)
        self.assertEqual(plan.expected_product_available, -1)


if __name__ == "__main__":
    unittest.main()
