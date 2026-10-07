from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any


@dataclass(frozen=True)
class AvailableInventoryMirrorPlan:
    target_available: int
    current_available: int
    current_warehouse_physical: int
    current_warehouse_available: int
    target_adjustment_amount: int
    target_warehouse_physical: int
    total_allocated: int
    total_pending_out: int
    other_available: int


@dataclass(frozen=True)
class PhysicalStockMirrorPlan:
    target_warehouse_physical: int
    target_adjustment_amount: int
    expected_product_available: int
    current_product_available: int
    current_warehouse_physical: int
    current_warehouse_available: int
    total_allocated: int
    total_pending_out: int
    other_available: int


class InventoryLedgerError(ValueError):
    pass


def number(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))


def stock_location_id(location: dict[str, Any]) -> str:
    nested = location.get("StockLocation")
    source = nested if isinstance(nested, dict) else location
    for key in ("StockLocationID", "StockLocationId", "ID", "Id"):
        value = source.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def location_available_from_ledger(location: dict[str, Any]) -> int:
    return (
        number(location.get("PhysicalStock"))
        - number(location.get("Allocated"))
        - number(location.get("PendingOut"))
    )


def deduction_signature(
    locations: list[dict[str, Any]], warehouse_stock_location_id: str
) -> str:
    rows = []
    for location in locations:
        location_id = stock_location_id(location)
        row = {
            "StockLocationID": location_id,
            "Allocated": number(location.get("Allocated")),
            "PendingOut": number(location.get("PendingOut")),
        }
        if location_id != str(warehouse_stock_location_id):
            row.update({
                "Available": number(location.get("Available")),
                "PhysicalStock": number(location.get("PhysicalStock")),
            })
        rows.append(row)
    return json.dumps(sorted(rows, key=lambda row: row["StockLocationID"]), sort_keys=True)


def build_available_inventory_mirror_plan(
    *,
    target_available: int,
    current_available: int,
    locations: list[dict[str, Any]],
    warehouse_stock_location_id: str,
) -> AvailableInventoryMirrorPlan:
    warehouse = [
        location
        for location in locations
        if stock_location_id(location) == str(warehouse_stock_location_id)
    ]
    if len(warehouse) != 1:
        raise InventoryLedgerError(
            f"warehouse_stock_missing_or_duplicate: count={len(warehouse)}"
        )

    for location in locations:
        allocated = number(location.get("Allocated"))
        pending_out = number(location.get("PendingOut"))
        physical = number(location.get("PhysicalStock"))
        available = number(location.get("Available"))
        if allocated < 0 or pending_out < 0 or physical < 0:
            raise InventoryLedgerError(
                f"negative_ledger_state: location={stock_location_id(location)}"
            )
        calculated_available = location_available_from_ledger(location)
        if available != calculated_available:
            raise InventoryLedgerError(
                "location_ledger_mismatch: "
                f"location={stock_location_id(location)}; available={available}; "
                f"physical={physical}; allocated={allocated}; pending_out={pending_out}; "
                f"calculated_available={calculated_available}"
            )

    calculated_product_available = sum(
        number(location.get("Available")) for location in locations
    )
    if current_available != calculated_product_available:
        raise InventoryLedgerError(
            "product_available_mismatch: "
            f"AvailableInventory={current_available}; "
            f"sum_location_available={calculated_product_available}"
        )

    other_locations = [
        location
        for location in locations
        if stock_location_id(location) != str(warehouse_stock_location_id)
    ]
    unexplained_physical = [
        stock_location_id(location)
        for location in other_locations
        if number(location.get("PhysicalStock")) != 0
    ]
    if unexplained_physical:
        raise InventoryLedgerError(
            "unexplained_non_warehouse_physical_stock: "
            + ",".join(unexplained_physical)
        )

    wh = warehouse[0]
    other_available = sum(number(location.get("Available")) for location in other_locations)
    target_adjustment_amount = int(target_available) - other_available
    target_physical = (
        target_adjustment_amount
        + number(wh.get("Allocated"))
        + number(wh.get("PendingOut"))
    )
    if target_physical < 0:
        raise InventoryLedgerError(
            f"negative_target_warehouse_physical: {target_physical}"
        )

    converged_adjustment = (
        number(wh.get("Available"))
        + int(target_available)
        - int(current_available)
    )
    if target_adjustment_amount != converged_adjustment:
        raise InventoryLedgerError(
            "adjustment_formula_disagreement: "
            f"ledger={target_adjustment_amount}; delta={converged_adjustment}"
        )

    return AvailableInventoryMirrorPlan(
        target_available=int(target_available),
        current_available=int(current_available),
        current_warehouse_physical=number(wh.get("PhysicalStock")),
        current_warehouse_available=number(wh.get("Available")),
        target_adjustment_amount=target_adjustment_amount,
        target_warehouse_physical=target_physical,
        total_allocated=sum(number(location.get("Allocated")) for location in locations),
        total_pending_out=sum(number(location.get("PendingOut")) for location in locations),
        other_available=other_available,
    )


def build_physical_stock_mirror_plan(
    *,
    target_warehouse_physical: int,
    current_available: int,
    locations: list[dict[str, Any]],
    warehouse_stock_location_id: str,
) -> PhysicalStockMirrorPlan:
    # Reuse the full ledger validation without using its available-inventory target.
    validated = build_available_inventory_mirror_plan(
        target_available=int(current_available),
        current_available=int(current_available),
        locations=locations,
        warehouse_stock_location_id=warehouse_stock_location_id,
    )
    if int(target_warehouse_physical) < 0:
        raise InventoryLedgerError(
            f"negative_target_warehouse_physical: {target_warehouse_physical}"
        )
    warehouse = next(
        location
        for location in locations
        if stock_location_id(location) == str(warehouse_stock_location_id)
    )
    warehouse_allocated = number(warehouse.get("Allocated"))
    warehouse_pending = number(warehouse.get("PendingOut"))
    target_adjustment = (
        int(target_warehouse_physical) - warehouse_allocated - warehouse_pending
    )
    expected_available = target_adjustment + validated.other_available
    ledger_expected_available = (
        int(target_warehouse_physical)
        - validated.total_allocated
        - validated.total_pending_out
    )
    if expected_available != ledger_expected_available:
        raise InventoryLedgerError(
            "physical_target_formula_disagreement: "
            f"location_sum={expected_available}; ledger={ledger_expected_available}"
        )
    observed_delta_adjustment = (
        validated.current_warehouse_available
        + int(target_warehouse_physical)
        - validated.current_warehouse_physical
    )
    if target_adjustment != observed_delta_adjustment:
        raise InventoryLedgerError(
            "physical_adjustment_formula_disagreement: "
            f"ledger={target_adjustment}; delta={observed_delta_adjustment}"
        )
    return PhysicalStockMirrorPlan(
        target_warehouse_physical=int(target_warehouse_physical),
        target_adjustment_amount=target_adjustment,
        expected_product_available=expected_available,
        current_product_available=int(current_available),
        current_warehouse_physical=validated.current_warehouse_physical,
        current_warehouse_available=validated.current_warehouse_available,
        total_allocated=validated.total_allocated,
        total_pending_out=validated.total_pending_out,
        other_available=validated.other_available,
    )
