#!/usr/bin/env python3
"""
Optimize inbound purchases (materials) from Great Plains lanes to minimize total landed cost.

- Matches the provided mathematics exactly, including procurement tax on selectable components.
- Evaluates all (vendor, mode) options discovered in GP for the requested item and quantity.
- Supports single-run CLI and batch "sheet" mode (CSV/Excel).
- Falls back to an in-memory demo dataset if DB is not configured.

Author: You & ChatGPT
"""
from __future__ import annotations

import os
import math
import json
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Tuple
from math import exp, ceil

# Optional deps used only when present
try:
    import pyodbc  # For MS SQL / GP
except Exception:  # pragma: no cover
    pyodbc = None

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None


# =========================
#      Data structures
# =========================

@dataclass
class ModeParams:
    """Freight mode parameters (Cap_m in tons)."""
    name: str
    fixed_cost: float                   # F_m
    variable_cost_per_ton_mile: float   # c_m
    capacity: float                     # Cap_m (tons per load)


@dataclass
class VendorLane:
    """
    A purchasable lane for an item from a vendor. This will be 'expanded'
    into per-mode candidates (one Material per allowed mode) during evaluation.
    """
    item_code: str
    item_name: str
    vendor_id: str
    vendor_name: str

    # price curve p(Q) = p_min + (p_max - p_min) * exp(-lambda * Q)
    p_min: float
    p_max: float
    lambda_: float

    fixed_order_cost: float             # F_i
    weight_per_unit_ton: float          # w_i (tons/unit in the BUYING UoM)
    distance_mile: float                # D_i (miles from vendor/source)
    allowed_modes: List[str]            # e.g., ["TRUCK", "RAIL", "WATER"]

    # procurement/use tax on selected components
    tax_rate: float = 0.0               # τ_i in [0,1)
    tax_on_price: bool = True           # α_i
    tax_on_fixed_order: bool = False    # β_i
    tax_on_var_freight: bool = False    # γ_i
    tax_on_fixed_freight: bool = False  # δ_i


@dataclass
class Material:  # identical to your prior Material, but vendor context is carried separately
    name: str
    quantity: float                     # Q_i (buying units)
    p_min: float
    p_max: float
    lambda_: float
    fixed_order_cost: float

    weight_per_unit: float              # tons/unit
    distance: float                     # miles
    mode: str

    tax_rate: float = 0.0
    tax_on_price: bool = True
    tax_on_fixed_order: bool = False
    tax_on_var_freight: bool = False
    tax_on_fixed_freight: bool = False


@dataclass
class Product:
    name: str
    quantity: float                     # Q'_p (selling units)
    fixed_manufacturing_cost: float     # F'_p
    variable_linear_cost: float         # v'_p
    variable_quadratic_cost: float      # ρ'_p
    weight_per_unit: float              # tons/unit
    distance: float                     # miles
    mode: str
    fixed_order_cost: float = 0.0


@dataclass
class OptionBreakdown:
    """Cost components for one (vendor, mode) option for a material."""
    vendor_id: str
    vendor_name: str
    mode: str
    shipments: int
    price_per_unit: float
    price_component: float
    fixed_order_component: float
    fixed_freight_component: float
    variable_freight_component: float
    tax_component: float
    total_cost: float


@dataclass
class PlanRow:
    """Decision for one requested (item_code, quantity)."""
    item_code: str
    item_name: str
    quantity: float
    chosen_vendor_id: str
    chosen_vendor_name: str
    chosen_mode: str
    shipments: int
    unit_price: float
    price_component: float
    fixed_order_component: float
    fixed_freight_component: float
    variable_freight_component: float
    tax_component: float
    total_inbound_cost: float


# =========================
#     Core math blocks
# =========================

def unit_price(quantity: float, p_min: float, p_max: float, lambda_: float) -> float:
    """p(Q) = p_min + (p_max - p_min) * exp(-λ Q) ; if Q <= 0, return p_max."""
    if quantity <= 0:
        return p_max
    return p_min + (p_max - p_min) * exp(-lambda_ * quantity)


def shipment_count(total_weight: float, capacity: float) -> int:
    """ceil(total_weight / capacity), 0 if no weight."""
    if total_weight <= 0:
        return 0
    return int(ceil(total_weight / capacity))


def transport_components(weight_tons: float, distance_miles: float, mode: ModeParams) -> tuple[float, float, int]:
    """Return (fixed, variable, shipments) for freight."""
    if weight_tons <= 0:
        return 0.0, 0.0, 0
    loads = shipment_count(weight_tons, mode.capacity)
    fixed = loads * mode.fixed_cost
    variable = mode.variable_cost_per_ton_mile * distance_miles * weight_tons
    return fixed, variable, loads


def procurement_cost_without_tax(mat: Material) -> float:
    """F_i + p_i(Q_i) Q_i"""
    price = unit_price(mat.quantity, mat.p_min, mat.p_max, mat.lambda_)
    return mat.fixed_order_cost + price * mat.quantity


def inbound_transport_cost(mat: Material, mode: ModeParams) -> float:
    """ceil(w_i Q_i / Cap_m) F_m + c_m D_i w_i Q_i"""
    weight = mat.quantity * mat.weight_per_unit
    fixed, var, _ = transport_components(weight, mat.distance, mode)
    return fixed + var


def procurement_tax(mat: Material, mode: ModeParams) -> float:
    """
    τ_i * ( α p(Q)Q + β F_i + γ c D w Q + δ ceil(wQ/Cap) F_m )
    """
    if mat.tax_rate <= 0.0:
        return 0.0

    price = unit_price(mat.quantity, mat.p_min, mat.p_max, mat.lambda_)
    price_component = price * mat.quantity if mat.tax_on_price else 0.0

    fixed_order_component = mat.fixed_order_cost if mat.tax_on_fixed_order else 0.0

    weight = mat.quantity * mat.weight_per_unit
    fixed_freight, var_freight, _ = transport_components(weight, mat.distance, mode)

    var_freight_component = var_freight if mat.tax_on_var_freight else 0.0
    fixed_freight_component = fixed_freight if mat.tax_on_fixed_freight else 0.0

    tax_base = price_component + fixed_order_component + var_freight_component + fixed_freight_component
    return mat.tax_rate * tax_base


# =========================
#   GP access / extraction
# =========================

class GPDataSource:
    """
    Thin data-access wrapper. In production, point the SQL in these methods
    to real GP tables/views (or create views that shape exactly what we need).
    """

    def __init__(self,
                 dsn: Optional[str] = None,
                 driver: Optional[str] = None,
                 server: Optional[str] = None,
                 database: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 fallback_demo: bool = True):
        self.dsn = dsn
        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.fallback_demo = fallback_demo
        self._conn = None

    # ---------- connection ----------
    def connect(self):
        if self._conn is not None:
            return self._conn
        if pyodbc is None:
            if self.fallback_demo:
                return None
            raise RuntimeError("pyodbc not installed and no fallback allowed.")
        try:
            if self.dsn:
                self._conn = pyodbc.connect(f"DSN={self.dsn}")
            else:
                # Typical SQL Server connection string (edit as needed)
                driver = self.driver or "{ODBC Driver 17 for SQL Server}"
                self._conn = pyodbc.connect(
                    f"DRIVER={driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
                )
        except Exception:
            if not self.fallback_demo:
                raise
            self._conn = None
        return self._conn

    # ---------- queries ----------
    def fetch_mode_params(self) -> Dict[str, ModeParams]:
        """
        Expect a GP view like: gp_ModeParams(ModeName, FixedCost, VarCostPerTonMile, CapacityTons)
        """
        conn = self.connect()
        modes: Dict[str, ModeParams] = {}

        if conn is None:  # fallback demo
            modes = {
                "WATER": ModeParams("WATER", fixed_cost=6000.0, variable_cost_per_ton_mile=0.04, capacity=1500.0),
                "RAIL":  ModeParams("RAIL",  fixed_cost=1500.0, variable_cost_per_ton_mile=0.10, capacity=90.0),
                "TRUCK": ModeParams("TRUCK", fixed_cost=250.0,  variable_cost_per_ton_mile=0.30, capacity=24.0),
            }
            return modes

        sql = """
            SELECT ModeName, FixedCost, VarCostPerTonMile, CapacityTons
            FROM gp_ModeParams WITH (NOLOCK)
        """
        cur = conn.cursor()
        for row in cur.execute(sql):
            modes[row.ModeName] = ModeParams(
                name=row.ModeName,
                fixed_cost=float(row.FixedCost),
                variable_cost_per_ton_mile=float(row.VarCostPerTonMile),
                capacity=float(row.CapacityTons),
            )
        cur.close()
        return modes

    def fetch_vendor_lanes_for_item(self, item_code: str) -> List[VendorLane]:
        """
        Expect a GP view like:
          gp_VendorLanesForItem (
             ItemCode, ItemName, VendorID, VendorName,
             PMin, PMax, Lambda, FixedOrderCost,
             WeightPerUnitTon, DistanceMile,
             AllowedModesCSV,  -- e.g. 'TRUCK,RAIL'
             TaxRate, TaxOnPrice, TaxOnFixedOrder, TaxOnVarFreight, TaxOnFixedFreight
          )
        Weight should already be in TONS per BUYING UNIT; do conversions in the view.
        """
        conn = self.connect()
        lanes: List[VendorLane] = []

        if conn is None:  # fallback demo
            # Demo: two vendors for NPK02020, both allow TRUCK & RAIL; different distances/curves.
            if item_code.upper() == "NPK02020":
                lanes.append(VendorLane(
                    item_code="NPK02020",
                    item_name="20-20-20 Premix",
                    vendor_id="V001",
                    vendor_name="PhosphateCo",
                    p_min=490.0, p_max=560.0, lambda_=0.012,
                    fixed_order_cost=90.0,
                    weight_per_unit_ton=1.0,
                    distance_mile=1100.0,
                    allowed_modes=["WATER", "RAIL"],
                    tax_rate=0.07, tax_on_price=True,
                    tax_on_fixed_order=False, tax_on_var_freight=False, tax_on_fixed_freight=False
                ))
                lanes.append(VendorLane(
                    item_code="NPK02020",
                    item_name="20-20-20 Premix",
                    vendor_id="V007",
                    vendor_name="NitroAg LLC",
                    p_min=300.0, p_max=350.0, lambda_=0.01,
                    fixed_order_cost=80.0,
                    weight_per_unit_ton=1.0,
                    distance_mile=900.0,
                    allowed_modes=["TRUCK", "RAIL"],
                    tax_rate=0.07, tax_on_price=True,
                    tax_on_fixed_order=False, tax_on_var_freight=True, tax_on_fixed_freight=True
                ))
            return lanes

        sql = """
            SELECT
                ItemCode, ItemName, VendorID, VendorName,
                PMin, PMax, Lambda, FixedOrderCost,
                WeightPerUnitTon, DistanceMile,
                AllowedModesCSV,
                TaxRate, TaxOnPrice, TaxOnFixedOrder, TaxOnVarFreight, TaxOnFixedFreight
            FROM gp_VendorLanesForItem WITH (NOLOCK)
            WHERE ItemCode = ?
        """
        cur = conn.cursor()
        for row in cur.execute(sql, (item_code,)):
            allowed = [m.strip().upper() for m in str(row.AllowedModesCSV or "").split(",") if m.strip()]
            lanes.append(VendorLane(
                item_code=row.ItemCode,
                item_name=row.ItemName,
                vendor_id=row.VendorID,
                vendor_name=row.VendorName,
                p_min=float(row.PMin),
                p_max=float(row.PMax),
                lambda_=float(row.Lambda),
                fixed_order_cost=float(row.FixedOrderCost),
                weight_per_unit_ton=float(row.WeightPerUnitTon),
                distance_mile=float(row.DistanceMile),
                allowed_modes=allowed,
                tax_rate=float(row.TaxRate or 0.0),
                tax_on_price=bool(row.TaxOnPrice),
                tax_on_fixed_order=bool(row.TaxOnFixedOrder),
                tax_on_var_freight=bool(row.TaxOnVarFreight),
                tax_on_fixed_freight=bool(row.TaxOnFixedFreight),
            ))
        cur.close()
        return lanes


# =========================
#   Evaluation / Optimize
# =========================

def evaluate_option(quantity: float,
                    lane: VendorLane,
                    mode_params: ModeParams) -> OptionBreakdown:
    """
    Compute full inbound cost for a single (vendor lane, mode) option for the requested quantity.
    Matches the provided formula (procurement tax included).
    """
    # Build a 'Material' for the formula
    mat = Material(
        name=lane.item_name,
        quantity=float(quantity),
        p_min=lane.p_min,
        p_max=lane.p_max,
        lambda_=lane.lambda_,
        fixed_order_cost=lane.fixed_order_cost,
        weight_per_unit=lane.weight_per_unit_ton,
        distance=lane.distance_mile,
        mode=mode_params.name,
        tax_rate=lane.tax_rate,
        tax_on_price=lane.tax_on_price,
        tax_on_fixed_order=lane.tax_on_fixed_order,
        tax_on_var_freight=lane.tax_on_var_freight,
        tax_on_fixed_freight=lane.tax_on_fixed_freight,
    )

    # Components
    price = unit_price(mat.quantity, mat.p_min, mat.p_max, mat.lambda_)
    price_component = price * mat.quantity
    fixed_order_component = mat.fixed_order_cost

    weight = mat.quantity * mat.weight_per_unit
    fixed_freight, variable_freight, shipments = transport_components(weight, mat.distance, mode_params)

    tax_component = procurement_tax(mat, mode_params)

    total_cost = (
        price_component +
        fixed_order_component +
        fixed_freight +
        variable_freight +
        tax_component
    )

    return OptionBreakdown(
        vendor_id=lane.vendor_id,
        vendor_name=lane.vendor_name,
        mode=mode_params.name,
        shipments=shipments,
        price_per_unit=price,
        price_component=price_component,
        fixed_order_component=fixed_order_component,
        fixed_freight_component=fixed_freight,
        variable_freight_component=variable_freight,
        tax_component=tax_component,
        total_cost=total_cost,
    )


def optimize_single_item(ds: GPDataSource, item_code: str, quantity: float) -> PlanRow:
    """
    For one (item_code, quantity), query all vendor lanes, expand across allowed modes,
    evaluate costs, and return the argmin (best total inbound cost).
    """
    lanes = ds.fetch_vendor_lanes_for_item(item_code)
    if not lanes:
        raise ValueError(f"No vendor lanes found in GP for item '{item_code}'.")

    mode_map = ds.fetch_mode_params()
    if not mode_map:
        raise ValueError("No freight modes defined in GP (gp_ModeParams).")

    # Evaluate every lane × allowed mode
    best: Optional[Tuple[VendorLane, OptionBreakdown]] = None
    for lane in lanes:
        for mode_name in lane.allowed_modes:
            mode_name = mode_name.upper()
            if mode_name not in mode_map:
                continue
            breakdown = evaluate_option(quantity, lane, mode_map[mode_name])
            if (best is None) or (breakdown.total_cost < best[1].total_cost):
                best = (lane, breakdown)

    if best is None:
        raise ValueError(f"No viable mode found for item '{item_code}' among candidate lanes.")

    lane, b = best
    return PlanRow(
        item_code=item_code,
        item_name=lane.item_name,
        quantity=float(quantity),
        chosen_vendor_id=b.vendor_id,
        chosen_vendor_name=b.vendor_name,
        chosen_mode=b.mode,
        shipments=b.shipments,
        unit_price=b.price_per_unit,
        price_component=b.price_component,
        fixed_order_component=b.fixed_order_component,
        fixed_freight_component=b.fixed_freight_component,
        variable_freight_component=b.variable_freight_component,
        tax_component=b.tax_component,
        total_inbound_cost=b.total_cost,
    )


def optimize_from_sheet(ds: GPDataSource,
                        path: str,
                        out_path: Optional[str] = None) -> List[PlanRow]:
    """
    Batch mode: read a CSV or Excel with at least columns: ItemCode, Quantity
    Returns a list of PlanRow and optionally writes a result file next to the input.
    """
    if pd is None:
        raise RuntimeError("pandas is required for batch sheet processing, please `pip install pandas`.")

    # Load
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    required = {"ItemCode", "Quantity"}
    missing = required - set(map(str, df.columns))
    if missing:
        raise ValueError(f"Input sheet missing required columns: {sorted(missing)}")

    plans: List[PlanRow] = []
    for _, row in df.iterrows():
        item = str(row["ItemCode"]).strip()
        qty = float(row["Quantity"])
        plan = optimize_single_item(ds, item, qty)
        plans.append(plan)

    # Save results if requested or default next to input
    if out_path is None:
        out_path = os.path.splitext(path)[0] + "_plan.csv"

    out_df = pd.DataFrame([asdict(p) for p in plans])
    if out_path.lower().endswith((".xlsx", ".xls")):
        out_df.to_excel(out_path, index=False)
    else:
        out_df.to_csv(out_path, index=False)

    return plans


# =========================
#           CLI
# =========================

def _print_plan(plan: PlanRow) -> None:
    print(f"\nOptimal plan for {plan.item_code} - {plan.item_name} (Q={plan.quantity:,.2f} units)")
    print(f"  Vendor: {plan.chosen_vendor_id} - {plan.chosen_vendor_name}")
    print(f"  Mode:   {plan.chosen_mode}  |  Shipments: {plan.shipments}")
    print("  Cost breakdown:")
    print(f"    Unit price:                ${plan.unit_price:,.4f}")
    print(f"    Price component:           ${plan.price_component:,.2f}")
    print(f"    Fixed order component:     ${plan.fixed_order_component:,.2f}")
    print(f"    Fixed freight component:   ${plan.fixed_freight_component:,.2f}")
    print(f"    Variable freight component:${plan.variable_freight_component:,.2f}")
    print(f"    Procurement tax component: ${plan.tax_component:,.2f}")
    print(f"  ==> Total inbound cost:      ${plan.total_inbound_cost:,.2f}\n")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Optimize inbound purchase from GP vendor lanes.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    one = sub.add_parser("one", help="Optimize a single item")
    one.add_argument("--item", required=True, help="Item code (e.g., NPK02020)")
    one.add_argument("--qty", required=True, type=float, help="Quantity (in buying units)")

    batch = sub.add_parser("sheet", help="Optimize a batch sheet (CSV or Excel)")
    batch.add_argument("--in", dest="inp", required=True, help="Path to input sheet with ItemCode,Quantity")
    batch.add_argument("--out", dest="outp", help="Optional output path (csv/xlsx)")

    # DB args (optional; if omitted we fall back to demo)
    parser.add_argument("--dsn", help="ODBC DSN name (optional)")
    parser.add_argument("--driver", help="ODBC driver (e.g., {ODBC Driver 17 for SQL Server})")
    parser.add_argument("--server", help="SQL Server host")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--username", help="DB username")
    parser.add_argument("--password", help="DB password")
    parser.add_argument("--no-fallback", action="store_true", help="Disable demo fallback")

    args = parser.parse_args()

    ds = GPDataSource(
        dsn=args.dsn,
        driver=args.driver,
        server=args.server,
        database=args.database,
        username=args.username,
        password=args.password,
        fallback_demo=not args.no_fallback
    )

    if args.cmd == "one":
        plan = optimize_single_item(ds, args.item, args.qty)
        _print_plan(plan)
        print(json.dumps(asdict(plan), indent=2))
    elif args.cmd == "sheet":
        plans = optimize_from_sheet(ds, args.inp, out_path=args.outp)
        for p in plans:
            _print_plan(p)
        # Also echo a compact JSON array for downstream piping
        print(json.dumps([asdict(p) for p in plans], indent=2))


if __name__ == "__main__":
    main()
