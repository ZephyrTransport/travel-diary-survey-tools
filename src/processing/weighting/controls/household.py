# ruff: noqa: D102
"""Household-level weighting controls.

Each class maps raw survey / PUMS values into coarser category ints for
household-level weighting targets.

All ``survey_expr`` / ``pums_expr`` overrides implement the interface
documented in [`ControlTarget`][processing.weighting.controls.base.ControlTarget]
— individual method docstrings are omitted for brevity (ruff noqa: D102).
"""

import polars as pl

from data_canon.codebook.households import IncomeBroad
from processing.weighting.controls.base import (
    ControlLevel,
    ControlTarget,
    breakpoint_expr,
    identity_expr,
)
from processing.weighting.controls.enums import (
    HHChildrenCategory,
    HHSizeCategory,
    HHVehiclesCategory,
    HHWorkersCategory,
    TotalCategory,
)


class HHSizeControl(ControlTarget):
    """Household size (1-10+)."""

    name = "h_size"
    level = ControlLevel.HOUSEHOLD
    description = "Household size"
    categories = HHSizeCategory
    survey_fields = ("_n_persons",)
    pums_fields = ("NP",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("_n_persons").clip(1, 10).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.col("NP").clip(1, 10).cast(pl.Int16)


class HHIncomeControl(ControlTarget):
    """Household income (canonical IncomeBroad bins)."""

    name = "h_income"
    level = ControlLevel.HOUSEHOLD
    description = "Household income"
    categories = IncomeBroad
    survey_fields = ("income_bin",)
    pums_fields = ("HINCP",)

    def survey_expr(self) -> pl.Expr:
        return identity_expr("income_bin", IncomeBroad)

    def pums_expr(self) -> pl.Expr:
        return breakpoint_expr("HINCP", IncomeBroad)


class HHWorkersControl(ControlTarget):
    """Number of workers in household (0-5+)."""

    name = "h_workers"
    level = ControlLevel.HOUSEHOLD
    description = "Workers in household"
    categories = HHWorkersCategory
    survey_fields = ("_n_workers",)
    pums_fields = ()  # derived from person-level ESR in recode_pums_households

    def survey_expr(self) -> pl.Expr:
        return pl.col("_n_workers").clip(0, 5).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.col("_n_workers").clip(0, 5).cast(pl.Int16)


class HHVehiclesControl(ControlTarget):
    """Vehicles in household (0-6+)."""

    name = "h_vehicles"
    level = ControlLevel.HOUSEHOLD
    description = "Vehicles in household"
    categories = HHVehiclesCategory
    survey_fields = ("num_vehicles",)
    pums_fields = ("VEH",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("num_vehicles").clip(0, 6).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        # VEH is -1 / null for GQ records (already filtered) and
        # occasionally for housing-unit records with missing data.
        # Map any non-positive / null → 0 vehicles so every HH lands
        # in exactly one category.
        v = pl.col("VEH")
        return pl.when(v.is_null() | (v < 0)).then(0).otherwise(v.clip(0, 6)).cast(pl.Int16)


class HHChildrenControl(ControlTarget):
    """Children in household (0-5+)."""

    name = "h_children"
    level = ControlLevel.HOUSEHOLD
    description = "Children in household"
    categories = HHChildrenCategory
    survey_fields = ("_n_children",)
    pums_fields = ()  # derived from person-level AGEP in recode_pums_households

    def survey_expr(self) -> pl.Expr:
        return pl.col("_n_children").clip(0, 5).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.col("_n_children").clip(0, 5).cast(pl.Int16)


class HHTotalControl(ControlTarget):
    """Structural control: total households (incidence = 1 per HH)."""

    name = "h_total"
    level = ControlLevel.HOUSEHOLD
    description = "Total households"
    categories = TotalCategory
    survey_fields = ()
    pums_fields = ()
    structural = True

    def survey_expr(self) -> pl.Expr:
        return pl.lit(1).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.lit(1).cast(pl.Int16)
