# ruff: noqa: D102
"""Household-level weighting controls.

Each class maps raw survey / PUMS values into coarser category ints for
household-level weighting targets.

All ``survey_expr`` / ``pums_expr`` overrides implement the interface
documented in [`ControlTarget`][processing.weighting.controls.base.ControlTarget]
— individual method docstrings are omitted for brevity (ruff noqa: D102).
"""

from enum import IntEnum

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
    HHVehiclesPerDriverCategory,
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

    pums_expr = survey_expr


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
        # VEH = -1 is N/A (vacant homes and group quarters, both dropped by
        # clean_pums); left null so a stray one stops the run at the null check.
        v = pl.col("VEH")
        return pl.when(v >= 0).then(v.clip(0, 6)).cast(pl.Int16)


class HHVehiclesPerDriverControl(ControlTarget):
    """Vehicles in household against drivers: none, fewer, as many, more.

    A driver is anyone of driving age (16+); neither side has license data.
    """

    name = "h_vehicles_per_driver"
    level = ControlLevel.HOUSEHOLD
    description = "Vehicles per driver"
    categories = HHVehiclesPerDriverCategory
    survey_fields = ("num_vehicles", "_n_drivers")
    pums_fields = ("VEH",)  # drivers derived from person-level AGEP in recode_pums_households

    @staticmethod
    def _per_driver(vehicles: pl.Expr) -> pl.Expr:
        drivers = pl.col("_n_drivers")
        return (
            pl.when(vehicles.is_null())
            .then(None)
            .when(vehicles == 0)
            .then(HHVehiclesPerDriverCategory.NONE)
            .when(vehicles < drivers)
            .then(HHVehiclesPerDriverCategory.LESS_THAN_DRIVERS)
            .when(vehicles == drivers)
            .then(HHVehiclesPerDriverCategory.ONE_TO_ONE)
            .otherwise(HHVehiclesPerDriverCategory.MORE_THAN_DRIVERS)
            .cast(pl.Int16)
        )

    def survey_expr(self) -> pl.Expr:
        return self._per_driver(pl.col("num_vehicles"))

    def pums_expr(self) -> pl.Expr:
        # VEH = -1 is N/A and stays null, as in h_vehicles, so a stray one stops the run.
        v = pl.col("VEH")
        return self._per_driver(pl.when(v >= 0).then(v))


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

    pums_expr = survey_expr


class HHPumaControl(ControlTarget):
    """Home PUMA: one category per crosswalk PUMA, plus outside the region.

    The categories depend on the geography, so the control is built at run time
    by ``register_puma_control``. It is never imputed: where a household lives is
    not a guess from its other answers, so a missing PUMA fails the sum check.
    """

    name = "h_puma"
    level = ControlLevel.HOUSEHOLD
    description = "Home PUMA"
    survey_fields = ("home_puma",)
    pums_fields = ("PUMA",)
    imputable = False

    def __init__(self, puma_ids: list[str]) -> None:
        """Number the PUMAs from 1; 0 is a PUMS household outside the region."""
        self._codes = {puma: i for i, puma in enumerate(puma_ids, start=1)}
        members = {"OUTSIDE_REGION": 0} | {f"PUMA_{p}": i for p, i in self._codes.items()}
        self.categories = IntEnum("HHPumaCategory", members)

    def survey_expr(self) -> pl.Expr:
        return pl.col("home_puma").replace_strict(self._codes, default=None, return_dtype=pl.Int16)

    def pums_expr(self) -> pl.Expr:
        # Allocation drops the outside rows; the PUMS is statewide until then.
        return pl.col("PUMA").replace_strict(self._codes, default=0, return_dtype=pl.Int16)


class HHTargetZoneControl(ControlTarget):
    """Home target zone: one category per zone of the ``target_zones`` file.

    The categories depend on the geography, so the control is built at run time
    by ``register_target_zone_control``. A PUMS record's zone is known only once
    the crosswalk has allocated it, one copy per zone its PUMA reaches, so it is
    recoded after allocation. It is never imputed: a household in no zone fails
    the sum check.
    """

    name = "h_target_zone"
    level = ControlLevel.HOUSEHOLD
    description = "Home target zone"
    survey_fields = ("target_zone_id",)
    pums_fields = ()  # allocation adds target_zone_id; nothing is fetched for it
    imputable = False
    pums_after_allocation = True

    def __init__(self, zone_ids: list[str]) -> None:
        """Number the zones from 1, in the order given."""
        self._codes = {zone: i for i, zone in enumerate(zone_ids, start=1)}
        self.categories = IntEnum("HHTargetZoneCategory", dict(self._codes))

    def survey_expr(self) -> pl.Expr:
        return pl.col("target_zone_id").replace_strict(
            self._codes, default=None, return_dtype=pl.Int16
        )

    pums_expr = survey_expr


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

    pums_expr = survey_expr
