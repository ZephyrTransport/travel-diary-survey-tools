"""Base class and helpers for weighting controls.

Contains ``ControlLevel``, ``ControlTarget`` base class, and shared
expression helpers used by the household and person control subclasses.
"""

import logging
from enum import Enum, StrEnum
from itertools import product
from math import prod

import polars as pl

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

_SENTINEL_NAMES = frozenset({"MISSING", "PNTA"})

# Cross-tab validation thresholds
MAX_CROSSTAB_CELLS = 500  # Hard limit per individual cross-tab control
INFO_CROSSTAB_CELLS = 100  # Info log threshold

# Total category validation thresholds (across all controls)
MAX_TOTAL_CATEGORIES = 200  # Hard limit for total categories across all controls
INFO_TOTAL_CATEGORIES = 100  # Info log threshold for total categories


def identity_expr(col: str, categories: type[Enum]) -> pl.Expr:
    """Pass-through valid values, null for sentinels or unknown.

    Basically, we use the canonical enum directly, just map sentinels to null.
    """
    sentinels = [m.value for m in categories if m.name in _SENTINEL_NAMES]
    valid = [m.value for m in categories if m.name not in _SENTINEL_NAMES]
    return (
        pl.when(pl.col(col).is_null() | pl.col(col).is_in(sentinels))
        .then(None)
        .when(pl.col(col).is_in(valid))
        .then(pl.col(col))
        .otherwise(None)
        .cast(pl.Int16)
    )


def breakpoint_expr(col: str, categories: type[Enum]) -> pl.Expr:
    """Build a when/then chain from a LabeledEnum with ``BREAKPOINTS``.

    Zips ``categories.BREAKPOINTS`` with the non-sentinel members so that
    each breakpoint maps ``col < bp`` → the corresponding member value,
    with the final member as the ``otherwise`` catch-all.
    """
    members = [m for m in categories if m.name not in _SENTINEL_NAMES]
    breakpoints: list[int] = categories.BREAKPOINTS  # type: ignore[attr-defined]
    c = pl.col(col)
    expr = pl.when(c.is_null()).then(None)
    for bp, member in zip(breakpoints, members, strict=False):
        expr = expr.when(c < bp).then(member.value)
    return expr.otherwise(members[-1].value).cast(pl.Int16)


# ══════════════════════════════════════════════════════════════════════════
# Base class
# ══════════════════════════════════════════════════════════════════════════


class ControlLevel(StrEnum):
    """Whether a control is at the household or person level."""

    HOUSEHOLD = "household"
    PERSON = "person"


class ControlTarget:
    """Base class for a single weighting control.

    Subclasses set class attributes and override ``survey_expr`` /
    ``pums_expr`` to return native Polars expressions.

    Attributes:
        name: Registry key, e.g. ``"h_size"``.
        level: ``HOUSEHOLD`` or ``PERSON``.
        description: Human-readable label.
        categories: ``IntEnum`` or ``LabeledEnum`` for output bins.
        survey_fields: Canonical survey column names (metadata).
        pums_fields: PUMS column names (metadata).
    """

    name: str
    level: ControlLevel
    description: str
    categories: type[Enum]
    survey_fields: tuple[str, ...]
    pums_fields: tuple[str, ...]
    structural: bool = False

    def survey_expr(self) -> pl.Expr:
        """Polars expression mapping survey columns → control int (Int16)."""
        msg = f"{type(self).__name__}.survey_expr() not implemented"
        raise NotImplementedError(msg)

    def pums_expr(self) -> pl.Expr:
        """Polars expression mapping PUMS columns → control int (Int16)."""
        msg = f"{type(self).__name__}.pums_expr() not implemented"
        raise NotImplementedError(msg)

    @property
    def valid_members(self) -> list[tuple[int, str]]:
        """``(value, name)`` for each non-sentinel output category."""
        return [(m.value, m.name) for m in self.categories if m.name not in _SENTINEL_NAMES]


class CrosstabControlTarget(ControlTarget):
    """Base class for N-dimensional cross-tabulated weighting control.

    Cross-tabs are built from the cartesian product of **effective**
    dimension members — after any per-dimension merges have been applied.
    Merges are resolved at registration time so the enum, expression,
    and validation all reflect the actual cell count.

    Subclasses set these additional attributes:

    - ``dim_controls`` : tuple of ControlTarget instances to cross-tabulate
    - ``categories`` : IntEnum generated at registration from effective dims
    - ``dim_value_groups`` : per-dimension list of (name, values) tuples
      describing the effective members (may include merged groups)

    The ``survey_expr()`` / ``pums_expr()`` methods are implemented
    automatically to map dimension column values to composite indices.
    """

    dim_controls: tuple[ControlTarget, ...]
    dim_value_groups: list[list[tuple[str, list[int]]]]

    def __init__(self) -> None:
        """Validate cross-tab dimensions on initialization."""
        # Validate cell count limits
        cell_count = self._compute_cell_count()

        if cell_count > MAX_CROSSTAB_CELLS:
            msg = (
                f"Crosstab '{self.name}' has {cell_count} cells (max: {MAX_CROSSTAB_CELLS}).\n"
                f"Dimensions: {[(c.name, len(c.valid_members)) for c in self.dim_controls]}\n"
                f"Pre-merge dimension categories before creating the crosstab."
            )
            raise ValueError(msg)

        if cell_count > INFO_CROSSTAB_CELLS:
            logger.info(
                "Large crosstab: '%s' has %d cells. Ensure adequate sample size in all zones.\n"
                "Dimensions: %s",
                self.name,
                cell_count,
                [(c.name, len(c.valid_members)) for c in self.dim_controls],
            )

    def _compute_cell_count(self) -> int:
        """Compute total cells in cross-tab (product of effective dimension sizes)."""
        return prod(len(groups) for groups in self.dim_value_groups)

    def survey_expr(self) -> pl.Expr:
        """Combine dimension control columns into composite key.

        Requires dimension control columns to already exist in the
        DataFrame (recoded earlier in the control loop).  Returns
        sequential integer (0, 1, 2, ...) corresponding to position
        in the cartesian product of dimension categories.
        """
        return self._composite_expr()

    def pums_expr(self) -> pl.Expr:
        """Combine dimension control columns into composite key.

        Requires dimension control columns to already exist in the
        DataFrame (recoded earlier in the control loop).  Uses same
        mapping as survey_expr().
        """
        return self._composite_expr()

    def _composite_expr(self) -> pl.Expr:
        """Build a when/then chain mapping dimension column values to composite index.

        Uses ``dim_value_groups`` so merged groups correctly match
        multiple original values via ``is_in()``.
        """
        # Build cartesian product of effective groups across dimensions
        cells: list[tuple[tuple[str, list[int]], ...]] = list(product(*self.dim_value_groups))

        result = pl.when(pl.lit(value=False)).then(None)
        for idx, cell in enumerate(cells):
            condition = pl.lit(value=True)
            for dim_ctrl, (_, values) in zip(self.dim_controls, cell, strict=False):
                if len(values) == 1:
                    condition = condition & (pl.col(dim_ctrl.name) == values[0])
                else:
                    condition = condition & pl.col(dim_ctrl.name).is_in(values)
            result = result.when(condition).then(idx)

        return result.otherwise(None).cast(pl.Int16)
