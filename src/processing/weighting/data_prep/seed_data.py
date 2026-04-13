"""Survey seed-data preparation for weighting.

Uses ``ControlTarget.survey_expr()`` to recode canonical survey data into
control-category ints via native Polars expressions (vectorised, no
``map_elements``).  Every control is handled uniformly.

Survey field mapping is hardcoded in each
[`ControlTarget`][processing.weighting.controls.base.ControlTarget]
subclass — the ``survey_fields`` class attribute declares which canonical
survey columns the control reads, and ``survey_expr()`` returns the Polars
expression that maps those values to control-category ints.  For example:

* ``HHIncomeControl.survey_fields = ("income_bin",)`` — reads the
  already-binned ``income_bin`` column via ``identity_expr``.
* ``GenderControl.survey_fields = ("gender",)`` — maps ``Gender`` enum
  values to ``GenderCategory`` ints via ``replace_strict``.
* ``HHSizeControl.survey_fields = ("_n_persons",)`` — clips the
  person-count aggregate column to [1, 10].

No YAML config is involved; the mapping lives entirely in the control
class definitions in [`processing.weighting.controls`][processing.weighting.controls].
"""

import logging

import polars as pl

from data_canon.codebook.persons import AgeCategory, Employment
from processing.weighting.controls.base import (
    ControlLevel,
    ControlTarget,
    CrosstabControlTarget,
)
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.validation.checksums import check_recode_nulls

logger = logging.getLogger(__name__)


# -- Public API ------------------------------------------------------------
def recode_survey_households(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    targets: list[str],
    strict_nulls: bool = False,
) -> pl.DataFrame:
    """Recode canonical survey households into control categories.

    Args:
        households: Canonical households table (must have ``hh_id``).
        persons: Canonical persons table (must have ``hh_id``).
        targets: Registry keys to recode.  Person-level keys are ignored.
        strict_nulls: If ``True``, raise on any null recode output.

    Raises:
        ValueError: Unknown target.
        KeyError: Missing required column.
    """
    hh_ctrls = resolve_targets(targets, ControlLevel.HOUSEHOLD)
    if not hh_ctrls:
        return households

    # Pre-aggregate person → household counts for derived HH controls.
    # These become plain columns (_n_persons, _n_workers, _n_children)
    # that the control expressions read directly.
    _person_aggs: dict[str, pl.Expr] = {
        "h_size": pl.len().alias("_n_persons"),
        "h_workers": (
            pl.col("employment")
            .is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                ]
            )
            .sum()
            .cast(pl.Int32)
            .alias("_n_workers")
        ),
        "h_children": (
            pl.col("age")
            .is_in(
                [
                    AgeCategory.AGE_UNDER_5.value,
                    AgeCategory.AGE_5_TO_15.value,
                    AgeCategory.AGE_16_TO_17.value,
                ]
            )
            .sum()
            .cast(pl.Int32)
            .alias("_n_children")
        ),
    }
    requested = {c.name for c in hh_ctrls}
    aggs = [expr for name, expr in _person_aggs.items() if name in requested]

    if aggs:
        counts = persons.group_by("hh_id").agg(aggs)
        hh = households.join(counts, on="hh_id", how="left")
    else:
        hh = households

    for ctrl in hh_ctrls:
        _require_fields(hh, ctrl)
        hh = _apply_recode(hh, ctrl)

    check_recode_nulls(
        hh,
        targets,
        level=ControlLevel.HOUSEHOLD,
        id_col="hh_id",
        source_label="survey",
        strict=strict_nulls,
    )
    return hh


def recode_survey_persons(
    persons: pl.DataFrame,
    targets: list[str],
    strict_nulls: bool = False,
) -> pl.DataFrame:
    """Recode canonical survey persons into control categories.

    Args:
        persons: Canonical persons table.
        targets: Registry keys to recode.  Household-level keys are ignored.
        strict_nulls: If ``True``, raise on any null recode output.

    Raises:
        ValueError: Unknown target.
        KeyError: Missing required column.
    """
    p_ctrls = resolve_targets(targets, ControlLevel.PERSON)
    df = persons

    for ctrl in p_ctrls:
        logger.info("Recoding survey person control '%s'...", ctrl.name)
        _require_fields(df, ctrl)
        df = _apply_recode(df, ctrl)

    check_recode_nulls(
        df,
        targets,
        level=ControlLevel.PERSON,
        id_col="person_id",
        source_label="survey",
        strict=strict_nulls,
    )
    return df


# -- Internals -------------------------------------------------------------


def _apply_recode(df: pl.DataFrame, ctrl: ControlTarget) -> pl.DataFrame:
    """Add ``{name}`` column via ``ctrl.survey_expr()``."""
    # Cross-tab controls derive their values from other control expressions
    # (no direct survey fields)
    if isinstance(ctrl, CrosstabControlTarget):
        return df.with_columns(ctrl.survey_expr().alias(ctrl.name))

    # Add null columns for any optional fields absent from the DataFrame
    for f in ctrl.survey_fields:
        if f not in df.columns:
            df = df.with_columns(pl.lit(None).alias(f))
    return df.with_columns(ctrl.survey_expr().alias(ctrl.name))


def _require_fields(df: pl.DataFrame, ctrl: ControlTarget) -> None:
    """Raise ``KeyError`` if the primary survey field is absent."""
    # Structural controls (h_total, p_total) have no survey fields.
    if ctrl.structural:
        return
    # Cross-tab controls derive from other controls (dimension expressions)
    if isinstance(ctrl, CrosstabControlTarget):
        # Check that dimension controls have been recoded first
        for dim_ctrl in ctrl.dim_controls:
            if dim_ctrl.name not in df.columns:
                msg = (
                    f"Cross-tab '{ctrl.name}' requires dimension control '{dim_ctrl.name}' "
                    f"to be recoded first. Have: {sorted(df.columns)}"
                )
                raise KeyError(msg)
        return
    if not ctrl.survey_fields:
        msg = (
            f"Target '{ctrl.name}' has no survey_fields defined. "
            f"Set structural=True if this is intentional."
        )
        raise ValueError(msg)
    # Optional secondary fields (e.g., school_type for p_student) may be
    # absent; we only require the first (primary) field.
    if ctrl.survey_fields[0] not in df.columns:
        msg = (
            f"Target '{ctrl.name}' requires column '{ctrl.survey_fields[0]}'. "
            f"Have: {sorted(df.columns)}"
        )
        raise KeyError(msg)
