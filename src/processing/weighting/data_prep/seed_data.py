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

# The persons fields each household count is built from (see _person_aggs).
_COUNTED_FROM: dict[str, tuple[str, ...]] = {
    "h_size": ("hh_id",),
    "h_workers": ("employment",),
    "h_children": ("age",),
    "h_vehicles_per_driver": ("age",),
}

# The imputation step keeps each column's original values under this suffix.
_ORIGINAL_SUFFIX = "_preimputed"


# -- Public API ------------------------------------------------------------
def restore_original_answers(
    households: pl.DataFrame,
    persons: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Put the survey's own answers back in place of the imputation step's values.

    Blanks then reach `fill_null_incidence`, which spreads each one across a
    control's groups instead of committing it to one. Returns copies for weighting
    to recode, so the published columns keep their imputed values.

    Raises:
        ValueError: Neither table has a ``<column>_preimputed`` column.
    """
    tables = (households, persons)
    if not any(c.endswith(_ORIGINAL_SUFFIX) for df in tables for c in df.columns):
        msg = (
            "use_imputed_values is false, but no column ends in "
            f"'{_ORIGINAL_SUFFIX}'. Run imputation with stash_preimputed: true, "
            "or set it to true if nothing is imputed."
        )
        raise ValueError(msg)

    restored = [
        df.with_columns(
            [
                pl.col(c).alias(c.removesuffix(_ORIGINAL_SUFFIX))
                for c in df.columns
                if c.endswith(_ORIGINAL_SUFFIX)
            ]
        )
        for df in tables
    ]
    return restored[0], restored[1]


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
        ValueError: A required column is null on every row.
    """
    hh_ctrls = resolve_targets(targets, ControlLevel.HOUSEHOLD)
    if not hh_ctrls:
        return households

    # Pre-aggregate person → household counts for derived HH controls.
    # These become plain columns (_n_persons, _n_workers, _n_children, _n_drivers)
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
                    # Has a job and an income; PUMS counts "with a job but not at work".
                    Employment.EMPLOYED_FURLOUGHED.value,
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
        # Assuming here that anyone of driving age (16+) is considered a driver.
        "h_vehicles_per_driver": (
            (pl.col("age") >= AgeCategory.AGE_16_TO_17.value)
            .sum()
            .cast(pl.Int32)
            .alias("_n_drivers")
        ),
    }
    requested = {c.name for c in hh_ctrls}
    aggs = [expr for name, expr in _person_aggs.items() if name in requested]

    # A count is never null, so check the persons fields behind it before counting.
    for name, fields in _COUNTED_FROM.items():
        if name in requested:
            _require_values(persons, name, fields, "persons")

    if aggs:
        counts = persons.group_by("hh_id").agg(aggs)
        hh = households.join(counts, on="hh_id", how="left")
    else:
        hh = households

    for ctrl in hh_ctrls:
        _require_fields(hh, ctrl)
        hh = hh.with_columns(ctrl.survey_expr().alias(ctrl.name))

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
        ValueError: A required column is null on every row.
    """
    p_ctrls = resolve_targets(targets, ControlLevel.PERSON)
    df = persons

    for ctrl in p_ctrls:
        logger.info("Recoding survey person control '%s'...", ctrl.name)
        _require_fields(df, ctrl)
        df = df.with_columns(ctrl.survey_expr().alias(ctrl.name))

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


def _require_values(df: pl.DataFrame, name: str, fields: tuple[str, ...], table: str) -> None:
    """Raise if any of *fields* is absent from *df* or null on every row.

    Partly null is fine: non-workers have no ``commute_freq``. Entirely null means
    the control would recode from nothing, which it does without complaint.
    """
    missing = [f for f in fields if f not in df.columns]
    if missing:
        msg = f"Control '{name}' requires {table} column(s) {missing}. Have: {sorted(df.columns)}"
        raise KeyError(msg)
    empty = [f for f in fields if df.height and df[f].null_count() == df.height]
    if empty:
        msg = (
            f"Control '{name}' reads {table} column(s) {empty}, which are null on every "
            f"row. Supply the values, or remove '{name}' from the controls."
        )
        raise ValueError(msg)


def _require_fields(df: pl.DataFrame, ctrl: ControlTarget) -> None:
    """Raise if any of the control's survey fields is absent or entirely null."""
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
    table = "households" if ctrl.level is ControlLevel.HOUSEHOLD else "persons"
    _require_values(df, ctrl.name, ctrl.survey_fields, table)
