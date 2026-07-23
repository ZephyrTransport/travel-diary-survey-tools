"""Canonical completeness and model-usability logic.

Two distinct, complementary flags live on the canonical tables:

* ``complete`` -- **survey reporting completeness**: did the respondent fully
  report this record (and its ancestors)? Overnight and partial tours are still
  *valid survey data*, so they keep ``complete = True``. This is the descriptor.

* ``model_usable`` -- **admissibility to the tour-based (activity-based) model**:
  is this record part of a well-formed tour structure the model can consume? It
  is derived as ``complete AND valid-tour-structure AND day-rule`` and is the
  single gate that both the CT-RAMP/DaySim drop and the weighting zeroing read.
  Nothing gates on ``complete`` directly.

This module is the one place that logic lives, so the two concepts never
compete. :func:`compute_model_usable` is applied once by the ``flag_model_usable``
pipeline step; downstream consumers only read the resulting flags.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from pipeline.decoration import step

logger = logging.getLogger(__name__)

# Completeness cascade: (parent_table, child_table, join_key). A child is
# effectively complete only if it and its parent are complete; incompleteness
# flows household -> person -> day -> trips/tours.
COMPLETENESS_CASCADE = [
    ("households", "persons", "hh_id"),
    ("persons", "days", "person_id"),
    ("days", "unlinked_trips", "day_id"),
    ("days", "linked_trips", "day_id"),
    ("days", "joint_trips", "day_id"),
    ("days", "tours", "day_id"),
]

# Trip tables whose model-usability follows the tour they belong to (tour_id).
_TOUR_MEMBER_TABLES = ("unlinked_trips", "linked_trips")


def cascade_completeness(tables: dict[str, pl.DataFrame | None]) -> None:
    """Flag reporting completeness from households down through the hierarchy, in place.

    A record is effectively complete only if it *and* every ancestor is
    complete. Incompleteness flows downward: an incomplete household forces all
    its persons, days, trips and tours incomplete; an incomplete person forces
    its days/trips/tours; an incomplete day forces its trips/tours.

    Each child table's ``complete`` column is overwritten with
    ``own_complete AND parent_complete``. A null own flag is treated as
    incomplete; a missing parent (orphan) does not force incompleteness. Tables
    missing the ``complete`` column or the join key are left unchanged. Because
    the cascade runs parent-before-child, each parent is already effective when
    its children are processed. Idempotent: re-running never changes a result.
    """
    for parent, child, key in COMPLETENESS_CASCADE:
        parent_df = tables.get(parent)
        child_df = tables.get(child)
        if parent_df is None or child_df is None:
            continue
        if (
            "complete" not in parent_df.columns
            or "complete" not in child_df.columns
            or key not in child_df.columns
        ):
            continue

        parent_flag = parent_df.select(
            key, pl.col("complete").fill_null(value=False).alias("_parent_complete")
        )
        child_df = child_df.join(parent_flag, on=key, how="left")
        tables[child] = child_df.with_columns(
            (
                pl.col("complete").fill_null(value=False)
                & pl.col("_parent_complete").fill_null(value=True)
            ).alias("complete")
        ).drop("_parent_complete")


def _tour_model_usable_expr(
    *, require_valid_tours: bool, has_quality: bool, has_category: bool
) -> pl.Expr:
    """model_usable expression for the tours table.

    A tour is model-usable when its (cascaded) reporting is complete and, if
    ``require_valid_tours``, its structure is admissible: VALID quality (not
    single-trip, loop, missing-anchor, change-mode, spatial-gap, indeterminate)
    AND COMPLETE category (starts and ends at home). This matches the CT-RAMP /
    DaySim drop criterion exactly.
    """
    usable = pl.col("complete").fill_null(value=False)
    if require_valid_tours and has_quality:
        usable = usable & (pl.col("tour_data_quality") == TourDataQuality.VALID.value)
    if require_valid_tours and has_category:
        usable = usable & (pl.col("tour_category") == TourCategory.COMPLETE.value)
    return usable


def compute_model_usable(
    tables: dict[str, pl.DataFrame | None],
    *,
    require_valid_tours: bool = True,
) -> None:
    """Stamp the ``model_usable`` gate on every table, in place.

    Runs :func:`cascade_completeness` first (idempotent) so ``complete`` reflects
    ancestry, then derives ``model_usable`` per level:

    * tours: ``complete AND VALID quality AND COMPLETE category`` (when
      ``require_valid_tours``; otherwise just ``complete``).
    * days: ``complete AND (no tours OR at least one model_usable tour)`` -- a
      genuine no-travel day stays usable, but a day whose only tours are all
      inadmissible is not (its travel could not be turned into a usable tour).
    * unlinked/linked trips: ``complete AND the tour they belong to is usable``
      (a trip with no tour is not model-usable, matching the CT-RAMP drop).
    * households / persons / joint trips: ``complete`` (no tour structure of
      their own; joint-trip weight follows its member linked trips).

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        require_valid_tours: If True (default), fold tour structural validity
            into the gate. If False, ``model_usable`` reduces to reporting
            completeness (invalid tours stay usable).
    """
    cascade_completeness(tables)

    # -- Tours ---------------------------------------------------------------
    tours = tables.get("tours")
    if tours is not None and "complete" in tours.columns:
        tables["tours"] = tours.with_columns(
            _tour_model_usable_expr(
                require_valid_tours=require_valid_tours,
                has_quality="tour_data_quality" in tours.columns,
                has_category="tour_category" in tours.columns,
            ).alias("model_usable")
        )
        tours = tables["tours"]

    # -- Days (upward rule: needs a usable tour, unless it is a no-travel day) -
    days = tables.get("days")
    if days is not None and "complete" in days.columns:
        base = pl.col("complete").fill_null(value=False)
        if tours is not None and "model_usable" in tours.columns and "day_id" in tours.columns:
            day_has_usable = tours.group_by("day_id").agg(
                pl.col("model_usable").any().alias("_day_has_usable_tour")
            )
            days = days.join(day_has_usable, on="day_id", how="left")
            # null == the day has no tours at all -> a legitimate no-travel day.
            tables["days"] = days.with_columns(
                (base & pl.col("_day_has_usable_tour").fill_null(value=True)).alias("model_usable")
            ).drop("_day_has_usable_tour")
        else:
            tables["days"] = days.with_columns(base.alias("model_usable"))

    # -- Households / persons / joint trips: model_usable == complete ---------
    for name in ("households", "persons", "joint_trips"):
        df = tables.get(name)
        if df is not None and "complete" in df.columns:
            tables[name] = df.with_columns(
                pl.col("complete").fill_null(value=False).alias("model_usable")
            )

    # -- Member trips follow their tour --------------------------------------
    tour_usable = None
    if tours is not None and "model_usable" in tours.columns:
        tour_usable = tours.select("tour_id", pl.col("model_usable").alias("_tour_usable"))
    for name in _TOUR_MEMBER_TABLES:
        df = tables.get(name)
        if df is None or "complete" not in df.columns:
            continue
        base = pl.col("complete").fill_null(value=False)
        if tour_usable is not None and "tour_id" in df.columns:
            df = df.join(tour_usable, on="tour_id", how="left")
            tables[name] = df.with_columns(
                (base & pl.col("_tour_usable").fill_null(value=False)).alias("model_usable")
            ).drop("_tour_usable")
        else:
            tables[name] = df.with_columns(base.alias("model_usable"))


def _log_gate_summary(tables: dict[str, pl.DataFrame | None]) -> None:
    """Log, per table, how many records are complete vs model-usable."""
    lines = [
        "Model-usability gate applied.",
        '  "complete" = survey reporting (partials/overnights included); '
        '"model_usable" = admissible to the tour-based model.',
        "",
        f"  {'table':<16}{'rows':>10}{'complete':>12}{'model_usable':>14}{'net-new':>10}",
    ]
    for name, df in tables.items():
        if df is None or "model_usable" not in df.columns:
            continue
        n = df.height
        n_complete = df.filter(pl.col("complete").fill_null(value=False)).height
        n_usable = df.filter(pl.col("model_usable")).height
        # net-new = valid survey data the model still cannot use
        net_new = n_complete - n_usable
        lines.append(f"  {name:<16}{n:>10,}{n_complete:>12,}{n_usable:>14,}{net_new:>10,}")
    logger.info("\n".join(lines))


@step(
    requires={
        "days": {"day_id", "complete"},
        "tours": {"tour_id", "day_id", "complete"},
    },
)
def flag_model_usable(
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    require_valid_tours: bool = True,
) -> dict[str, pl.DataFrame]:
    """Stamp the canonical ``model_usable`` gate on every table.

    This is the single place the modelling gate is computed. Downstream steps
    (weighting, the CT-RAMP/DaySim formatters) only *read* ``model_usable``; none
    of them re-derive completeness or tour validity. Because the step is cached,
    the flag is inspectable in the canonical output alongside ``complete``.

    See [`compute_model_usable`][processing.completeness.compute_model_usable]
    for the per-level rules.

    Args:
        households: Canonical households.
        persons: Canonical persons.
        days: Canonical person-days.
        unlinked_trips: Canonical unlinked trips.
        linked_trips: Canonical linked trips.
        joint_trips: Aggregated joint trips.
        tours: Canonical tours (with ``tour_data_quality`` / ``tour_category``).
        require_valid_tours: If True (default), fold strict tour structure into
            the gate. If False, ``model_usable`` reduces to reporting
            completeness, so partial/invalid tours stay model-usable.

    Returns:
        The provided tables, each with a ``model_usable`` column added.
    """
    tables: dict[str, pl.DataFrame | None] = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
    }

    compute_model_usable(tables, require_valid_tours=require_valid_tours)
    _log_gate_summary(tables)

    return {name: df for name, df in tables.items() if df is not None}
