"""Shared weight hierarchy constants and propagation helpers.

Used by both ``weighting`` and ``existing_weights`` (pre-computed)
steps to propagate household weights down through the canonical table hierarchy.

# Weight derivation

|Table      |Weight Column           |Derivation
|-----------|------------------------|-----------------------------------------
|households |``hh_weight``           |Direct from balancer
|persons    |``person_weight``       |Carry forward ``hh_weight`` via ``hh_id``
|days       |``day_weight``          |Carry forward ``person_weight`` via ``person_id``
|unlinked   |``unlinked_trip_weight``| Carry forward ``day_weight`` via ``day_id``
|linked     |``linked_trip_weight``  |Mean of constituent ``unlinked_trip_weight``
|tours      |``tour_weight``         |Mean of constituent ``linked_trip_weight``

# Checksums (logged as warnings if violated)

* ``sum(person_weight) ≈ sum(hh_weight x persons_per_hh)``
* ``sum(day_weight) ≈ sum(person_weight x complete_travel_days)``
* ``sum(unlinked_trip_weight) ≈ sum(day_weight x trips_per_day)``

# Gate column

Records excluded by the *gate column* receive a weight of **0** after the
carry-forward join, so they never contribute to downstream aggregations (the
aggregation step already excludes zeros).  The gate defaults to
``model_usable`` -- the modelling gate stamped by the ``flag_model_usable``
step (see [`processing.completeness`][processing.completeness]) -- so the
weighted sample matches the tours CT-RAMP/DaySim actually keep.  Pass
``complete`` instead to weight the whole valid survey, including partial and
overnight tours.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)

# Canonical mapping: config_key -> (table_name, id_column, weight_column)
WEIGHT_CONFIG_MAPPING: dict[str, tuple[str, str, str]] = {
    "household_weights": ("households", "hh_id", "hh_weight"),
    "person_weights": ("persons", "person_id", "person_weight"),
    "day_weights": ("days", "day_id", "day_weight"),
    "unlinked_trip_weights": ("unlinked_trips", "unlinked_trip_id", "unlinked_trip_weight"),
    "linked_trip_weights": ("linked_trips", "linked_trip_id", "linked_trip_weight"),
    "joint_trip_weights": ("joint_trips", "joint_trip_id", "joint_trip_weight"),
    "tour_weights": ("tours", "tour_id", "tour_weight"),
}

# Derived lookup: table_name -> weight_column
WEIGHT_COLUMNS: dict[str, str] = {
    table: weight for table, _, weight in WEIGHT_CONFIG_MAPPING.values()
}

# Carry-forward: (parent_table, child_table, join_key, child_weight_col)
CARRY_FORWARD = [
    ("households", "persons", "hh_id", "person_weight"),
    ("persons", "days", "person_id", "day_weight"),
    ("days", "unlinked_trips", "day_id", "unlinked_trip_weight"),
]

# Aggregate (mean): (source_table, target_table, group_key, target_weight_col)
AGGREGATE = [
    ("unlinked_trips", "linked_trips", "linked_trip_id", "linked_trip_weight"),
    ("linked_trips", "joint_trips", "joint_trip_id", "joint_trip_weight"),
    ("linked_trips", "tours", "tour_id", "tour_weight"),
]

# All canonical table names in hierarchy order
TABLE_NAMES = [
    "households",
    "persons",
    "days",
    "unlinked_trips",
    "linked_trips",
    "joint_trips",
    "tours",
]


def collect_tables(
    *,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame | None]:
    """Bundle canonical tables into a dict keyed by table name.

    Kind of silly, but handles Nones and keeps the table names straight in one place.
    """
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
    }


def safe_join_weight(
    df: pl.DataFrame,
    weight_df: pl.DataFrame,
    on: str,
) -> pl.DataFrame:
    """Left-join *weight_df* onto *df*, dropping any pre-existing weight columns."""
    new_cols = [c for c in weight_df.columns if c != on]
    for c in new_cols:
        if c in df.columns:
            df = df.drop(c)
    return df.join(weight_df, on=on, how="left")


def propagate_weights(  # noqa: C901, PLR0912
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    skip: set[str] | None = None,
    gate_column: str | None = "model_usable",
) -> None:
    """Carry forward and aggregate weights through the hierarchy.

    Modifies *tables* and *has_weight* **in place**.

    Args:
        tables: Mutable dict of table_name → DataFrame (or None).
        has_weight: Mutable dict tracking which tables already have a
            weight column and the column name.
            E.g. ``{"households": "hh_weight"}``.
        skip: Table names to skip (e.g. tables that already have
            externally provided weights).
        gate_column: Boolean column deciding which records may carry weight.
            Records where it is False get a weight of 0 after each
            carry-forward join. Defaults to ``model_usable`` (the modelling
            gate); pass ``complete`` to weight the whole valid survey including
            partial/overnight tours, or None to skip zeroing entirely.
    """
    skip = skip or set()

    # Carry forward: parent weight -> child via join
    for parent, child, join_key, weight_col in CARRY_FORWARD:
        if child in skip:
            continue
        child_df = tables.get(child)
        if child_df is None:
            continue

        if parent not in has_weight:
            msg = (
                f"Cannot derive {weight_col} for {child}: "
                f"parent table {parent} has no weight column"
            )
            raise ValueError(msg)

        parent_df = tables.get(parent)
        if parent_df is None:
            msg = f"Cannot derive {weight_col} for {child}: parent table {parent} is None"
            raise ValueError(msg)

        if join_key not in child_df.columns:
            msg = f"Cannot derive weight: {child} missing join key {join_key}"
            raise ValueError(msg)

        parent_weight = has_weight[parent]
        logger.info("Deriving %s from %s via %s", weight_col, parent_weight, join_key)

        w = parent_df.select(join_key, parent_weight).rename({parent_weight: weight_col})
        tables[child] = safe_join_weight(child_df, w, join_key)

        # Zero out weight for records the gate excludes
        if gate_column and gate_column in tables[child].columns:
            tables[child] = tables[child].with_columns(
                pl.when(pl.col(gate_column).fill_null(value=False))
                .then(pl.col(weight_col))
                .otherwise(0.0)
                .alias(weight_col)
            )

        has_weight[child] = weight_col

    # Aggregate: mean weight from source grouped by key
    for source, target, group_key, weight_col in AGGREGATE:
        if target in skip:
            continue
        target_df = tables.get(target)
        if target_df is None:
            continue

        source_df = tables.get(source)
        if source_df is None:
            msg = f"Cannot derive {weight_col} for {target}: source table {source} is None"
            raise ValueError(msg)

        if source not in has_weight:
            msg = (
                f"Cannot derive {weight_col} for {target}: "
                f"source table {source} has no weight column"
            )
            raise ValueError(msg)

        if group_key not in source_df.columns:
            msg = f"Cannot derive {weight_col}: source {source} missing {group_key}"
            raise ValueError(msg)

        src_weight = has_weight[source]
        logger.info("Deriving %s from mean of %s", weight_col, src_weight)

        agg = source_df.group_by(group_key).agg(
            pl.col(src_weight)
            .filter(pl.col(src_weight).is_not_null() & (pl.col(src_weight) != 0))
            .mean()
            .fill_null(0)
            .alias(weight_col),
        )
        tables[target] = safe_join_weight(target_df, agg, group_key)
        has_weight[target] = weight_col


def non_null_tables(tables: dict[str, pl.DataFrame | None]) -> dict[str, pl.DataFrame]:
    """Return only the non-None entries from a tables dict."""
    return {k: v for k, v in tables.items() if v is not None}
