"""Supplied weights onto usable households, persons and days.

Households scale by one factor per household size; persons copy their
household, a dropped person's share staying inside it; a person's days carry
exactly that person's weight.
"""

import logging

import polars as pl

from processing.weighting.core.hierarchy import LEVELS
from processing.weighting.core.propagation import (
    distribute_within_scope,
    is_usable,
    zero_unusable,
)

logger = logging.getLogger(__name__)


# Rescale factors within this distance of 1.0 are floating-point drift, not work.
SCALE_TOL = 1e-9


# Known populations: always gated and held to their supplied totals.
POPULATION_TABLES = frozenset({"households", "persons", "person_days"})


def apply_usability(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
) -> dict[str, float]:
    """Redistribute supplied weights onto usable records, preserving each total.

    Unlike the computed path, the vendor's anchor cannot be re-balanced: their
    weights already sum to their population estimate, so dropping records must
    not shrink that total. Each supplied weight is first conserved within its
    declared scope ([`distribute_within_scope`]
    [processing.weighting.core.propagation.distribute_within_scope]);
    whatever that leaves stranded — scopes with no usable record at all — is
    then closed with one table-wide factor. Modifies *tables* in place.

    Args:
        tables: Mutable dict of table_name → DataFrame (or None).
        has_weight: Dict of table_name → weight column name.
        usability_flag_col: Boolean column deciding which records may carry weight
            -- a usability profile stamped upstream, or ``survey_complete`` to weight the
            whole valid survey.

    Returns:
        table -> the supplied total, for the placement check to hold each
        table to.
    """
    supplied_totals: dict[str, float] = {}
    for table_name, weight_col in has_weight.items():
        df = tables.get(table_name)
        if df is None or usability_flag_col not in df.columns:
            continue

        supplied_total = df.select(pl.col(weight_col).sum()).item() or 0.0
        supplied_totals[table_name] = supplied_total
        scope = LEVELS[table_name].scope

        if scope is None:
            # The anchor has no scope to spread within; zero and rescale below.
            df = df.with_columns(zero_unusable(weight_col, usability_flag_col))
        else:
            # Raises when the scope column is absent -- quietly conserving the
            # weight over some other grouping would change the numbers silently.
            df = distribute_within_scope(
                df,
                weight_col=weight_col,
                scope=scope,
                usability_flag_col=usability_flag_col,
                table=table_name,
            )

        placed_total = df.select(pl.col(weight_col).sum()).item() or 0.0
        if placed_total > 0 and supplied_total > 0:
            scale = supplied_total / placed_total
            if abs(scale - 1.0) > SCALE_TOL:
                df = df.with_columns((pl.col(weight_col) * scale).alias(weight_col))
                logger.info(
                    "%s: scaled %s by %.4f to preserve the supplied total (%.1f). This covers "
                    "weight that no usable record in its %s could take.",
                    table_name,
                    weight_col,
                    scale,
                    supplied_total,
                    scope or "table",
                )
        elif supplied_total > 0:
            logger.warning(
                "%s: no usable record carries weight; the supplied total (%.1f) is lost",
                table_name,
                supplied_total,
            )

        tables[table_name] = df
    return supplied_totals


def _size_label(group: int, top_group: int) -> str:
    """A size group as a reader would say it: ``3``, or ``6+`` for the top group."""
    return f"{group}+" if group == top_group else str(group)


def gate_households_by_size(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
    top_group: int,
) -> float:
    """Scale kept households by one factor per household size, in place.

    Size is the number of persons; sizes from *top_group* up share one group.
    Within each group::

        household = supplied x (group's supplied total / group's kept supplied total)

    Because every household in a group has the same size, this holds the
    group's person total as well as its household total -- one factor for all
    sizes would not, when the dropped households are larger or smaller than the
    kept ones. A household is kept when it is usable.

    Returns:
        The supplied household total, for the placement check to hold.

    Raises:
        ValueError: If there is no persons table (household size is the number
            of persons), or a size group carries supplied weight but has no kept
            household to put it on.
    """
    households, persons = tables["households"], tables.get("persons")
    if households is None:  # pragma: no cover - checked by the caller
        return 0.0
    if persons is None:
        msg = (
            "Household weights are scaled by household size, which is the number of "
            "persons, so the persons table is needed."
        )
        raise ValueError(msg)
    if top_group < 1:
        msg = f"household_size_top_group must be at least 1, got {top_group}"
        raise ValueError(msg)
    col = has_weight["households"]
    weight = pl.col(col).fill_null(0.0)

    sizes = persons.group_by("hh_id").agg(pl.len().alias("_size"))
    hh = households.join(sizes, on="hh_id", how="left").with_columns(
        pl.min_horizontal(pl.col("_size").fill_null(0), pl.lit(top_group)).alias("_group"),
        is_usable(usability_flag_col).alias("_kept"),
    )
    groups = (
        hh.group_by("_group")
        .agg(
            pl.len().alias("_n"),
            pl.col("_kept").sum().alias("_n_kept"),
            weight.sum().alias("_supplied"),
            weight.filter(pl.col("_kept")).sum().alias("_kept_supplied"),
        )
        .sort("_group")
    )

    empty = groups.filter((pl.col("_supplied") > 0) & (pl.col("_kept_supplied") <= 0))
    if empty.height:
        row = empty.row(0, named=True)
        msg = (
            f"household size {_size_label(row['_group'], top_group)}: {row['_supplied']:.1f} "
            f"of {col} was supplied over {row['_n']} household(s), but there is no kept "
            f"household of that size to carry it under {usability_flag_col}. Lower "
            "household_size_top_group to group this size with smaller ones."
        )
        logger.error(msg)
        raise ValueError(msg)

    groups = groups.with_columns(
        pl.when(pl.col("_kept_supplied") > 0)
        .then(pl.col("_supplied") / pl.col("_kept_supplied"))
        .otherwise(0.0)
        .alias("_factor")
    )
    for row in groups.iter_rows(named=True):
        logger.info(
            "households of size %s: %d of %d kept; %s scaled by %.4f",
            _size_label(row["_group"], top_group),
            row["_n_kept"],
            row["_n"],
            col,
            row["_factor"],
        )

    # A dropped household goes to 0, and one the supplier never weighted stays null.
    tables["households"] = (
        hh.join(groups.select("_group", "_factor"), on="_group", how="left")
        .with_columns(
            pl.when(pl.col("_kept"))
            .then(pl.col(col) * pl.col("_factor"))
            .otherwise(pl.col(col) * 0.0)
            .alias(col)
        )
        .drop("_size", "_group", "_kept", "_factor")
    )
    return float(households.select(weight.sum()).item())


def persons_follow_household(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
) -> None:
    """Give each household's weight to its usable persons, in place.

    A household stands for all its persons, so its persons together carry its
    weight times its number of persons. The usable ones share it in proportion
    to their supplied weights::

        person = household x n_persons x supplied / sum(supplied of the usable persons)

    so a dropped person's share stays in the household, and household weight x
    number of persons = the sum of its persons' weights.
    """
    persons, households = tables["persons"], tables["households"]
    if persons is None or households is None:  # pragma: no cover - checked by the caller
        return
    person_col, hh_col = has_weight["persons"], has_weight["households"]
    claim = pl.col(person_col).fill_null(0.0)
    supplied_total = float(persons.select(claim.sum()).item())

    per_hh = persons.group_by("hh_id").agg(
        pl.len().alias("_n"), claim.filter(is_usable(usability_flag_col)).sum().alias("_carried")
    )
    persons = (
        persons.join(per_hh, on="hh_id", how="left")
        .join(households.select("hh_id", pl.col(hh_col).alias("_hh")), on="hh_id", how="left")
        .with_columns(
            pl.when(is_usable(usability_flag_col) & (pl.col("_carried") > 0))
            .then(pl.col("_hh") * pl.col("_n") * claim / pl.col("_carried"))
            .otherwise(pl.col(person_col) * 0.0)
            .alias(person_col)
        )
        .drop("_n", "_carried", "_hh")
    )
    tables["persons"] = persons

    placed = float(persons.select(pl.col(person_col).fill_null(0.0).sum()).item())
    logger.info(
        "persons: %s copies the household; total %.1f against %.1f supplied (%+.3f%%)",
        person_col,
        placed,
        supplied_total,
        (placed / supplied_total - 1) * 100 if supplied_total else 0.0,
    )


def follow_parent(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    table: str,
    *,
    usability_flag_col: str,
) -> None:
    """Share each parent's final weight among *table*'s usable records, in place.

    Each record takes its parent's weight in proportion to its own claim::

        w = claim x parent_final / sum(usable claims under the parent)

    so a parent's usable records sum to the parent, and a record the supplier
    weighted 0 stays 0. A parent whose final weight is positive but that has no
    usable claim under it is stranded: its weight reaches no record here, and is
    logged for the totals check to report.
    """
    level = LEVELS[table]
    df, parent_df = tables[table], tables[level.parent]  # type: ignore[index]
    if df is None or parent_df is None:
        return
    key, weight_col = level.key, has_weight[table]
    usable = is_usable(usability_flag_col)
    claim = pl.col(weight_col).fill_null(0.0)

    usable_claims = df.group_by(key).agg(claim.filter(usable).sum().alias("_usable_claim"))
    parents = parent_df.select(
        key,
        pl.col(has_weight[level.parent]).fill_null(0.0).alias("_parent"),  # type: ignore[index]
    ).join(usable_claims, on=key, how="left")

    stranded = parents.filter(
        (pl.col("_parent") > 0) & (pl.col("_usable_claim").fill_null(0.0) <= 0)
    )
    if stranded.height:
        logger.warning(
            "%s: %d %s(s) carry weight but no usable %s with a supplied weight; %.1f of "
            "%s reaches no record (sample %s: %s)",
            table,
            stranded.height,
            level.parent,
            table,
            float(stranded["_parent"].sum()),
            has_weight[level.parent],  # type: ignore[index]
            key,
            stranded[key].head(10).to_list(),
        )

    tables[table] = (
        df.join(parents, on=key, how="left")
        .with_columns(
            pl.when(usable & (pl.col("_usable_claim") > 0))
            .then(claim * pl.col("_parent") / pl.col("_usable_claim"))
            .otherwise(0.0)
            .alias(weight_col)
        )
        .drop("_parent", "_usable_claim")
    )
    logger.info("%s: %s follows %s", table, weight_col, has_weight[level.parent])  # type: ignore[index]
