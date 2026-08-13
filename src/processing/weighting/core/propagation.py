"""Walk the weight hierarchy, moving weights between canonical tables.

Used by both the ``weighting`` (computed) and ``existing_weights`` (supplied)
steps. The shape being walked is declared in [`processing.weighting.core.hierarchy`]
[processing.weighting.core.hierarchy]; this module only moves weights along it.

# The distribution rule

A record is admitted by a *usability flag* -- ``model_usable``, stamped once by
the ``cascade_completeness`` step (see [`processing.completeness`]
[processing.completeness]), so the weighted sample matches the tours the travel
models actually keep.  Pass ``complete`` instead to weight the whole valid
survey, including partial and overnight tours.

Unusable records carry no weight, but their population does not vanish: it is
carried by the usable records in the same *scope*.  Give each record a **claim**
equal to its parent's weight -- what it would carry if usable -- and the usable
claims share the scope's whole claim:

    w = claim x sum(claims in scope) / sum(claims of usable in scope)

Because every record in a scope is scaled by the same factor, the relative
weights inside a household are untouched, and the conservation identity holds
by construction:

    sum(weight in scope) == sum(claim in scope)

# Days: the average-day split

Days use a different DOWN rule (``split=True``): a person's usable days jointly
represent that person *once*, not once each --

    day_weight = person_weight / n_usable_days        (usable days; 0 otherwise)
    sum(day_weight over a person's days) == person_weight

the vendor's own convention (their weighted days are exactly ``person_weight /
n``), with our usability flag deciding which days count.  Summing day weights
therefore yields *persons on an average day*, not person-days.

Conservation is strictly **within the person -- never pooled across a
household**: pooling would siphon day-weight from members with fewer usable
days toward members with more, silently distorting person-day totals while
still passing any household-level checksum.  A person with no usable day keeps
their person weight (person weights stay calibrated to the person controls) but
contributes nothing at the day level; that shortfall is reported by the
checksum, never rescaled away.  Unsurveyable persons (e.g. unrelated household
members) carry no day rows at all, matching the vendor's day table.

Where a whole scope is unusable there is no denominator anywhere and the weight
is genuinely stranded.  That is reported as a shortfall, never silently rescaled.
"""

import logging

import polars as pl

from processing.weighting.core.hierarchy import (
    HIERARCHY,
    LEVELS,
    TABLE_NAMES,
    Agg,
    Flow,
    Level,
)

logger = logging.getLogger(__name__)


def collect_tables(**tables: pl.DataFrame | None) -> dict[str, pl.DataFrame | None]:
    """Bundle canonical tables into a dict keyed by table name.

    Every table in the hierarchy is present, defaulting to None, so callers can
    pass whatever subset they hold.

    Args:
        **tables: Canonical tables by name.

    Returns:
        One entry per hierarchy table, None where not supplied.

    Raises:
        TypeError: If a name is not part of the hierarchy.
    """
    unknown = set(tables) - set(LEVELS)
    if unknown:
        msg = f"collect_tables got unknown table(s): {sorted(unknown)}"
        raise TypeError(msg)
    return {name: tables.get(name) for name in TABLE_NAMES}


def safe_join_weight(df: pl.DataFrame, weight_df: pl.DataFrame, on: str) -> pl.DataFrame:
    """Left-join *weight_df* onto *df*, dropping any pre-existing weight columns."""
    new_cols = [c for c in weight_df.columns if c != on]
    for c in new_cols:
        if c in df.columns:
            df = df.drop(c)
    return df.join(weight_df, on=on, how="left")


def is_usable(usability_flag_col: str) -> pl.Expr:
    """True when a record may carry weight; a null flag counts as unusable."""
    return pl.col(usability_flag_col).fill_null(value=False)


def distribute_within_scope(
    df: pl.DataFrame,
    *,
    weight_col: str,
    scope: str,
    usability_flag_col: str,
    table: str,
) -> pl.DataFrame:
    """Share each scope's claim among its usable records, returning a new frame.

    Each record arrives holding its *claim* -- the weight it would carry if it
    were usable. Usable records then split the scope's whole claim in proportion
    to their own, so the scope's total is conserved and every record in it is
    scaled by the same factor::

        w = claim x sum(claims in scope) / sum(claims of usable in scope)

    A scope with no usable record has no denominator; its claim is stranded and
    logged, for the checksum to report as a shortfall.

    Args:
        df: Table holding its claim in *weight_col*.
        weight_col: Weight column, replaced in place by the distributed weight.
        scope: Column naming the group within which the claim is conserved.
        usability_flag_col: Boolean column deciding which records carry weight.
        table: Table name, for logging.

    Returns:
        *df* with *weight_col* distributed across the usable records.
    """
    if usability_flag_col not in df.columns or weight_col not in df.columns:
        return df
    if scope not in df.columns:
        msg = f"Cannot distribute {weight_col}: {table} is missing its scope column {scope!r}"
        raise ValueError(msg)

    usable = is_usable(usability_flag_col)
    n_unusable = df.filter(~usable).height
    if n_unusable == 0:
        return df

    claim = pl.col(weight_col).fill_null(0.0)
    totals = df.group_by(scope).agg(
        claim.sum().alias("_claim"),
        claim.filter(usable).sum().alias("_usable_claim"),
    )
    stranded = totals.filter(pl.col("_usable_claim") <= 0)

    df = (
        df.join(totals, on=scope, how="left")
        .with_columns(
            pl.when(usable & (pl.col("_usable_claim") > 0))
            .then(claim * pl.col("_claim") / pl.col("_usable_claim"))
            .otherwise(0.0)
            .alias(weight_col)
        )
        .drop("_claim", "_usable_claim")
    )

    logger.info(
        "%s: %d/%d records unusable by %s; %s conserved within %s%s",
        table,
        n_unusable,
        df.height,
        usability_flag_col,
        weight_col,
        scope,
        f" ({stranded.height} {scope}(s) stranded {float(stranded['_claim'].sum()):.1f})"
        if stranded.height
        else "",
    )
    return df


def split_among_usable(
    df: pl.DataFrame,
    *,
    weight_col: str,
    key: str,
    usability_flag_col: str | None,
    table: str,
) -> pl.DataFrame:
    """Divide each parent's weight equally among its usable children, in place.

    The average-day rule: a parent's children jointly represent the parent
    *once*, not once each ::

        w = parent_weight / n_usable_children    (usable children; 0 otherwise)

    so the usable children of every parent sum exactly to the parent's weight.
    This is the vendor's own day-weight convention (``day_weight =
    person_weight / n_weighted_days``), with our usability flag deciding which
    children count. There is **no cross-parent pooling**: a parent whose
    children are all unusable is a reported shortfall, never re-homed onto
    other parents' children.

    Args:
        df: Child table holding each row's parent weight in *weight_col*.
        weight_col: Weight column, replaced in place by the split weight.
        key: Column identifying the parent record.
        usability_flag_col: Boolean column deciding which children carry
            weight, or None to split among all children.
        table: Table name, for logging.

    Returns:
        *df* with *weight_col* split among each parent's usable children.
    """
    usable = (
        is_usable(usability_flag_col)
        if usability_flag_col and usability_flag_col in df.columns
        else pl.lit(value=True)
    )
    counts = df.group_by(key).agg(
        usable.sum().alias("_n_usable"),
        pl.col(weight_col).fill_null(0.0).first().alias("_parent_weight"),
    )

    stranded = counts.filter((pl.col("_n_usable") == 0) & (pl.col("_parent_weight") > 0))
    if stranded.height:
        logger.info(
            "%s: %d %s(s) kept no usable child; %.1f of %s stays unrepresented "
            "(reported by the checksum, never pooled across parents)",
            table,
            stranded.height,
            key,
            float(stranded["_parent_weight"].sum()),
            weight_col,
        )

    df = (
        df.join(counts.select(key, "_n_usable"), on=key, how="left")
        .with_columns(
            pl.when(usable & (pl.col("_n_usable") > 0))
            .then(pl.col(weight_col) / pl.col("_n_usable"))
            .otherwise(0.0)
            .alias(weight_col)
        )
        .drop("_n_usable")
    )
    logger.info("%s: %s split equally among each %s's usable children", table, weight_col, key)
    return df


def _carry_down(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    level: Level,
    usability_flag_col: str | None,
) -> None:
    """Derive *level*'s weight from its parent and distribute it, in place."""
    child_df = tables[level.table]
    parent_df = tables.get(level.parent)  # type: ignore[arg-type]
    if child_df is None:
        return

    if level.parent not in has_weight:
        msg = (
            f"Cannot derive {level.weight_col} for {level.table}: "
            f"parent table {level.parent} has no weight column"
        )
        raise ValueError(msg)
    if parent_df is None:
        msg = (
            f"Cannot derive {level.weight_col} for {level.table}: "
            f"parent table {level.parent} is None"
        )
        raise ValueError(msg)
    if level.key not in child_df.columns:
        msg = f"Cannot derive weight: {level.table} missing join key {level.key}"
        raise ValueError(msg)

    parent_weight = has_weight[level.parent]  # type: ignore[index]
    logger.info("Deriving %s from %s via %s", level.weight_col, parent_weight, level.key)

    # Every child starts holding its parent's weight -- its claim.
    claims = parent_df.select(level.key, parent_weight).rename({parent_weight: level.weight_col})
    child_df = safe_join_weight(child_df, claims, level.key)  # type: ignore[arg-type]

    if level.split:
        # Average-day convention: children jointly represent the parent once.
        child_df = split_among_usable(
            child_df,
            weight_col=level.weight_col,
            key=level.key,  # type: ignore[arg-type]
            usability_flag_col=usability_flag_col,
            table=level.table,
        )
    elif usability_flag_col:
        child_df = distribute_within_scope(
            child_df,
            weight_col=level.weight_col,
            scope=level.scope,  # type: ignore[arg-type]
            usability_flag_col=usability_flag_col,
            table=level.table,
        )

    tables[level.table] = child_df
    has_weight[level.table] = level.weight_col


def _aggregate_up(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    level: Level,
    usability_flag_col: str | None,
) -> None:
    """Combine *level*'s usable members into its weight, in place."""
    target_df = tables[level.table]
    source_df = tables.get(level.parent)  # type: ignore[arg-type]
    if target_df is None:
        return

    if source_df is None:
        msg = (
            f"Cannot derive {level.weight_col} for {level.table}: "
            f"source table {level.parent} is None"
        )
        raise ValueError(msg)
    if level.parent not in has_weight:
        msg = (
            f"Cannot derive {level.weight_col} for {level.table}: "
            f"source table {level.parent} has no weight column"
        )
        raise ValueError(msg)
    if level.key not in source_df.columns:
        msg = f"Cannot derive {level.weight_col}: source {level.parent} missing {level.key}"
        raise ValueError(msg)

    src_weight = has_weight[level.parent]  # type: ignore[index]
    logger.info("Deriving %s from %s of %s", level.weight_col, level.agg.name.lower(), src_weight)

    # Members that carried weight in. A zero-weight member was already re-homed
    # onto its own scope's usable records, so it is neither averaged nor counted.
    carried = pl.col(src_weight).filter(
        pl.col(src_weight).is_not_null() & (pl.col(src_weight) != 0)
    )
    combine = carried.sum() if level.agg is Agg.SUM else carried.mean()

    agg = source_df.group_by(level.key).agg(combine.fill_null(0).alias(level.weight_col))
    target_df = safe_join_weight(target_df, agg, level.key)  # type: ignore[arg-type]

    # A grouping is never more usable than its members: an unusable record
    # carries no weight even where the combination came out positive.
    if usability_flag_col and usability_flag_col in target_df.columns:
        target_df = target_df.with_columns(
            pl.when(is_usable(usability_flag_col))
            .then(pl.col(level.weight_col))
            .otherwise(0.0)
            .alias(level.weight_col)
        )

    tables[level.table] = target_df
    has_weight[level.table] = level.weight_col


def propagate_weights(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    skip: set[str] | None = None,
    usability_flag_col: str | None = "model_usable",
) -> None:
    """Walk the hierarchy, deriving every weight that is not already supplied.

    Modifies *tables* and *has_weight* **in place**. Levels are visited in
    declaration order, so each parent is weighted before its children.

    Args:
        tables: Mutable dict of table_name → DataFrame (or None).
        has_weight: Mutable dict tracking which tables already have a weight
            column and its name. E.g. ``{"households": "hh_weight"}``.
        skip: Table names to leave alone (e.g. externally supplied weights).
        usability_flag_col: Boolean column deciding which records may carry
            weight. Defaults to ``model_usable``; pass ``complete`` to weight the
            whole valid survey including partial/overnight tours, or None to give
            every record its claim regardless of usability (split levels still
            divide the parent weight equally, over *all* children).
    """
    skip = skip or set()

    for level in HIERARCHY:
        if level.flow is None or level.table in skip or tables.get(level.table) is None:
            continue
        if level.flow is Flow.DOWN:
            _carry_down(tables, has_weight, level, usability_flag_col)
        else:
            _aggregate_up(tables, has_weight, level, usability_flag_col)


def non_null_tables(tables: dict[str, pl.DataFrame | None]) -> dict[str, pl.DataFrame]:
    """Return only the non-None entries from a tables dict."""
    return {k: v for k, v in tables.items() if v is not None}
