"""Supplied weights onto usable trips and tours, and the checks after redistribution.

A trip moves with its day; linked trips and tours are the mean of their final
trips, and joint trips and joint tours the sum of their kept members.
"""

import logging

import polars as pl

from processing.weighting.core.hierarchy import LEVELS, weight_columns_for
from processing.weighting.core.propagation import is_usable, zero_unusable
from processing.weighting.existing.population import SCALE_TOL, apply_usability

logger = logging.getLogger(__name__)


TRAVEL_TABLES = frozenset({"unlinked_trips", "linked_trips", "tours"})
"""Unlinked trips follow their day; linked trips and tours are the mean of their final trips.

``preserve_travel_totals`` decides only whether a kept day's dropped trips hand
their weight to its kept ones.
"""


# Overlays on travel: always the sum of their kept members, derived last.
JOINT_TABLES = frozenset({"joint_trips", "joint_tours"})


def gate_travel(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
    preserve_travel_totals: bool,
) -> dict[str, float]:
    """Gate the travel tables, each record starting from its pre-gate claim, in place.

    Used only where a travel weight cannot follow its day: a linked trip or
    tour weight supplied as a file, or unlinked trips supplied without day
    weights. When days and unlinked trips are both supplied, the trips follow
    their day ([`trips_follow_their_day`]) and this does not see them.

    A claim is the weight a record would carry if every trip and tour were
    usable: the supplied weight, or the weight derived from its usable day.

    With *preserve_travel_totals*, a dropped record's claim moves to the kept
    records on its day and anything left stranded closes region-wide, so each
    table keeps its claim total. Without it, a kept record keeps exactly its
    claim and a dropped one carries nothing.

    Returns:
        table -> the claim total the placement check holds it to; empty when
        the totals are not preserved.
    """
    travel = {t: c for t, c in has_weight.items() if t in TRAVEL_TABLES}
    if preserve_travel_totals:
        return apply_usability(tables, travel, usability_flag_col=usability_flag_col)
    for table_name, weight_col in travel.items():
        df = tables.get(table_name)
        if df is None or usability_flag_col not in df.columns:
            continue
        tables[table_name] = df.with_columns(zero_unusable(weight_col, usability_flag_col))
    return {}


def check_weight_factors(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    supplied: dict[str, pl.DataFrame],
    limits: dict[str, float],
) -> None:
    """Raise if any record's weight grew past its limit times its supplied weight.

    The factor is a record's final weight over its own supplied weight, so it
    counts everything that moved weight onto it: a household's size factor, a
    person taking a dropped member's share, a day taking a dropped day's.
    """
    for table, limit in limits.items():
        df = tables.get(table)
        if df is None:
            continue
        level, col = LEVELS[table], has_weight[table]
        factors = (
            df.select(level.id_col, col)
            .join(
                supplied[table].select(level.id_col, pl.col(level.weight_col).alias("_supplied")),
                on=level.id_col,
                how="inner",
            )
            .filter(pl.col("_supplied") > 0)
            .with_columns((pl.col(col).fill_null(0.0) / pl.col("_supplied")).alias("_factor"))
        )
        over = factors.filter(pl.col("_factor") > limit + SCALE_TOL).sort(
            ["_factor", level.id_col], descending=[True, False]
        )
        if over.is_empty():
            logger.info(
                "  %s: every record within %.2fx its supplied weight (largest %.4f)",
                table,
                limit,
                float(factors["_factor"].max() or 0.0),  # type: ignore[arg-type]
            )
            continue
        worst = over.head(10)
        msg = (
            f"{over.height} {table} record(s) grew past {limit}x their supplied weight "
            f"(max_weight_factor). Largest, {level.id_col}: {worst[level.id_col].to_list()} "
            f"at {[round(f, 4) for f in worst['_factor'].to_list()]}x."
        )
        logger.error(msg)
        raise ValueError(msg)


def check_usable_were_supplied(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
) -> None:
    """Stop on usable records the supplier gave no weight (0 or none), or pass.

    Supplied weights only move between records, in proportion to what was
    supplied, so such a record would end at 0 and reach the model counting for
    nothing. Stopping here names the cause: this profile admits records outside
    the supplier's universe. Every table is checked before stopping, so one run
    shows the whole gap rather than its first level.

    Raises:
        ValueError: Naming each table, how many records, and the weight column.
    """
    gaps: list[str] = []
    for table, weight_col in has_weight.items():
        df = tables.get(table)
        if df is None or usability_flag_col not in df.columns:
            continue
        usable = df.filter(is_usable(usability_flag_col))
        zero = usable.filter(pl.col(weight_col).fill_null(0.0) <= 0).height
        if zero:
            gaps.append(
                f"{table}: {zero} of {usable.height} record(s) admitted by "
                f"{usability_flag_col} were supplied no {weight_col} (0 or none)"
            )
    if gaps:
        msg = (
            "; ".join(gaps) + ". The profile admits records the supplier did not weight; "
            "make them unusable under it, or supply their weights."
        )
        logger.error(msg)
        raise ValueError(msg)


def trips_follow_their_day(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    supplied_days: pl.DataFrame,
    *,
    usability_flag_col: str,
    preserve_travel_totals: bool,
) -> None:
    """Scale each unlinked trip by its day's factor, in place.

    A day's factor is its final weight over its supplied weight, so a trip
    moves exactly as its day did -- a dropped day's weight went to the person's
    other days, and the trips on those days carry it::

        w = claim x day_final / day_supplied

    With *preserve_travel_totals*, a kept day's dropped trips also hand their
    claim to its kept trips, in proportion::

        w x sum(claims on the day) / sum(usable claims on the day)

    Raises:
        ValueError: If a trip carries a supplied weight but its day was
            supplied 0 or not at all -- there is no factor to apply, and
            guessing one would invent the supplier's intent.
    """
    trips = tables["unlinked_trips"]
    if trips is None:
        return
    trip_col, day_col = has_weight["unlinked_trips"], has_weight["person_days"]
    usable = is_usable(usability_flag_col)
    claim = pl.col(trip_col).fill_null(0.0)

    factors = (
        tables["person_days"]  # type: ignore[union-attr]
        .select("person_day_id", pl.col(day_col).fill_null(0.0).alias("_day_final"))
        .join(
            supplied_days.select(
                "person_day_id", pl.col("person_day_weight").alias("_day_supplied")
            ),
            on="person_day_id",
            how="left",
        )
    )
    trips = trips.join(factors, on="person_day_id", how="left")

    orphaned = trips.filter((claim > 0) & (pl.col("_day_supplied").fill_null(0.0) <= 0))
    if orphaned.height:
        msg = (
            f"{orphaned.height} unlinked_trips record(s) carry a supplied {trip_col} but "
            f"their day has a supplied person_day_weight of 0 or none, so there is no day factor "
            f"to scale them by. Sample unlinked_trip_id: "
            f"{orphaned['unlinked_trip_id'].head(10).to_list()}."
        )
        logger.error(msg)
        raise ValueError(msg)

    move = pl.lit(1.0)
    if preserve_travel_totals:
        within_day = trips.group_by("person_day_id").agg(
            claim.sum().alias("_claim"), claim.filter(usable).sum().alias("_usable_claim")
        )
        trips = trips.join(within_day, on="person_day_id", how="left")
        move = pl.col("_claim") / pl.col("_usable_claim")

    trips = trips.with_columns(
        pl.when(usable & (claim > 0))
        .then(claim * pl.col("_day_final") / pl.col("_day_supplied") * move)
        .otherwise(0.0)
        .alias(trip_col)
    )
    tables["unlinked_trips"] = trips.drop(
        "_day_final", "_day_supplied", "_claim", "_usable_claim", strict=False
    )
    logger.info(
        "unlinked_trips: %s follows its day%s",
        trip_col,
        ", with dropped trips' claims moved to the kept trips on the day"
        if preserve_travel_totals
        else "",
    )


def warn_null_weights(tables: dict[str, pl.DataFrame | None], profile: str | None) -> None:
    """Report weights that came out null -- a record no supplied total covered."""
    for table, weight in weight_columns_for(profile).items():
        df = tables.get(table)
        if df is None or weight not in df.columns:
            continue
        null_count = df.select(pl.col(weight).is_null().sum()).item()
        if null_count:
            logger.warning(
                "Table %s has %d / %d NULL values in weight column %s",
                table,
                null_count,
                len(df),
                weight,
            )
