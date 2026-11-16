"""Location classification helper functions for tour extraction.

This module contains functions for:
- Classifying trip origins/destinations as HOME, WORK, SCHOOL, or OTHER from
  the habitual location each end is at
- Per-day work/school anchor flags
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from processing.habitual_locations import MatchConfig, match_trip_ends

logger = logging.getLogger(__name__)


# Location kinds, and the match (from ``match_trip_ends``) that says a trip end
# is at one. Home counts every home, so a second home bounds tours like the
# primary one. Work and school count only the reported locations: observed ones
# act as anchors on days the reported one was not visited (``add_anchor_flags``).
_LOCATION_KINDS = {
    "home": "home_id",
    "work": "reported_work_id",
    "school": "reported_school_id",
}


def classify_trip_locations(
    linked_trips: pl.DataFrame,
    habitual_locations: pl.DataFrame,
    config: MatchConfig,
    person_days: pl.DataFrame,
) -> pl.DataFrame:
    """Classify trip origins and destinations against a person's habitual locations.

    Whether an end is *at* a location is decided once, by ``match_trip_ends``:
    within the buffer, with a purpose that agrees, where a day's stated start
    and end at home also count at its first origin and last destination. This
    only reads that answer. Purpose alone never makes an end home, work or
    school.

    Args:
        linked_trips: Trip data with o/d coordinates and purpose codes.
        habitual_locations: Habitual locations.
        config: Matching rules.
        person_days: Days with ``begin_day``/``end_day``.

    Returns:
        Trips with added columns:
        - _o_is_home, _o_is_work, _o_is_school (and _d_ equivalents)
        - o_location_type, d_location_type (LocationType value)
        - the private match columns from ``match_trip_ends``
    """
    logger.info("Classifying trip locations against habitual locations...")
    linked_trips = match_trip_ends(linked_trips, habitual_locations, config, person_days)
    linked_trips = linked_trips.with_columns(
        pl.col(f"_{end}_{match}").is_not_null().alias(f"_{end}_is_{name}")
        for name, match in _LOCATION_KINDS.items()
        for end in ("o", "d")
    )
    linked_trips = _add_location_types(linked_trips)
    logger.info("Location classification complete")
    return linked_trips


def add_anchor_flags(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Say which workplace or school each trip end is anchored at, per day.

    A tour anchor is the place a person is *based* for the day, and the rule is
    the same for work and school: a trip end is at the anchor if it is at one of
    the person's *reported* locations of that kind, or — on days they did not
    visit a reported one at all — at one of their *observed* ones. So someone can
    be based at a different office on different days, while a stop near the
    usual office on an office day stays a subtour.

    Args:
        linked_trips: Trips from ``classify_trip_locations``, carrying the
            ``_{o,d}_{reported,observed}_{work,school}_id`` matches,
            ``person_id`` and ``person_day_id``.

    Returns:
        Trips with ``_{o,d}_{work,school}_anchor_id`` (which location, for
        telling anchors apart) and ``_{o,d}_at_{work,school}`` flags, and the
        per-source matches dropped.
    """
    logger.info("Computing anchor flags from habitual locations...")
    for name in ("work", "school"):
        visited_reported = (
            (
                pl.col(f"_o_reported_{name}_id").is_not_null()
                | pl.col(f"_d_reported_{name}_id").is_not_null()
            )
            .any()
            .over(["person_id", "person_day_id"])
        )
        linked_trips = linked_trips.with_columns(
            pl.when(pl.col(f"_{end}_reported_{name}_id").is_not_null())
            .then(pl.col(f"_{end}_reported_{name}_id"))
            .when(~visited_reported)
            .then(pl.col(f"_{end}_observed_{name}_id"))
            .alias(f"_{end}_{name}_anchor_id")
            for end in ("o", "d")
        ).with_columns(
            pl.col(f"_{end}_{name}_anchor_id").is_not_null().alias(f"_{end}_at_{name}")
            for end in ("o", "d")
        )
    return linked_trips.drop(
        f"_{end}_{source}_{name}_id"
        for end in ("o", "d")
        for source in ("reported", "observed")
        for name in ("work", "school")
    )


def _add_location_types(df: pl.DataFrame) -> pl.DataFrame:
    """Determine primary location type based on priority.

    Priority order: HOME > WORK > SCHOOL > OTHER

    Args:
        df: DataFrame with location flag columns

    Returns:
        DataFrame with o_location_type and d_location_type columns
    """

    def build_location_expr(prefix: str) -> pl.Expr:
        """Build expression for location type with priority order."""
        expr = pl.lit(LocationType.OTHER)
        # Reverse priority order: HOME > WORK > SCHOOL > OTHER
        for loc_type in [
            LocationType.SCHOOL,
            LocationType.WORK,
            LocationType.HOME,
        ]:
            col_name = f"{prefix}_is_{loc_type.name.lower()}"
            expr = pl.when(pl.col(col_name)).then(pl.lit(loc_type)).otherwise(expr)
        return expr

    return df.with_columns(
        [
            build_location_expr("_o").alias("o_location_type"),
            build_location_expr("_d").alias("d_location_type"),
        ]
    )
