"""The one test of whether a point is at a habitual location.

A point is at a location when it is within the buffer of it *and* its purpose
agrees with the location's kind. An unknown purpose agrees with nothing: a stop
near the office with no purpose could be work or a coffee, so it is neither.
At a day's first origin and last destination, the respondent's answer to where
the day began or ended is a second purpose: "home" or "other home" agrees with a
home.

One exception: at the person's own front door — within ``at_address_meters`` of
their primary home — any purpose is at that home. There the purpose describes
what they are doing, not a different place, and refusing it welds two tours
into one.

Nothing is recoded. A work-related stop stays work-related; it is evidence of
where the person is, not a rewrite of what they said they were doing.
"""

import polars as pl

from data_canon.codebook.days import BeginEndDay
from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import Purpose, PurposeCategory, PurposeToCategoryMap
from utils.helpers import expr_haversine

from .habitual_location_configs import MatchConfig

# Answers to "where did the day begin/end?" that name a home, as the purpose
# that says the same thing.
STATED_DAY_PURPOSE = {
    BeginEndDay.HOME.value: Purpose.HOME,
    BeginEndDay.OTHER_HOME.value: Purpose.OTHER_RESIDENCE,
}

# What match_trip_ends reports for each trip end, as (name, location type,
# source or None for any source): the best matching location of that kind.
_END_MATCHES = [
    ("home", LocationType.HOME, None),
    ("reported_work", LocationType.WORK, LocationSource.REPORTED),
    ("observed_work", LocationType.WORK, LocationSource.OBSERVED),
    ("reported_school", LocationType.SCHOOL, LocationSource.REPORTED),
    ("observed_school", LocationType.SCHOOL, LocationSource.OBSERVED),
]


def _purpose_agrees(
    location_type: pl.Expr,
    is_primary: pl.Expr,
    purpose: pl.Expr,
    category: pl.Expr,
) -> pl.Expr:
    """Whether a purpose agrees with a location's kind.

    "Went to another residence" agrees only with a home that is *not* the
    primary one: it is how people code arriving at their own second home, but
    never how they code coming back to their main one. A work-related purpose
    agrees with a workplace: it says the person is working, and respondents use
    it for their own workplace as readily as for a meeting elsewhere. It says
    nothing about *which* workplace; the distance settles that.
    """
    return (
        pl.when(location_type == LocationType.HOME.value)
        .then(
            (category == PurposeCategory.HOME.value)
            | (~is_primary.fill_null(value=True) & (purpose == Purpose.OTHER_RESIDENCE.value))
        )
        .when(location_type == LocationType.WORK.value)
        .then(
            (category == PurposeCategory.WORK.value)
            | (category == PurposeCategory.WORK_RELATED.value)
        )
        .when(location_type == LocationType.SCHOOL.value)
        .then(category == PurposeCategory.SCHOOL.value)
        .otherwise(pl.lit(value=False))
        .fill_null(value=False)
    )


def with_stated_day_ends(points: pl.DataFrame, person_days: pl.DataFrame) -> pl.DataFrame:
    """Add where the respondent said the day began or ended, as a purpose.

    Sets ``stated_purpose`` / ``stated_purpose_category`` at a day's first
    origin and last destination when the answer names a home; they are null
    everywhere else, including every day the survey did not ask.

    Args:
        points: Points with ``person_day_id``, ``is_day_start`` and ``is_day_end``.
        person_days: Days with ``person_day_id``, ``begin_day`` and ``end_day``.

    Returns:
        The points with ``stated_purpose`` and ``stated_purpose_category``.
    """
    stated_purpose = {code: p.value for code, p in STATED_DAY_PURPOSE.items()}
    stated_category = {
        code: PurposeToCategoryMap.get_category(p).value for code, p in STATED_DAY_PURPOSE.items()
    }
    answer = (
        pl.when(pl.col("is_day_start"))
        .then(pl.col("begin_day"))
        .when(pl.col("is_day_end"))
        .then(pl.col("end_day"))
    )
    return (
        points.join(
            person_days.select("person_day_id", "begin_day", "end_day"),
            on="person_day_id",
            how="left",
        )
        .with_columns(
            answer.replace_strict(stated_purpose, default=None, return_dtype=pl.Int64).alias(
                "stated_purpose"
            ),
            answer.replace_strict(stated_category, default=None, return_dtype=pl.Int64).alias(
                "stated_purpose_category"
            ),
        )
        .drop("begin_day", "end_day")
    )


def match_points(
    points: pl.DataFrame,
    locations: pl.DataFrame,
    config: MatchConfig,
) -> pl.DataFrame:
    """Every (point, location) pair where the point is at the location.

    Within the buffer, and the trip purpose or the stated day start/end agrees
    with the location's kind. A corner store by the house or lunch by the office
    is within the buffer but says it is somewhere else, so it is not at home or
    at work. The exception is the person's own front door: within
    ``at_address_meters`` of their primary home, any purpose is at that home.

    Args:
        points: Points from :func:`with_stated_day_ends`, with ``_pt``
            (unique), ``person_id``, ``lat``, ``lon``, ``purpose``,
            ``purpose_category``, ``stated_purpose`` and
            ``stated_purpose_category``.
        locations: Habitual locations with ``habitual_location_id``,
            ``person_id``, ``location_type``, ``is_primary``, ``source``, ``lat``
            and ``lon``.
        config: Provides the buffer.

    Returns:
        One row per match with ``_pt``, ``habitual_location_id``,
        ``location_type``, ``is_primary``, ``source`` and ``_d`` (metres).
    """
    kind, primary = pl.col("location_type"), pl.col("is_primary")
    return (
        points.select(
            "_pt",
            "person_id",
            "lat",
            "lon",
            "purpose",
            "purpose_category",
            "stated_purpose",
            "stated_purpose_category",
        )
        .join(
            locations.select(
                "habitual_location_id",
                "person_id",
                "location_type",
                "is_primary",
                "source",
                pl.col("lat").alias("_loc_lat"),
                pl.col("lon").alias("_loc_lon"),
            ),
            on="person_id",
            how="inner",
        )
        .with_columns(
            expr_haversine(
                pl.col("lat"), pl.col("lon"), pl.col("_loc_lat"), pl.col("_loc_lon")
            ).alias("_d"),
            (
                _purpose_agrees(kind, primary, pl.col("purpose"), pl.col("purpose_category"))
                | _purpose_agrees(
                    kind, primary, pl.col("stated_purpose"), pl.col("stated_purpose_category")
                )
            ).alias("_agrees"),
        )
        .with_columns(
            (
                pl.col("_agrees")
                | (
                    (kind == LocationType.HOME.value)
                    & primary.fill_null(value=False)
                    & (pl.col("_d") <= config.at_address_meters)
                )
            ).alias("_agrees")
        )
        .filter((pl.col("_d") <= config.buffer_meters) & pl.col("_agrees"))
        .select("_pt", "habitual_location_id", "location_type", "is_primary", "source", "_d")
    )


def best_match(matches: pl.DataFrame) -> pl.DataFrame:
    """Pick one location per point: the nearest, then the lower identifier.

    Which kinds compete is the caller's choice: ``match_trip_ends`` filters to
    one kind first, ``build_location_days`` lets every kind compete. The
    identifier breaks exact ties so the answer never depends on row order.
    """
    return (
        matches.sort(["_pt", "_d", "habitual_location_id"])
        .group_by("_pt", maintain_order=True)
        .first()
    )


def match_trip_ends(
    linked_trips: pl.DataFrame,
    locations: pl.DataFrame,
    config: MatchConfig,
    person_days: pl.DataFrame,
) -> pl.DataFrame:
    """Say which habitual location each trip origin and destination is at.

    Adds private columns for each end (``o`` and ``d``), each the nearest
    matching location of that kind or null:

    - ``_{end}_home_id`` — any home;
    - ``_{end}_at_other_home`` — that home is not the primary one;
    - ``_{end}_reported_work_id``, ``_{end}_observed_work_id``,
      ``_{end}_reported_school_id``, ``_{end}_observed_school_id``.

    Kinds are matched separately, so a home office is both home and work.

    Args:
        linked_trips: Linked trips with ``linked_trip_id``, ``person_id``,
            ``person_day_id``, ``depart_time``, o/d coordinates and o/d purpose
            columns.
        locations: Habitual locations with ``habitual_location_id``.
        config: Provides the buffer.
        person_days: Days with ``begin_day``/``end_day`` (see
            :func:`with_stated_day_ends`).

    Returns:
        The trips with those columns added.
    """
    order = pl.col("depart_time").rank("ordinal").over(["person_id", "person_day_id"])
    for end in ("o", "d"):
        points = linked_trips.select(
            pl.col("linked_trip_id").alias("_pt"),
            "person_id",
            "person_day_id",
            pl.col(f"{end}_lat").alias("lat"),
            pl.col(f"{end}_lon").alias("lon"),
            pl.col(f"{end}_purpose").alias("purpose"),
            pl.col(f"{end}_purpose_category").alias("purpose_category"),
            ((order == 1) & (end == "o")).alias("is_day_start"),
            ((order == pl.len().over(["person_id", "person_day_id"])) & (end == "d")).alias(
                "is_day_end"
            ),
        )
        matches = match_points(with_stated_day_ends(points, person_days), locations, config)
        for name, loc_type, source in _END_MATCHES:
            of_kind = pl.col("location_type") == loc_type.value
            if source is not None:
                of_kind = of_kind & (pl.col("source") == source.value)
            best = best_match(matches.filter(of_kind))
            columns = [
                pl.col("_pt").alias("linked_trip_id"),
                pl.col("habitual_location_id").alias(f"_{end}_{name}_id"),
            ]
            if loc_type == LocationType.HOME:
                columns.append(
                    (~pl.col("is_primary").fill_null(value=False)).alias(f"_{end}_at_other_home")
                )
            linked_trips = linked_trips.join(best.select(columns), on="linked_trip_id", how="left")
        linked_trips = linked_trips.with_columns(
            pl.col(f"_{end}_at_other_home").fill_null(value=False)
        )
    return linked_trips
