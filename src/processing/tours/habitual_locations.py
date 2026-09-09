"""Build the habitual-location tables (issue #71).

A *habitual location* is a place a person has a standing relationship with —
their home, their workplace, their school — as opposed to somewhere they merely
happened to stop. These are the tall replacement for the wide scalar
``home_lat/lon``, ``work_lat/lon``, ``school_lat/lon`` columns produced by
``prepare_person_locations``.

Two tables come out of here, because identity and presence have different
existence conditions. A habitual location exists whether or not the person went
there during the diary period — a reported workplace belonging to someone who
teleworked all week is still their workplace — so it cannot live on a table
keyed by day::

    habitual_locations      one row per (person, kind, number)
                            who / where / how we know

    habitual_location_days  one row per (location, day) the person was present
                            how long / how many visits / how the day related

For example::

    habitual_locations
      person  type  num  is_primary  source
      1       WORK  1    True        reported
      1       WORK  2    False       observed   <- a Tue/Thu offsite
      2       HOME  1    True        reported
      2       HOME  2    False       observed   <- an other home

    habitual_location_days
      person  location    day  dwell_minutes  n_visits  day_start  day_end
      1       WORK #2     ..   210            1         False      False
      2       HOME #2     ..   None           1         False      True

The day table records *facts*, never conclusions: how long somebody was
somewhere and how the day related to it, but never which location "was" the
day's anchor. Different consumers weigh those facts differently — by duration,
by recurrence, by a purpose hierarchy — and that judgement belongs to them.

Three builders:

- ``build_presence_episodes`` turns linked trips into one row per stay: every
  trip destination, plus each day's first origin (where the person began the
  day, which no destination records).
- ``build_reported_locations`` unpivots the reported scalar locations into tall
  rows (``source = REPORTED``), reproducing exactly the coordinates the scalar
  model carries today.
- ``derive_observed_locations`` promotes places a person keeps using into
  ``WORK``/``SCHOOL``/``HOME`` rows (``source = OBSERVED``).

``build_habitual_locations`` combines them and produces both tables.

Two points are "the same place" when they are within a radius of one another
(``HabitualLocationConfig.same_location_radius_meters``, per kind). That single
definition does all the spatial work here: it clusters repeat visits into one
location, and — because a person's reported location joins the clustering as a
seed — it is also what decides an observed cluster *is* their reported one. The
mechanics are pairwise haversine plus connected components, the same approach
``joint_trips.clique_detection`` uses to group mutually-matching trips.

Promotion rules differ by kind because the evidence does. Work and school use
dwell time. Home cannot: ``d_activity_duration`` is ``-1`` for every home-purpose
arrival and ``-2`` for the last trip of a day, so the overnight stay that makes
somewhere a home is exactly the case with no measurable duration. Home is
promoted on sleeping there or returning across days instead.

Nothing in tour extraction consumes the day table yet; it is foundation for
anchor resolution, tour purpose, partial-tour recovery and day-level analysis.
"""

import logging
from collections.abc import Callable
from typing import NamedTuple

import networkx as nx
import polars as pl
from pydantic import BaseModel, Field

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import Purpose, PurposeCategory
from utils.create_ids import create_concatenated_id
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)

# Sentinel values of d_activity_duration that are not real dwell times
# (see LinkedTripModel.d_activity_duration): -1 = destination is home,
# -2 = last trip of the person-day (no subsequent departure).
_DWELL_SENTINELS = (-1, -2)

# location_num is packed into habitual_location_id as two digits, so a person
# cannot hold more than this many locations of one kind.
_MAX_LOCATION_NUM = 99

# Table columns in schema order (see HabitualLocationModel and
# HabitualLocationDayModel).
_LOCATION_COLUMNS = [
    "habitual_location_id",
    "person_id",
    "location_type",
    "location_num",
    "is_primary",
    "lat",
    "lon",
    "source",
]
_DAY_COLUMNS = [
    "habitual_location_id",
    "person_id",
    "day_id",
    "location_type",
    "dwell_minutes",
    "n_visits",
    "is_day_start",
    "is_day_end",
]

# Reported scalar location columns, mapped to their location type.
_REPORTED_SCALARS = [
    (LocationType.HOME, "home_lat", "home_lon"),
    (LocationType.WORK, "work_lat", "work_lon"),
    (LocationType.SCHOOL, "school_lat", "school_lon"),
]


class HabitualLocationConfig(BaseModel):
    """Rules for promoting observed places into habitual locations.

    Work and school are promoted on dwell time, which separates a real worksite
    or campus from a brief stop. Home is promoted on evidence of staying: the
    person's day ended there, or they returned across days. Home cannot use
    dwell at all, because activity duration is a sentinel for every home arrival
    and for the last trip of a day (see the module docstring).

    Recurrence is deliberately not required for work and school:
    ``min_distinct_days`` defaults to 1 (off), because some survey platforms
    collect only a single travel day and a recurrence rule would exclude those
    respondents outright. Home's day rule is an *alternative* to the overnight
    signal rather than an additional requirement, for the same reason.
    """

    min_dwell_minutes: float = Field(
        default=90.0,
        ge=0,
        description=(
            "Minimum activity duration in minutes (the location's longest "
            "observed stay) for a work or school location to be promoted."
        ),
    )
    min_distinct_days: int = Field(
        default=1,
        ge=1,
        description=(
            "Optional minimum number of distinct travel days a work or school "
            "location was visited. Defaults to 1 (no filter)."
        ),
    )
    min_home_days: int = Field(
        default=2,
        ge=1,
        description=(
            "Number of distinct days a residence must be visited to be promoted "
            "when no overnight stay was observed there."
        ),
    )
    same_location_radius_meters: dict[LocationType, float] = Field(
        default={
            LocationType.HOME: 500.0,
            LocationType.WORK: 110.0,
            LocationType.SCHOOL: 110.0,
        },
        description=(
            "Distance in metres within which two points are treated as the same "
            "place. Used both to cluster repeat visits and to decide an observed "
            "place is the person's reported one. Home is generous because a "
            "household home is a geocoded address while trip ends are GPS "
            "traces, and a genuine second home is kilometres away regardless."
        ),
    )


def _dwell_gate(config: "HabitualLocationConfig") -> pl.Expr:
    """Promotion rule for work and school: a long enough stay."""
    return (pl.col("_max_dwell") >= config.min_dwell_minutes) & (
        pl.col("_n_days") >= config.min_distinct_days
    )


def _residence_gate(config: "HabitualLocationConfig") -> pl.Expr:
    """Promotion rule for home: slept there, or came back on another day."""
    return pl.col("_any_day_end") | (pl.col("_n_days") >= config.min_home_days)


class _ObservedPool(NamedTuple):
    """One kind of observed location: which stays qualify, and what promotes them."""

    location_type: LocationType
    candidates: pl.Expr
    gate: Callable[["HabitualLocationConfig"], pl.Expr]


# Observed location kinds. Work and school pool the "related" purpose with its
# base, which captures worksites and campuses that are not the reported primary
# one. Home keys on the detailed OTHER_RESIDENCE purpose: respondents use it for
# a second home or another parent's house, and it is the only purpose that says
# "a residence that is not the one you told us about". Temporary lodging is
# excluded — a hotel is not a habitual location.
_OBSERVED_POOLS = [
    _ObservedPool(
        LocationType.WORK,
        pl.col("purpose_category").is_in(
            [PurposeCategory.WORK.value, PurposeCategory.WORK_RELATED.value]
        ),
        _dwell_gate,
    ),
    _ObservedPool(
        LocationType.SCHOOL,
        pl.col("purpose_category").is_in(
            [PurposeCategory.SCHOOL.value, PurposeCategory.SCHOOL_RELATED.value]
        ),
        _dwell_gate,
    ),
    _ObservedPool(
        LocationType.HOME,
        pl.col("purpose") == Purpose.OTHER_RESIDENCE.value,
        _residence_gate,
    ),
]


def build_presence_episodes(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Turn linked trips into one row per stay somewhere.

    Every trip destination is a stay. Each day's *first origin* is also a stay —
    the person was there before they set off, and no destination records it,
    which is what makes ``is_day_start`` underivable from destinations alone.
    Later origins are skipped: they repeat the previous trip's destination.

    Args:
        linked_trips: Linked trips with ``person_id``, ``day_id``, ``o_lat``,
            ``o_lon``, ``d_lat``, ``d_lon``, ``depart_time``, purpose columns
            and ``d_activity_duration``.

    Returns:
        One row per stay with ``person_id``, ``day_id``, ``lat``, ``lon``,
        ``purpose``, ``purpose_category``, ``dwell_minutes`` (null where the
        duration is a sentinel or the stay is a day's first origin),
        ``is_day_start`` and ``is_day_end``.
    """
    ordered = linked_trips.sort(["person_id", "day_id", "depart_time"]).with_columns(
        pl.int_range(0, pl.len()).over(["person_id", "day_id"]).alias("_trip_idx"),
        pl.len().over(["person_id", "day_id"]).alias("_n_trips"),
    )

    destinations = ordered.select(
        "person_id",
        "day_id",
        pl.col("d_lat").alias("lat"),
        pl.col("d_lon").alias("lon"),
        pl.col("d_purpose").alias("purpose"),
        pl.col("d_purpose_category").alias("purpose_category"),
        pl.when(pl.col("d_activity_duration").is_in(_DWELL_SENTINELS))
        .then(None)
        .otherwise(pl.col("d_activity_duration"))
        .cast(pl.Float64)
        .alias("dwell_minutes"),
        pl.lit(value=False).alias("is_day_start"),
        (pl.col("_trip_idx") == pl.col("_n_trips") - 1).alias("is_day_end"),
    )

    first_origins = ordered.filter(pl.col("_trip_idx") == 0).select(
        "person_id",
        "day_id",
        pl.col("o_lat").alias("lat"),
        pl.col("o_lon").alias("lon"),
        pl.col("o_purpose").alias("purpose"),
        pl.col("o_purpose_category").alias("purpose_category"),
        # The stay began before the diary day, so its length is unknown.
        pl.lit(None, dtype=pl.Float64).alias("dwell_minutes"),
        pl.lit(value=True).alias("is_day_start"),
        pl.lit(value=False).alias("is_day_end"),
    )

    return pl.concat([destinations, first_origins], how="vertical")


def build_reported_locations(person_locations: pl.DataFrame) -> pl.DataFrame:
    """Unpivot the reported scalar locations into tall rows.

    Args:
        person_locations: Wide per-person table with ``person_id`` and
            ``home_lat/lon``, ``work_lat/lon``, ``school_lat/lon`` columns (as
            produced by ``prepare_person_locations``).

    Returns:
        Tall table with one row per reported (non-null) location, without
        ``location_num`` (assigned by ``build_habitual_locations``). Every row
        has ``source = REPORTED`` and ``is_primary = True``.
    """
    frames = [
        person_locations.filter(
            pl.col(lat_col).is_not_null() & pl.col(lon_col).is_not_null()
        ).select(
            pl.col("person_id"),
            pl.lit(loc_type.value, dtype=pl.Int64).alias("location_type"),
            pl.lit(value=True).alias("is_primary"),
            pl.col(lat_col).alias("lat"),
            pl.col(lon_col).alias("lon"),
            pl.lit(LocationSource.REPORTED.value, dtype=pl.Int64).alias("source"),
        )
        for loc_type, lat_col, lon_col in _REPORTED_SCALARS
        if lat_col in person_locations.columns and lon_col in person_locations.columns
    ]
    return pl.concat(frames, how="vertical")


def _label_clusters(points: pl.DataFrame, radius_meters: float) -> pl.DataFrame:
    """Group each person's points into clusters of mutually nearby points.

    Two points join the same cluster when they are within ``radius_meters`` of
    one another, and clusters are the connected components that follow. Points
    can therefore chain: A near B and B near C puts all three together even if A
    and C are further apart. At the radii used here that spread is bounded, and
    a contiguous blob of GPS noise around one building is the intended reading.

    Args:
        points: Points with ``_pt`` (a unique integer), ``person_id``, ``lat``
            and ``lon``.
        radius_meters: Distance within which two points are the same place.

    Returns:
        The input with a ``_cluster`` label column.
    """
    right = points.select(
        pl.col("_pt").alias("_pt_other"),
        "person_id",
        pl.col("lat").alias("_lat_other"),
        pl.col("lon").alias("_lon_other"),
    )
    pairs = (
        points.select("_pt", "person_id", "lat", "lon")
        .join(right, on="person_id", how="inner")
        .filter(pl.col("_pt") < pl.col("_pt_other"))
        .with_columns(
            expr_haversine(
                pl.col("lat"),
                pl.col("lon"),
                pl.col("_lat_other"),
                pl.col("_lon_other"),
            ).alias("_d")
        )
        .filter(pl.col("_d") <= radius_meters)
        .select("_pt", "_pt_other")
    )

    graph = nx.Graph()
    graph.add_nodes_from(points["_pt"].to_list())
    graph.add_edges_from(pairs.iter_rows())
    labels = {
        point: cluster
        for cluster, component in enumerate(nx.connected_components(graph))
        for point in component
    }
    return points.with_columns(
        pl.col("_pt").replace_strict(labels, return_dtype=pl.Int64).alias("_cluster")
    )


def _derive_pool(
    episodes: pl.DataFrame,
    reported: pl.DataFrame,
    pool: _ObservedPool,
    config: HabitualLocationConfig,
) -> pl.DataFrame:
    """Cluster one pool's qualifying stays and promote those that pass its gate.

    The person's reported location of this kind joins the clustering as a seed
    point. Any cluster it lands in *is* the reported location, so that cluster
    is not promoted again as an observed one — the same radius that groups
    repeat visits also does the de-duplication.
    """
    radius = config.same_location_radius_meters[pool.location_type]

    candidates = episodes.filter(pool.candidates).select(
        "person_id",
        "day_id",
        "lat",
        "lon",
        "dwell_minutes",
        "is_day_end",
    )
    seeds = reported.filter(pl.col("location_type") == pool.location_type.value).select(
        "person_id",
        pl.lit(None, dtype=candidates.schema["day_id"]).alias("day_id"),
        "lat",
        "lon",
        pl.lit(None, dtype=pl.Float64).alias("dwell_minutes"),
        pl.lit(value=False).alias("is_day_end"),
    )
    points = (
        pl.concat(
            [
                candidates.with_columns(pl.lit(value=False).alias("_is_seed")),
                seeds.with_columns(pl.lit(value=True).alias("_is_seed")),
            ],
            how="vertical",
        )
        .with_row_index("_pt")
        .with_columns(pl.col("_pt").cast(pl.Int64))
    )
    if points.height == 0:
        return pl.DataFrame(schema=_observed_schema())

    labelled = _label_clusters(points, radius)

    clusters = labelled.group_by(["person_id", "_cluster"]).agg(
        pl.col("_is_seed").any().alias("_has_reported"),
        pl.col("day_id").drop_nulls().n_unique().cast(pl.Int64).alias("_n_days"),
        pl.col("dwell_minutes").max().cast(pl.Float64).alias("_max_dwell"),
        pl.col("is_day_end").any().alias("_any_day_end"),
        pl.col("lat").filter(~pl.col("_is_seed")).mean().alias("lat"),
        pl.col("lon").filter(~pl.col("_is_seed")).mean().alias("lon"),
    )

    promoted = clusters.filter(~pl.col("_has_reported") & pool.gate(config))
    return promoted.select(
        "person_id",
        pl.lit(pool.location_type.value, dtype=pl.Int64).alias("location_type"),
        pl.lit(None, dtype=pl.Boolean).alias("is_primary"),
        "lat",
        "lon",
        pl.lit(LocationSource.OBSERVED.value, dtype=pl.Int64).alias("source"),
        pl.col("_max_dwell"),
        pl.col("_n_days"),
    )


def derive_observed_locations(
    episodes: pl.DataFrame,
    reported: pl.DataFrame,
    config: HabitualLocationConfig | None = None,
) -> pl.DataFrame:
    """Promote places a person keeps using into observed habitual locations.

    Args:
        episodes: Stays, as produced by ``build_presence_episodes``.
        reported: Tall reported locations, as produced by
            ``build_reported_locations``. Used as clustering seeds so an
            observed cluster coinciding with a reported location is not promoted
            a second time.
        config: Promotion rules (defaults if not given).

    Returns:
        Tall table of observed HOME/WORK/SCHOOL locations without
        ``location_num``. Each row has ``source = OBSERVED`` and
        ``is_primary = None`` (primacy unknown), plus the private ``_max_dwell``
        and ``_n_days`` columns used to order locations within a kind.
    """
    config = config or HabitualLocationConfig()
    frames = [_derive_pool(episodes, reported, pool, config) for pool in _OBSERVED_POOLS]
    return pl.concat(frames, how="vertical")


def _resolve_observed_primacy(locations: pl.DataFrame) -> pl.DataFrame:
    """Set ``is_primary`` on observed rows relative to the reported locations.

    An observed location is *not* primary (``False``) when the person has a
    reported location of the same kind (that reported one is the primary), and
    *unknown* (``None``) when they have none — e.g. a multi-site worker with no
    single usual workplace. Reported rows keep ``is_primary = True``.
    """
    reported_kinds = (
        locations.filter(pl.col("source") == LocationSource.REPORTED.value)
        .select("person_id", "location_type")
        .unique()
        .with_columns(pl.lit(value=True).alias("_has_reported"))
    )
    return (
        locations.join(reported_kinds, on=["person_id", "location_type"], how="left")
        .with_columns(
            pl.when(pl.col("source") == LocationSource.REPORTED.value)
            .then(pl.lit(value=True))
            .when(pl.col("_has_reported").fill_null(value=False))
            .then(pl.lit(value=False))
            .otherwise(pl.lit(None, dtype=pl.Boolean))
            .alias("is_primary")
        )
        .drop("_has_reported")
    )


def _assign_location_id(locations: pl.DataFrame) -> pl.DataFrame:
    """Number locations within each (person, kind) and mint their identifier.

    Reported locations sort ahead of observed; observed sort by descending
    dwell, then by how many days they were seen. The first location of each kind
    is numbered 1, and the identifier packs the kind and that number onto the
    person: ``person_id * 1000 + location_type * 100 + location_num``.
    """
    numbered = (
        locations.with_columns(
            (pl.col("source") != LocationSource.REPORTED.value).cast(pl.Int8).alias("_source_order")
        )
        .sort(
            ["person_id", "location_type", "_source_order", "_max_dwell", "_n_days"],
            descending=[False, False, False, True, True],
            nulls_last=True,
        )
        .with_columns(
            (pl.int_range(0, pl.len()).over(["person_id", "location_type"]) + 1).alias(
                "location_num"
            )
        )
        .drop("_source_order", "_max_dwell", "_n_days")
    )

    overflow = numbered.filter(pl.col("location_num") > _MAX_LOCATION_NUM)
    if overflow.height > 0:
        msg = (
            f"{overflow.height} location(s) exceed location_num "
            f"{_MAX_LOCATION_NUM}, which habitual_location_id cannot encode."
        )
        raise ValueError(msg)

    numbered = numbered.with_columns(
        (pl.col("location_type") * 100 + pl.col("location_num")).alias("_location_suffix")
    )
    return create_concatenated_id(
        numbered,
        output_col="habitual_location_id",
        parent_id_col="person_id",
        sequence_col="_location_suffix",
        sequence_padding=3,
    ).drop("_location_suffix")


def build_location_days(
    episodes: pl.DataFrame,
    locations: pl.DataFrame,
    config: HabitualLocationConfig,
) -> pl.DataFrame:
    """Aggregate stays into one row per habitual location per day.

    Each stay is attributed to the nearest habitual location within that
    location's radius; stays that are not at any habitual location are dropped,
    since this table is about habitual locations rather than all travel.
    Attribution is purely spatial — a meal taken at one's workplace is still
    presence at the workplace — because these are presence facts, not claims
    about what the person was doing.

    Args:
        episodes: Stays, as produced by ``build_presence_episodes``.
        locations: Habitual locations with ``habitual_location_id``.
        config: Provides the per-kind radius.

    Returns:
        Table conforming to ``HabitualLocationDayModel``.
    """
    radii = pl.DataFrame(
        {
            "location_type": [loc_type.value for loc_type in config.same_location_radius_meters],
            "_radius": list(config.same_location_radius_meters.values()),
        },
        schema={"location_type": pl.Int64, "_radius": pl.Float64},
    )

    candidates = (
        episodes.with_row_index("_episode")
        .join(
            locations.select(
                "habitual_location_id",
                "person_id",
                "location_type",
                pl.col("lat").alias("_loc_lat"),
                pl.col("lon").alias("_loc_lon"),
            ),
            on="person_id",
            how="inner",
        )
        .join(radii, on="location_type", how="inner")
        .with_columns(
            expr_haversine(
                pl.col("lat"), pl.col("lon"), pl.col("_loc_lat"), pl.col("_loc_lon")
            ).alias("_d")
        )
        .filter(pl.col("_d") <= pl.col("_radius"))
    )

    # One stay belongs to one location: the nearest, with the identifier
    # breaking ties so the result does not depend on row order.
    nearest = (
        candidates.sort(["_episode", "_d", "habitual_location_id"]).group_by("_episode").first()
    )

    days = (
        nearest.group_by(["habitual_location_id", "person_id", "day_id", "location_type"])
        .agg(
            pl.col("dwell_minutes").sum().alias("_dwell_sum"),
            pl.col("dwell_minutes").count().alias("_dwell_measured"),
            pl.len().alias("n_visits"),
            pl.col("is_day_start").any().alias("is_day_start"),
            pl.col("is_day_end").any().alias("is_day_end"),
        )
        .with_columns(
            # No measurable stay is not the same as a zero-length stay.
            pl.when(pl.col("_dwell_measured") == 0)
            .then(None)
            .otherwise(pl.col("_dwell_sum"))
            .cast(pl.Float64)
            .alias("dwell_minutes")
        )
        .with_columns(pl.col("n_visits").cast(pl.Int64))
    )

    return days.select(_DAY_COLUMNS).sort(["person_id", "day_id", "habitual_location_id"])


def build_habitual_locations(
    person_locations: pl.DataFrame,
    linked_trips: pl.DataFrame | None = None,
    config: HabitualLocationConfig | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build the habitual-location tables from reported and observed evidence.

    Args:
        person_locations: Wide per-person reported locations (see
            ``build_reported_locations``).
        linked_trips: Optional linked trips, used to promote observed locations
            and to build the per-day table. If omitted, only reported locations
            are returned and the day table is empty.
        config: Promotion rules (defaults if not given).

    Returns:
        ``(habitual_locations, habitual_location_days)`` conforming to
        ``HabitualLocationModel`` and ``HabitualLocationDayModel``.
    """
    config = config or HabitualLocationConfig()
    reported = build_reported_locations(person_locations)

    if linked_trips is None or linked_trips.height == 0:
        locations = _assign_location_id(
            reported.with_columns(
                pl.lit(None, dtype=pl.Float64).alias("_max_dwell"),
                pl.lit(None, dtype=pl.Int64).alias("_n_days"),
            )
        )
        locations = locations.select(_LOCATION_COLUMNS).sort(
            ["person_id", "location_type", "location_num"]
        )
        return locations, pl.DataFrame(schema=_day_schema())

    if "d_activity_duration" not in linked_trips.columns:
        msg = (
            "build_habitual_locations requires the d_activity_duration column "
            "(linked-trip field added in #68)."
        )
        raise ValueError(msg)

    episodes = build_presence_episodes(linked_trips)
    observed = derive_observed_locations(episodes, reported, config)

    locations = _resolve_observed_primacy(
        pl.concat(
            [
                reported.with_columns(
                    pl.lit(None, dtype=pl.Float64).alias("_max_dwell"),
                    pl.lit(None, dtype=pl.Int64).alias("_n_days"),
                ),
                observed,
            ],
            how="vertical",
        )
    )
    locations = _assign_location_id(locations)
    locations = locations.select(_LOCATION_COLUMNS).sort(
        ["person_id", "location_type", "location_num"]
    )

    days = build_location_days(episodes, locations, config)

    logger.info(
        "Built habitual locations: %d (%d reported, %d observed) with %d location-days",
        locations.height,
        locations.filter(pl.col("source") == LocationSource.REPORTED.value).height,
        locations.filter(pl.col("source") == LocationSource.OBSERVED.value).height,
        days.height,
    )

    return locations, days


def _observed_schema() -> dict[str, pl.DataType]:
    """Column types for an empty set of observed locations."""
    return {
        "person_id": pl.Int64,
        "location_type": pl.Int64,
        "is_primary": pl.Boolean,
        "lat": pl.Float64,
        "lon": pl.Float64,
        "source": pl.Int64,
        "_max_dwell": pl.Float64,
        "_n_days": pl.Int64,
    }


def _day_schema() -> dict[str, pl.DataType]:
    """Column types for an empty habitual_location_days table."""
    return {
        "habitual_location_id": pl.Int64,
        "person_id": pl.Int64,
        "day_id": pl.Int64,
        "location_type": pl.Int64,
        "dwell_minutes": pl.Float64,
        "n_visits": pl.Int64,
        "is_day_start": pl.Boolean,
        "is_day_end": pl.Boolean,
    }
