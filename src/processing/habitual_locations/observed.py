"""Observed locations: places found by clustering the person's stays.

Two kinds of evidence are clustered, each within the buffer:

- Workplaces and schools: stops made for that purpose that lasted long enough.
- Homes: where days the respondent said began or ended at a home began or
  ended.

A cluster within the buffer of a reported location of the same kind *is* that
location, so it is not added. Nothing else is ever clustered: a corner store by
the house or lunch by the office says it is somewhere else.
"""

import networkx as nx
import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import PurposeCategory
from utils.helpers import expr_haversine

from .habitual_location_configs import HabitualLocationConfig
from .matching import STATED_DAY_PURPOSE

# Kinds found from long stops, and the purpose categories of the stops that feed
# each. Work-related stops count towards a workplace, but only at their own
# (much longer) cutoff: respondents describe their own workplace as
# work-related as readily as "primary workplace", so excluding them found no
# second workplace at all for anyone who reported one.
OBSERVED_KINDS = {
    LocationType.WORK: (PurposeCategory.WORK, PurposeCategory.WORK_RELATED),
    LocationType.SCHOOL: (PurposeCategory.SCHOOL,),
}

# Answers to "where did the day begin/end?" that name a home of the person's.
STATED_HOMES = list(STATED_DAY_PURPOSE)


def label_clusters(points: pl.DataFrame, radius_meters: float) -> pl.DataFrame:
    """Group each person's points into clusters of mutually nearby points.

    Two points join the same cluster when they are within ``radius_meters`` of
    one another, and clusters are the connected components that follow. Points
    can therefore chain: A near B and B near C puts all three together even if A
    and C are further apart, which is the intended reading of a contiguous blob
    of GPS noise around one site.

    Args:
        points: Points with ``_pt`` (a unique integer), ``person_id``, ``lat``
            and ``lon``.
        radius_meters: Distance within which two points are the same place.

    Returns:
        The input with a ``_cluster`` label column: the smallest ``_pt`` in the
        cluster, so labels do not depend on the order components are found in.
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
        point: min(component) for component in nx.connected_components(graph) for point in component
    }
    return points.with_columns(
        pl.col("_pt").replace_strict(labels, return_dtype=pl.Int64).alias("_cluster")
    )


def _new_places(
    stops: pl.DataFrame,
    known: pl.DataFrame,
    config: HabitualLocationConfig,
) -> pl.DataFrame:
    """Cluster stops into places, leaving out those at a known location.

    The known locations join the clustering as seed points, so a cluster any of
    them falls into *is* that location and is not returned: it is already in the
    registry, exactly as reported.

    Args:
        stops: Evidence stops with ``person_id``, ``person_day_id``, ``lat``, ``lon``
            and ``seen_at``.
        known: Locations of the same kind with ``person_id``, ``lat``, ``lon``.
        config: Provides the buffer.

    Returns:
        One row per new place with ``person_id``, ``lat``, ``lon`` (the mean of
        its stops), ``_n_days`` and ``_first_seen``.
    """
    evidence = stops.select(
        "person_id",
        "person_day_id",
        "lat",
        "lon",
        "seen_at",
        pl.lit(value=False).alias("_is_seed"),
    )
    seeds = known.select(
        "person_id",
        pl.lit(None, dtype=evidence.schema["person_day_id"]).alias("person_day_id"),
        "lat",
        "lon",
        pl.lit(None, dtype=evidence.schema["seen_at"]).alias("seen_at"),
        pl.lit(value=True).alias("_is_seed"),
    )
    points = (
        pl.concat([evidence, seeds], how="vertical")
        .with_row_index("_pt")
        .with_columns(pl.col("_pt").cast(pl.Int64))
    )
    # Rows stay in _pt order within each group, so the means are summed in the
    # same order every run.
    labelled = label_clusters(points, config.buffer_meters)
    places = labelled.group_by(["person_id", "_cluster"], maintain_order=True).agg(
        pl.col("_is_seed").any().alias("_at_known"),
        pl.col("person_day_id").drop_nulls().n_unique().cast(pl.Int64).alias("_n_days"),
        pl.col("seen_at").min().alias("_first_seen"),
        pl.col("lat").filter(~pl.col("_is_seed")).mean().alias("lat"),
        pl.col("lon").filter(~pl.col("_is_seed")).mean().alias("lon"),
    )
    return places.filter(~pl.col("_at_known")).select(
        "person_id", "lat", "lon", "_n_days", "_first_seen"
    )


def _as_locations(
    places: pl.DataFrame,
    location_type: LocationType,
    is_primary: bool | None,
) -> pl.DataFrame:
    """Shape new places as observed locations of one kind."""
    return places.select(
        "person_id",
        pl.lit(location_type.value, dtype=pl.Int64).alias("location_type"),
        pl.lit(is_primary, dtype=pl.Boolean).alias("is_primary"),
        "lat",
        "lon",
        pl.lit(LocationSource.OBSERVED.value, dtype=pl.Int64).alias("source"),
        "_first_seen",
    )


def observed_locations(
    episodes: pl.DataFrame,
    reported: pl.DataFrame,
    location_type: LocationType,
    config: HabitualLocationConfig,
) -> pl.DataFrame:
    """Workplaces or schools the person went to for that purpose and stayed at.

    A stop counts as evidence when its purpose is one of the kind's own and it
    lasted at least the cutoff for that purpose -- 90 minutes by default, 45 for
    a college class, four hours for a work-related stop. Stops are filtered
    *before* they are clustered, so a short visit neither makes nor moves a
    location. Whether one is the primary is left open here (see
    ``numbering.number_observed``).
    """
    by_purpose = {p.value: m for p, m in config.min_dwell_minutes_by_purpose.items()}
    categories = [c.value for c in OBSERVED_KINDS[location_type]]
    stops = episodes.filter(
        pl.col("purpose_category").is_in(categories)
        & (
            pl.col("dwell_minutes")
            >= pl.col("purpose").replace_strict(
                by_purpose, default=config.min_dwell_minutes, return_dtype=pl.Float64
            )
        )
    )
    known = reported.filter(pl.col("location_type") == location_type.value)
    places = _new_places(stops, known, config).filter(pl.col("_n_days") >= config.min_distinct_days)
    return _as_locations(places, location_type, is_primary=None)


def observed_homes(
    episodes: pl.DataFrame,
    person_days: pl.DataFrame,
    reported: pl.DataFrame,
    config: HabitualLocationConfig,
) -> pl.DataFrame:
    """Homes the respondent said a day began or ended at, beyond the reported ones.

    A day stated to begin at home or at "your/their other home" puts a home at
    the day's first origin; one stated to end there, at its last destination.
    Those points are clustered like any evidence. A cluster at a home already
    reported is that home; one away from every reported home is another home —
    the respondent said it is theirs, and it is not the one they reported. It is
    never the primary home, which only the survey's reported home can be.

    Args:
        episodes: Stays, as produced by ``build_presence_episodes``.
        person_days: Days with ``person_day_id``, ``begin_day`` and ``end_day``.
        reported: Reported locations.
        config: Provides the buffer.

    Returns:
        Observed HOME rows, not primary.
    """
    stated = episodes.join(
        person_days.select("person_day_id", "begin_day", "end_day"), on="person_day_id", how="inner"
    ).filter(
        (pl.col("is_day_start") & pl.col("begin_day").is_in(STATED_HOMES))
        | (pl.col("is_day_end") & pl.col("end_day").is_in(STATED_HOMES))
    )
    known = reported.filter(pl.col("location_type") == LocationType.HOME.value)
    return _as_locations(_new_places(stated, known, config), LocationType.HOME, is_primary=False)
