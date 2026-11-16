"""Tour purpose, primary destination, and when the tour was there.

This module contains functions for:
- Choosing each tour's primary destination and so its purpose
- Timing the arrival at and departure from that destination
"""

import logging

import polars as pl

from data_canon.codebook.trips import PurposeCategory
from utils.helpers import expr_haversine

from .priority_utils import (
    add_mode_priority_column,
    add_purpose_priority_column,
    add_purpose_score_column,
)
from .tour_configs import TourConfig

logger = logging.getLogger(__name__)


def calculate_tour_purpose_and_destination(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Calculate tour purpose and primary destination from trip data.

    The tour purpose is the purpose of the first candidate trip after ordering
    by ``config.tour_purpose_method``:

    - ``"score"``: duration-weighted, highest score wins, so a long
      discretionary activity can outrank a brief mandatory one; ties go to
      the longer activity, then the lower trip id.
    - ``"hierarchy"``: priority rank; activity duration breaks ties only.

    A trip cannot supply the purpose when it is the *return leg* -- the last
    trip, landing back on the anchor: that arrival is the tour closing, not an
    activity. A partial tour's last trip never reaches the anchor, so its
    destination is a genuine candidate; excluding it purely for being last left
    every one-trip tour with a null purpose. Mode changes are never
    candidates. A tour with no candidate at all -- an anchor-to-anchor loop,
    or one whose every stop was a mode change -- gets a null purpose and is
    flagged NO_DESTINATION downstream.

    Args:
        linked_trips: Trip data with tour_num and subtour_num
        config: TourConfig with purpose hierarchy

    Returns:
        Tuple of (enhanced_linked_trips, tour_purp_and_coords):
        - enhanced_linked_trips: All trips with tour_id, priorities, and flags
        - tour_purp_and_coords: Aggregated tour purpose and destination coords
    """
    logger.info("Calculating tour purpose and primary destination...")
    # Add mode priority and activity duration for selection logic.
    linked_trips = add_mode_priority_column(
        linked_trips, config.mode_hierarchy, alias="_mode_priority"
    )
    linked_trips = linked_trips.with_columns(
        pl.col("d_activity_duration")
        .cast(pl.Float64)
        .fill_null(config.default_activity_duration_minutes)
        .alias("_activity_duration")
    )

    # The return leg to the anchor and mode changes are not candidates (see docstring)
    is_last_trip = pl.col("linked_trip_id").rank("ordinal").over("tour_id") == pl.col(
        "linked_trip_id"
    ).count().over("tour_id")
    anchor_reached = (
        pl.when(pl.col("subtour_num") > 0)
        .then(pl.col("_d_at_work") | pl.col("_d_at_school"))
        .otherwise(pl.col("_d_is_home"))
    )
    linked_trips = linked_trips.with_columns(
        [
            is_last_trip.alias("_is_last_trip"),
            # Mode changes are never activities, so they can never be the
            # primary destination however long the dwell.
            (
                (is_last_trip & anchor_reached.fill_null(value=False))
                | (pl.col("d_purpose_category") == PurposeCategory.CHANGE_MODE.value)
            ).alias("_is_not_a_destination"),
        ]
    )

    # Order candidates so the tour purpose is the first row per tour
    if config.tour_purpose_method == "score":
        linked_trips = add_purpose_score_column(linked_trips, config, alias="_purpose_score")
        non_last = linked_trips.filter(~pl.col("_is_not_a_destination")).sort(
            # score wins; ties broken by longer activity then lowest trip id so
            # the selection is deterministic.
            ["tour_id", "_purpose_score", "_activity_duration", "linked_trip_id"],
            descending=[False, True, True, False],
            nulls_last=True,
        )
    else:
        linked_trips = add_purpose_priority_column(linked_trips, config, alias="_purpose_priority")
        non_last = linked_trips.filter(~pl.col("_is_not_a_destination")).sort(
            ["tour_id", "_purpose_priority", "_activity_duration"],
            descending=[False, False, True],
        )

    tour_purp_and_coords = non_last.group_by("tour_id", maintain_order=True).agg(
        [
            pl.col("d_purpose_category").first().alias("tour_purpose"),
            pl.col("d_lat").first().alias("_primary_d_lat"),
            pl.col("d_lon").first().alias("_primary_d_lon"),
            pl.col("d_location_type").first().alias("_primary_d_type"),
        ]
    )

    linked_trips = linked_trips.join(tour_purp_and_coords, on="tour_id", how="left")

    return linked_trips, tour_purp_and_coords


def calculate_destination_times(
    linked_trips: pl.DataFrame,
    buffer_meters: float,
) -> pl.DataFrame:
    """Calculate arrival and departure times at primary destination.

    A trip arrives at or departs from the primary destination when its end is
    within the habitual-location buffer of it.

    Candidate trips are the ones that could *be* the destination -- the same
    ``_is_not_a_destination`` test purpose selection uses, so the two cannot
    disagree about which trip the destination is. Selecting on "not the last
    trip" instead assumed every tour returns to its anchor: a one-trip tour's
    only trip is also its last, so it matched no candidate rule and came out
    with no destination at all, while purpose selection had already given it one.

    ``dest_depart_time`` stays null when nothing departed the destination. A
    tour cut by the diary edge arrived somewhere and stopped being observed;
    inventing a departure would put a time on an event that did not happen.

    Departure times join on *left*, not full: a departure is a departure from
    the destination, so a tour with no destination has none. An outer join let
    a tour whose every trip was ruled out as a candidate still pick up a
    departure time, and then all its trips classified as inbound, leaving it
    with no outbound leg and no mode for it.

    Args:
        linked_trips: Enhanced trip data with primary destination coordinates
        buffer_meters: The habitual-location buffer: a trip end this close to
            the primary destination is at it.

    Returns:
        DataFrame with dest_arrive_time, dest_depart_time, and
        dest_linked_trip_id per tour_id
    """
    logger.info("Calculating destination arrival and departure times...")
    # A return to the primary destination is a trip end within the buffer of it.
    linked_trips = linked_trips.with_columns(
        [
            expr_haversine(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("_primary_d_lat"),
                pl.col("_primary_d_lon"),
            ).alias("_dist_d_to_primary"),
            expr_haversine(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("_primary_d_lat"),
                pl.col("_primary_d_lon"),
            ).alias("_dist_o_to_primary"),
        ]
    ).with_columns(
        [
            (pl.col("_dist_d_to_primary") <= buffer_meters).alias("_arrives_at_primary"),
            (pl.col("_dist_o_to_primary") <= buffer_meters).alias("_departs_from_primary"),
        ]
    )

    # Arrival: the last candidate trip ending at the destination
    dest_arrive = (
        linked_trips.filter(~pl.col("_is_not_a_destination") & pl.col("_arrives_at_primary"))
        .group_by("tour_id")
        .agg(
            [
                pl.col("arrive_time").max().alias("dest_arrive_time"),
                pl.col("linked_trip_id").max().alias("dest_linked_trip_id"),
            ]
        )
    )

    # Fallback: first candidate trip, if the distance threshold was too tight.
    dest_arrive_fallback = (
        linked_trips.filter(~pl.col("_is_not_a_destination"))
        .group_by("tour_id")
        .agg(
            [
                pl.col("arrive_time").first().alias("dest_arrive_time"),
                pl.col("linked_trip_id").first().alias("dest_linked_trip_id"),
            ]
        )
    )

    dest_depart = (
        linked_trips.filter(pl.col("_departs_from_primary"))
        .group_by("tour_id")
        .agg(pl.col("depart_time").max().alias("dest_depart_time"))
    )

    # Fallback: use last trip before home if distance threshold too restrictive
    dest_depart_fallback = (
        linked_trips.filter(~pl.col("_is_last_trip"))
        .group_by("tour_id")
        .agg(pl.col("depart_time").last().alias("dest_depart_time"))
    )

    dest_times = (
        dest_arrive_fallback.join(
            dest_arrive.select(["tour_id", "dest_arrive_time", "dest_linked_trip_id"]),
            on="tour_id",
            how="left",
            suffix="_dist",
        )
        .with_columns(
            [
                pl.coalesce(["dest_arrive_time_dist", "dest_arrive_time"]).alias(
                    "dest_arrive_time"
                ),
                pl.coalesce(["dest_linked_trip_id_dist", "dest_linked_trip_id"]).alias(
                    "dest_linked_trip_id"
                ),
            ]
        )
        .select(["tour_id", "dest_arrive_time", "dest_linked_trip_id"])
        # Left, not full: no destination, no departure (see docstring)
        .join(
            dest_depart_fallback.join(dest_depart, on="tour_id", how="left", suffix="_dist")
            .with_columns(
                pl.coalesce(["dest_depart_time_dist", "dest_depart_time"]).alias("dest_depart_time")
            )
            .select(["tour_id", "dest_depart_time"]),
            on="tour_id",
            how="left",
        )
    )

    return dest_times
