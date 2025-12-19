"""Tour aggregation helper functions for tour extraction.

This module contains functions for:
- Aggregating trips to tour-level records
- Computing tour attributes (purpose, mode, timing)
- Assigning half-tour classification
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import (
    TourCategory,
    TourDirection,
    TourType,
)
from utils.helpers import expr_haversine

from .priority_utils import (
    add_activity_duration_column,
    add_mode_priority_column,
    add_purpose_priority_column,
)
from .tour_configs import TourConfig

logger = logging.getLogger(__name__)

# Constants
# A tour requires at least some round-trip structure, even if not to/from home
MIN_TRIPS_FOR_VALID_TOUR = 2


def _calculate_tour_purp_and_dest(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Calculate tour purpose and primary destination from trip data.

    Determines tour purpose from the highest priority non-last trip, with
    activity duration as a tie-breaker. Returns enhanced trip data with
    purpose priorities and primary destination coordinates.

    Args:
        linked_trips: Trip data with tour_num and subtour_num
        config: TourConfig with purpose hierarchy

    Returns:
        Tuple of (enhanced_linked_trips, tour_purp_and_coords):
        - enhanced_linked_trips: All trips with tour_id, priorities, and flags
        - tour_purp_and_coords: Aggregated tour purpose and destination coords
    """
    logger.info("Calculating tour purpose and primary destination...")
    # Add priorities and activity duration for selection logic
    linked_trips = add_purpose_priority_column(
        linked_trips, config, alias="_purpose_priority"
    )
    linked_trips = add_mode_priority_column(
        linked_trips, config.mode_hierarchy, alias="_mode_priority"
    )
    linked_trips = add_activity_duration_column(
        linked_trips,
        config.default_activity_duration_minutes,
        alias="_activity_duration",
    )

    # Mark last trip (excluded from purpose selection)
    linked_trips = linked_trips.with_columns(
        [
            (
                pl.col("linked_trip_id").rank("ordinal").over("tour_id")
                == pl.col("linked_trip_id").count().over("tour_id")
            ).alias("_is_last_trip"),
        ]
    )

    # Determine tour purpose and primary destination from non-last trips
    # Note: Single-trip tours will have null purpose
    # and will be filtered out later
    non_last = linked_trips.filter(~pl.col("_is_last_trip")).sort(
        ["tour_id", "_purpose_priority", "_activity_duration"],
        descending=[False, False, True],
    )

    tour_purp_and_coords = non_last.group_by(
        "tour_id", maintain_order=True
    ).agg(
        [
            pl.col("d_purpose_category").first().alias("tour_purpose"),
            pl.col("d_lat").first().alias("_primary_d_lat"),
            pl.col("d_lon").first().alias("_primary_d_lon"),
            pl.col("_d_location_type").first().alias("_primary_d_type"),
        ]
    )

    linked_trips = linked_trips.join(
        tour_purp_and_coords, on="tour_id", how="left"
    )

    return linked_trips, tour_purp_and_coords


def _calculate_destination_times(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> pl.DataFrame:
    """Calculate arrival and departure times at primary destination.

    Uses distance thresholds based on location type to identify when trips
    arrive at or depart from the primary destination.

    Args:
        linked_trips: Enhanced trip data with primary destination coordinates
        config: TourConfig with distance thresholds

    Returns:
        DataFrame with dest_arrive_time, dest_depart_time, and
        dest_linked_trip_id per tour_id
    """
    logger.info("Calculating destination arrival and departure times...")
    # Calculate distances to primary destination and apply thresholds
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
            pl.when(pl.col("_primary_d_type") == LocationType.HOME)
            .then(pl.lit(config.distance_thresholds[LocationType.HOME]))
            .when(pl.col("_primary_d_type") == LocationType.WORK)
            .then(pl.lit(config.distance_thresholds[LocationType.WORK]))
            .when(pl.col("_primary_d_type") == LocationType.SCHOOL)
            .then(pl.lit(config.distance_thresholds[LocationType.SCHOOL]))
            .otherwise(pl.lit(config.distance_thresholds[LocationType.HOME]))
            .alias("_threshold"),
        ]
    ).with_columns(
        [
            (pl.col("_dist_d_to_primary") <= pl.col("_threshold")).alias(
                "_arrives_at_primary"
            ),
            (pl.col("_dist_o_to_primary") <= pl.col("_threshold")).alias(
                "_departs_from_primary"
            ),
        ]
    )

    # Aggregate arrive times (exclude last trip) and depart times (all trips)
    # Use distance filtering with fallback to trip sequence
    dest_arrive = (
        linked_trips.filter(
            ~pl.col("_is_last_trip") & pl.col("_arrives_at_primary")
        )
        .group_by("tour_id")
        .agg(
            [
                pl.col("arrive_time").max().alias("dest_arrive_time"),
                pl.col("linked_trip_id").max().alias("dest_linked_trip_id"),
            ]
        )
    )

    # Fallback: use first non-last trip if distance threshold too restrictive
    dest_arrive_fallback = (
        linked_trips.filter(~pl.col("_is_last_trip"))
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
            dest_arrive.select(
                ["tour_id", "dest_arrive_time", "dest_linked_trip_id"]
            ),
            on="tour_id",
            how="left",
            suffix="_dist",
        )
        .with_columns(
            [
                pl.coalesce(
                    ["dest_arrive_time_dist", "dest_arrive_time"]
                ).alias("dest_arrive_time"),
                pl.coalesce(
                    ["dest_linked_trip_id_dist", "dest_linked_trip_id"]
                ).alias("dest_linked_trip_id"),
            ]
        )
        .select(["tour_id", "dest_arrive_time", "dest_linked_trip_id"])
        .join(
            dest_depart_fallback.join(
                dest_depart, on="tour_id", how="left", suffix="_dist"
            )
            .with_columns(
                pl.coalesce(
                    ["dest_depart_time_dist", "dest_depart_time"]
                ).alias("dest_depart_time")
            )
            .select(["tour_id", "dest_depart_time"]),
            on="tour_id",
            how="full",
            coalesce=True,
        )
    )

    return dest_times


def _aggregate_and_classify_tours(
    linked_trips: pl.DataFrame,
    tour_purpose_and_coords: pl.DataFrame,
    config: TourConfig,
) -> pl.DataFrame:
    """Aggregate trip data to tour level and classify tour categories.

    Groups trips by tour and calculates tour-level attributes including mode,
    timing, locations, and counts. Classifies tours as work-based subtours or
    by boundary type (complete, partial start/end/both).

    Args:
        linked_trips: Enhanced trip data with priorities and flags
        tour_purpose_and_coords: Tour purpose and destination coordinates
        config: TourConfig with classification settings

    Returns:
        Tour-level DataFrame with all attributes and classifications
    """
    logger.info("Aggregating and classifying tours...")

    # Calculate destination arrival/departure times
    dest_times = _calculate_destination_times(linked_trips, config)

    tours = linked_trips.group_by("tour_id").agg(
        [
            # Identifiers (tour_id is automatically included from group_by)
            pl.col("person_id").first(),
            pl.col("hh_id").first(),
            pl.col("day_id").first(),
            pl.col("tour_num").first(),
            pl.col("subtour_num").first(),
            pl.col("parent_tour_id").first(),
            pl.col("linked_trip_id").first().alias("origin_linked_trip_id"),
            # Tour mode (highest priority)
            pl.col("mode_type")
            .sort_by("_mode_priority")
            .last()
            .alias("tour_mode"),
            # Origin timing and locations
            pl.col("depart_time").min().alias("origin_depart_time"),
            pl.col("arrive_time").max().alias("origin_arrive_time"),
            pl.col("o_lat").first(),
            pl.col("o_lon").first(),
            pl.col("d_lat").last(),
            pl.col("d_lon").last(),
            pl.col("_o_location_type").first().alias("o_location_type"),
            pl.col("_d_location_type").last().alias("d_location_type"),
            # Counts
            pl.col("linked_trip_id").count().alias("trip_count"),
            (pl.col("linked_trip_id").count() - 1).alias("stop_count"),
            # Flags for classification
            pl.col("subtour_num").first().alias("_subtour_num"),
            pl.col("_o_is_home").first().alias("_o_is_home"),
            pl.col("_d_is_home").last().alias("_d_is_home"),
        ]
    )

    # Join purpose and destination timing
    tours = tours.join(
        tour_purpose_and_coords.select(["tour_id", "tour_purpose"]),
        on="tour_id",
        how="left",
    ).join(dest_times, on="tour_id", how="left")

    # Flag single-trip tours (incomplete tours with only one trip)
    # A valid tour must have at least 2 trips: one leaving and one returning
    tours = tours.with_columns(
        [
            (pl.col("trip_count") < MIN_TRIPS_FOR_VALID_TOUR).alias(
                "single_trip_tour"
            )
        ]
    )

    single_trip_count = tours.filter(pl.col("single_trip_tour")).height
    logger.info(
        "Tours: %d total, %d single-trip tours (<%d trips)",
        len(tours),
        single_trip_count,
        MIN_TRIPS_FOR_VALID_TOUR,
    )

    # Classify tour category based on actual tour structure
    # Validation will separately flag data quality issues (tour_num=0, etc.)
    tours = tours.with_columns(
        [
            pl.when(pl.col("_subtour_num") > 0)
            .then(pl.lit(TourType.WORK_BASED))
            .when(pl.col("_o_is_home") & pl.col("_d_is_home"))
            .then(pl.lit(TourCategory.COMPLETE))
            .when(pl.col("_o_is_home") & ~pl.col("_d_is_home"))
            .then(pl.lit(TourCategory.PARTIAL_END))
            .when(~pl.col("_o_is_home") & pl.col("_d_is_home"))
            .then(pl.lit(TourCategory.PARTIAL_START))
            .otherwise(pl.lit(TourCategory.PARTIAL_BOTH))
            .alias("tour_category"),
        ]
    ).sort(["person_id", "day_id", "origin_depart_time"])

    return tours


def _assign_half_tour(
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Assign half-tour classification based on primary destination.

    Classifies each trip as:
    - OUTBOUND: Trips before first arrival at primary destination
    - INBOUND: Trips after final departure from primary destination
    - SUBTOUR: Work-based subtour trips

    Args:
        linked_trips: Linked trips with tour_id assignments
        tours: Tour table with dest_arrive_time and dest_depart_time

    Returns:
        Linked trips with half_tour_type (TourDirection enum) column added
        and tours with outbound and inbound modes added
    """
    logger.info("Assigning half-tour classification...")

    # Join destination times from tours table
    # tour_id already matches between linked_trips and tours
    linked_trips = linked_trips.join(
        tours.select(
            [
                "tour_id",
                "dest_arrive_time",
                "dest_depart_time",
            ]
        ),
        on=["tour_id"],
        how="left",
    )

    # Classify half-tour type based on trip timing relative to
    # primary destination arrival/departure
    linked_trips = linked_trips.with_columns(
        [
            # Subtours are identified by subtour_num > 0
            pl.when(pl.col("subtour_num") > 0)
            .then(pl.lit(TourDirection.SUBTOUR))
            # Outbound: trip arrives before or at first arrival at primary dest
            .when(pl.col("arrive_time") <= pl.col("dest_arrive_time"))
            .then(pl.lit(TourDirection.OUTBOUND))
            # Inbound: trip departs after final departure from primary dest
            .when(pl.col("depart_time") >= pl.col("dest_depart_time"))
            .then(pl.lit(TourDirection.INBOUND))
            # Default to outbound if times are null (shouldn't happen)
            .otherwise(pl.lit(TourDirection.OUTBOUND))
            .alias("tour_direction"),
        ]
    )

    # Aggregate half-tour modes after tour_direction exists
    # Sort entire group first, then filter and take last
    half_tour_modes = (
        linked_trips.sort("_mode_priority")
        .group_by("tour_id")
        .agg(
            [
                pl.col("mode_type")
                .filter(
                    pl.col("tour_direction") == TourDirection.OUTBOUND.value
                )
                .last()
                .alias("outbound_mode"),
                pl.col("mode_type")
                .filter(pl.col("tour_direction") == TourDirection.INBOUND.value)
                .last()
                .alias("inbound_mode"),
            ]
        )
    )

    # Join half-tour modes to tours
    tours = tours.join(half_tour_modes, on="tour_id", how="left")

    # Clean up temporary columns
    linked_trips = linked_trips.drop(
        [
            "dest_arrive_time",
            "dest_depart_time",
        ]
    )

    return linked_trips, tours


def aggregate_tour_attributes(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Aggregate trip data to tour-level records with attributes.

    Calculates tour attributes from trip data:
    - Tour purpose: Highest priority destination, with duration tie-breaker
      (When priorities equal, selects trip with longest activity duration)
    - Tour mode: Highest priority trip mode
    - Timing: First departure and last arrival
    - Counts: Number of trips and stops

    IMPORTANT: Work-based subtours are aggregated separately with their own
    tour_id (which includes subtour_num in the last 2 digits). The final output
    includes both home-based tours and work-based subtours as separate records.

    Args:
        linked_trips: Linked trips with tour_id
        config: TourConfig object with priority settings

    Returns:
        Tuple of: (enhanced_linked_trips, tours)
        - enhanced_linked_trips: Input trips with tour_id and subtour_id added
    """
    logger.info("Aggregating tour data...")

    # Calculate tour purpose and primary destination
    linked_trips, tour_purp_and_coords = _calculate_tour_purp_and_dest(
        linked_trips, config
    )

    # Aggregate to tour level and classify
    tours = _aggregate_and_classify_tours(
        linked_trips, tour_purp_and_coords, config
    )

    # Assign half-tour classification using tours table
    linked_trips, tours = _assign_half_tour(linked_trips, tours)

    return linked_trips, tours
