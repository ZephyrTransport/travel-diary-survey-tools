"""Tour aggregation helper functions for tour extraction.

This module contains functions for:
- Flagging each trip end against its own tour's anchor
- Aggregating trips to tour-level records and classifying them
- Assigning half-tour classification

The primary destination and tour purpose come from ``destination_helpers``.
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import (
    TourCategory,
    TourDirection,
    TourType,
)
from processing.habitual_locations import MatchConfig

from .destination_helpers import (
    calculate_destination_times,
    calculate_tour_purpose_and_destination,
)
from .tour_configs import TourConfig

logger = logging.getLogger(__name__)


def add_tour_anchor_flags(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Flag each trip end as being at its own tour's anchor.

    A tour's anchor is the place it is expected to leave from and come back to:
    home for a home-based tour, and -- for a subtour -- whatever the containing
    tour anchored on (``_anchor_location_type``, set by
    :func:`~processing.tours.detection_helpers.expand_anchor_periods`).

    Collapsing both cases into one pair of columns is what lets tour
    classification and validation ask "did this tour reach its anchor?" without
    first knowing which anchor applies. Before this existed, the anchor was
    assumed to be home, so an at-work subtour -- which by definition never
    touches home -- could never be classified COMPLETE and was dropped by every
    downstream gate.

    Args:
        linked_trips: Trips with ``subtour_num``, the home flags
            (``_o_is_home`` / ``_d_is_home``) and the anchor flags
            (``_o_at_work`` / ``_d_at_work`` / ``_o_at_school`` /
            ``_d_at_school``) plus ``_anchor_location_type``.

    Returns:
        The trips with ``_o_at_anchor`` / ``_d_at_anchor`` boolean columns.
    """
    is_subtour = pl.col("subtour_num") > 0
    anchored_at_school = pl.col("_anchor_location_type") == LocationType.SCHOOL.value
    return linked_trips.with_columns(
        [
            pl.when(~is_subtour)
            .then(pl.col(f"_{end}_is_home"))
            .when(anchored_at_school)
            .then(pl.col(f"_{end}_at_school"))
            .otherwise(pl.col(f"_{end}_at_work"))
            .fill_null(value=False)
            .alias(f"_{end}_at_anchor")
            for end in ("o", "d")
        ]
    )


def _aggregate_and_classify_tours(
    linked_trips: pl.DataFrame,
    tour_purpose_and_coords: pl.DataFrame,
    match: MatchConfig,
) -> pl.DataFrame:
    """Aggregate trip data to tour level and classify tour categories.

    Groups trips by tour and calculates tour-level attributes including mode,
    timing, locations, and counts. Records what each tour is anchored on
    (``tour_type``) and, separately, how completely it reaches that anchor
    (``tour_category``). These are two independent facts and must not share a
    column: ``TourType.WORK_BASED`` and ``TourCategory.PARTIAL_END`` are both
    2, so writing the type into ``tour_category`` made every subtour read as
    "start at home, end not at home". Data quality is graded separately, by
    ``validate_and_correct_tours``.

    Args:
        linked_trips: Enhanced trip data with priorities and flags
        tour_purpose_and_coords: Tour purpose and destination coordinates
        match: The habitual-location match rule, for returns to the destination

    Returns:
        Tour-level DataFrame with all attributes and classifications
    """
    logger.info("Aggregating and classifying tours...")

    # Calculate destination arrival/departure times
    dest_times = calculate_destination_times(linked_trips, match.buffer_meters)

    tours = linked_trips.group_by("tour_id").agg(
        [
            # Identifiers (tour_id is automatically included from group_by)
            pl.col("person_id").first(),
            pl.col("hh_id").first(),
            pl.col("person_day_id").first(),
            pl.col("tour_num").first(),
            pl.col("subtour_num").first(),
            pl.col("parent_tour_id").first(),
            pl.col("linked_trip_id").first().alias("origin_linked_trip_id"),
            # Tour mode (highest priority)
            pl.col("mode_type").sort_by("_mode_priority").last().alias("tour_mode"),
            # Origin timing and locations
            pl.col("depart_time").min().alias("origin_depart_time"),
            pl.col("arrive_time").max().alias("origin_arrive_time"),
            pl.col("o_lat").first(),
            pl.col("o_lon").first(),
            # Keep fallback destination coordinates and type for edge cases
            # (e.g., single-trip tours with no non-last trip)
            pl.col("d_lat").last().alias("_fallback_d_lat"),
            pl.col("d_lon").last().alias("_fallback_d_lon"),
            pl.col("o_location_type").first().alias("o_location_type"),
            pl.col("d_location_type").last().alias("_fallback_d_location_type"),
            # Counts
            pl.col("linked_trip_id").count().alias("trip_count"),
            # Flags for classification
            pl.col("_anchor_location_type").first().alias("_anchor_location_type"),
            pl.col("_o_at_anchor").first().alias("_o_at_anchor"),
            pl.col("_d_at_anchor").last().alias("_d_at_anchor"),
            *([pl.all("survey_complete")] if "survey_complete" in linked_trips.columns else []),
        ]
    )

    # Join purpose, primary purpose lat/lon and destination timing
    tours = (
        tours.join(
            tour_purpose_and_coords.select(
                [
                    "tour_id",
                    "tour_purpose",
                    pl.col("_primary_d_lat").alias("d_lat"),
                    pl.col("_primary_d_lon").alias("d_lon"),
                    pl.col("_primary_d_type").alias("d_location_type"),
                ]
            ),
            on="tour_id",
            how="left",
        )
        .with_columns(
            [
                pl.coalesce(["d_lat", "_fallback_d_lat"]).alias("d_lat"),
                pl.coalesce(["d_lon", "_fallback_d_lon"]).alias("d_lon"),
                # Keep d_location_type describing the same place as d_lat/d_lon:
                # the primary destination, falling back to the last trip.
                pl.coalesce(["d_location_type", "_fallback_d_location_type"]).alias(
                    "d_location_type"
                ),
            ]
        )
        .drop(["_fallback_d_lat", "_fallback_d_lon", "_fallback_d_location_type"])
        .join(dest_times, on="tour_id", how="left")
    )

    one_trip_count = tours.filter(pl.col("trip_count") == 1).height
    logger.info("Tours: %d total, %d with a single trip", len(tours), one_trip_count)

    # What the tour is anchored on, and separately how completely it reaches it
    is_subtour = pl.col("subtour_num") > 0
    anchored_at_school = pl.col("_anchor_location_type") == LocationType.SCHOOL.value
    tours = tours.with_columns(
        [
            pl.when(~is_subtour)
            .then(pl.lit(TourType.HOME_BASED))
            .when(anchored_at_school)
            .then(pl.lit(TourType.SCHOOL_BASED))
            .otherwise(pl.lit(TourType.WORK_BASED))
            .alias("tour_type"),
            pl.when(pl.col("_o_at_anchor") & pl.col("_d_at_anchor"))
            .then(pl.lit(TourCategory.COMPLETE))
            .when(pl.col("_o_at_anchor") & ~pl.col("_d_at_anchor"))
            .then(pl.lit(TourCategory.PARTIAL_END))
            .when(~pl.col("_o_at_anchor") & pl.col("_d_at_anchor"))
            .then(pl.lit(TourCategory.PARTIAL_START))
            .otherwise(pl.lit(TourCategory.PARTIAL_BOTH))
            .alias("tour_category"),
        ]
    ).sort(["person_id", "person_day_id", "origin_depart_time"])

    return tours


def _assign_half_tour(
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Assign half-tour classification based on primary destination.

    Classifies each trip as:
    - OUTBOUND: Trips before first arrival at primary destination
    - INBOUND: Trips after final departure from primary destination

    Every tour has exactly two halves relative to its *own* anchor and primary
    destination. That includes at-work subtours: a subtour is a separate tour
    row with its own ``tour_id``, so its ``dest_arrive_time`` /
    ``dest_depart_time`` already describe the subtour's own destination (e.g.
    the lunch stop), and the same before/after rule splits it into outbound and
    inbound. Direction and subtour membership are orthogonal facts -- membership
    is carried by ``parent_tour_id`` / ``subtour_num`` / ``tour_type`` -- so
    direction never encodes "this is a subtour".

    A tour with no primary destination has null destination times, so neither
    rule applies and its trips default to outbound.

    Args:
        linked_trips: Linked trips with tour_id assignments
        tours: Tour table with dest_arrive_time and dest_depart_time

    Returns:
        Linked trips with a ``tour_direction`` (TourDirection) column added
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
            # Outbound: trip arrives before or at first arrival at primary dest
            pl.when(pl.col("arrive_time") <= pl.col("dest_arrive_time"))
            .then(pl.lit(TourDirection.OUTBOUND))
            # Inbound: trip departs after final departure from primary dest
            .when(pl.col("depart_time") >= pl.col("dest_depart_time"))
            .then(pl.lit(TourDirection.INBOUND))
            # A tour with no primary destination has null times; outbound is the
            # default (see docstring)
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
                .filter(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
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
    match: MatchConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Aggregate trip data to tour-level records with attributes.

    Calculates tour attributes from trip data:
    - Tour purpose: the primary destination's purpose, chosen by
      ``config.tour_purpose_method`` (duration-weighted score, or priority
      rank with activity duration breaking ties)
    - Tour mode: Highest priority trip mode
    - Timing: First departure and last arrival
    - Counts: Number of trips

    Work- and school-based subtours are aggregated separately with their own
    tour_id (``tour_num * 1000 + subtour_num * 10`` after the person_day_id).
    The output holds home-based tours and subtours as separate records.

    Args:
        linked_trips: Linked trips with tour_id
        config: TourConfig object with priority settings
        match: The habitual-location match rule, for returns to the destination

    Returns:
        Tuple of: (enhanced_linked_trips, tours)
        - enhanced_linked_trips: Input trips with ``tour_direction`` added
    """
    logger.info("Aggregating tour data...")

    # Resolve each trip end against its own tour's anchor (home, or the
    # workplace/campus a subtour hangs off). Done once here so tour
    # classification and, later, validation read the same flags.
    linked_trips = add_tour_anchor_flags(linked_trips)

    # Calculate tour purpose and primary destination
    linked_trips, tour_purp_and_coords = calculate_tour_purpose_and_destination(
        linked_trips, config
    )

    # Aggregate to tour level and classify
    tours = _aggregate_and_classify_tours(linked_trips, tour_purp_and_coords, match)

    # Assign half-tour classification using tours table
    linked_trips, tours = _assign_half_tour(linked_trips, tours)

    return linked_trips, tours
