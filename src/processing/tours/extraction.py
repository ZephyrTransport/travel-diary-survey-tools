"""Tour building module for travel diary survey processing.

This module implements a hierarchical tour extraction algorithm that processes
linked trip data to identify and classify tours and subtours based on spatial
and temporal patterns.

!!! Algorithm

    The tour building process follows a seven-phase pipeline:

    # 1. Location Classification

    - Calculates haversine distances from trip endpoints to known locations
      (home, work, school) using person-specific coordinates
    - Classifies each trip origin/destination as HOME, WORK, SCHOOL, or OTHER
      based on configurable distance thresholds
    - Only matches work/school locations if person has those locations defined
    - Adds boolean flags: o_is_home, d_is_home, o_is_work, d_is_work, etc.

    # 2. Home-Based Tour Identification

    - Sorts trips by person, day, and departure time
    - Identifies tour boundaries by detecting:
        * Departures from home (o_is_home=True, d_is_home=False)
        * Returns to home (o_is_home=False, d_is_home=True)
        * Day boundaries (first trip of person-day)
    - Assigns sequential tour IDs within each person-day
    - Format: tour_id = (day_id * 100) + tour_sequence_number

    # 3. Anchor Period Expansion (CRITICAL for subtours)

    - For tours visiting usual anchor locations (work, school), expands the
      "at anchor" period by finding first arrival and last departure
    - Uses pure Polars window functions to identify anchor periods
    - Prevents subtours from being detected during travel to/from anchor
    - Generalizable: supports work, school, or future anchor types

    # 4. Anchor-Based Subtour Detection

    - Within expanded anchor periods, identifies subtours by detecting:
        * Departures from anchor (o_at_anchor=True, d_at_anchor=False)
        * Returns to anchor (o_at_anchor=False, d_at_anchor=True)
    - Assigns hierarchical subtour IDs
    - Format: subtour_id = (tour_id * 10) + subtour_sequence_number
    - Currently supports work-based subtours, extensible to school-based

    # 5. Tour Attribute Aggregation

    - Groups trips by tour_id (and subtour_id for subtours)
    - Computes tour-level attributes from constituent trips:
        * tour_purpose: Highest priority destination purpose (person-category
          specific hierarchy)
        * tour_mode: Highest priority travel mode (from configurable mode
          hierarchy)
        * origin_depart_time: First trip's departure time
        * dest_arrive_time: Last trip's arrival time
        * trip_count: Number of trips in tour
        * stop_count: Number of intermediate stops (trip_count - 1)

    - Assigns half-tour classification:
        * "outbound": Trips before primary destination
        * "inbound": Trips after primary destination
        * "subtour": Work-based subtour trips

    # 6. Joint Tour Identification

    - If joint_trips data provided, identifies tours where all trips involve
      same group of travelers
    - Assigns joint_tour_id to tours with stable participant groups
    - Links tour-level joint travel to trip-level joint travel

    # 7. Tour Validation and Correction

    - Validates tour structure consistency
    - Corrects data quality issues (e.g., inconsistent timing, missing values)
    - Adds tour_id and joint_tour_id to unlinked_trips for reference

    # Edge Case Handling is performed including

    - Incomplete tours (no return home at end of day)
    - Multi-day tours (spanning survey boundaries)
    - Missing work/school locations (null coordinates)
    - Non-sequential trip chains (spatial gaps)
    - Hierarchical tour structure: Home-based tours → Work-based subtours
    - Location classification robust to GPS/geododing errors via distance thresholds
    - Tour purpose reflects primary activity, not intermediate stops
    - Extensible design allows future additions (school-based subtours, other
      anchor types)
"""

import logging
from typing import Any

import polars as pl

from pipeline.decoration import step
from utils.create_ids import create_tour_ids

from .aggregation_helpers import aggregate_tour_attributes
from .detection_helpers import (
    detect_anchor_based_subtours,
    expand_anchor_periods,
    identify_home_based_tours,
)
from .habitual_locations import build_habitual_locations
from .joint_tour_helpers import build_joint_tours_table, identify_joint_tours
from .location_helpers import (
    add_anchor_flags,
    classify_trip_locations,
    prepare_person_locations,
)
from .tour_configs import TourConfig
from .validation_helpers import validate_and_correct_tours

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@step(
    requires={
        "households": {"hh_id", "home_lat", "home_lon"},
        "persons": {
            "person_id",
            "age",
            "employment",
            "student",
            "school_type",
            "work_lat",
            "work_lon",
            "school_lat",
            "school_lon",
        },
        "unlinked_trips": {"day_id", "linked_trip_id", "depart_time", "arrive_time"},
        "linked_trips": {
            "day_id",
            "joint_trip_id",
            "o_purpose",
            "o_purpose_category",
            "d_purpose",
            "d_purpose_category",
            "d_activity_duration",
            "mode_type",
        },
    },
    produces={
        "unlinked_trips": {"tour_id"},
        "linked_trips": {"tour_id"},
        "tours": {"tour_id"},
        "habitual_locations": {"habitual_location_id"},
        "habitual_location_days": {"habitual_location_id"},
    },
)
def extract_tours(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame | None = None,
    **kwargs: dict[str, Any],
) -> dict[str, pl.DataFrame]:
    """Extract hierarchical tour structures from linked trip data.

    Builds tour and subtour structures from linked trip sequences using spatial
    and temporal patterns. See module docstring for complete algorithm description.

    Args:
        persons: Person attributes including work/school locations. Used to identify
            anchor locations for tour/subtour detection.
        households: Household attributes including home locations. Home location is
            primary anchor for tour identification.
        unlinked_trips: Individual trip segments. Will receive tour_id assignment.
        linked_trips: Journey records with coordinates and timing. Required columns:
            person_id, day_id, o_lon, o_lat, d_lon, d_lat, depart_time, arrive_time.
        joint_trips: Optional joint trip aggregations. If provided, enables joint
            tour identification based on stable participant groups.
        **kwargs: Configuration parameters for TourConfig:

            - distance_thresholds: Dict of location type → distance threshold (meters).
              Default: {"home": 100, "work": 200, "school": 200}
            - mode_hierarchy: Mode priority for tour mode assignment (list).
              Higher index = higher priority.
            - purpose_hierarchy: Purpose priority by person type (dict).
              Maps person categories to ordered purpose lists.
            - person_category_expression: Polars expression to classify person
              categories (e.g., worker, student).

    Returns:
        Dictionary containing:

            - unlinked_trips: Original unlinked trips with tour_id, joint_tour_id
            - linked_trips: Trips with tour_id, subtour_id, half_tour, joint_tour_id
            - tours: Aggregated tour records with purpose, mode, timing, trip counts,
              and joint_tour_id
    """
    logger.info("Building tours from linked trip data...")

    config = TourConfig(**kwargs)  # pyright: ignore[reportArgumentType]

    # Prepare person location cache with categories
    person_locations = prepare_person_locations(
        persons,
        households,
        config.person_category_expression(),
    )

    # Build the habitual locations (reported home/work/school plus observed
    # worksites, campuses and other homes derived from travel), and the per-day
    # record of presence at them. The locations drive both classification and
    # anchor detection; the day table is foundation for later consumers and is
    # not read here.
    habitual_locations, habitual_location_days = build_habitual_locations(
        person_locations,
        linked_trips,
        config.habitual_locations,
    )

    msg = f"Processing {len(persons)} persons, {len(linked_trips)} trips"
    logger.info(msg)

    # Step 1: Classify trip ends against the habitual locations
    linked_trips_classified = classify_trip_locations(
        linked_trips,
        habitual_locations,
        config.distance_thresholds,
    ).join(
        person_locations.select(["person_id", "person_category"]),
        on="person_id",
        how="left",
    )

    # Anchor flags from the habitual locations (reported + observed work/school,
    # with per-day resolution). These drive anchor-period and subtour detection.
    linked_trips_classified = add_anchor_flags(
        linked_trips_classified,
        habitual_locations,
        config.distance_thresholds,
    )

    # Step 2: Identify home-based tours
    linked_trips_with_hb_tours = identify_home_based_tours(
        linked_trips=linked_trips_classified,
        check_multiday_gaps=config.check_multiday_gaps,
    )

    # Step 3: Expand anchor location periods (work, school, etc.)
    linked_trips_with_anchor_periods = expand_anchor_periods(linked_trips_with_hb_tours)

    # Step 4: Detect anchor-based subtours (work-based, school-based, etc.)
    linked_trips_with_subtours = detect_anchor_based_subtours(linked_trips_with_anchor_periods)

    # Step 5: Aggregation and tour classification
    # Create tour_id and parent_tour_id
    linked_trips_with_tour_ids = create_tour_ids(linked_trips_with_subtours)

    # Aggregate tour attributes, also adds tour direction (inbound/outbound)
    linked_trips_with_tour_dir, tours = aggregate_tour_attributes(
        linked_trips_with_tour_ids,
        config,
    )

    # Step 6: Identify joint tours (tours where all trips involve same group)
    if joint_trips is not None and len(joint_trips) > 0:
        linked_trips_with_tour_dir, tours = identify_joint_tours(
            linked_trips_with_tour_dir,
            tours,
        )
    else:
        # No joint trips, add null joint_tour_id columns
        linked_trips_with_tour_dir = linked_trips_with_tour_dir.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("joint_tour_id")
        )
        tours = tours.with_columns(pl.lit(None, dtype=pl.Int64).alias("joint_tour_id"))

    # Step 7: Validate tours and correct data quality issues
    tours = validate_and_correct_tours(
        tours,
        linked_trips_with_tour_dir,
        spatial_gap_threshold_meters=config.spatial_gap_threshold_meters,
    )

    # Step 8: Add tour_id and joint_tour_id to unlinked_trips
    unlinked_trips_with_tour_ids = unlinked_trips.join(
        linked_trips_with_tour_dir.select("linked_trip_id", "tour_id", "joint_tour_id"),
        on="linked_trip_id",
        how="left",
    )

    # Drop temporary columns, any starting with underscore
    for df in [linked_trips_with_tour_dir, tours]:
        _cols = df.columns
        for c in _cols:
            if c.startswith("_"):
                df.drop_in_place(c)

    msg = (
        f"Tour building complete: {len(linked_trips_with_tour_dir)} "
        f"linked trips, {len(tours)} tours."
        "\nTour count may increase due to sub-tours being identified."
    )
    logger.info(msg)

    # Step 9: Collapse the member tours into the canonical joint_tours table, so
    # the group is a record in its own right rather than a grouping key.
    joint_tours = build_joint_tours_table(tours)

    return {
        "unlinked_trips": unlinked_trips_with_tour_ids,
        "linked_trips": linked_trips_with_tour_dir,
        "tours": tours,
        "joint_tours": joint_tours,
        "habitual_locations": habitual_locations,
        "habitual_location_days": habitual_location_days,
    }
