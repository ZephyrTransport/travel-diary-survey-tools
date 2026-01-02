"""Tour building module for travel diary survey processing.

This module implements a hierarchical tour extraction algorithm that processes
linked trip data to identify and classify tours and subtours based on spatial
and temporal patterns.

Algorithm Overview:
-------------------
The tour building process follows a four-stage pipeline:
1. Location Classification
    - Calculates haversine distances from trip endpoints to known locations
      (home, work, school) using person-specific coordinates
    - Classifies each trip origin/destination as HOME, WORK, SCHOOL, or OTHER
      based on configurable distance thresholds
    - Only matches work/school locations if person has those locations defined
2. Home-Based Tour Identification
    - Sorts trips by person, day, and departure time
    - Identifies tour boundaries by detecting:
      * Departures from home (o_is_home=True, d_is_home=False)
      * Returns to home (!o_is_home=True, d_is_home=True)
      * Day boundaries (first trip of person-day)
    - Assigns sequential tour IDs within each person-day
    - Format: tour_id = (day_id * 100) + tour_sequence_number
3. Anchor Location Period Expansion (CRITICAL for subtours)
    - For tours visiting usual anchor locations (work, school), expands the
      "at anchor" period by finding first arrival and last departure
    - Uses pure Polars window functions to identify anchor periods
    - Prevents subtours from being detected during travel to/from anchor
    - Generalizable: supports work, school, or future anchor types
4. Anchor-Based Subtour Detection
    - Within expanded anchor periods, identifies subtours by detecting:
      * Departures from anchor (o_at_anchor=True, d_at_anchor=False)
      * Returns to anchor (o_at_anchor=False, d_at_anchor=True)
    - Assigns hierarchical subtour IDs
    - Format: subtour_id = (tour_id * 10) + subtour_sequence_number
    - Currently supports work-based subtours, extensible to school-based
5. Tour Attribute Aggregation
    - Computes tour-level attributes from constituent trips:
      * tour_purpose: Highest priority dest purpose (person-type specific)
      * tour_mode: Highest priority travel mode (from mode hierarchy)
      * origin_depart_time: First trip departure time
      * dest_arrive_time: Last trip arrival time
      * trip_count: Number of trips in tour
      * stop_count: Number of intermediate stops (trip_count - 1)
    - Half-tour assignment: outbound (before primary dest), inbound (after),
      or subtour (work-based subtours)

Configuration:
-------------
Tour building behavior is controlled by TourConfig which defines:
- distance_thresholds: Maximum distances (meters) for location matching
- purpose_priority_by_person_category: Purpose hierarchies by person type
- mode_hierarchy: Ordered list of modes (ascending priority)
- person_type_mapping: Maps person_type codes to PersonCategory enum

Output:
-------
Returns two DataFrames:
1. linked_trips_with_tour_ids: Input trips with tour_id, subtour_id, and
    tour attributes joined for analysis
2. tours: Aggregated tour records with computed attributes (one row per tour)
The algorithm handles edge cases including:
- Incomplete tours (no return home at end of day)
- Multi-day tours (spanning survey boundaries)
- Missing work/school locations (null coordinates)
- Non-sequential trip chains (spatial gaps)
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
from .location_helpers import (
    classify_trip_locations,
    prepare_person_locations,
)
from .person_type import derive_person_type
from .tour_configs import TourConfig
from .validation_helpers import validate_and_correct_tours

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@step()
def extract_tours(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    **kwargs: dict[str, Any],
) -> dict[str, pl.DataFrame]:
    """Extract tours from linked trip data.

    Pipeline processes linked trip data through tour building steps:
    1. Classify trip locations (home, work, school, other)
    2. Identify home-based tours (assigns tour_id)
    3. Expand anchor periods and detect subtours (assigns subtour_id)
    4. Aggregate to tour-level records with attributes
    5. Assign half-tour classification

    Args:
        persons: DataFrame with person attributes
        households: DataFrame with household attributes
        unlinked_trips: DataFrame with unlinked trip data
        linked_trips: DataFrame with linked trip data
        **kwargs: Additional configuration parameters for TourConfig

    Returns:
        Dict with keys:
        - linked_trips: Input trips with tour_id and subtour_id added
        - tours: Aggregated tour records with purpose, mode, timing
    """
    logger.info("Building tours from linked trip data...")

    config = TourConfig(**kwargs)

    # Derive person_type column
    persons_ptype = derive_person_type(persons)

    # Prepare person location cache with categories
    person_locations = prepare_person_locations(
        persons_ptype,
        households,
        config.person_type_mapping,
    )

    msg = f"Processing {len(persons_ptype)} persons, {len(linked_trips)} trips"
    logger.info(msg)

    # Step 1: Prepare person locations
    linked_trips_classified = classify_trip_locations(
        linked_trips,
        person_locations,
        config.distance_thresholds,
    )

    # Step 2: Identify home-based tours
    linked_trips_with_hb_tours = identify_home_based_tours(
        linked_trips=linked_trips_classified,
        check_multiday_gaps=config.check_multiday_gaps,
    )

    # Step 3: Expand anchor location periods (work, school, etc.)
    linked_trips_with_anchor_periods = expand_anchor_periods(
        linked_trips_with_hb_tours,
        person_locations,
        config.distance_thresholds,
    )

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

    # Step 6: Validate tours and correct data quality issues
    tours = validate_and_correct_tours(tours, linked_trips_with_tour_ids)

    # Step 7: Add tour_id to unlinked_trips
    unlinked_trips_with_tour_ids = unlinked_trips.join(
        linked_trips_with_tour_dir.select("linked_trip_id", "tour_id"),
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

    return {
        "persons": persons_ptype,
        "unlinked_trips": unlinked_trips_with_tour_ids,
        "linked_trips": linked_trips_with_tour_dir,
        "tours": tours,
    }
