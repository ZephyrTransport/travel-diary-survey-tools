"""Tour building module for travel diary survey processing.

This module implements a hierarchical tour extraction algorithm that processes
linked trip data to identify and classify tours and subtours based on spatial
and temporal patterns.

!!! Algorithm

    The tour building process runs these steps in order:

    # 1. Location Classification

    - Says which of the person's habitual locations (built by the
      ``detect_habitual_locations`` step) each trip origin/destination is at
    - Classifies each trip origin/destination as HOME, WORK, SCHOOL, or OTHER
      from the location it is at; any of the person's homes bounds tours
    - Adds boolean flags: o_is_home, d_is_home, o_is_work, d_is_work, etc.

    # 2. Home-Based Tour Identification

    - Sorts trips by person, day, and departure time
    - Identifies tour boundaries by detecting:
        * Departures from home (o_is_home=True, d_is_home=False)
        * Returns to home (o_is_home=False, d_is_home=True)
        * Day boundaries (first trip of person-day)
    - Numbers the tours within each person-day (``tour_num``)

    # 3. Anchor Period Expansion (CRITICAL for subtours)

    - For tours visiting usual anchor locations (work, school), expands the
      "at anchor" period by finding first arrival and last departure
    - Uses pure Polars window functions to identify anchor periods
    - Prevents subtours from being detected during travel to/from anchor
    - Generalizable: supports work, school, or future anchor types

    # 4. Anchor-Based Subtour Detection

    - Within expanded anchor periods, identifies subtours by detecting:
        * Departures from an anchor (o_at_anchor=True, d_at_anchor=False)
        * Returns to that same anchor (o_at_anchor=False, d_at_anchor=True)
    - Numbers the subtours within each tour (``subtour_num``); work- and
      school-based subtours alike

    # 5. Tour IDs and Attribute Aggregation

    - ``tour_id`` is the ``person_day_id`` followed by four digits,
      ``tour_num * 1000 + subtour_num * 10``; a subtour's ``parent_tour_id`` is
      its tour's id with ``subtour_num`` 0
    - Computes tour-level attributes from constituent trips:
        * tour_purpose: the primary destination's purpose, chosen by score
          (activity duration and purpose, the default) or by the
          person-category hierarchy (``tour_purpose_method``)
        * tour_mode: Highest priority travel mode (from configurable mode
          hierarchy)
        * origin_depart_time: First trip's departure time
        * dest_arrive_time: Last trip's arrival time
        * trip_count: Number of trips in tour

    - Assigns each trip's ``tour_direction``: outbound (before the primary
      destination) or inbound (after it). A subtour has two halves too;
      subtour membership is read from ``parent_tour_id``, not the direction

    # 6. Joint Tour Identification

    - If joint_trips data provided, identifies tours where all trips involve
      same group of travelers
    - Assigns joint_tour_id to tours with stable participant groups
    - Links tour-level joint travel to trip-level joint travel

    # 7. Tour Data Quality

    - Stamps each tour's ``tour_data_quality``: valid, or the first reason it
      is not a valid round trip (no destination, a spatial gap, another home,
      split across days, at the diary's edge). Nothing is corrected
    - Adds tour_id and joint_tour_id to unlinked_trips for reference

    # 8. Joint Tours Table

    - Collapses the member tours of each joint tour into ``joint_tours``

    # Edge Case Handling is performed including

    - Incomplete tours (no return home at end of day)
    - Multi-day tours (spanning survey boundaries)
    - Missing work/school locations (null coordinates)
    - Non-sequential trip chains (spatial gaps)
    - Hierarchical tour structure: Home-based tours → work- and school-based subtours
    - Location matching allows for GPS/geocoding error through one buffer
    - Tour purpose reflects primary activity, not intermediate stops
    - Extensible design allows further anchor types
"""

import logging

import polars as pl

from pipeline.decoration import Tables, step
from processing.habitual_locations import MatchConfig
from utils.create_ids import create_tour_ids

from .aggregation_helpers import aggregate_tour_attributes
from .detection_helpers import (
    detect_anchor_based_subtours,
    expand_anchor_periods,
    identify_home_based_tours,
)
from .joint_tour_helpers import (
    build_joint_tours_table,
    identify_joint_tours,
    without_joint_tours,
)
from .location_helpers import add_anchor_flags, classify_trip_locations
from .tour_configs import TourConfig
from .validation_helpers import validate_and_correct_tours

logger = logging.getLogger(__name__)


@step(
    requires={
        "persons": {"person_id", "age", "employment", "student"},
        "unlinked_trips": {"person_day_id", "linked_trip_id", "depart_time", "arrive_time"},
        "linked_trips": {
            "person_day_id",
            "joint_trip_id",
            "o_purpose",
            "o_purpose_category",
            "d_purpose",
            "d_purpose_category",
            "d_activity_duration",
            "mode_type",
        },
        "habitual_locations": {
            "habitual_location_id",
            "person_id",
            "location_type",
            "is_primary",
            "lat",
            "lon",
            "source",
        },
        "person_days": {"person_day_id", "hh_day_id", "travel_date", "begin_day", "end_day"},
        "joint_trips": set(),  # whether any exist
    },
    produces={
        "unlinked_trips": {"tour_id", "joint_tour_id"},
        "linked_trips": {"tour_id", "joint_tour_id"},
        "tours": {"tour_id", "joint_tour_id"},
        "joint_tours": {"joint_tour_id", "hh_id", "hh_day_id", "travel_date"},
    },
)
def extract_tours(
    tables: Tables,
    extraction: TourConfig,
    habitual_match: MatchConfig,
) -> dict[str, pl.DataFrame]:
    """Extract hierarchical tour structures from linked trip data.

    Builds tour and subtour structures from linked trip sequences using spatial
    and temporal patterns. See module docstring for complete algorithm description.

    Args:
        tables: ``persons`` (for the person category purpose priority is ranked
            by), ``unlinked_trips``, ``linked_trips``, ``habitual_locations``
            (from ``detect_habitual_locations``, read only), ``person_days``
            (a day stated to begin or end at home counts beside the trip purpose
            at its first origin or last destination) and ``joint_trips`` (joint
            tours are identified when there are any).
        extraction: Tour-building rules. See ``TourConfig``.
        habitual_match: Whether a trip end is at a habitual location, and the
            distance within which it returns to a tour's primary destination.
            The tours' own setting, separate from ``detect_habitual_locations``'.

    Returns:
        Dictionary containing:

            - unlinked_trips: Original unlinked trips with tour_id, joint_tour_id
            - linked_trips: Trips with tour_id, parent_tour_id, tour_direction,
              joint_tour_id
            - tours: Aggregated tour records with purpose, mode, timing, trip counts,
              tour_data_quality and joint_tour_id
            - joint_tours: One record per joint tour

        ``person_category`` (worker/student, joined on so purpose priority can
        be ranked) is dropped with the underscored working columns: it predates
        the underscore convention and is derived from ``persons``, not a survey
        fact.
    """
    logger.info("Building tours from linked trip data...")
    persons = tables["persons"]
    unlinked_trips = tables["unlinked_trips"]
    linked_trips = tables["linked_trips"]
    habitual_locations = tables["habitual_locations"]
    person_days = tables["person_days"]

    person_categories = persons.select("person_id", extraction.person_category_expression())

    logger.info("Processing %d persons, %d trips", len(persons), len(linked_trips))

    # Step 1: Say which habitual location each trip end is at, and classify it
    linked_trips_classified = classify_trip_locations(
        linked_trips,
        habitual_locations,
        habitual_match,
        person_days,
    ).join(person_categories, on="person_id", how="left")

    # Anchor flags from the habitual locations (reported + observed work/school,
    # with per-day resolution). These drive anchor-period and subtour detection.
    linked_trips_classified = add_anchor_flags(linked_trips_classified)

    # Step 2: Identify home-based tours
    linked_trips_with_hb_tours = identify_home_based_tours(
        linked_trips=linked_trips_classified,
        check_multiday_gaps=extraction.check_multiday_gaps,
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
        extraction,
        habitual_match,
    )

    # Step 6: Identify joint tours (tours where all trips involve same group)
    if len(tables["joint_trips"]) > 0:
        linked_trips_with_tour_dir, tours = identify_joint_tours(
            linked_trips_with_tour_dir,
            tours,
        )
    else:
        linked_trips_with_tour_dir, tours = without_joint_tours(linked_trips_with_tour_dir, tours)

    # Step 7: Grade each tour's data quality
    # Sorted: tours come out of group_bys in no fixed order, so runs would differ.
    tours = validate_and_correct_tours(
        tours,
        linked_trips_with_tour_dir,
        spatial_gap_threshold_meters=extraction.spatial_gap_threshold_meters,
    ).sort("tour_id")

    # Step 8: Add tour_id and joint_tour_id to unlinked_trips
    unlinked_trips_with_tour_ids = unlinked_trips.join(
        linked_trips_with_tour_dir.select("linked_trip_id", "tour_id", "joint_tour_id"),
        on="linked_trip_id",
        how="left",
    )

    # Drop working columns: underscored ones, and person_category (see docstring)
    linked_trips_with_tour_dir, tours = (
        df.drop([c for c in df.columns if c.startswith("_") or c == "person_category"])
        for df in (linked_trips_with_tour_dir, tours)
    )

    logger.info(
        "Tour building complete: %d linked trips, %d tours."
        "\nTour count may increase due to sub-tours being identified.",
        len(linked_trips_with_tour_dir),
        len(tours),
    )

    # Step 9: Collapse member tours into joint_tours, one record per group
    joint_tours = build_joint_tours_table(tours, person_days)

    return {
        "unlinked_trips": unlinked_trips_with_tour_ids,
        "linked_trips": linked_trips_with_tour_dir,
        "tours": tours,
        "joint_tours": joint_tours,
    }
