"""Joint trip detection step for identifying shared household trips.

This module implements the main pipeline step for detecting joint trips
where multiple household members travel together.
"""

import logging

import polars as pl

from pipeline.decoration import Tables, step
from utils.create_ids import create_concatenated_id

from .aggregation import (
    build_joint_trips_table,
    validate_against_num_travelers,
)
from .clique_detection import detect_disjoint_cliques
from .joint_trip_configs import JointTripConfig
from .similarity import (
    apply_buffer_filter,
    apply_mahalanobis_filter,
    compute_pairwise_distances,
)

logger = logging.getLogger(__name__)


def _filter_multi_person_households(
    linked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Pre-filter trips to only those in multi-person households."""
    persons_per_hh = linked_trips.group_by(["hh_id"]).agg(
        [pl.col("person_id").n_unique().alias("n_persons")]
    )
    multi_person_hhs = persons_per_hh.filter(
        pl.col("n_persons") >= 2  # noqa: PLR2004
    )
    candidate_trips = linked_trips.join(
        multi_person_hhs.select(["hh_id"]),
        on="hh_id",
        how="inner",
    )
    num_excluded = len(linked_trips) - len(candidate_trips)
    logger.info(
        "Pre-filter: %d trips in multi-person households (%d single-person trips excluded)",
        len(candidate_trips),
        num_excluded,
    )
    return candidate_trips


def _create_and_filter_trip_pairs(
    candidate_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Create trip pairs and apply deduplication and temporal filters.

    Pairs are formed within one household date only. Two members' trips that
    straddle the survey's day boundary fall on different dates, and a joint
    trip cannot belong to two.
    """
    trip_pairs = candidate_trips.join(
        candidate_trips.select(
            [
                "hh_id",
                "travel_date",
                "linked_trip_id",
                "person_id",
                "o_lat",
                "o_lon",
                "d_lat",
                "d_lon",
                "depart_time",
                "arrive_time",
            ]
        ).rename(
            {
                "linked_trip_id": "linked_trip_id_b",
                "person_id": "person_id_b",
                "o_lat": "o_lat_b",
                "o_lon": "o_lon_b",
                "d_lat": "d_lat_b",
                "d_lon": "d_lon_b",
                "depart_time": "depart_time_b",
                "arrive_time": "arrive_time_b",
            }
        ),
        on=["hh_id", "travel_date"],
        how="inner",
    )

    logger.info(
        "Created %s candidate trip pairs (before deduplication)",
        f"{len(trip_pairs):,}",
    )

    # Remove self-pairs (same trip or same person)
    trip_pairs = trip_pairs.filter(
        # Remove same trip
        (pl.col("linked_trip_id") != pl.col("linked_trip_id_b"))
        # Remove same person but different trips
        & (pl.col("person_id") != pl.col("person_id_b"))
        # Deduplicate by keeping only one direction (A->B, not B->A)
        & (pl.col("linked_trip_id") < pl.col("linked_trip_id_b"))
    )

    logger.info("After deduplication: %s pairs", f"{len(trip_pairs):,}")

    # Apply temporal overlap filter
    trip_pairs = trip_pairs.filter(
        pl.max_horizontal("depart_time", "depart_time_b")
        <= pl.min_horizontal("arrive_time", "arrive_time_b")
    )

    logger.info("After temporal overlap filter: %d pairs", len(trip_pairs))
    return trip_pairs


def _assign_joint_trip_ids(
    clique_assignments: pl.DataFrame,
    linked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Convert clique IDs to standardized joint_trip_ids."""
    # Join clique_id with household info
    cliques_with_hh = clique_assignments.join(
        linked_trips.select(["linked_trip_id", "hh_id"]),
        on="linked_trip_id",
        how="left",
    ).filter(pl.col("clique_id").is_not_null())

    if len(cliques_with_hh) == 0:
        # No cliques found, just add null joint_trip_id column
        return clique_assignments.drop("clique_id").with_columns(
            pl.lit(None, dtype=pl.Int64).alias("joint_trip_id")
        )

    # Group by clique_id to get household and create enumerators
    clique_to_hh = cliques_with_hh.group_by("clique_id").agg([pl.col("hh_id").first()])

    # Create household-scoped enumerators
    clique_to_hh = clique_to_hh.with_columns(
        pl.col("clique_id").rank(method="dense").over("hh_id").alias("joint_trip_num")
    )

    # Create standardized joint_trip_id: <hh_id> + <3 digit enumerator>
    clique_to_hh = create_concatenated_id(
        clique_to_hh,
        output_col="joint_trip_id",
        parent_id_col="hh_id",
        sequence_col="joint_trip_num",
        sequence_padding=3,
    )

    # Join joint_trip_id back to assignments
    return clique_assignments.join(
        clique_to_hh.select(["clique_id", "joint_trip_id"]),
        on="clique_id",
        how="left",
    ).drop("clique_id")


@step(
    requires={
        "linked_trips": {
            "person_day_id",
            "o_lat",
            "o_lon",
            "d_lat",
            "d_lon",
            "depart_time",
            "arrive_time",
        },
        "person_days": {"person_day_id", "hh_day_id", "travel_date"},
        "households": set(),  # counted for the log
    },
    produces={
        "linked_trips": {"joint_trip_id"},
        "joint_trips": {"joint_trip_id", "hh_id", "hh_day_id", "travel_date"},
    },
)
def detect_joint_trips(
    tables: Tables,
    matching: JointTripConfig,
) -> dict[str, pl.DataFrame]:
    """Detect joint trips among household members using similarity matching.

    Identifies trips where multiple household members traveled together by
    comparing origin-destination-time similarity using either strict buffer
    thresholds or Mahalanobis distance.

    Args:
        tables: ``linked_trips`` (journeys with coordinates and timing),
            ``households`` and ``person_days`` (with ``travel_date``: trips are
            paired only within one household date, and each joint trip records it).
        matching: How trips are matched: the method and its thresholds, from
            the config's ``matching:`` block. See ``JointTripConfig``.

    Returns:
        Dictionary containing:
            - linked_trips: Original trips with added joint_trip_id column
            - joint_trips: One row per shared trip (see Phase 5)

    Algorithm:
        # Phase 1: Household Pre-filtering

        1. Filter to households with 2+ members who took trips
        2. Reduces search space to only households where joint trips are possible

        # Phase 2: Pairwise Distance Calculation

        1. Within each multi-person household, compute pairwise distances between
           all trip combinations using 4D space:
            - Origin coordinates (o_lon, o_lat)
            - Destination coordinates (d_lon, d_lat)
            - Departure time
            - Arrival time
        2. Add the four distances as columns on each candidate pair

        # Phase 3: Similarity Filtering

        ## Buffer Method:

        - Filter trip pairs where:
            - Spatial distance (haversine) ≤ space_threshold_meters for both
              origin AND destination
            - Absolute time difference ≤ time_threshold_minutes for both
              departure AND arrival
        - Simple, interpretable thresholds

        ## Mahalanobis Method:

        - Calculate statistical distance using covariance matrix:
            - Accounts for correlated variations in space/time
            - Keeps pairs whose distance is below a cutoff set by
              confidence_level (see ``JointTripConfig.get_distance_threshold``)
        - More sophisticated, calibrated to actual joint trip patterns
        - Can capture joint trips more flexibly than fixed thresholds

        # Phase 4: Clique Detection

        1. Build graph where nodes = trips, edges = similar trip pairs
        2. Detect maximal cliques (groups of mutually-similar trips)
        3. Handle overlapping cliques by selecting disjoint set with maximum coverage
        4. Each clique represents one joint trip event
        5. A clique needs every pair similar: A with B and B with C makes A,B,C
           one joint trip only if A is also similar to C

        # Phase 5: Joint Trip Aggregation

        1. Assign unique joint_trip_id to each clique
        2. Create joint_trips table, one row per joint trip:
            - joint_trip_id, hh_id, travel_date
            - num_joint_travelers: number of member linked trips
            - survey_complete: true only if every member trip is complete
        3. Mark each member linked trip with its joint_trip_id
        4. Validate against reported num_travelers field if available

    Notes:
        - Only compares trips within one household date (joint trips across
          households, or across the survey's day boundary, not detected)
        - Mahalanobis method requires calibrated covariance matrix (see
          scripts/calibrate_joint_trip_covariance.py)
        - Clique detection prevents false positives from partial or
          coincidental matches
        - Handles survey reporting errors where respondents over/under-report
          number of travelers
        - Non-joint trips retain joint_trip_id = NULL
    """
    linked_trips = tables["linked_trips"]
    households = tables["households"]
    person_days = tables["person_days"]
    logger.info(
        "Detecting joint trips using %s method (%d trips, %d households)",
        matching.method,
        len(linked_trips),
        len(households),
    )

    # Pre-filter to household with 2+ persons, each trip on its travel date
    candidate_trips = _filter_multi_person_households(linked_trips).join(
        person_days.select("person_day_id", "travel_date"), on="person_day_id", how="left"
    )

    # Create trip pairs and apply filters
    trip_pairs = _create_and_filter_trip_pairs(candidate_trips)

    # Compute pairwise distances
    trip_pairs = compute_pairwise_distances(trip_pairs)

    # Apply filtering based on method
    if matching.method == "buffer":
        filtered_pairs = apply_buffer_filter(
            trip_pairs,
            matching.space_threshold_meters,
            matching.time_threshold_minutes,
        )
    else:  # mahalanobis
        filtered_pairs = apply_mahalanobis_filter(
            trip_pairs,
            matching.covariance,
            matching.get_distance_threshold(),
        )

    # Detect joint trips using clique detection
    clique_assignments, flagged_conflicts = detect_disjoint_cliques(
        filtered_pairs,
        linked_trips.select("linked_trip_id").to_series(),
    )

    # Log flagged conflicts if any
    if len(flagged_conflicts) > 0:
        logger.info(
            "Flagged %d clique conflicts resolved by quality ranking",
            len(flagged_conflicts),
        )
        if matching.log_discrepancies:
            # Show first 5 flagged conflicts
            for i, clique in enumerate(flagged_conflicts[:5], 1):
                logger.debug("Flagged clique %d: trips %s", i, clique)

    # Convert clique_id to standardized joint_trip_id
    joint_trip_assignments = _assign_joint_trip_ids(clique_assignments, linked_trips)

    # Join assignments back to linked_trips
    linked_trips_with_joints = linked_trips.join(
        joint_trip_assignments, on="linked_trip_id", how="left"
    )

    joint_trips_table = build_joint_trips_table(linked_trips_with_joints, person_days)
    if joint_trips_table.is_empty():
        logger.warning(
            "No joint trips found: %s trips in multi-person households, "
            "%s pairs overlapping in time, 0 pairs matched by method=%s. "
            "If this survey should have joint travel, check the data or the matching thresholds.",
            f"{candidate_trips.height:,}",
            f"{trip_pairs.height:,}",
            matching.method,
        )

    # Compare against reported num_travelers (logging only)
    validate_against_num_travelers(linked_trips_with_joints, matching.log_discrepancies)

    logger.info(
        "Joint trip detection completed: %d joint trip groups detected",
        len(joint_trips_table),
    )

    return {
        "linked_trips": linked_trips_with_joints,
        "joint_trips": joint_trips_table,
    }
