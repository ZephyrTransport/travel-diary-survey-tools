"""Joint trip detection step for identifying shared household trips.

This module implements the main pipeline step for detecting joint trips
where multiple household members travel together.
"""

import logging

import polars as pl

from pipeline.decoration import step

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


@step()
def detect_joint_trips(
    linked_trips: pl.DataFrame,
    households: pl.DataFrame,
    method: str = "buffer",
    time_threshold_minutes: float = 15.0,
    space_threshold_meters: float = 100.0,
    covariance: list[float] | list[list[float]] | None = None,
    confidence_level: float = 0.90,
    log_discrepancies: bool = False,
) -> dict[str, pl.DataFrame]:
    """Detect joint trips among household members using similarity matching.

    Identifies trips where multiple household members traveled together by
    comparing origin-destination-time similarity. Uses either strict buffer
    thresholds or Mahalanobis distance with configurable covariance.

    Args:
        linked_trips: DataFrame with trip coordinates and times
        households: DataFrame with household info (used for pre-filtering)
        method: Detection method ('buffer' or 'mahalanobis')
        time_threshold_minutes: Max time difference for buffer method
        space_threshold_meters: Max spatial distance for buffer method
        covariance: Diagonal (4 values) or full (4x4) covariance for
            Mahalanobis
        confidence_level: Confidence threshold for Mahalanobis (0-1).
            Higher = stricter. E.g., 0.90 is strict, 0.75 is moderate.
        log_discrepancies: If True, log DEBUG details when detected size
            differs from reported num_travelers

    Returns:
        Dictionary with updated linked_trips (with joint_trip_id) and
        new joint_trips table
    """
    # Validate and construct config
    config = JointTripConfig(
        method=method,
        time_threshold_minutes=time_threshold_minutes,
        space_threshold_meters=space_threshold_meters,
        covariance=covariance,
        confidence_level=confidence_level,
        log_discrepancies=log_discrepancies,
    )

    # Default covariance for Mahalanobis if not provided
    if config.method == "mahalanobis" and config.covariance is None:
        config.covariance = [7000, 7000, 20, 20]  # ~84m, ~84m, ~4.5min, ~4.5min
        logger.info("Using default diagonal covariance for Mahalanobis method")

    logger.info(
        "Detecting joint trips using %s method (%d trips, %d households)",
        config.method,
        len(linked_trips),
        len(households),
    )

    # Pre-filter to household with 2+ persons
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
        "Pre-filter: %d trips in multi-person households "
        "(%d single-person trips excluded)",
        len(candidate_trips),
        num_excluded,
    )

    if len(candidate_trips) == 0:
        logger.warning(
            "No multi-person households found. "
            "No joint trips possible, returning empty results."
        )
        linked_trips_with_joints = linked_trips.with_columns(
            [pl.lit(None, dtype=pl.Int64).alias("joint_trip_id")]
        )
        empty_joint_trips = pl.DataFrame(
            schema={
                "joint_trip_id": pl.Int64,
                "hh_id": pl.Int64,
                "day_id": pl.Int64,
                "num_joint_travelers": pl.Int64,
                "o_lat_mean": pl.Float64,
                "o_lon_mean": pl.Float64,
                "d_lat_mean": pl.Float64,
                "d_lon_mean": pl.Float64,
                "min_depart_time": pl.Datetime,
                "max_arrive_time": pl.Datetime,
            }
        )
        return {
            "linked_trips": linked_trips_with_joints,
            "joint_trips": empty_joint_trips,
        }

    # Self-join within household-day to create trip pairs
    trip_pairs = candidate_trips.join(
        candidate_trips.select(
            [
                "hh_id",
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
        on=["hh_id"],
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

    if len(trip_pairs) == 0:
        logger.warning(
            "No temporally overlapping trip pairs found. "
            "No joint trips detected, consider checking data quality."
        )
        linked_trips_with_joints = linked_trips.with_columns(
            [pl.lit(None, dtype=pl.Int64).alias("joint_trip_id")]
        )
        empty_joint_trips = pl.DataFrame(
            schema={
                "joint_trip_id": pl.Int64,
                "hh_id": pl.Int64,
                "day_id": pl.Int64,
                "num_joint_travelers": pl.Int64,
                "o_lat_mean": pl.Float64,
                "o_lon_mean": pl.Float64,
                "d_lat_mean": pl.Float64,
                "d_lon_mean": pl.Float64,
                "min_depart_time": pl.Datetime,
                "max_arrive_time": pl.Datetime,
            }
        )
        return {
            "linked_trips": linked_trips_with_joints,
            "joint_trips": empty_joint_trips,
        }

    # Compute pairwise distances
    trip_pairs = compute_pairwise_distances(trip_pairs)

    # Apply filtering based on method
    if config.method == "buffer":
        filtered_pairs = apply_buffer_filter(
            trip_pairs,
            config.space_threshold_meters,
            config.time_threshold_minutes,
        )
    else:  # mahalanobis
        filtered_pairs = apply_mahalanobis_filter(
            trip_pairs,
            config.covariance,
            config.distance_threshold,
        )

    if len(filtered_pairs) == 0:
        logger.warning(
            "No joint trips detected. Consider relaxing thresholds or "
            "checking data quality (GPS accuracy, time synchronization)."
        )
        linked_trips_with_joints = linked_trips.with_columns(
            [pl.lit(None, dtype=pl.Int64).alias("joint_trip_id")]
        )
        empty_joint_trips = pl.DataFrame(
            schema={
                "joint_trip_id": pl.Int64,
                "hh_id": pl.Int64,
                "day_id": pl.Int64,
                "num_joint_travelers": pl.Int64,
                "o_lat_mean": pl.Float64,
                "o_lon_mean": pl.Float64,
                "d_lat_mean": pl.Float64,
                "d_lon_mean": pl.Float64,
                "min_depart_time": pl.Datetime,
                "max_arrive_time": pl.Datetime,
            }
        )
        return {
            "linked_trips": linked_trips_with_joints,
            "joint_trips": empty_joint_trips,
        }

    # Detect joint trips using clique detection
    joint_trip_assignments, flagged_conflicts = detect_disjoint_cliques(
        filtered_pairs, linked_trips.select("linked_trip_id").to_series()
    )

    # Log flagged conflicts if any
    if len(flagged_conflicts) > 0:
        logger.info(
            "Flagged %d clique conflicts resolved by quality ranking",
            len(flagged_conflicts),
        )
        if log_discrepancies:
            # Show first 5 flagged conflicts
            for i, clique in enumerate(flagged_conflicts[:5], 1):
                logger.debug("Flagged clique %d: trips %s", i, clique)

    # Join assignments back to linked_trips
    linked_trips_with_joints = linked_trips.join(
        joint_trip_assignments, on="linked_trip_id", how="left"
    )

    # Build joint_trips aggregation table
    joint_trips_table = build_joint_trips_table(
        linked_trips_with_joints, joint_trip_assignments
    )

    # Validate against num_travelers (optional logging)
    validate_against_num_travelers(
        linked_trips_with_joints,
        joint_trip_assignments,
        config.log_discrepancies,
    )

    # Drop intermediate distance columns before returning
    cols_to_drop = {
        "origin_dist_m",
        "dest_dist_m",
        "depart_diff_min",
        "arrive_diff_min",
    }
    existing_cols_to_drop = [
        c for c in cols_to_drop if c in linked_trips_with_joints.columns
    ]
    if existing_cols_to_drop:
        linked_trips_with_joints = linked_trips_with_joints.drop(
            existing_cols_to_drop
        )

    logger.info(
        "Joint trip detection completed: %d joint trip groups detected",
        len(joint_trips_table),
    )

    return {
        "linked_trips": linked_trips_with_joints,
        "joint_trips": joint_trips_table,
    }
