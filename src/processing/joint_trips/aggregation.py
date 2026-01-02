"""Aggregation and validation helpers for joint trip detection.

This module builds the joint_trips table from detected cliques and validates
results against reported num_travelers field.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def build_joint_trips_table(
    linked_trips: pl.DataFrame,
    joint_trip_assignments: pl.DataFrame,
) -> pl.DataFrame:
    """Build joint_trips aggregation table from detected groups.

    Creates one row per unique joint_trip_id with aggregated attributes
    from member trips.

    Args:
        linked_trips: Original trips DataFrame with coordinates and times
        joint_trip_assignments: Output from detect_disjoint_cliques with
            linked_trip_id and joint_trip_id columns

    Returns:
        DataFrame with joint_trips schema (one row per joint_trip_id)
    """
    # Join assignments back to trips
    trips_with_joints = linked_trips.join(
        joint_trip_assignments,
        on="linked_trip_id",
        how="left",
        suffix="_assigned",
    )

    # Filter to only trips that are part of joint trips
    joint_members = trips_with_joints.filter(pl.col("joint_trip_id").is_not_null())

    if len(joint_members) == 0:
        # No joint trips detected, return empty table with proper schema
        return pl.DataFrame(
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

    # Aggregate by joint_trip_id
    joint_trips_table = joint_members.group_by("joint_trip_id").agg(
        [
            # Household and day (should be same within group)
            pl.col("hh_id").first(),
            pl.col("day_id").first(),
            # Count of travelers
            pl.col("linked_trip_id").n_unique().alias("num_joint_travelers"),
            # Mean coordinates
            pl.col("o_lat").mean().alias("o_lat_mean"),
            pl.col("o_lon").mean().alias("o_lon_mean"),
            pl.col("d_lat").mean().alias("d_lat_mean"),
            pl.col("d_lon").mean().alias("d_lon_mean"),
            # Mean time windows
            pl.col("depart_time").mean().alias("depart_time_mean"),
            pl.col("arrive_time").mean().alias("depart_arrive_mean"),
        ]
    )

    return joint_trips_table


def validate_against_num_travelers(
    linked_trips: pl.DataFrame,
    joint_trip_assignments: pl.DataFrame,
    log_discrepancies: bool,
) -> None:
    """Validate detected joint trips against reported num_travelers field.

    Compares detected joint trip group sizes with the num_travelers field
    reported in the survey. Logs INFO-level summary and optional DEBUG-level
    discrepancies.

    Args:
        linked_trips: Original trips DataFrame with num_travelers column
        joint_trip_assignments: Detected assignments with joint_trip_id
        log_discrepancies: If True, log DEBUG-level details for each mismatch
    """
    # Join assignments to get joint_trip_id on trips
    trips_with_joints = linked_trips.join(
        joint_trip_assignments,
        on="linked_trip_id",
        how="left",
        suffix="_assigned",
    )

    # Get detected joint trip sizes
    joint_sizes = (
        trips_with_joints.filter(pl.col("joint_trip_id").is_not_null())
        .group_by("joint_trip_id")
        .agg([pl.col("linked_trip_id").n_unique().alias("detected_size")])
    )

    # Join back to trips to compare with num_travelers
    trips_with_sizes = trips_with_joints.join(joint_sizes, on="joint_trip_id", how="left").filter(
        pl.col("joint_trip_id").is_not_null()
    )

    if len(trips_with_sizes) == 0:
        logger.info("No joint trips detected, no validation against num_travelers")
        return

    # Compare detected vs reported
    matches = trips_with_sizes.filter(pl.col("detected_size") == pl.col("num_travelers"))
    mismatches = trips_with_sizes.filter(pl.col("detected_size") != pl.col("num_travelers"))

    total = len(trips_with_sizes)
    num_matches = len(matches)
    match_rate = num_matches / total if total > 0 else 0

    logger.info(
        "Detected joint trips validation: %d/%d trips match reported num_travelers (%.1f%%)",
        num_matches,
        total,
        100 * match_rate,
    )

    if log_discrepancies and len(mismatches) > 0:
        logger.debug("Found %d trips with mismatched joint trip sizes:", len(mismatches))
        for row in mismatches.head(20).iter_rows(named=True):
            logger.debug(
                "  Trip %d (hh=%d, person=%d): detected=%d, reported=%d",
                row["linked_trip_id"],
                row["hh_id"],
                row["person_id"],
                row["detected_size"],
                row["num_travelers"],
            )
        if len(mismatches) > 20:  # noqa: PLR2004
            logger.debug("  ... and %d more discrepancies", len(mismatches) - 20)
