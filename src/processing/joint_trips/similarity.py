"""Similarity computation helpers for joint trip detection.

This module provides functions to compute pairwise distances between trips
and apply filtering logic for both buffer and Mahalanobis methods.
"""

import logging

import numpy as np
import polars as pl

from utils.helpers import expr_haversine as haversine_dist

logger = logging.getLogger(__name__)


def compute_pairwise_distances(trip_pairs: pl.DataFrame) -> pl.DataFrame:
    """Compute spatial and temporal distances for trip pairs.

    Takes self-joined trip pairs and computes:
    - origin_dist_m: Haversine distance between origins (meters)
    - dest_dist_m: Haversine distance between destinations (meters)
    - depart_diff_min: Absolute difference in departure times (minutes)
    - arrive_diff_min: Absolute difference in arrival times (minutes)

    Args:
        trip_pairs: DataFrame with columns from self-join:
            - o_lat, o_lon, d_lat, d_lon (first trip)
            - o_lat_b, o_lon_b, d_lat_b, d_lon_b (second trip)
            - depart_time, arrive_time (first trip)
            - depart_time_b, arrive_time_b (second trip)

    Returns:
        Input DataFrame with four additional columns:
            origin_dist_m, dest_dist_m, depart_diff_min, arrive_diff_min
    """
    return trip_pairs.with_columns(
        [
            # Origin distance in meters
            haversine_dist(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("o_lat_b"),
                pl.col("o_lon_b"),
            ).alias("origin_dist_m"),
            # Destination distance in meters
            haversine_dist(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("d_lat_b"),
                pl.col("d_lon_b"),
            ).alias("dest_dist_m"),
            # Depart time difference in minutes (absolute)
            (
                (pl.col("depart_time") - pl.col("depart_time_b"))
                .dt.total_minutes()
                .abs()
            ).alias("depart_diff_min"),
            # Arrive time difference in minutes (absolute)
            (
                (pl.col("arrive_time") - pl.col("arrive_time_b"))
                .dt.total_minutes()
                .abs()
            ).alias("arrive_diff_min"),
        ]
    )


def apply_buffer_filter(
    trip_pairs: pl.DataFrame,
    space_threshold_meters: float,
    time_threshold_minutes: float,
) -> pl.DataFrame:
    """Apply strict buffer filtering to trip pairs.

    Uses strict AND logic: all four dimensions (origin, destination, depart,
    arrive) must pass their respective thresholds.

    Args:
        trip_pairs: DataFrame with distance columns from
            compute_pairwise_distances
        space_threshold_meters: Maximum spatial distance in meters
        time_threshold_minutes: Maximum time difference in minutes

    Returns:
        Filtered DataFrame containing only pairs passing all four thresholds
    """
    required_cols = {
        "origin_dist_m",
        "dest_dist_m",
        "depart_diff_min",
        "arrive_diff_min",
    }
    missing = required_cols - set(trip_pairs.columns)
    if missing:
        msg = (
            f"Missing required distance columns: {missing}. "
            "Run compute_pairwise_distances first."
        )
        raise ValueError(msg)

    filtered = trip_pairs.filter(
        (pl.col("origin_dist_m") < space_threshold_meters)
        & (pl.col("dest_dist_m") < space_threshold_meters)
        & (pl.col("depart_diff_min") < time_threshold_minutes)
        & (pl.col("arrive_diff_min") < time_threshold_minutes)
    )

    logger.debug(
        "Buffer filter: %d/%d pairs passed (%.1f%%)",
        len(filtered),
        len(trip_pairs),
        100 * len(filtered) / len(trip_pairs) if len(trip_pairs) > 0 else 0,
    )

    return filtered


def apply_mahalanobis_filter(
    trip_pairs: pl.DataFrame,
    covariance: list[float] | list[list[float]],
    distance_threshold: float,
) -> pl.DataFrame:
    """Apply Mahalanobis distance filtering to trip pairs.

    Computes statistical distance on 4D vector [origin_m, dest_m,
    depart_min, arrive_min] using provided covariance. Supports
    diagonal (list of 4) or full (4x4 matrix) covariance.

    Args:
        trip_pairs: DataFrame with distance columns from
            compute_pairwise_distances
        covariance: Diagonal variances (4 values) or full covariance
            (4x4)
        distance_threshold: Maximum Mahalanobis distance (chi-squared threshold)

    Returns:
        Filtered DataFrame containing only pairs below distance threshold

    Raises:
        ValueError: If covariance matrix is singular (condition number > 1000)
    """
    required_cols = {
        "origin_dist_m",
        "dest_dist_m",
        "depart_diff_min",
        "arrive_diff_min",
    }
    missing = required_cols - set(trip_pairs.columns)
    if missing:
        msg = (
            f"Missing required distance columns: {missing}. "
            "Run compute_pairwise_distances first."
        )
        raise ValueError(msg)

    # Convert covariance to numpy array
    if isinstance(covariance[0], (int, float)):
        # Diagonal covariance
        cov_matrix = np.diag(covariance)
    else:
        # Full covariance
        cov_matrix = np.array(covariance)

    # Check condition number
    condition_num = np.linalg.cond(cov_matrix)
    if condition_num > 1000:  # noqa: PLR2004
        msg = (
            f"Covariance matrix is near-singular "
            f"(condition number: {condition_num:.1f}). "
            "Consider using diagonal covariance instead of full matrix."
        )
        raise ValueError(msg)

    # Compute inverse
    cov_inv = np.linalg.inv(cov_matrix)

    # Extract difference vectors as numpy array
    deltas = trip_pairs.select(
        [
            "origin_dist_m",
            "dest_dist_m",
            "depart_diff_min",
            "arrive_diff_min",
        ]
    ).to_numpy()

    # Compute Mahalanobis distance for each pair
    # d = sqrt(delta^T * Sigma^-1 * delta)
    mahal_distances = np.array(
        [np.sqrt(delta @ cov_inv @ delta) for delta in deltas]
    )

    # Filter pairs below threshold
    mask = mahal_distances < distance_threshold
    filtered = trip_pairs.filter(pl.lit(mask))

    logger.debug(
        "Mahalanobis filter: %d/%d pairs passed (%.1f%%)",
        len(filtered),
        len(trip_pairs),
        100 * len(filtered) / len(trip_pairs) if len(trip_pairs) > 0 else 0,
    )

    return filtered
