"""Configuration models and calibration utilities for joint trip detection.

ALGORITHM DESIGN NOTES:
========================

This module implements joint trip detection using two methods:

1. BUFFER METHOD (debugging/calibration):
   Strict AND across four dimensions: origin distance, destination distance,
   departure time, arrival time. Uses Haversine for spatial matching and
   absolute time differences. All four must pass thresholds.

2. MAHALANOBIS METHOD (production):
   Statistical distance on 4D vector
   [Δorigin_m, Δdest_m, Δdepart_min, Δarrive_min].
   Supports diagonal covariance (assumes independence) or full covariance matrix
   (captures correlations). More robust to GPS noise via compensation.

CALIBRATION WORKFLOW:
---------------------
1. Run detection with method='buffer' to find high-confidence joint trips
2. Use estimate_covariance_from_detected_pairs() to analyze buffer results
3. Helper scans threshold range and recommends optimal distance_threshold
4. Copy diagonal or full covariance to config for production runs
5. Run detection with method='mahalanobis' using calibrated parameters

TEMPORAL OVERLAP:
-----------------
Trips must have overlapping time windows to be joint (travelers physically
together). Pre-filter: max(depart_A, depart_B) <= min(arrive_A, arrive_B)
"""

from typing import Literal

import numpy as np
import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator
from scipy import stats

from utils.helpers import expr_haversine as haversine_dist


class JointTripConfig(BaseModel):
    """Configuration for joint trip detection.

    Attributes:
        method: Detection method ('buffer' for strict thresholds,
            'mahalanobis' for statistical distance)
        time_threshold_minutes: Maximum time difference for buffer method
        space_threshold_meters: Maximum spatial distance for buffer method
        covariance: Diagonal variances (list of 4) or full covariance matrix
            (4x4 list of lists) in units [m^2, m^2, min^2, min^2].
            None defaults to [7000, 7000, 20, 20] based on empirical
            variance from BATS 2023: origin ~84m std (7142 m² var),
            dest ~84m (7089 m²), depart ~4.7min (22 min²),
            arrive ~4.6min (21 min²).
        confidence_level: Confidence threshold for joint trip detection
            (mahalanobis method only). Represents confidence that detected
            pairs are truly joint trips. E.g., 0.90 means 90% confidence
            (strict - fewer detections). Higher values = stricter matching.
        log_discrepancies: If True, log DEBUG-level details when detected
            joint trip size differs from reported num_travelers field
    """

    method: Literal["buffer", "mahalanobis"] = Field(
        default="buffer",
        description=(
            "Detection method: buffer (strict) or mahalanobis (statistical)"
        ),
    )

    time_threshold_minutes: float = Field(
        default=15.0,
        ge=0,
        description="Maximum time difference in minutes for buffer method",
    )

    space_threshold_meters: float = Field(
        default=100.0,
        ge=0,
        description="Maximum spatial distance in meters for buffer method",
    )

    covariance: list[float] | list[list[float]] | None = Field(
        default=None,
        description=(
            "Diagonal (4 values) or full (4x4) covariance matrix in units "
            "[origin_m^2, dest_m^2, depart_min^2, arrive_min^2]. "
            "None defaults to diagonal [7000, 7000, 20, 20] based on "
            "empirical variance from BATS 2023 household joint trips: "
            "origin ~84m std dev (7142 m² var), dest ~84m (7089 m²), "
            "depart ~4.7min (22 min²), arrive ~4.6min (21 min²)."
        ),
    )

    confidence_level: float = Field(
        default=0.90,
        gt=0,
        lt=1,
        description=(
            "Confidence threshold for joint trip detection "
            "(mahalanobis method only). Represents confidence that "
            "detected pairs are truly joint trips. E.g., 0.90 means "
            "90% confidence (strict matching). Higher = stricter."
        ),
    )

    log_discrepancies: bool = Field(
        default=False,
        description=(
            "If True, log DEBUG-level details when detected joint trip size "
            "differs from reported num_travelers"
        ),
    )

    @staticmethod
    def _validate_diagonal_covariance(v: list[float]) -> None:
        """Validate diagonal covariance vector."""
        if len(v) != 4:  # noqa: PLR2004
            msg = f"Diagonal covariance must have 4 values, got {len(v)}"
            raise ValueError(msg)
        if any(x <= 0 for x in v):
            msg = "All diagonal covariance values must be positive"
            raise ValueError(msg)

    @staticmethod
    def _validate_full_covariance(v: list[list[float]]) -> None:
        """Validate full covariance matrix."""
        if len(v) != 4:  # noqa: PLR2004
            msg = f"Full covariance must be 4x4, got {len(v)} rows"
            raise ValueError(msg)
        if any(len(row) != 4 for row in v):  # noqa: PLR2004
            msg = "Full covariance must be 4x4 (all rows length 4)"
            raise ValueError(msg)

        # Check symmetry
        for i in range(4):
            for j in range(i + 1, 4):
                if abs(v[i][j] - v[j][i]) > 1e-6:  # noqa: PLR2004
                    msg = "Covariance matrix must be symmetric"
                    raise ValueError(msg)

        # Check positive definiteness (all eigenvalues > 0)
        eigenvalues = np.linalg.eigvals(np.array(v))
        if any(eigenvalues <= 0):
            msg = (
                "Covariance matrix must be positive definite "
                "(all eigenvalues > 0)"
            )
            raise ValueError(msg)

    @field_validator("covariance")
    @classmethod
    def validate_covariance(
        cls, v: list[float] | list[list[float]] | None
    ) -> list[float] | list[list[float]] | None:
        """Validate covariance shape and values."""
        if v is None:
            return None

        if not isinstance(v, list) or len(v) == 0:
            msg = "Covariance must be a non-empty list"
            raise ValueError(msg)

        # Check if diagonal (1D list) or full (2D list)
        if isinstance(v[0], (int, float)):
            cls._validate_diagonal_covariance(v)
        elif isinstance(v[0], list):
            cls._validate_full_covariance(v)
        else:
            msg = "Covariance must be list of floats or list of lists"
            raise TypeError(msg)

        return v

    @model_validator(mode="after")
    def validate_method_params(self) -> "JointTripConfig":
        """Ensure required parameters are provided for each method."""
        if self.method == "mahalanobis" and self.covariance is None:
            msg = (
                "covariance must be provided for mahalanobis method. "
                "Use default [7000, 7000, 20, 20] or run calibration."
            )
            raise ValueError(msg)
        return self

    def get_distance_threshold(self) -> float:
        """Convert confidence level to chi-squared threshold.

        For 4D Mahalanobis distance (origin, dest, depart, arrive),
        the squared distance follows chi-squared distribution with df=4.

        Higher confidence_level → lower threshold → stricter matching.
        E.g., confidence_level=0.90 means accept only bottom 10%
        (very strict).

        Returns:
            Chi-squared threshold value for use in similarity filtering
        """
        if self.method != "mahalanobis":
            return 0.0  # Not used for buffer method
        # Invert confidence for chi-squared percentile
        # confidence=0.90 means accept only bottom 10% (strict)
        return float(stats.chi2.ppf(1 - self.confidence_level, df=4))

    class ConfigDict:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True


def _validate_covariance_inputs(
    joint_trips: pl.DataFrame, linked_trips: pl.DataFrame
) -> None:
    """Validate inputs for covariance estimation."""
    required_joint_cols = {"joint_trip_id", "hh_id", "day_id"}
    required_trip_cols = {
        "linked_trip_id",
        "hh_id",
        "day_id",
        "person_id",
        "o_lat",
        "o_lon",
        "d_lat",
        "d_lon",
        "depart_time",
        "arrive_time",
        "joint_trip_id",
    }

    missing_joint = required_joint_cols - set(joint_trips.columns)
    missing_trip = required_trip_cols - set(linked_trips.columns)

    if missing_joint:
        msg = (
            f"joint_trips missing required columns: {missing_joint}. "
            "Run detect_joint_trips with method='buffer' first."
        )
        raise ValueError(msg)

    if missing_trip:
        msg = f"linked_trips missing required columns: {missing_trip}"
        raise ValueError(msg)


def _compute_joint_trip_pairs(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Compute pairwise differences for joint trip members."""
    joint_trip_members = linked_trips.filter(
        pl.col("joint_trip_id").is_not_null()
    )

    if len(joint_trip_members) == 0:
        msg = (
            "No joint trips detected in buffer run. "
            "Cannot estimate covariance from empty set."
        )
        raise ValueError(msg)

    # Create all pairwise combinations within each joint_trip_id
    pairs = joint_trip_members.join(
        joint_trip_members.select(
            [
                "joint_trip_id",
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
        on="joint_trip_id",
        how="inner",
    ).filter(pl.col("linked_trip_id") < pl.col("linked_trip_id_b"))

    if len(pairs) == 0:
        msg = "No pairs found in detected joints (all single-trip groups?)"
        raise ValueError(msg)

    # Compute differences
    return pairs.with_columns(
        [
            haversine_dist(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("o_lat_b"),
                pl.col("o_lon_b"),
            ).alias("delta_origin_m"),
            haversine_dist(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("d_lat_b"),
                pl.col("d_lon_b"),
            ).alias("delta_dest_m"),
            (
                (pl.col("depart_time") - pl.col("depart_time_b"))
                .dt.total_minutes()
                .abs()
            ).alias("delta_depart_min"),
            (
                (pl.col("arrive_time") - pl.col("arrive_time_b"))
                .dt.total_minutes()
                .abs()
            ).alias("delta_arrive_min"),
        ]
    )


def estimate_covariance_from_detected_pairs(
    joint_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    threshold_range: list[float] | None = None,
) -> dict:
    """Estimate covariance from buffer-detected joint trip pairs.

    This calibration helper analyzes joint trips detected using the buffer
    method to compute covariance parameters for the Mahalanobis method.
    It also scans a range of distance thresholds to recommend optimal values.

    Workflow:
        1. Run detect_joint_trips with method='buffer'
        2. Pass output to this function
        3. Copy returned diagonal or full covariance to config
        4. Use recommended_threshold for distance_threshold
        5. Run detect_joint_trips with method='mahalanobis'

    Args:
        joint_trips: Output from detect_joint_trips (joint_trips table)
        linked_trips: Original linked_trips DataFrame with coordinates/times
        threshold_range: List of distance thresholds to scan for recommendation.
            Defaults to [1.5, 2.0, 2.5, 3.0, 3.5]

    Returns:
        Dictionary containing:
            - diagonal: List of 4 variances
              [origin_m^2, dest_m^2, depart_min^2, arrive_min^2]
            - full: 4x4 covariance matrix as list of lists
            - correlation: 4x4 correlation matrix for inspecting
              relationships
            - threshold_analysis: DataFrame with columns
              [threshold, detected_pairs, avg_clique_size,
              buffer_recall, marginal_increase]
            - recommended_threshold: Float, optimal threshold based
              on inflection

    Raises:
        ValueError: If joint_trips or linked_trips missing required columns
    """
    if threshold_range is None:
        threshold_range = [1.5, 2.0, 2.5, 3.0, 3.5]

    _validate_covariance_inputs(joint_trips, linked_trips)
    pairs = _compute_joint_trip_pairs(linked_trips)

    # Extract difference vectors as numpy array
    delta_cols = [
        "delta_origin_m",
        "delta_dest_m",
        "delta_depart_min",
        "delta_arrive_min",
    ]
    deltas = pairs.select(delta_cols).to_numpy()

    # Compute covariance and correlation
    cov_matrix = np.cov(deltas.T)
    diagonal_vars = np.diag(cov_matrix).tolist()

    # Correlation matrix
    std_devs = np.sqrt(np.diag(cov_matrix))
    corr_matrix = cov_matrix / np.outer(std_devs, std_devs)

    # Threshold scanning
    threshold_results = []
    num_buffer_pairs = len(pairs)

    for threshold in threshold_range:
        # Compute Mahalanobis distance for each pair
        cov_inv = np.linalg.inv(cov_matrix)
        mahal_distances = np.array(
            [np.sqrt(delta @ cov_inv @ delta) for delta in deltas]
        )

        # Count pairs below threshold
        detected = np.sum(mahal_distances < threshold)
        recall = detected / num_buffer_pairs

        # Average clique size (approximate)
        avg_clique_size = 2.0  # Pairs are always size 2

        threshold_results.append(
            {
                "threshold": threshold,
                "detected_pairs": int(detected),
                "buffer_recall": recall,
                "avg_clique_size": avg_clique_size,
            }
        )

    # Create analysis DataFrame
    threshold_df = pl.DataFrame(threshold_results)

    # Compute marginal increases
    threshold_df = threshold_df.with_columns(
        [
            (
                (pl.col("detected_pairs") - pl.col("detected_pairs").shift(1))
                / (pl.col("threshold") - pl.col("threshold").shift(1))
            ).alias("marginal_increase")
        ]
    )

    # Find recommended threshold (before spike)
    # Look for where marginal increase jumps >50%
    marginal_vals = threshold_df["marginal_increase"].to_list()[1:]
    recommended_idx = 0
    for i in range(len(marginal_vals) - 1):
        if marginal_vals[i] > 0 and marginal_vals[i + 1] > 0:
            pct_jump = (
                marginal_vals[i + 1] - marginal_vals[i]
            ) / marginal_vals[i]
            if pct_jump > 0.5:  # noqa: PLR2004
                recommended_idx = i
                break
    else:
        # No spike found, recommend threshold with 95% recall
        recall_vals = threshold_df["buffer_recall"].to_list()
        for i, r in enumerate(recall_vals):
            if r >= 0.95:  # noqa: PLR2004
                recommended_idx = i
                break

    recommended_threshold = threshold_range[recommended_idx]

    return {
        "diagonal": diagonal_vars,
        "full": cov_matrix.tolist(),
        "correlation": corr_matrix.tolist(),
        "threshold_analysis": threshold_df,
        "recommended_threshold": recommended_threshold,
    }
