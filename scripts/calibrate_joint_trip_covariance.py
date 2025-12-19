"""One-off script to calibrate Mahalanobis covariance for joint trip detection.

This script analyzes actual trip data to estimate appropriate covariance values
by examining variance in distances/times between trips from the same household.

Usage:
    uv run python scripts/calibrate_joint_trip_covariance.py
"""

from pathlib import Path

import numpy as np
import polars as pl

from pipeline.pipeline import Pipeline
from processing.joint_trips.detect_joint_trips import detect_joint_trips
from processing.joint_trips.similarity import compute_pairwise_distances

# Constants for candidate filtering
MIN_PERSONS_PER_HH = 2
MAX_SPACE_THRESHOLD_M = 500
MAX_TIME_THRESHOLD_MIN = 30
PERCENTILE_95 = 95
CONSERVATIVE_DIVISOR = 2


def find_candidate_joint_trips(trips: pl.DataFrame) -> pl.DataFrame:
    """Find likely joint trip pairs using loose criteria."""
    # Filter to multi-person households
    persons_per_hh = trips.group_by("hh_id").agg(
        pl.col("person_id").n_unique().alias("n_persons")
    )
    multi_person_hhs = persons_per_hh.filter(
        pl.col("n_persons") >= MIN_PERSONS_PER_HH
    )

    candidate_trips = trips.join(
        multi_person_hhs.select("hh_id"), on="hh_id", how="inner"
    )

    # Self-join within households to create pairs
    trip_pairs = candidate_trips.join(
        candidate_trips, on="hh_id", suffix="_b"
    ).filter(pl.col("person_id") < pl.col("person_id_b"))

    # Compute distances
    trip_pairs = compute_pairwise_distances(trip_pairs)

    # Apply loose filters to find likely joint trips
    likely_joint = trip_pairs.filter(
        (pl.col("origin_dist_m") <= MAX_SPACE_THRESHOLD_M)
        & (pl.col("dest_dist_m") <= MAX_SPACE_THRESHOLD_M)
        & (pl.col("depart_diff_min") <= MAX_TIME_THRESHOLD_MIN)
        & (pl.col("arrive_diff_min") <= MAX_TIME_THRESHOLD_MIN)
    )

    print(f"Found {len(likely_joint):,} likely joint trip pairs")
    return likely_joint


def analyze_variance(likely_joint: pl.DataFrame) -> dict:
    """Compute variance statistics for each dimension."""
    # Get the distance/time differences
    origin_dists = likely_joint["origin_dist_m"].to_numpy()
    dest_dists = likely_joint["dest_dist_m"].to_numpy()
    depart_diffs = likely_joint["depart_diff_min"].to_numpy()
    arrive_diffs = likely_joint["arrive_diff_min"].to_numpy()

    # Compute statistics
    return {
        "origin": {
            "mean": float(np.mean(origin_dists)),
            "std": float(np.std(origin_dists)),
            "var": float(np.var(origin_dists)),
            "median": float(np.median(origin_dists)),
            "p95": float(np.percentile(origin_dists, PERCENTILE_95)),
        },
        "dest": {
            "mean": float(np.mean(dest_dists)),
            "std": float(np.std(dest_dists)),
            "var": float(np.var(dest_dists)),
            "median": float(np.median(dest_dists)),
            "p95": float(np.percentile(dest_dists, PERCENTILE_95)),
        },
        "depart": {
            "mean": float(np.mean(depart_diffs)),
            "std": float(np.std(depart_diffs)),
            "var": float(np.var(depart_diffs)),
            "median": float(np.median(depart_diffs)),
            "p95": float(np.percentile(depart_diffs, PERCENTILE_95)),
        },
        "arrive": {
            "mean": float(np.mean(arrive_diffs)),
            "std": float(np.std(arrive_diffs)),
            "var": float(np.var(arrive_diffs)),
            "median": float(np.median(arrive_diffs)),
            "p95": float(np.percentile(arrive_diffs, PERCENTILE_95)),
        },
    }


def _print_dimension_stats(name: str, stats: dict, unit: str) -> None:
    """Print statistics for one dimension."""
    print(f"\n{name}:")
    print(f"  Mean:     {stats['mean']:>8.1f} {unit}")
    print(f"  Std Dev:  {stats['std']:>8.1f} {unit}")
    print(f"  Variance: {stats['var']:>8.1f} {unit}²")
    print(f"  Median:   {stats['median']:>8.1f} {unit}")
    print(f"  95th %:   {stats['p95']:>8.1f} {unit}")


def _print_header(n_pairs: int) -> None:
    """Print the analysis header."""
    print("\n" + "=" * 70)
    print("JOINT TRIP COVARIANCE CALIBRATION ANALYSIS")
    print("=" * 70)
    print(f"\nAnalyzed {n_pairs:,} likely joint trip pairs")
    print(
        "\nVariance Analysis "
        "(for trips from same household within loose thresholds):"
    )
    print("-" * 70)


def _print_recommendations(stats: dict) -> None:
    """Print recommended covariance values."""
    print("\n" + "=" * 70)
    print("RECOMMENDED COVARIANCE DIAGONAL VALUES")
    print("=" * 70)

    # Calculate recommended values based on variance
    origin_var = stats["origin"]["var"]
    dest_var = stats["dest"]["var"]
    depart_var = stats["depart"]["var"]
    arrive_var = stats["arrive"]["var"]

    print("\nBased on observed variance in likely joint trips:")
    print(
        f"  covariance: [{origin_var:.0f}, {dest_var:.0f}, "
        f"{depart_var:.0f}, {arrive_var:.0f}]"
    )

    # Also provide conservative estimates (using 95th percentile as ~2 std devs)
    origin_var_conservative = (
        stats["origin"]["p95"] / CONSERVATIVE_DIVISOR
    ) ** 2
    dest_var_conservative = (stats["dest"]["p95"] / CONSERVATIVE_DIVISOR) ** 2
    depart_var_conservative = (
        stats["depart"]["p95"] / CONSERVATIVE_DIVISOR
    ) ** 2
    arrive_var_conservative = (
        stats["arrive"]["p95"] / CONSERVATIVE_DIVISOR
    ) ** 2

    print("\nConservative estimate (based on 95th percentile):")
    print(
        f"  covariance: [{origin_var_conservative:.0f}, "
        f"{dest_var_conservative:.0f}, {depart_var_conservative:.0f}, "
        f"{arrive_var_conservative:.0f}]"
    )

    print("\nCurrent default in code:")
    print("  covariance: [7000, 7000, 20, 20]")
    print("  (std devs: ~84m, ~84m, ~4.5min, ~4.5min)")


def _print_interpretation() -> None:
    """Print interpretation guidance."""
    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)
    print(
        """
The covariance diagonal represents the variance (std_dev²) of measurement
"noise" or acceptable differences between joint trips in each dimension:
  [origin_var_m², dest_var_m², depart_var_min², arrive_var_min²]

- Use the OBSERVED variance if you want to match the typical spread in
  your actual data (more detections)

- Use the CONSERVATIVE estimate if you want stricter matching that only
  catches trips very close in all dimensions (fewer detections)

- Adjust confidence_level (0-1) to control strictness:
  * 0.90 = very strict (only catch obvious joint trips)
  * 0.75 = moderate (recommended starting point)
  * 0.50 = permissive (catch more potential joint trips)
"""
    )


def print_analysis(stats: dict, n_pairs: int) -> None:
    """Print analysis results and recommendations."""
    _print_header(n_pairs)
    _print_dimension_stats("ORIGIN DISTANCE (meters)", stats["origin"], "m")
    _print_dimension_stats("DESTINATION DISTANCE (meters)", stats["dest"], "m")
    _print_dimension_stats(
        "DEPARTURE TIME DIFFERENCE (minutes)", stats["depart"], "min"
    )
    _print_dimension_stats(
        "ARRIVAL TIME DIFFERENCE (minutes)", stats["arrive"], "min"
    )
    _print_recommendations(stats)
    _print_interpretation()


if __name__ == "__main__":
    # Project configuration
    project_path = Path(__file__).parent.parent / "projects" / "bats_2023"
    config_path = project_path / "config.yaml"
    cache_dir = Path(__file__).parent.parent / ".cache"

    # Initialize pipeline and load cached data
    pipeline = Pipeline(config_path=str(config_path), caching=str(cache_dir))
    linked_trips = pipeline.get_data("linked_trips", step="link_trips")
    households = pipeline.get_data("households")

    print(
        "Loaded %s trips, %s households",
        f"{len(linked_trips):,}",
        f"{len(households):,}",
    )

    # Run joint trip detection with buffer method
    print("Running joint trip detection with buffer method...")
    result = detect_joint_trips(
        linked_trips=linked_trips,
        households=households,
        method="buffer",
        time_threshold_minutes=float(MAX_TIME_THRESHOLD_MIN),
        space_threshold_meters=float(MAX_SPACE_THRESHOLD_M),
    )

    n_joint_trips = len(result["joint_trips"])
    n_trips_in_joint = (
        result["linked_trips"]
        .filter(pl.col("joint_trip_id").is_not_null())
        .height
    )

    print(
        "Detected %s joint trips involving %s trips",
        f"{n_joint_trips:,}",
        f"{n_trips_in_joint:,}",
    )

    # Analyze variance in candidate pairs
    likely_joint = find_candidate_joint_trips(linked_trips)
    stats = analyze_variance(likely_joint)

    # Print analysis
    print_analysis(stats, len(likely_joint))

    # Save outputs
    likely_joint.write_parquet(project_path / "joint_trip_candidates.parquet")
    result["joint_trips"].write_parquet(
        project_path / "detected_joint_trips.parquet"
    )
    print("Saved results to project directory")
