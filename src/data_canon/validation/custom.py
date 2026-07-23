"""Custom validation checks for travel survey data.

This module contains DataFrame-level validation checks that run during the
custom validator phase (after row-level validation). Users can add checks here
and register them to tables using CUSTOM_VALIDATORS.

These checks are for more complex validation logic that requires multiple tables
or spanning multiple rows. Most checks can be done using built-in validators,
or at the row-level.

To add a new check:
1. Define a function that takes one or more DataFrames and returns list[str]
2. Add it to CUSTOM_VALIDATORS dict below, mapping table name to check functions
3. The check will automatically run when that table is validated
"""

import logging
from collections.abc import Callable

import polars as pl

from data_canon.codebook.tours import TourDataQuality
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)

# Registry of custom validators
# Format: {table_name: [check_function1, check_function2, ...]}
# Each check function should return list[str] of error messages
CUSTOM_VALIDATORS: dict[str, list[Callable]] = {
    "households": [],
    "persons": [],
    "days": [],
    "unlinked_trips": [],
    "linked_trips": [],
    "tours": [],
    "habitual_locations": [],
    "habitual_location_days": [],
}


# Example check functions below:
def check_for_teleports(unlinked_trips: pl.DataFrame) -> list[str]:
    """Check for when trip destination is too far from next trip origin."""
    errors = []
    max_distance = 1000  # Define threshold distance in meters

    # Compare o_lat/o_lon of the next trip to d_lat/d_lon of current trip
    # Compute distance, and compare to threshold over person_id and day_id
    teleports = (
        unlinked_trips.with_columns(
            pl.col("d_lat").alias("current_d_lat"),
            pl.col("d_lon").alias("current_d_lon"),
            pl.col("o_lat").shift(-1).over(["person_id", "day_id"]).alias("next_o_lat"),
            pl.col("o_lon").shift(-1).over(["person_id", "day_id"]).alias("next_o_lon"),
        )
        .with_columns(
            expr_haversine(
                pl.col("current_d_lat"),
                pl.col("current_d_lon"),
                pl.col("next_o_lat"),
                pl.col("next_o_lon"),
            ).alias("distance_meters")
        )
        .filter(pl.col("distance_meters") > max_distance)
        .select(
            pl.col("unlinked_trip_id"),
            pl.col("person_id"),
            pl.col("day_id"),
            pl.col("distance_meters"),
        )
    )

    if len(teleports) > 0:
        trip_ids = teleports["unlinked_trip_id"].to_list()[:5]
        errors.append(
            f"Found {len(teleports)} trips where destination "
            f"is more than {max_distance}m away from next trip origin. "
            f"Sample trip IDs: {trip_ids}"
        )
    return errors


def check_single_trip_tour_flag_consistency(
    tours: pl.DataFrame, linked_trips: pl.DataFrame
) -> list[str]:
    """Verify single_trip_tour flag matches actual trip count.

    This validates the business logic that sets the single_trip_tour flag.
    Tours with trip_count=1 should have single_trip_tour=True, and tours
    with trip_count>=2 should have single_trip_tour=False.

    Args:
        tours: Tour records with single_trip_tour flag
        linked_trips: Trip records to count per tour

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []

    # Count trips per tour
    trip_counts = linked_trips.group_by("tour_id").agg(pl.len().alias("actual_trip_count"))

    # Join with tours and check consistency
    inconsistent = tours.join(trip_counts, on="tour_id", how="left").filter(
        # Flag says single-trip but has multiple trips
        (pl.col("single_trip_tour") & (pl.col("actual_trip_count") != 1))
        # Flag says multi-trip but has only one trip
        | (~pl.col("single_trip_tour") & (pl.col("actual_trip_count") == 1))
    )

    if len(inconsistent) > 0:
        tour_ids = inconsistent["tour_id"].to_list()[:5]
        errors.append(
            f"Found {len(inconsistent)} tours where single_trip_tour flag "
            f"doesn't match actual trip count. Sample tour IDs: {tour_ids}"
        )

    return errors


def check_valid_tours_are_complete(tours: pl.DataFrame) -> list[str]:
    """Verify tours flagged VALID are genuinely complete.

    A tour marked ``tour_data_quality == VALID`` must not be a single-trip tour
    and must have a non-null ``tour_purpose``. This guards the downstream
    formatters, which keep only VALID tours: a tour mislabeled VALID but missing
    its purpose would survive that filter and leak into the model output (in
    CT-RAMP, silently coerced to the OTHDISCR catch-all purpose).

    ``TourModel.validate_complete_tours`` enforces the converse (non-single-trip
    tours have a purpose); this check keys on ``tour_data_quality`` instead, so
    the two are complementary rather than redundant.

    Args:
        tours: Tour records with tour_data_quality, single_trip_tour, tour_purpose

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []

    # tour_data_quality is produced during extraction; skip if absent
    # (e.g. schema-only empty frames).
    if "tour_data_quality" not in tours.columns:
        return errors

    invalid = tours.filter(
        (pl.col("tour_data_quality") == TourDataQuality.VALID.value)
        & (pl.col("single_trip_tour") | pl.col("tour_purpose").is_null())
    )

    if len(invalid) > 0:
        tour_ids = invalid["tour_id"].to_list()[:5]
        errors.append(
            f"Found {len(invalid)} tours flagged VALID that are single-trip "
            f"or have a null tour_purpose. Sample tour IDs: {tour_ids}"
        )

    return errors


# Distance (meters) above which a junction between consecutive trips is a gap.
# Matches the tour extractor's default spatial_gap_threshold_meters so this
# tripwire and the SPATIAL_GAP drop-flag agree on what counts as discontinuous.
_SPATIAL_GAP_THRESHOLD_M = 1000.0

# Fraction of junctions that may be discontinuous before this is treated as a
# systemic failure rather than normal survey noise. Real BATS-2023 runs sit near
# ~1% at 1 km; this ceiling only trips on a broken coordinate system (e.g.
# swapped lat/lon making every trip teleport), which is what we want to catch.
_SPATIAL_GAP_MAX_RATE = 0.15

# Minimum junctions before the rate ceiling is enforced. A rate over a handful of
# junctions is noise (one genuine gap in a 4-trip fixture is already 25%), so
# below this the check only reports and never fails. A systemic break shows a
# high rate at any scale, so real full-survey runs still trip the ceiling.
_SPATIAL_GAP_MIN_JUNCTIONS = 1000


def check_trip_spatial_continuity(linked_trips: pl.DataFrame) -> list[str]:
    """Report spatially discontinuous trip junctions (missing legs / teleports).

    Within a person-day, one trip's destination should be at (or near) the next
    trip's origin. A larger jump means the diary skipped a connecting leg. The
    tour extractor already flags the affected tour ``SPATIAL_GAP`` so formatters
    drop it; this is the upstream tripwire.

    Discontinuities are inherent survey noise (~1% of junctions), so this check
    does NOT fail on their mere presence - it logs the observed count and rate,
    and only raises when the rate exceeds ``_SPATIAL_GAP_MAX_RATE``, which
    signals a systemic problem (e.g. a swapped/rescaled coordinate column) rather
    than ordinary respondent gaps.

    Args:
        linked_trips: Linked trips with person_id, day_id, depart_time and
            o/d coordinates

    Returns:
        List of error messages (empty unless the discontinuity rate is
        implausibly high)
    """
    errors: list[str] = []

    required = {"person_id", "day_id", "depart_time", "o_lat", "o_lon", "d_lat", "d_lon"}
    if not required.issubset(linked_trips.columns):
        return errors

    junctions = (
        linked_trips.sort(["person_id", "day_id", "depart_time"])
        .with_columns(
            pl.col("o_lat").shift(-1).over(["person_id", "day_id"]).alias("_next_o_lat"),
            pl.col("o_lon").shift(-1).over(["person_id", "day_id"]).alias("_next_o_lon"),
        )
        # Keep only real junctions (a following trip exists in the same day).
        .filter(pl.col("_next_o_lat").is_not_null())
        .with_columns(
            expr_haversine(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("_next_o_lat"),
                pl.col("_next_o_lon"),
            ).alias("_gap_m")
        )
    )

    n_junctions = len(junctions)
    if n_junctions == 0:
        return errors

    discontinuous = junctions.filter(pl.col("_gap_m") > _SPATIAL_GAP_THRESHOLD_M)
    n_gaps = len(discontinuous)
    rate = n_gaps / n_junctions

    logger.info(
        "Trip spatial continuity: %d of %d junctions (%.2f%%) exceed %.0fm.",
        n_gaps,
        n_junctions,
        rate * 100,
        _SPATIAL_GAP_THRESHOLD_M,
    )

    if n_junctions >= _SPATIAL_GAP_MIN_JUNCTIONS and rate > _SPATIAL_GAP_MAX_RATE:
        sample = discontinuous["linked_trip_id"].to_list()[:5]
        errors.append(
            f"Spatial discontinuity rate {rate:.1%} exceeds the {_SPATIAL_GAP_MAX_RATE:.0%} "
            f"ceiling ({n_gaps} of {n_junctions} junctions jump more than "
            f"{_SPATIAL_GAP_THRESHOLD_M:.0f}m). This likely indicates a systemic "
            f"coordinate problem rather than ordinary survey gaps. "
            f"Sample linked_trip_ids: {sample}"
        )

    return errors


# Register the tour validators
CUSTOM_VALIDATORS["tours"].append(check_single_trip_tour_flag_consistency)
CUSTOM_VALIDATORS["tours"].append(check_valid_tours_are_complete)

# Register the linked-trip validators
CUSTOM_VALIDATORS["linked_trips"].append(check_trip_spatial_continuity)
