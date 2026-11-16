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

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.tours import (
    TourCategory,
    TourDataQuality,
)
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)

# Junctions between consecutive trips farther apart than this are gaps; matches
# the tour extractor's default spatial_gap_threshold_meters.
_SPATIAL_GAP_THRESHOLD_M = 1000.0
# Gap rate above which the coordinates are taken as broken, not merely noisy.
_SPATIAL_GAP_MAX_RATE = 0.15
# Below this many junctions the rate is noise, so the check only reports.
_SPATIAL_GAP_MIN_JUNCTIONS = 1000

# How far apart two copies of one reported place may be (about 0.1 m).
_SAME_PLACE_DEGREES = 1e-6
# The coordinate columns each reported primary location repeats:
# (location type, table, lat column, lon column).
_COORDINATE_COLUMNS = [
    (LocationType.HOME, "households", "home_lat", "home_lon"),
    (LocationType.WORK, "persons", "work_lat", "work_lon"),
    (LocationType.SCHOOL, "persons", "school_lat", "school_lon"),
]

# Registry of custom validators
# Format: {table_name: [check_function1, check_function2, ...]}
# Each check function should return list[str] of error messages
CUSTOM_VALIDATORS: dict[str, list[Callable]] = {
    "households": [],
    "persons": [],
    "person_days": [],
    "unlinked_trips": [],
    "linked_trips": [],
    "tours": [],
    "habitual_locations": [],
    "habitual_location_days": [],
}


def check_trip_count_matches_quality(tours: pl.DataFrame, linked_trips: pl.DataFrame) -> list[str]:
    """Verify trip_count matches the trips, and NO_DESTINATION means what it says.

    ``trip_count`` is checked against the linked trips rather than against
    another tour column: both are derived in the extractor, so they would agree
    even if the extractor were wrong. Only the trips themselves are independent.

    ``NO_DESTINATION`` is checked in both directions against its actual
    definition: a *closed* tour that aggregation found no purpose for. A partial
    tour may also lack a purpose -- half of it went unobserved -- and is graded
    on its open end instead, so a null purpose alone does not imply the code.
    A closed tour without a purpose may also be ``OTHER_HOME``: a move between
    two of the person's homes, where the move is the reason there is no stop.

    Args:
        tours: Tour records with trip_count, tour_data_quality and tour_purpose
        linked_trips: Trip records to count per tour

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []
    if "tour_data_quality" not in tours.columns or "trip_count" not in tours.columns:
        return errors

    actual = linked_trips.group_by("tour_id").agg(pl.len().alias("actual_trip_count"))
    joined = tours.join(actual, on="tour_id", how="left")

    miscounted = joined.filter(pl.col("trip_count") != pl.col("actual_trip_count"))
    if len(miscounted) > 0:
        tour_ids = miscounted["tour_id"].to_list()[:5]
        errors.append(
            f"Found {len(miscounted)} tours whose trip_count does not match the "
            f"number of linked trips. Sample tour IDs: {tour_ids}"
        )

    if {"tour_purpose", "tour_category"} <= set(tours.columns):
        quality = pl.col("tour_data_quality")
        no_dest = quality == TourDataQuality.NO_DESTINATION.value
        moved_home = quality == TourDataQuality.OTHER_HOME.value
        nothing_to_anchor_on = pl.col("tour_purpose").is_null() & (
            pl.col("tour_category") == TourCategory.COMPLETE.value
        )
        drifted = joined.filter(
            (no_dest & ~nothing_to_anchor_on) | (nothing_to_anchor_on & ~no_dest & ~moved_home)
        )
        if len(drifted) > 0:
            tour_ids = drifted["tour_id"].to_list()[:5]
            errors.append(
                f"Found {len(drifted)} tours where NO_DESTINATION disagrees with "
                f"being a closed tour without a purpose (other than a move between "
                f"homes, OTHER_HOME). Sample tour IDs: {tour_ids}"
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
        tours: Tour records with tour_data_quality, trip_count, tour_purpose

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []

    # tour_data_quality is produced during extraction; skip if absent
    # (e.g. schema-only empty frames).
    if "tour_data_quality" not in tours.columns:
        return errors

    single_trip = pl.col("trip_count") == 1
    invalid = tours.filter(
        (pl.col("tour_data_quality") == TourDataQuality.VALID.value)
        & (single_trip | pl.col("tour_purpose").is_null())
    )

    if len(invalid) > 0:
        tour_ids = invalid["tour_id"].to_list()[:5]
        errors.append(
            f"Found {len(invalid)} tours flagged VALID that are single-trip "
            f"or have a null tour_purpose. Sample tour IDs: {tour_ids}"
        )

    return errors


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

    Below ``_SPATIAL_GAP_MIN_JUNCTIONS`` the rate is not enforced: one genuine
    gap in a 4-trip fixture is already 25%. A systemic break shows a high rate
    at any scale, so full-survey runs still trip the ceiling.

    Args:
        linked_trips: Linked trips with person_id, person_day_id, depart_time and
            o/d coordinates

    Returns:
        List of error messages (empty unless the discontinuity rate is
        implausibly high)
    """
    errors: list[str] = []

    required = {"person_id", "person_day_id", "depart_time", "o_lat", "o_lon", "d_lat", "d_lon"}
    if not required.issubset(linked_trips.columns):
        return errors

    junctions = (
        linked_trips.sort(["person_id", "person_day_id", "depart_time"])
        .with_columns(
            pl.col("o_lat").shift(-1).over(["person_id", "person_day_id"]).alias("_next_o_lat"),
            pl.col("o_lon").shift(-1).over(["person_id", "person_day_id"]).alias("_next_o_lon"),
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


def check_reported_locations_match_coordinates(
    habitual_locations: pl.DataFrame,
    households: pl.DataFrame,
    persons: pl.DataFrame,
) -> list[str]:
    """Check the reported primary locations and the coordinate columns say the same.

    Each reported primary home, workplace and school repeats a pair of
    coordinate columns (``home_lat/lon`` on households, ``work_*``/``school_*``
    on persons) that other steps still read. Every such pair must have its row,
    and every row its pair, within ``_SAME_PLACE_DEGREES``, so the two can never
    drift apart. The copies normally agree exactly; the tolerance absorbs a float
    round-trip (single precision, a CSV written and read back) without admitting
    a genuinely different place.

    Args:
        habitual_locations: Habitual locations.
        households: Households with ``hh_id`` and the home coordinates.
        persons: Persons with ``person_id``, ``hh_id`` and work/school coordinates.

    Returns:
        One message per location type that disagrees.
    """
    errors: list[str] = []
    tables = {
        "households": persons.select("person_id", "hh_id").join(households, on="hh_id"),
        "persons": persons,
    }
    primaries = habitual_locations.filter(
        (pl.col("source") == LocationSource.REPORTED.value) & pl.col("is_primary")
    )
    for kind, table, lat, lon in _COORDINATE_COLUMNS:
        if not {lat, lon} <= set(tables[table].columns):
            continue
        reported = tables[table].filter(pl.col(lat).is_not_null() & pl.col(lon).is_not_null())
        reported = reported.select(
            "person_id",
            pl.col(lat).cast(pl.Float64).alias("_a_lat"),
            pl.col(lon).cast(pl.Float64).alias("_a_lon"),
        )
        rows = primaries.filter(pl.col("location_type") == kind.value).select(
            "person_id",
            pl.col("lat").cast(pl.Float64).alias("_r_lat"),
            pl.col("lon").cast(pl.Float64).alias("_r_lon"),
        )
        joined = reported.join(rows, on="person_id", how="full", coalesce=True)
        differ = joined.filter(
            pl.col("_a_lat").is_null()
            | pl.col("_r_lat").is_null()
            | ((pl.col("_a_lat") - pl.col("_r_lat")).abs() > _SAME_PLACE_DEGREES)
            | ((pl.col("_a_lon") - pl.col("_r_lon")).abs() > _SAME_PLACE_DEGREES)
        )
        if differ.height > 0:
            errors.append(
                f"{differ.height} person(s) have a reported primary {kind.name} location "
                f"that does not match {table}.{lat}/{lon} (sample person_ids: "
                f"{differ['person_id'].to_list()[:5]})"
            )
    return errors


# Register the tour validators
CUSTOM_VALIDATORS["tours"].append(check_trip_count_matches_quality)
CUSTOM_VALIDATORS["tours"].append(check_valid_tours_are_complete)

# Register the linked-trip validators
CUSTOM_VALIDATORS["linked_trips"].append(check_trip_spatial_continuity)

# Register the habitual-location validators
CUSTOM_VALIDATORS["habitual_locations"].append(check_reported_locations_match_coordinates)
