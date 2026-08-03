"""Tour validation and correction helper functions.

This module contains functions for:
- Validating tour data quality
- Identifying problematic tours (tour_num=0, single-trip, missing anchor)
- Correcting tour_category for definitive error cases
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)

# Coordinate columns required to detect spatial gaps between consecutive trips.
_GAP_COORD_COLS = ("o_lat", "o_lon", "d_lat", "d_lon", "depart_time")


def _spatial_gap_flags(
    linked_trips: pl.DataFrame,
    threshold_meters: float,
) -> pl.DataFrame:
    """Flag tours that contain an internal spatial gap (a missing leg).

    Within each tour, trips are ordered by departure time and the haversine
    distance from one trip's destination to the next trip's origin is measured.
    A gap larger than ``threshold_meters`` means the diary skipped a connecting
    trip: the person was at one place and the next recorded trip begins
    elsewhere. Left alone, ``identify_home_based_tours`` welds such trips into a
    single tour (boundary detection keys only on home touches, never on spatial
    continuity), so a home-to-home tour can silently teleport mid-tour and still
    read as COMPLETE/VALID. Flagging lets the formatters drop it as a unit.

    Returns one row per tour_id with a ``_has_spatial_gap`` boolean. If the
    coordinate columns are absent (e.g. schema-only frames or unit tests without
    coordinates), every tour is reported as gap-free.

    Args:
        linked_trips: Linked trips with tour_id and o/d coordinates
        threshold_meters: Gap distance above which a junction is discontinuous

    Returns:
        DataFrame with columns [tour_id, _has_spatial_gap]
    """
    if any(c not in linked_trips.columns for c in _GAP_COORD_COLS):
        return (
            linked_trips.select("tour_id")
            .unique()
            .with_columns(pl.lit(value=False).alias("_has_spatial_gap"))
        )

    ordered = linked_trips.sort(["tour_id", "depart_time"]).with_columns(
        [
            pl.col("d_lat").shift(1).over("tour_id").alias("_prev_d_lat"),
            pl.col("d_lon").shift(1).over("tour_id").alias("_prev_d_lon"),
        ]
    )
    ordered = ordered.with_columns(
        expr_haversine(
            pl.col("_prev_d_lat"),
            pl.col("_prev_d_lon"),
            pl.col("o_lat"),
            pl.col("o_lon"),
        ).alias("_junction_gap_m")
    )
    return ordered.group_by("tour_id").agg(
        # any() ignores nulls, so a tour's first trip (no previous dest) and
        # single-trip tours (no internal junction) never trigger the flag.
        (pl.col("_junction_gap_m") > threshold_meters).any().alias("_has_spatial_gap")
    )


def _diagnose_problem_tours(
    tours: pl.DataFrame,
    zero_tour_trips: pl.DataFrame,
) -> None:
    """Log detailed diagnostics for problematic tours.

    Args:
        tours: Tours DataFrame with tour_data_quality assigned
        zero_tour_trips: Subset of trips with tour_num=0
    """
    # Diagnostics for INDETERMINATE tours
    indeterminate_tours = tours.filter(pl.col("tour_data_quality") == TourDataQuality.INDETERMINATE)
    if len(indeterminate_tours) > 0:
        logger.warning(
            "Diagnosing %d INDETERMINATE tours...",
            len(indeterminate_tours),
        )

        # Analyze by anchor pattern
        home_pattern = (
            indeterminate_tours.group_by(["_has_anchor_origin", "_has_anchor_dest"])
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        logger.warning("INDETERMINATE tours by anchor pattern:")
        for row in home_pattern.iter_rows(named=True):
            origin = "anchor" if row["_has_anchor_origin"] else "away"
            dest = "anchor" if row["_has_anchor_dest"] else "away"
            logger.warning(
                "  Origin=%s, Dest=%s: %d tours",
                origin,
                dest,
                row["count"],
            )

        # Analyze by trip count
        trip_count_dist = (
            indeterminate_tours.group_by("trip_count")
            .agg(pl.len().alias("count"))
            .sort("trip_count")
        )
        logger.warning("INDETERMINATE tours by trip count:")
        for row in trip_count_dist.iter_rows(named=True):
            logger.warning("  %d trips: %d tours", row["trip_count"], row["count"])

        # Sample details
        sample_tours = indeterminate_tours.head(5)
        sample_tour_ids = sample_tours["tour_id"].to_list()

        logger.warning("Sample INDETERMINATE tour details (first 5):")
        for tour_id in sample_tour_ids:
            tour_info = indeterminate_tours.filter(pl.col("tour_id") == tour_id).row(0, named=True)
            logger.warning(
                "  Tour %s: person=%d, day=%d, trips=%d, category=%s, "
                "anchor_origin=%s, anchor_dest=%s",
                tour_id,
                tour_info["person_id"],
                tour_info["day_id"],
                tour_info["trip_count"],
                TourCategory(tour_info["tour_category"]).label,
                tour_info["_has_anchor_origin"],
                tour_info["_has_anchor_dest"],
            )

            # Show constituent trips
            tour_trips = zero_tour_trips.filter(pl.col("tour_id") == tour_id)
            logger.warning("    Trips in tour %s:", tour_id)
            for trip_row in tour_trips.select(
                ["linked_trip_id", "depart_time", "_o_is_home", "_d_is_home"]
            ).iter_rows(named=True):
                is_loop = trip_row["_o_is_home"] and trip_row["_d_is_home"]
                loop_flag = " [LOOP]" if is_loop else ""
                logger.warning(
                    "      Trip %d: depart=%s, o_home=%s, d_home=%s%s",
                    trip_row["linked_trip_id"],
                    trip_row["depart_time"],
                    trip_row["_o_is_home"],
                    trip_row["_d_is_home"],
                    loop_flag,
                )


def validate_and_correct_tours(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    spatial_gap_threshold_meters: float = 1000.0,
) -> pl.DataFrame:
    """Validate tours and correct tour_category for definitive error cases.

    Assigns TourDataQuality enum values based on detected issues:
    - VALID: Properly formed tour with tour_num > 0, multiple trips
    - INDETERMINATE: tour_num == 0 (tour start detection failed, cause unknown)
    - SINGLE_TRIP: Only one trip in tour (always incomplete)
    - CHANGE_MODE: Change mode as primary purpose (trip linking failure)
    - SPATIAL_GAP: Internal junction jumps > threshold (a missing leg)
    - MISSING_ANCHOR: Neither origin nor destination at the tour's anchor

    Corrects tour_category for definitive cases:
    - Single-trip tours → PARTIAL_BOTH
    - Missing anchor → PARTIAL_BOTH

    Args:
        tours: Aggregated tour DataFrame with tour_num, trip_count, etc.
        linked_trips: Linked trips with tour_id and the ``_o_at_anchor`` /
            ``_d_at_anchor`` flags from ``add_tour_anchor_flags``
        spatial_gap_threshold_meters: Gap distance (meters) above which a tour's
            internal junction is treated as a missing leg (SPATIAL_GAP).

    Returns:
        Tours DataFrame with added tour_data_quality column and
        corrected tour_category where appropriate
    """
    logger.info("Validating tour data quality...")

    # Diagnostic logging for tour_num==0 trips
    zero_tour_trips = linked_trips.filter(pl.col("tour_num") == 0)
    n_invalid_tours = zero_tour_trips["tour_id"].n_unique()
    if len(zero_tour_trips) > 0:
        logger.warning(
            "Found %d invalid tours involving %d trips for "
            "%d persons and %d households (tour_num=0).",
            n_invalid_tours,
            len(zero_tour_trips),
            zero_tour_trips["person_id"].n_unique(),
            zero_tour_trips["hh_id"].n_unique(),
        )

    # Check whether the tour touches its own anchor at either end. For a
    # home-based tour that anchor is home; for a subtour it is the workplace or
    # campus it hangs off. ``aggregate_tour_attributes`` resolves the two into
    # ``_o_at_anchor`` / ``_d_at_anchor`` so this check never has to assume home.
    anchor_check = linked_trips.group_by("tour_id").agg(
        [
            pl.col("_o_at_anchor").any().alias("_has_anchor_origin"),
            pl.col("_d_at_anchor").any().alias("_has_anchor_dest"),
        ]
    )

    tours = tours.join(anchor_check, on="tour_id", how="left")

    # Flag tours whose trips teleport across a data gap (missing connecting leg)
    gap_check = _spatial_gap_flags(linked_trips, spatial_gap_threshold_meters)
    tours = tours.join(gap_check, on="tour_id", how="left")

    # Assign data quality flags (priority order matters)
    # Note: Loop trips are a specific type of single-trip tour
    # (trip starts and ends at the tour's anchor)
    # Note: the anchor check applies to every tour, home-based or not -- a
    # subtour is anchored at its workplace/campus, so it is checked against
    # that, not against home.
    # Note: CHANGE_MODE purpose indicates trip linking failure
    # (mode changes should be merged with adjacent trips)
    tours = tours.with_columns(
        [
            # Check conditions in order of specificity
            pl.when(
                (pl.col("trip_count") == 1)
                & pl.col("_has_anchor_origin")
                & pl.col("_has_anchor_dest")
            )
            .then(pl.lit(TourDataQuality.LOOP_TRIP))
            .when(pl.col("trip_count") == 1)
            .then(pl.lit(TourDataQuality.SINGLE_TRIP))
            .when(pl.col("tour_purpose") == PurposeCategory.CHANGE_MODE)
            .then(pl.lit(TourDataQuality.CHANGE_MODE))
            .when(pl.col("_has_spatial_gap").fill_null(value=False))
            .then(pl.lit(TourDataQuality.SPATIAL_GAP))
            .when(~pl.col("_has_anchor_origin") & ~pl.col("_has_anchor_dest"))
            .then(pl.lit(TourDataQuality.MISSING_ANCHOR))
            .when(pl.col("tour_num") == 0)
            .then(pl.lit(TourDataQuality.INDETERMINATE))
            .otherwise(pl.lit(TourDataQuality.VALID))
            .alias("tour_data_quality")
        ]
    )

    # Correct tour_category for definitive error cases. A tour that never
    # reaches its anchor at either end is PARTIAL_BOTH by construction; stating
    # it explicitly keeps the category honest for single-trip tours too.
    # Note: Loop trips should remain COMPLETE (correct structure) if they run
    # from the anchor back to the anchor.
    tours = tours.with_columns(
        [
            pl.when(
                (pl.col("tour_data_quality") == TourDataQuality.SINGLE_TRIP)
                | (pl.col("tour_data_quality") == TourDataQuality.MISSING_ANCHOR)
            )
            .then(pl.lit(TourCategory.PARTIAL_BOTH))
            .otherwise(pl.col("tour_category"))
            .alias("tour_category")
        ]
    )

    # Log validation summary
    quality_summary = (
        tours.group_by("tour_data_quality").agg(pl.len().alias("count")).sort("tour_data_quality")
    )

    # Report all quality levels, including those with 0 count
    quality_counts = {
        row["tour_data_quality"]: row["count"] for row in quality_summary.iter_rows(named=True)
    }

    logger.info("Tour data quality summary:")
    for quality_enum in TourDataQuality:
        count = quality_counts.get(quality_enum.value, 0)
        logger.info("  %s: %d", quality_enum.label, count)

    # Warn if invalid tours found
    invalid_count = tours.filter(pl.col("tour_data_quality") != TourDataQuality.VALID).height
    if invalid_count > 0:
        logger.warning(
            "Found %d tours with data quality issues.\n"
            "These tours should be filtered in formatters.",
            invalid_count,
        )

    # Run detailed diagnostics
    _diagnose_problem_tours(tours, zero_tour_trips)

    # Drop temporary columns
    tours = tours.drop(["_has_anchor_origin", "_has_anchor_dest", "_has_spatial_gap"])

    return tours
