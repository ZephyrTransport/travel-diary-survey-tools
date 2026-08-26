"""Tour validation helper functions.

This module contains functions for:
- Grading each tour's structure into a ``tour_data_quality`` code
- Detecting spatial gaps (a missing leg) between consecutive trips
- Asserting that tour boundary detection assigned every trip to a tour
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import TourCategory, TourDataQuality
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


def _open_end_facts(
    linked_trips: pl.DataFrame,
    habitual_locations: pl.DataFrame | None,
    boundary_gap_threshold_meters: float,
    home_threshold_meters: float,
) -> pl.DataFrame:
    """Describe each tour's two ends against the person's surrounding travel.

    A partial tour is only interpretable next to what the person did before and
    after it, so this walks each person's whole trip sequence -- across diary
    days, which is exactly where tours are cut -- and reports, per tour:

    * ``_start_other_home`` / ``_end_other_home`` -- that end sits on another
      home this person is known to have, which tours are not built around.
    * ``_start_resumes`` / ``_end_resumes`` -- the adjacent trip picks up at the
      same place, so the journey continues and was merely split.
    * ``_start_edge`` / ``_end_edge`` -- there is no adjacent trip at all; the
      diary begins or ends here.
    * ``_boundary_gap`` -- the adjacent trip starts somewhere else entirely, so
      a connecting leg is missing. Same defect as an internal spatial gap, just
      at the tour boundary rather than inside it.

    Returns one row per tour_id. Missing coordinate columns yield all-false
    facts so schema-only frames and coordinate-free unit tests still grade.
    """
    facts = linked_trips.select("tour_id").unique()
    required = (*_GAP_COORD_COLS, "arrive_time", "person_id")
    if any(c not in linked_trips.columns for c in required):
        # Without coordinates nothing can be said about what surrounds a tour,
        # which is precisely the diary-edge case: no observed trip either side.
        # Defaulting the edges to true keeps a partial tour partial, where
        # defaulting them to false would silently grade it VALID.
        return facts.with_columns(
            [
                pl.lit(value=True).alias("_start_edge"),
                pl.lit(value=True).alias("_end_edge"),
                *(
                    pl.lit(value=False).alias(c)
                    for c in (
                        "_start_other_home",
                        "_end_other_home",
                        "_start_resumes",
                        "_end_resumes",
                        "_boundary_gap",
                    )
                ),
            ]
        )

    # Neighbouring trips in person order, deliberately ignoring day_id: the
    # diary day boundary is the thing being measured, not a barrier to it.
    seq = linked_trips.sort(["person_id", "depart_time", "arrive_time"]).with_columns(
        [
            pl.col("o_lat").shift(-1).over("person_id").alias("_nx_lat"),
            pl.col("o_lon").shift(-1).over("person_id").alias("_nx_lon"),
            pl.col("d_lat").shift(1).over("person_id").alias("_pv_lat"),
            pl.col("d_lon").shift(1).over("person_id").alias("_pv_lon"),
        ]
    )
    ends = (
        seq.sort(["tour_id", "depart_time"])
        .group_by("tour_id")
        .agg(
            [
                pl.col("person_id").first(),
                pl.col("o_lat").first().alias("_s_lat"),
                pl.col("o_lon").first().alias("_s_lon"),
                pl.col("d_lat").last().alias("_e_lat"),
                pl.col("d_lon").last().alias("_e_lon"),
                pl.col("_pv_lat").first(),
                pl.col("_pv_lon").first(),
                pl.col("_nx_lat").last(),
                pl.col("_nx_lon").last(),
            ]
        )
    )

    # Distance from each open end to whatever the person did next to it.
    ends = ends.with_columns(
        [
            expr_haversine(
                pl.col("_s_lat"), pl.col("_s_lon"), pl.col("_pv_lat"), pl.col("_pv_lon")
            ).alias("_start_jump"),
            expr_haversine(
                pl.col("_e_lat"), pl.col("_e_lon"), pl.col("_nx_lat"), pl.col("_nx_lon")
            ).alias("_end_jump"),
        ]
    )

    ends = ends.with_columns(
        [
            pl.col("_pv_lat").is_null().alias("_start_edge"),
            pl.col("_nx_lat").is_null().alias("_end_edge"),
            (pl.col("_start_jump") <= boundary_gap_threshold_meters)
            .fill_null(value=False)
            .alias("_start_resumes"),
            (pl.col("_end_jump") <= boundary_gap_threshold_meters)
            .fill_null(value=False)
            .alias("_end_resumes"),
        ]
    )
    # A leg is missing when the person reappears somewhere else. Absence of an
    # adjacent trip is the diary ending, not a gap, so edges are excluded.
    ends = ends.with_columns(
        (
            (~pl.col("_start_edge") & ~pl.col("_start_resumes"))
            | (~pl.col("_end_edge") & ~pl.col("_end_resumes"))
        ).alias("_boundary_gap")
    )

    other_home = _other_home_flags(ends, habitual_locations, home_threshold_meters)
    return ends.join(other_home, on="tour_id", how="left").with_columns(
        [
            pl.col("_start_other_home").fill_null(value=False),
            pl.col("_end_other_home").fill_null(value=False),
        ]
    )


def _other_home_flags(
    ends: pl.DataFrame,
    habitual_locations: pl.DataFrame | None,
    home_threshold_meters: float,
) -> pl.DataFrame:
    """Flag tour ends sitting on another home of the same person.

    Any non-primary home in ``habitual_locations`` counts, whether the
    respondent reported it or the pipeline derived it from their travel --
    a second residence is a second residence either way. Tours are simply not
    built around them, so one that begins or ends at one reads as truncated
    when it is not. Saying so outright beats reporting a truncation that did
    not happen.
    """
    blank = ends.select("tour_id").with_columns(
        pl.lit(value=False).alias("_start_other_home"),
        pl.lit(value=False).alias("_end_other_home"),
    )
    if habitual_locations is None or habitual_locations.is_empty():
        return blank
    needed = {"person_id", "location_type", "is_primary", "lat", "lon"}
    if not needed <= set(habitual_locations.columns):
        return blank

    secondary = habitual_locations.filter(
        (pl.col("location_type") == LocationType.HOME.value) & (~pl.col("is_primary"))
    ).select("person_id", pl.col("lat").alias("_h_lat"), pl.col("lon").alias("_h_lon"))
    if secondary.is_empty():
        return blank

    paired = ends.select("tour_id", "person_id", "_s_lat", "_s_lon", "_e_lat", "_e_lon").join(
        secondary, on="person_id", how="inner"
    )
    near = paired.with_columns(
        [
            (
                expr_haversine(
                    pl.col("_s_lat"), pl.col("_s_lon"), pl.col("_h_lat"), pl.col("_h_lon")
                )
                <= home_threshold_meters
            ).alias("_s_near"),
            (
                expr_haversine(
                    pl.col("_e_lat"), pl.col("_e_lon"), pl.col("_h_lat"), pl.col("_h_lon")
                )
                <= home_threshold_meters
            ).alias("_e_near"),
        ]
    )
    return near.group_by("tour_id").agg(
        pl.col("_s_near").any().alias("_start_other_home"),
        pl.col("_e_near").any().alias("_end_other_home"),
    )


def _assert_tours_assigned(linked_trips: pl.DataFrame) -> None:
    """Raise if any trip was never assigned to a tour.

    Every first trip of a person-day starts a tour -- it either leaves the
    anchor, loops at it, or begins away from it -- so ``tour_num`` is always
    >= 1. A trip without one means boundary detection itself broke, which
    invalidates the whole tour table rather than the one row. Failing here beats
    stamping a "cause unknown" flag and letting a broken table flow downstream.

    Raises:
        ValueError: If any trip has a null or non-positive ``tour_num``.
    """
    if "tour_num" not in linked_trips.columns:
        return
    unassigned = linked_trips.filter(pl.col("tour_num").is_null() | (pl.col("tour_num") < 1))
    if unassigned.height:
        msg = (
            f"{unassigned.height} trips were never assigned to a tour, across "
            f"{unassigned['person_id'].n_unique()} persons. Tour boundary "
            f"detection failed; the tour table cannot be trusted."
        )
        raise ValueError(msg)


def validate_and_correct_tours(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    habitual_locations: pl.DataFrame | None = None,
    spatial_gap_threshold_meters: float = 1000.0,
    home_threshold_meters: float = 100.0,
) -> pl.DataFrame:
    """Stamp ``tour_data_quality``: why each tour is not a valid round trip.

    Assigns :class:`TourDataQuality` by first match. The order is deliberate --
    a tour can fail several ways at once and the code should name the one that
    decides what to do with it:

    1. ``NO_DESTINATION`` -- a closed tour with nothing to anchor on. Subsumes
       the old loop-trip code and the change-mode-only case alike. Restricted to
       closed tours on purpose: a partial tour lacks an activity because half of
       it went unobserved, which its open-end code already says.
    2. ``PARTIAL_OTHER_HOME`` -- an open end is a home this person is already
       known to stay at. Ranked above the gap checks because the tour is not
       really truncated; the anchor is.
    3. ``SPATIAL_GAP`` -- a leg is missing, inside the tour or at its boundary.
    4. ``PARTIAL_DAY_SPLIT`` -- the journey continues from the same place on the
       next diary day. Recoverable by stitching.
    5. ``PARTIAL_DIARY_EDGE`` -- the diary begins or ends at that open end.
       Nothing to recover; the survey simply stopped watching.

    ``tour_category`` is left exactly as aggregation found it. It reports which
    ends are open; this reports why, and the two are read together.

    Args:
        tours: Aggregated tours with tour_category and tour_purpose.
        linked_trips: Linked trips with tour_id, coordinates and times.
        habitual_locations: Observed habitual locations, used to recognise a
            person's other homes. Omitted, no tour is graded OTHER_HOME.
        spatial_gap_threshold_meters: Distance above which a junction between
            consecutive trips counts as a missing leg.
        home_threshold_meters: Distance within which a tour end is treated as
            being at a known home.

    Returns:
        Tours with a ``tour_data_quality`` column added.
    """
    logger.info("Validating tour data quality...")

    _assert_tours_assigned(linked_trips)

    gap_check = _spatial_gap_flags(linked_trips, spatial_gap_threshold_meters)
    open_ends = _open_end_facts(
        linked_trips,
        habitual_locations,
        spatial_gap_threshold_meters,
        home_threshold_meters,
    )
    facts = [c for c in open_ends.columns if c.startswith("_")]
    tours = tours.join(gap_check, on="tour_id", how="left").join(
        open_ends.select("tour_id", *facts), on="tour_id", how="left"
    )

    # Which ends are unanchored is already decided; consult only those.
    start_open = pl.col("tour_category").is_in(
        [TourCategory.PARTIAL_START.value, TourCategory.PARTIAL_BOTH.value]
    )
    end_open = pl.col("tour_category").is_in(
        [TourCategory.PARTIAL_END.value, TourCategory.PARTIAL_BOTH.value]
    )

    def at_open_end(flag: str) -> pl.Expr:
        """True when *flag* holds at an end that is actually open."""
        return (start_open & pl.col(f"_start_{flag}").fill_null(value=False)) | (
            end_open & pl.col(f"_end_{flag}").fill_null(value=False)
        )

    tours = tours.with_columns(
        # Only a *closed* tour can be faulted for having no destination: it went
        # out and came back with nothing in between. A partial tour has no
        # activity because half of it was never observed, so its open end is the
        # fact worth reporting, not the missing stop.
        pl.when(
            pl.col("tour_purpose").is_null()
            & (pl.col("tour_category") == TourCategory.COMPLETE.value)
        )
        .then(pl.lit(TourDataQuality.NO_DESTINATION))
        .when(at_open_end("other_home"))
        .then(pl.lit(TourDataQuality.PARTIAL_OTHER_HOME))
        .when(
            pl.col("_has_spatial_gap").fill_null(value=False)
            | (pl.col("_boundary_gap").fill_null(value=False) & (start_open | end_open))
        )
        .then(pl.lit(TourDataQuality.SPATIAL_GAP))
        .when(at_open_end("resumes"))
        .then(pl.lit(TourDataQuality.PARTIAL_DAY_SPLIT))
        .when(at_open_end("edge"))
        .then(pl.lit(TourDataQuality.PARTIAL_DIARY_EDGE))
        .otherwise(pl.lit(TourDataQuality.VALID))
        .alias("tour_data_quality")
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

    return tours.drop([c for c in tours.columns if c.startswith("_")])
