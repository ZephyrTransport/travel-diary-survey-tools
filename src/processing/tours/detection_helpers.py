"""Tour detection helper functions for tour extraction.

This module contains functions for:
- Identifying home-based tour boundaries
- Expanding anchor location periods (work, school)
- Detecting anchor-based subtours (work-based, school-based)
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType

logger = logging.getLogger(__name__)


def identify_home_based_tours(
    linked_trips: pl.DataFrame,
    check_multiday_gaps: bool = False,
) -> pl.DataFrame:
    """Identify home-based tours from classified trip data.

    Creates tour boundaries for sequences of trips, classifying each tour
    by whether it starts/ends at home using TourCategory enum:
    - COMPLETE: Starts at home, ends at home
    - PARTIAL_END: Starts at home, doesn't end at home
    - PARTIAL_START: Doesn't start at home, ends at home
    - PARTIAL_BOTH: Neither starts nor ends at home

    Tours are identified by detecting:
    1. Departures from home (o_is_home=True, d_is_home=False)
    2. Returns to home (o_is_home=False, d_is_home=True)
    3. Multi-day gaps (if check_multiday_gaps=True)

    Tours can be filtered downstream using the tour_category column:
    - Filter to TourCategory.COMPLETE for legacy compatibility
    - Include partial tours for more comprehensive analysis

    Args:
        linked_trips: Classified linked trips with location type flags
        check_multiday_gaps: Whether to check for multi-day gaps

    Returns:
        Linked trips with tour_id, tour_category
    """
    logger.info("Identifying home-based tours...")

    # Sort trips by person, day, and time
    linked_trips = linked_trips.sort(["person_id", "day_id", "depart_time"])

    # Mark trip characteristics for tour boundary detection
    is_leaving_home = pl.col("_o_is_home") & ~pl.col("_d_is_home")
    is_returning_home = ~pl.col("_o_is_home") & pl.col("_d_is_home")
    is_loop_trip = pl.col("_o_is_home") & pl.col("_d_is_home")
    # Use rank with tiebreakers to handle duplicate departure times
    is_first_trip = pl.col("depart_time").rank("ordinal").over(["person_id", "day_id"]) == 1
    is_last_trip = pl.col("depart_time") == pl.col("depart_time").max().over(
        ["person_id", "day_id"]
    )

    # Check for multi-day gaps if configured
    if check_multiday_gaps:
        day_gap = (pl.col("day_id") - pl.col("day_id").shift(1)).over(["person_id"])
        has_gap = day_gap > 1
    else:
        has_gap = pl.lit(value=False)

    # Check if previous trip returned home
    prev_returned_home = (
        is_returning_home.shift(1).over(["person_id", "day_id"]).fill_null(value=False)
    )

    # Tour starts when:
    # 1. Leaving home (origin=home, dest!=home)
    # 2. Loop trip (origin=home, dest=home)
    # 3. First trip AND not at home (partial tour)
    # 4. Multi-day gap AND not at home
    # 5. Previous trip returned home (even if next tour is partial)
    tour_starts_leaving = is_leaving_home
    tour_starts_loop = is_loop_trip
    tour_starts_away = is_first_trip & ~pl.col("_o_is_home")
    tour_starts_gap = has_gap & ~pl.col("_o_is_home")
    tour_starts_after_home = prev_returned_home

    tour_starts = (
        tour_starts_leaving
        | tour_starts_loop
        | tour_starts_away
        | tour_starts_gap
        | tour_starts_after_home
    ).cast(pl.Int32)

    # Tour ends when: returning home OR last trip
    tour_ends = (is_returning_home | is_last_trip).cast(pl.Int32)

    # Assign tour numbers by cumulative sum of tour starts
    linked_trips = linked_trips.with_columns(
        [
            is_leaving_home.alias("_leaving_home"),
            is_returning_home.alias("_returning_home"),
            tour_starts.alias("_tour_starts"),
            tour_ends.alias("_tour_ends"),
        ]
    ).with_columns(
        [
            pl.col("_tour_starts").cum_sum().over(["person_id", "day_id"]).alias("tour_num"),
        ]
    )

    # Clean up temporary columns
    linked_trips = linked_trips.drop(
        [
            "_tour_starts",
            "_tour_ends",
        ]
    )

    logger.info("Home-based tour identification complete")
    return linked_trips


def expand_anchor_periods(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Expand work/school anchor periods for tours using the anchor flags.

    For tours whose trips visit a person's work or school location, this finds
    the first arrival and last departure at that anchor, so subtours are only
    detected WITHIN the anchor period, not during travel to/from it. For example:
        Home -> Work -> Lunch -> Work -> Errand -> Home
    Only Lunch (between the two work visits) is a subtour, not Errand.

    The anchor flags ``_o_at_work``/``_d_at_work`` (and ``_at_school``) come from
    ``classify_trip_locations``: a trip end is at the work anchor when it is
    within the work threshold of *any* of the person's habitual work locations
    (reported or observed), or is a WORK-purpose end. Because observed
    alternate worksites are habitual work locations, a day at an alternate
    workplace anchors there naturally — no per-day derivation needed.

    Args:
        linked_trips: Classified trips with tour_num and anchor flags
            (_o_at_work, _d_at_work, _o_at_school, _d_at_school).

    Returns:
        Trips with _anchor_period_start_trip_num, _anchor_period_end_trip_num,
        and _anchor_location_type. Anchor flags are retained for subtour detection.
    """
    logger.info("Expanding anchor location periods...")

    # Add trip sequence number within tour for tracking positions
    linked_trips = linked_trips.with_columns(
        pl.col("linked_trip_id")
        .rank("ordinal")
        .over(["person_id", "day_id", "tour_num"])
        .alias("_trip_num_in_tour"),
    )

    # A trip touches an anchor if either of its ends is at that anchor.
    linked_trips = linked_trips.with_columns(
        (pl.col("_o_at_work") | pl.col("_d_at_work")).alias("_at_work"),
        (pl.col("_o_at_school") | pl.col("_d_at_school")).alias("_at_school"),
    )

    # For each tour, find the first and last trip at each anchor type.
    for name in ["work", "school"]:
        linked_trips = linked_trips.with_columns(
            pl.when(pl.col(f"_at_{name}"))
            .then(pl.col("_trip_num_in_tour"))
            .otherwise(None)
            .min()
            .over(["person_id", "day_id", "tour_num"])
            .alias(f"_{name}_period_start"),
            pl.when(pl.col(f"_at_{name}"))
            .then(pl.col("_trip_num_in_tour"))
            .otherwise(None)
            .max()
            .over(["person_id", "day_id", "tour_num"])
            .alias(f"_{name}_period_end"),
        )

    # Determine primary anchor type for tours with anchors.
    # Priority: Work > School (matches person type priority).
    linked_trips = linked_trips.with_columns(
        pl.when(pl.col("_work_period_start").is_not_null())
        .then(pl.lit(LocationType.WORK))
        .when(pl.col("_school_period_start").is_not_null())
        .then(pl.lit(LocationType.SCHOOL))
        .otherwise(None)
        .alias("_anchor_location_type"),
        pl.when(pl.col("_work_period_start").is_not_null())
        .then(pl.col("_work_period_start"))
        .when(pl.col("_school_period_start").is_not_null())
        .then(pl.col("_school_period_start"))
        .otherwise(None)
        .alias("_anchor_period_start_trip_num"),
        pl.when(pl.col("_work_period_end").is_not_null())
        .then(pl.col("_work_period_end"))
        .when(pl.col("_school_period_end").is_not_null())
        .then(pl.col("_school_period_end"))
        .otherwise(None)
        .alias("_anchor_period_end_trip_num"),
    )

    # Clean up temporary columns. Keep the _o_at_work / _d_at_work / _o_at_school
    # / _d_at_school anchor flags, which subtour detection reads.
    drop_cols = [
        "_at_work",
        "_at_school",
        "_work_period_start",
        "_work_period_end",
        "_school_period_start",
        "_school_period_end",
    ]
    linked_trips = linked_trips.drop(drop_cols)

    logger.info("Anchor location period expansion complete")
    return linked_trips


def _assign_subtour_nums(
    trip_nums: list[int],
    o_at_anchor: list[bool],
    d_at_anchor: list[bool],
    anchor_start: int,
    anchor_end: int,
) -> tuple[list[int], int]:
    """Number the subtours inside one tour's anchor period.

    A subtour opens on a trip leaving the anchor and closes on one returning to
    it. ``anchor_start`` and ``anchor_end`` are the first and last trips
    *touching* the anchor, which normally are the commute in and the commute
    out and belong to the parent tour.

    That holds only while something leaves the anchor again after the subtour.
    When the tour ends at the anchor -- ``Home -> Work -> Lunch -> Work`` --
    the last trip touching it *is* the subtour's return leg, so excluding it by
    position dropped that leg and left the subtour open. Exclude the trailing
    trip only when it leaves.

    Args:
        trip_nums: Trip sequence numbers within the tour, in order.
        o_at_anchor: Whether each trip's origin is at the anchor.
        d_at_anchor: Whether each trip's destination is at the anchor.
        anchor_start: First trip number touching the anchor.
        anchor_end: Last trip number touching the anchor.

    Returns:
        Subtour number per trip (0 for parent-tour trips), and how many
        subtours closed.
    """
    subtour_nums = [0] * len(trip_nums)
    subtour_num = 0
    closed = 0
    in_subtour = False

    for idx, trip_num in enumerate(trip_nums):
        is_leaving_anchor = o_at_anchor[idx] and not d_at_anchor[idx]
        is_returning_anchor = not o_at_anchor[idx] and d_at_anchor[idx]

        if trip_num <= anchor_start or trip_num > anchor_end:
            continue
        if trip_num == anchor_end and not is_returning_anchor:
            continue

        if is_leaving_anchor and not in_subtour:
            in_subtour = True
            subtour_num += 1
            subtour_nums[idx] = subtour_num
        elif in_subtour and not is_returning_anchor:
            subtour_nums[idx] = subtour_num
        elif in_subtour:
            subtour_nums[idx] = subtour_num
            in_subtour = False
            closed += 1

    # A chain that never came back is not a subtour. The boundary rule closes
    # the common case, but a trip both leaving and arriving at an anchor (two
    # habitual worksites) counts as neither, so a chain can still end open.
    # Hand those trips back rather than claim a round trip the trips do not show.
    if in_subtour:
        subtour_nums = [0 if n == subtour_num else n for n in subtour_nums]

    return subtour_nums, closed


def detect_anchor_based_subtours(
    linked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Detect anchor-based subtours using hybrid loop approach.

    LEGACY REFERENCE: 03a-tour_extract_week.py lines 578-600
    MATCHES LEGACY: Only detects subtours within expanded anchor periods

    This is a hybrid approach that uses fast Polars vectorized operations
    to detect home-based tours, expand anchor periods, then this function
    uses a slower but more flexible/understandable loop-based approach to
    detect subtours. The computational cost is acceptable since subtours
    are relatively rare compared to overall trips/tours. Perhaps future
    versions could optimize this further.

    It loops over tours with anchor periods and detects subtours by finding
    leave/return patterns. Uses the anchor_period markers from
    expand_anchor_periods() to know where to look for subtours.

    A subtour is detected when:
    1. Trip leaves anchor location (o_at_anchor, !d_at_anchor)
    2. Trip returns to anchor location (!o_at_anchor, d_at_anchor)
    3. Both trips are WITHIN the expanded anchor period

    This prevents false subtour detection on trips to/from home.

    Args:
        linked_trips: Trips with anchor period markers

    Returns:
        Trips with subtour_id assigned to subtour trips
    """
    logger.info("Detecting anchor-based subtours...")

    # Initialize subtour_num to 0 for all parent tours
    linked_trips = linked_trips.with_columns(
        pl.lit(0, dtype=pl.Int8).alias("subtour_num"),
    )

    # Ensure sorted for partition_by to maintain order
    linked_trips = linked_trips.sort(["person_id", "day_id", "tour_num", "_trip_num_in_tour"])

    # Partition by tour using Polars partition_by
    tour_groups = linked_trips.partition_by(
        ["person_id", "day_id", "tour_num"],
        maintain_order=True,
        as_dict=False,
    )

    # Process each tour
    logger.info("Processing %d tours for subtour detection...", len(tour_groups))
    subtour_counter = 0
    modified_tours = []
    for i, tour_df in enumerate(tour_groups):
        # Progress update every 30,000 tours
        if i % 30000 == 0:
            pct = round((i / len(tour_groups)) * 100)
            logger.info(
                "Subtour detection progress: %d%% -- %d of %d tours processed",
                pct,
                i,
                len(tour_groups),
            )

        # Get tour-level metadata from first row
        first_row = tour_df.row(0, named=True)

        # Skip if no anchor period
        if first_row.get("_anchor_period_start_trip_num") is None:
            modified_tours.append(tour_df)
            continue

        anchor_start = first_row["_anchor_period_start_trip_num"]
        anchor_end = first_row["_anchor_period_end_trip_num"]
        anchor_type = first_row["_anchor_location_type"]

        # Check if there are trips within the anchor period beyond just
        # arrival/departure (anchor_end > anchor_start + 1 means there
        # are intermediate trips)
        if anchor_end <= anchor_start + 1:
            modified_tours.append(tour_df)
            continue

        # Work with Polars columns directly to avoid dict conversion issues
        trip_nums = tour_df["_trip_num_in_tour"].to_list()

        # Get anchor location flags based on anchor type
        # Pulled outside inner loop to filter once per tour
        if anchor_type == LocationType.WORK.value:
            # Distance-based work anchor flags: WORK_RELATED errands away from any
            # habitual work location read as "away from anchor" (and become
            # subtours), while a habitual work location is treated as the anchor.
            o_at_anchor = tour_df["_o_at_work"].to_list()
            d_at_anchor = tour_df["_d_at_work"].to_list()
        elif anchor_type == LocationType.SCHOOL.value:
            o_at_anchor = tour_df["_o_at_school"].to_list()
            d_at_anchor = tour_df["_d_at_school"].to_list()
        else:
            # Unknown anchor type, skip
            modified_tours.append(tour_df)
            continue

        subtour_nums, closed = _assign_subtour_nums(
            trip_nums,
            o_at_anchor,
            d_at_anchor,
            anchor_start,
            anchor_end,
        )
        subtour_counter += closed

        # Update tour DataFrame with subtour assignments
        updated_tour_df = tour_df.with_columns(
            pl.Series("subtour_num", subtour_nums, dtype=pl.Int8),
        )

        modified_tours.append(updated_tour_df)

    # Concatenate all tours back together
    linked_trips_with_subtours = pl.concat(modified_tours)

    # tour_num, subtour_num, and parent_tour_id are now set for subtour trips
    # They will be used for ID creation and parent tracking during aggregation

    logger.info("Detected %s anchor-based subtours", subtour_counter)
    return linked_trips_with_subtours
