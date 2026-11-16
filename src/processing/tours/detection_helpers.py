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
    """Number the home-based tours within each person-day.

    A new tour starts at a trip that:
    1. Leaves home (o_is_home=True, d_is_home=False), or loops at it
       (both ends home)
    2. Is the day's first trip and does not start at home (a partial tour)
    3. Follows a multi-day gap and does not start at home (if
       ``check_multiday_gaps``)
    4. Follows a trip that returned home, even if the next tour is partial

    Whether each tour starts and ends at its anchor (``tour_category``) is
    decided later, during aggregation.

    Args:
        linked_trips: Classified linked trips with location type flags
        check_multiday_gaps: Whether to check for multi-day gaps

    Returns:
        Linked trips with ``tour_num``
    """
    logger.info("Identifying home-based tours...")

    linked_trips = linked_trips.sort(["person_id", "person_day_id", "depart_time"])

    o_home, d_home = pl.col("_o_is_home"), pl.col("_d_is_home")
    # Ordinal rank breaks ties between duplicate departure times
    is_first_trip = pl.col("depart_time").rank("ordinal").over(["person_id", "person_day_id"]) == 1

    if check_multiday_gaps:
        day_gap = (pl.col("person_day_id") - pl.col("person_day_id").shift(1)).over(["person_id"])
        has_gap = day_gap > 1
    else:
        has_gap = pl.lit(value=False)

    prev_returned_home = (
        (~o_home & d_home).shift(1).over(["person_id", "person_day_id"]).fill_null(value=False)
    )

    tour_starts = (
        o_home  # leaving home, or a loop trip at it
        | (is_first_trip & ~o_home)
        | (has_gap & ~o_home)
        | prev_returned_home
    ).cast(pl.Int32)

    linked_trips = linked_trips.with_columns(
        tour_starts.cum_sum().over(["person_id", "person_day_id"]).alias("tour_num")
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
    ``add_anchor_flags``: a trip end is at the work anchor when it is at a
    reported workplace, or -- on a day no reported workplace was visited -- at
    an observed one. School works the same way.

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
        .over(["person_id", "person_day_id", "tour_num"])
        .alias("_trip_num_in_tour"),
    )

    # A trip touches an anchor if either of its ends is at that anchor.
    linked_trips = linked_trips.with_columns(
        (pl.col("_o_at_work") | pl.col("_d_at_work")).alias("_at_work"),
        (pl.col("_o_at_school") | pl.col("_d_at_school")).alias("_at_school"),
    )

    # For each tour, find the first and last trip at each anchor type.
    tour = ["person_id", "person_day_id", "tour_num"]
    for name in ["work", "school"]:
        at_anchor = pl.when(pl.col(f"_at_{name}")).then(pl.col("_trip_num_in_tour"))
        linked_trips = linked_trips.with_columns(
            at_anchor.min().over(tour).alias(f"_{name}_period_start"),
            at_anchor.max().over(tour).alias(f"_{name}_period_end"),
        )

    # Determine primary anchor type for tours with anchors.
    # Priority: Work > School (matches person type priority).
    linked_trips = linked_trips.with_columns(
        pl.when(pl.col("_work_period_start").is_not_null())
        .then(pl.lit(LocationType.WORK))
        .when(pl.col("_school_period_start").is_not_null())
        .then(pl.lit(LocationType.SCHOOL))
        .alias("_anchor_location_type"),
        pl.coalesce("_work_period_start", "_school_period_start").alias(
            "_anchor_period_start_trip_num"
        ),
        pl.coalesce("_work_period_end", "_school_period_end").alias("_anchor_period_end_trip_num"),
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
    o_anchor: list[int | None],
    d_anchor: list[int | None],
    anchor_start: int,
    anchor_end: int,
) -> tuple[list[int], int]:
    """Number the subtours inside one tour's anchor period.

    A subtour leaves one workplace or school and comes back to *that same one*.
    A chain that arrives at a different anchor instead — work 1, coffee, work 2
    — is not a subtour: its trips stay on the parent tour, and the new anchor
    can start subtours of its own. A trip straight between two anchors is a
    parent-tour trip too.

    ``anchor_start`` and ``anchor_end`` are the first and last trips *touching*
    an anchor, which normally are the commute in and the commute out and belong
    to the parent tour. That holds only while something leaves the anchor again
    after the subtour. When the tour ends at the anchor -- ``Home -> Work ->
    Lunch -> Work`` -- the last trip touching it *is* the subtour's return leg,
    so the trailing trip is excluded only when it leaves.

    Args:
        trip_nums: Trip sequence numbers within the tour, in order.
        o_anchor: Which anchor each trip's origin is at, or None.
        d_anchor: Which anchor each trip's destination is at, or None.
        anchor_start: First trip number touching an anchor.
        anchor_end: Last trip number touching an anchor.

    Returns:
        Subtour number per trip (0 for parent-tour trips), and how many
        subtours closed.
    """
    subtour_nums = [0] * len(trip_nums)
    subtour_num = 0
    closed = 0
    left_from = None

    for idx, trip_num in enumerate(trip_nums):
        origin, destination = o_anchor[idx], d_anchor[idx]
        is_returning_anchor = origin is None and destination is not None

        if trip_num <= anchor_start or trip_num > anchor_end:
            continue
        if trip_num == anchor_end and not is_returning_anchor:
            continue

        if left_from is None:
            if origin is not None and destination is None:
                left_from = origin
                subtour_num += 1
                subtour_nums[idx] = subtour_num
        elif destination is None:
            subtour_nums[idx] = subtour_num
        elif destination == left_from:
            subtour_nums[idx] = subtour_num
            left_from = None
            closed += 1
        else:
            # Arrived at another anchor: the chain was never a round trip.
            subtour_nums = [0 if n == subtour_num else n for n in subtour_nums]
            subtour_num -= 1
            left_from = None

    # A chain that never came back is not a subtour either.
    if left_from is not None:
        subtour_nums = [0 if n == subtour_num else n for n in subtour_nums]

    return subtour_nums, closed


def detect_anchor_based_subtours(
    linked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Detect anchor-based subtours using hybrid loop approach.

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
    1. Trip leaves an anchor location (o at anchor, d not)
    2. A later trip returns to that same anchor location (o not, d at it)
    3. Both trips are WITHIN the expanded anchor period

    This prevents false subtour detection on trips to/from home.

    Args:
        linked_trips: Trips with anchor period markers

    Returns:
        Trips with ``subtour_num`` (0 for trips not on a subtour)
    """
    logger.info("Detecting anchor-based subtours...")

    # Initialize subtour_num to 0 for all parent tours
    linked_trips = linked_trips.with_columns(
        pl.lit(0, dtype=pl.Int8).alias("subtour_num"),
    )

    # Ensure sorted for partition_by to maintain order
    linked_trips = linked_trips.sort(
        ["person_id", "person_day_id", "tour_num", "_trip_num_in_tour"]
    )

    # Partition by tour using Polars partition_by
    tour_groups = linked_trips.partition_by(
        ["person_id", "person_day_id", "tour_num"],
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

        # Which workplace or school each trip end is at, so a subtour can be
        # held to returning where it left from.
        if anchor_type == LocationType.WORK.value:
            name = "work"
        elif anchor_type == LocationType.SCHOOL.value:
            name = "school"
        else:
            # Unknown anchor type, skip
            modified_tours.append(tour_df)
            continue

        subtour_nums, closed = _assign_subtour_nums(
            trip_nums,
            tour_df[f"_o_{name}_anchor_id"].to_list(),
            tour_df[f"_d_{name}_anchor_id"].to_list(),
            anchor_start,
            anchor_end,
        )
        subtour_counter += closed

        # Update tour DataFrame with subtour assignments
        updated_tour_df = tour_df.with_columns(
            pl.Series("subtour_num", subtour_nums, dtype=pl.Int8),
        )

        modified_tours.append(updated_tour_df)

    # Concatenate all tours back together. Rechunk: one chunk per tour makes
    # every downstream expression orders of magnitude slower.
    linked_trips_with_subtours = pl.concat(modified_tours, rechunk=True)

    logger.info("Detected %s anchor-based subtours", subtour_counter)
    return linked_trips_with_subtours
