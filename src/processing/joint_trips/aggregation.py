"""Aggregation and validation helpers for joint trip detection.

This module builds the joint_trips table from detected cliques and validates
results against reported num_travelers field.
"""

import logging

import polars as pl

from processing.usability import MIN_JOINT_PARTICIPANTS

logger = logging.getLogger(__name__)


def household_day_of(members: pl.DataFrame, person_days: pl.DataFrame, key: str) -> pl.DataFrame:
    """Return each grouping's household-day, ``hh_day_id`` and ``travel_date``, from its members.

    A joint trip or joint tour is shared by several people, so it belongs to
    the household-day, not to any one member's day. Every member's day must
    fall on the same household-day; a grouping whose members sit on two, or on a
    day *person_days* does not hold, raises rather than picking one.

    Args:
        members: Member records with *key* and ``person_day_id``.
        person_days: Days with ``person_day_id``, ``hh_day_id`` and ``travel_date``.
        key: The grouping id, e.g. ``joint_trip_id``.

    Returns:
        One row per *key* with its ``hh_day_id`` and ``travel_date``.

    Raises:
        ValueError: If a grouping's members span more than one household-day, or
            a member's day is missing from *person_days*.
    """
    per_group = (
        members.select(key, "person_day_id")
        .join(
            person_days.select("person_day_id", "hh_day_id", "travel_date"),
            on="person_day_id",
            how="left",
        )
        .group_by(key)
        .agg(
            pl.col("hh_day_id").first(),
            pl.col("travel_date").first(),
            pl.col("hh_day_id").n_unique().alias("_n_days"),
            pl.col("hh_day_id").null_count().alias("_n_missing"),
        )
    )
    broken = per_group.filter((pl.col("_n_days") > 1) | (pl.col("_n_missing") > 0))
    if not broken.is_empty():
        msg = (
            f"{broken.height} {key} grouping(s) do not sit on one household-day: their "
            f"members' days fall on different household-days, or a member's day is "
            f"missing from person_days. Sample {key}: {broken[key].head(5).to_list()}."
        )
        raise ValueError(msg)
    return per_group.select(key, "hh_day_id", "travel_date")


def build_joint_trips_table(
    linked_trips: pl.DataFrame,
    person_days: pl.DataFrame,
) -> pl.DataFrame:
    """Build the joint_trips linking table from detected groups.

    One row per joint_trip_id, recording which household-day the
    group belongs to and how many member trips it holds. Where and when they travelled
    stays on the member trips: collapsing those into one place or time is a
    choice of rule, and it is left to whoever needs the answer.

    Args:
        linked_trips: Linked trips carrying ``person_id`` and the detected
            ``joint_trip_id``
        person_days: Days with ``person_day_id``, ``hh_day_id`` and ``travel_date``; see
            :func:`household_day_of`.

    Returns:
        DataFrame with joint_trips schema (one row per joint_trip_id)
    """
    joint_members = linked_trips.filter(pl.col("joint_trip_id").is_not_null())

    if len(joint_members) == 0:
        # No joint trips detected, return empty table with proper schema
        return pl.DataFrame(
            schema={
                "joint_trip_id": pl.Int64,
                "hh_id": pl.Int64,
                "hh_day_id": pl.Int64,
                "travel_date": person_days.schema["travel_date"],
                "num_joint_travelers": pl.Int64,
            }
        )

    # Aggregate by joint_trip_id
    agg_exprs = [
        pl.col("hh_id").first(),
        # Count of travelers
        pl.col("linked_trip_id").n_unique().alias("num_joint_travelers"),
    ]

    # Propagate complete: a joint trip is complete only if all member trips are complete
    if "survey_complete" in joint_members.columns:
        agg_exprs.append(pl.all("survey_complete").alias("survey_complete"))

    _validate_grouping_membership(joint_members)

    # Sorted: group_by returns groups in no fixed order, so the table's rows
    # would otherwise come out shuffled each run.
    return (
        joint_members.group_by("joint_trip_id")
        .agg(agg_exprs)
        .join(
            household_day_of(joint_members, person_days, "joint_trip_id"),
            on="joint_trip_id",
            how="left",
        )
        .sort("joint_trip_id")
    )


def _validate_grouping_membership(joint_members: pl.DataFrame) -> None:
    """Raise when a joint grouping does not have two participants behind it.

    A joint trip *is* its member trips, and travelling together takes at least
    :data:`~processing.usability.MIN_JOINT_PARTICIPANTS` people. A grouping
    standing for fewer is not joint travel, and anything reading the group as a
    party -- the weighting among them -- would overstate it. Two trips by the
    same person are not a group either, so the count is of people, not rows.

    Raises:
        ValueError: If any grouping has fewer than two distinct participants.
    """
    if "person_id" not in joint_members.columns:
        return

    counts = joint_members.group_by("joint_trip_id").agg(
        pl.col("person_id").n_unique().alias("_n_participants")
    )
    undersized = counts.filter(pl.col("_n_participants") < MIN_JOINT_PARTICIPANTS)
    if undersized.is_empty():
        return

    ids = undersized["joint_trip_id"].to_list()
    preview = ids[:10]
    msg = (
        f"{len(undersized)} joint trip(s) have fewer than {MIN_JOINT_PARTICIPANTS} "
        f"distinct participants. Joint trip IDs: {preview}"
        f"{'...' if len(ids) > len(preview) else ''}"
    )
    raise ValueError(msg)


def validate_against_num_travelers(
    linked_trips: pl.DataFrame,
    log_discrepancies: bool,
) -> None:
    """Validate detected joint trips against reported num_travelers field.

    Compares detected joint trip group sizes with the num_travelers field
    reported in the survey. Logs INFO-level summary and optional DEBUG-level
    discrepancies.

    Args:
        linked_trips: Linked trips with ``num_travelers`` and the detected
            ``joint_trip_id``
        log_discrepancies: If True, log DEBUG-level details for each mismatch
    """
    joint_sizes = (
        linked_trips.filter(pl.col("joint_trip_id").is_not_null())
        .group_by("joint_trip_id")
        .agg([pl.col("linked_trip_id").n_unique().alias("detected_size")])
    )

    # Join back to trips to compare with num_travelers
    trips_with_sizes = linked_trips.join(joint_sizes, on="joint_trip_id", how="left").filter(
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
