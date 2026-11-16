"""CT-RAMP ids that carry the survey day.

CT-RAMP has no day dimension, so a multi-day respondent becomes one household
and person record per day, with the day encoded into ``hh_id`` and
``person_id``.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def incorporate_day_into_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
    person_days: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Expand records to person-day level and encode the survey day into CTRAMP IDs.

    CTRAMP treats each model run as a single day. Since BATS respondents have
    multiple survey days, each person-day must appear as a distinct "household"
    and "person" in the CTRAMP input files so tours and trips can be attributed
    to the correct day without double-counting.

    ID encoding (preserves the canonical hierarchical structure):
        - CTRAMP hh_id  = hh_day_id = hh_id * 100 + day_num   (unique per household-day)
        - CTRAMP person_id = person_day_id                (unique per person-day)

    Both formulas are invertible: the original IDs can always be recovered from
    the CTRAMP IDs.

    A household or person expanded to X day-records carries weight W/X on each,
    so the weights still sum to W; a household's days are its household-days,
    not its person-day rows. ``telecommute_time`` is carried onto persons when
    present, as ``format_persons``' work-from-home indicator.

    ``person_days`` is already limited to the profile's usable days (and so to
    its travel days). Tours, linked trips and joint trips are kept only on those
    days, so every one of them joins back to a person record; their order is
    kept.

    Args:
        households: Canonical households (one row per household).
        persons: Canonical persons (one row per person).
        tours: Canonical tours (one row per tour, already has person_day_id).
        linked_trips: Canonical linked trips (one row per trip, already has person_day_id).
        joint_trips: Canonical joint trips (one row per joint trip, with its
            ``hh_day_id``).
        person_days: Canonical person-days with person_id, hh_id, person_day_id and hh_day_id.

    Returns:
        Tuple of (households, persons, tours, linked_trips, joint_trips) where
        each table is expanded to the person-day level and hh_id / person_id
        reflect the day-encoded IDs.
    """
    # original person_id -> (ctramp_person_id, ctramp_hh_id[, telecommute_time])
    _day_cols = [
        pl.col("person_id"),
        pl.col("person_day_id").alias("ctramp_person_id"),
        pl.col("hh_day_id").alias("ctramp_hh_id"),
    ]
    if "telecommute_time" in person_days.columns:
        _day_cols.append(pl.col("telecommute_time"))
    person_day_map = person_days.select(_day_cols)

    # Days per household and per person, to divide the weights by.
    hh_day_unique = person_days.select("hh_id", pl.col("hh_day_id").alias("ctramp_hh_id")).unique()
    hh_num_days = hh_day_unique.group_by("hh_id").agg(pl.len().alias("num_hh_days"))
    person_num_days = person_days.group_by("person_id").agg(pl.len().alias("num_person_days"))

    # Build a household-day map: original hh_id → [ctramp_hh_id, num_hh_days]
    hh_day_map = hh_day_unique.join(hh_num_days, on="hh_id")

    n_hh_before = len(households)
    n_per_before = len(persons)

    # Expand households: one row per household-day; scale hh_weight accordingly
    households = (
        households.join(hh_day_map, on="hh_id", how="inner")
        .drop("hh_id")
        .rename({"ctramp_hh_id": "hh_id"})
    )
    if "hh_weight" in households.columns:
        households = households.with_columns(
            (pl.col("hh_weight") / pl.col("num_hh_days")).alias("hh_weight")
        )
    households = households.drop("num_hh_days")

    # Expand persons: one row per person-day; scale person_weight accordingly
    persons = (
        persons.join(person_day_map, on="person_id", how="inner")
        .join(person_num_days, on="person_id", how="left")
        .drop(["hh_id", "person_id"])
        .rename({"ctramp_hh_id": "hh_id", "ctramp_person_id": "person_id"})
    )
    if "person_weight" in persons.columns:
        persons = persons.with_columns(
            (pl.col("person_weight") / pl.col("num_person_days")).alias("person_weight")
        )
    persons = persons.drop("num_person_days")

    logger.info(
        "Expanded to person-day level: %d households → %d household-days, "
        "%d persons → %d person-days",
        n_hh_before,
        len(households),
        n_per_before,
        len(persons),
    )

    valid_days = person_days.select("person_day_id", "hh_day_id")

    def to_day_ids(df: pl.DataFrame) -> pl.DataFrame:
        """Keep *df* on the usable days, with the day-encoded hh_id and person_id."""
        if len(df) == 0 or "person_day_id" not in df.columns:
            return df
        return (
            df.join(valid_days, on="person_day_id", how="inner", maintain_order="left")
            .with_columns(
                pl.col("hh_day_id").alias("hh_id"), pl.col("person_day_id").alias("person_id")
            )
            .drop("hh_day_id")
        )

    tours = to_day_ids(tours)
    linked_trips = to_day_ids(linked_trips)

    # A joint trip belongs to its household-day.
    if len(joint_trips) > 0:
        joint_trips = joint_trips.join(
            valid_days.select("hh_day_id").unique(), on="hh_day_id", how="semi"
        ).with_columns(pl.col("hh_day_id").alias("hh_id"))

    return households, persons, tours, linked_trips, joint_trips
