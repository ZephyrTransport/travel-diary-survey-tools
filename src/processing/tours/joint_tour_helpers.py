"""Joint tour identification for linking individual tours taken together.

This module identifies which individual tours are "joint" - where all trips
throughout the tour involve the same stable group of 2+ household members.
Joint tours are linked via joint_tour_id while maintaining individual tour
records in the canonical data model.

Algorithm:
----------
1. Find eligible tours: every trip in the tour is joint, and there are 2+ trips
2. Count the tour's stable group: participants present on ALL of its joint trips
3. Keep tours whose stable group has 2+ people
4. Assign joint_tour_id per occasion: tours sharing the same set of
   joint_trip_ids are the same outing and get the same id
5. Drop singletons: an id held by only one person is not a joint tour

Every step works off ``linked_trips``' existing (person_id, tour_id,
joint_trip_id) relation, so participation stays a row-per-fact join rather than
being collapsed into list columns.

Example:
--------
Person A, B, C depart together (joint_trip_id=1001):
  - Trip 1: A,B,C -> shopping (joint_trip_id=1001)
  - Trip 2: A,B,C -> home (joint_trip_id=1001)
  Result: A, B, C all get same joint_tour_id

Person A, B, C depart, C drops off:
  - Trip 1: A,B,C -> school (joint_trip_id=1001)
  - Trip 2: A,B -> home (joint_trip_id=1002)
  Result: A and B get joint_tour_id (stable pair), C gets NULL

The same pair on two separate outings get two different joint_tour_ids, because
the outings have different joint_trip_ids.
"""

import logging

import polars as pl

from utils.create_ids import create_concatenated_id

logger = logging.getLogger(__name__)

# A tour needs at least this many trips to establish a pattern of travelling together
MIN_TRIPS_FOR_JOINT_TOUR = 2
# A joint tour needs at least this many participants, by definition
MIN_JOINT_TOUR_PARTICIPANTS = 2


def identify_joint_tours(
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Identify joint tours by analyzing stable participant groups.

    Tours are considered joint if all constituent trips involve the same
    stable group of 2+ household members throughout the entire tour.

    Args:
        linked_trips: Trip data with tour_id and joint_trip_id
        tours: Tour-level records to update with joint_tour_id

    Returns:
        Tuple of (updated_linked_trips, updated_tours) with joint_tour_id
        column added
    """
    logger.info("Identifying joint tours from joint trips...")

    trips_with_tours = linked_trips.filter(pl.col("tour_id").is_not_null())
    joint_trip_members = trips_with_tours.filter(pl.col("joint_trip_id").is_not_null())

    if joint_trip_members.is_empty():
        logger.info("No joint trips found, skipping joint tour identification")
        return _without_joint_tours(linked_trips, tours)

    eligible_tours = _find_eligible_tours(trips_with_tours, joint_trip_members)

    if eligible_tours.is_empty():
        logger.info("No tours where all trips are joint, skipping joint tour identification")
        return _without_joint_tours(linked_trips, tours)

    # One row per (tour, joint_trip_id). This is the relation linked_trips
    # already holds; keep it long rather than collapsing to a list per tour.
    tour_joint_trips = (
        joint_trip_members.join(eligible_tours, on=["person_id", "tour_id"], how="inner")
        .select(["person_id", "tour_id", "hh_id", "joint_trip_id"])
        .unique()
    )

    stable_groups = _count_stable_participants(tour_joint_trips, joint_trip_members)
    valid_joint_tours = stable_groups.filter(
        pl.col("stable_group_size") >= MIN_JOINT_TOUR_PARTICIPANTS
    )

    if valid_joint_tours.is_empty():
        logger.info("No stable joint tour groups found")
        return _without_joint_tours(linked_trips, tours)

    tours_with_joint_id = _assign_joint_tour_ids(
        tour_joint_trips.join(
            valid_joint_tours.select(["person_id", "tour_id"]),
            on=["person_id", "tour_id"],
            how="inner",
        )
    )
    tours_with_joint_id = _drop_singleton_joint_tours(tours_with_joint_id)

    # Join joint_tour_id back to original tables
    linked_trips = linked_trips.join(
        tours_with_joint_id.select(["person_id", "tour_id", "joint_tour_id"]),
        on=["person_id", "tour_id"],
        how="left",
    )

    tours = tours.join(
        tours_with_joint_id.select(["tour_id", "joint_tour_id"]),
        on="tour_id",
        how="left",
    )

    _validate_joint_tours_have_joint_trips(linked_trips)

    joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
    logger.info(
        "Identified %d individual tours as joint (%d unique joint tour groups)",
        len(joint_tours),
        joint_tours.select("joint_tour_id").n_unique(),
    )

    return linked_trips, tours


def _validate_joint_tours_have_joint_trips(linked_trips: pl.DataFrame) -> None:
    """Raise when a tour is marked joint but holds no joint trip at all.

    ``joint_tour_id`` is only meaningful as a claim about shared travel, so a
    tour carrying one while none of its trips was shared is self-contradictory
    however the id is defined.

    Deliberately weaker than the rule this module currently assigns ids under.
    Step 1 admits a tour only when *every* trip is joint, which suits a model
    that has no way to express a half-shared tour, but a survey does: a parent
    who drops a child and drives on to work made one tour, part of it together.
    Should ``joint_tour_id`` later be widened to cover those, this check still
    holds -- so it does not have to be revisited to make that change, and
    consumers needing the stricter reading enforce it themselves.

    Raises:
        ValueError: If any joint tour has no joint trip among its trips.
    """
    required = {"joint_tour_id", "joint_trip_id"}
    if not required.issubset(linked_trips.columns):
        return

    empty_tours = (
        linked_trips.filter(pl.col("joint_tour_id").is_not_null())
        .group_by("joint_tour_id")
        .agg(pl.col("joint_trip_id").is_not_null().any().alias("_has_joint_trip"))
        .filter(~pl.col("_has_joint_trip"))
    )
    if empty_tours.is_empty():
        return

    tour_ids = empty_tours["joint_tour_id"].to_list()
    preview = tour_ids[:10]
    msg = (
        f"{len(tour_ids)} tour(s) carry a joint_tour_id but hold no joint trip; "
        f"a tour cannot be shared travel when none of its trips was shared. "
        f"Joint tour IDs: {preview}"
        f"{'...' if len(tour_ids) > len(preview) else ''}"
    )
    raise ValueError(msg)


def build_joint_tours_table(tours: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical ``joint_tours`` table from tours carrying a joint id.

    One row per ``joint_tour_id``, mirroring how ``build_joint_trips_table``
    collapses member trips into a joint trip. The member tours stay authoritative
    for everything tour-shaped; this table exists so the group itself can be
    counted, validated and weighted once.

    Args:
        tours: Canonical tours, after ``identify_joint_tours`` has stamped
            ``joint_tour_id``.

    Returns:
        One row per joint tour with ``hh_id``, ``day_id``, ``num_participants``
        and, where the member tours carry it, ``complete``.
    """
    schema = {
        "joint_tour_id": pl.Int64,
        "hh_id": pl.Int64,
        "day_id": pl.Int64,
        "num_participants": pl.Int64,
    }
    if "joint_tour_id" not in tours.columns:
        return pl.DataFrame(schema=schema)

    members = tours.filter(pl.col("joint_tour_id").is_not_null())
    if members.is_empty():
        return pl.DataFrame(schema=schema)

    agg = [
        pl.col("hh_id").first(),
        pl.col("day_id").first(),
        pl.col("person_id").n_unique().alias("num_participants"),
    ]
    # A joint tour is only as complete as its least complete member.
    if "complete" in members.columns:
        agg.append(pl.all("complete").alias("complete"))

    joint_tours = members.group_by("joint_tour_id").agg(agg).sort("joint_tour_id")

    logger.info(
        "Built %d joint tours from %d member tours",
        joint_tours.height,
        members.height,
    )
    return joint_tours


def _without_joint_tours(
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Add a null joint_tour_id column to both tables.

    Args:
        linked_trips: Trip data
        tours: Tour-level records

    Returns:
        Tuple of (linked_trips, tours) each with a null joint_tour_id column
    """
    null_id = pl.lit(None, dtype=pl.Int64).alias("joint_tour_id")
    return linked_trips.with_columns(null_id), tours.with_columns(null_id)


def _find_eligible_tours(
    trips_with_tours: pl.DataFrame,
    joint_trip_members: pl.DataFrame,
) -> pl.DataFrame:
    """Find tours where every trip is joint and there are enough trips.

    A tour only counts if all of its trips are joint - a tour with a solo leg is
    not a joint tour. Single-trip tours are excluded because one trip cannot
    establish a pattern of travelling together.

    Args:
        trips_with_tours: All trips that belong to a tour
        joint_trip_members: The subset of those trips that are joint

    Returns:
        DataFrame of eligible (person_id, tour_id)
    """
    total_trips = trips_with_tours.group_by(["person_id", "tour_id"]).agg(
        pl.col("linked_trip_id").count().alias("_total_trips")
    )
    joint_trips = joint_trip_members.group_by(["person_id", "tour_id"]).agg(
        pl.col("linked_trip_id").count().alias("_joint_trips")
    )

    return (
        joint_trips.join(total_trips, on=["person_id", "tour_id"], how="left")
        .filter(
            (pl.col("_joint_trips") == pl.col("_total_trips"))
            & (pl.col("_total_trips") >= MIN_TRIPS_FOR_JOINT_TOUR)
        )
        .select(["person_id", "tour_id"])
    )


def _count_stable_participants(
    tour_joint_trips: pl.DataFrame,
    joint_trip_members: pl.DataFrame,
) -> pl.DataFrame:
    """Count the people present on ALL of a tour's joint trips.

    The stable group is the intersection of participants across the tour's joint
    trips. Expressed relationally, a person is in it when the number of the
    tour's joint trips they appear on equals the tour's joint trip count - so
    someone who drops off partway is excluded.

    Args:
        tour_joint_trips: One row per (person_id, tour_id, hh_id, joint_trip_id)
        joint_trip_members: Joint trips, used to resolve who was on each one

    Returns:
        DataFrame with person_id, tour_id, hh_id, stable_group_size
    """
    # Who was on each joint trip - the inverse of linked_trips.joint_trip_id
    participants = joint_trip_members.select(
        "joint_trip_id",
        pl.col("person_id").alias("_participant"),
    ).unique()

    trips_per_tour = tour_joint_trips.group_by(["person_id", "tour_id"]).agg(
        pl.col("joint_trip_id").n_unique().alias("_n_trips")
    )

    return (
        tour_joint_trips.join(participants, on="joint_trip_id", how="left")
        .group_by(["person_id", "tour_id", "hh_id", "_participant"])
        .agg(pl.col("joint_trip_id").n_unique().alias("_n_present"))
        .join(trips_per_tour, on=["person_id", "tour_id"], how="left")
        .filter(pl.col("_n_present") == pl.col("_n_trips"))
        .group_by(["person_id", "tour_id", "hh_id"])
        .agg(pl.col("_participant").n_unique().alias("stable_group_size"))
    )


def _assign_joint_tour_ids(
    tour_joint_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Assign joint_tour_id to tours sharing the same joint trip occasion.

    Tours that share the exact same set of joint_trip_ids are the same occasion
    and get the same joint_tour_id. The same stable group on a different
    occasion has different joint_trip_ids and so a different joint_tour_id.
    IDs follow the pattern ``<hh_id><2-digit-sequence>``.

    The occasion key is the tour's sorted joint_trip_ids joined into a string,
    which is unique per set because the ids are sorted and delimited.

    Args:
        tour_joint_trips: One row per (person_id, tour_id, hh_id, joint_trip_id)

    Returns:
        DataFrame with person_id, tour_id, joint_tour_id
    """
    tours_with_group_key = tour_joint_trips.group_by(["person_id", "tour_id", "hh_id"]).agg(
        pl.col("joint_trip_id").unique().sort().cast(pl.String).str.join("_").alias("group_key")
    )

    # Assign a sequential id to each unique occasion within the household
    unique_groups = (
        tours_with_group_key.select(["hh_id", "group_key"]).unique().sort(["hh_id", "group_key"])
    )
    unique_groups = unique_groups.with_columns(
        pl.col("group_key").rank(method="dense").over("hh_id").alias("joint_tour_num")
    )
    unique_groups = create_concatenated_id(
        unique_groups,
        output_col="joint_tour_id",
        parent_id_col="hh_id",
        sequence_col="joint_tour_num",
        sequence_padding=2,
    )

    return tours_with_group_key.join(
        unique_groups.select(["hh_id", "group_key", "joint_tour_id"]),
        on=["hh_id", "group_key"],
        how="left",
    ).select(["person_id", "tour_id", "joint_tour_id"])


def _drop_singleton_joint_tours(
    tours_with_joint_id: pl.DataFrame,
) -> pl.DataFrame:
    """Null out any joint_tour_id shared by fewer than two persons.

    In edge cases only one person's tour survives the eligibility filters for a
    given occasion, which would leave a joint tour with a single participant.

    Args:
        tours_with_joint_id: DataFrame with person_id, tour_id, joint_tour_id

    Returns:
        The same frame with singleton joint_tour_ids set to null
    """
    shared_ids = (
        tours_with_joint_id.group_by("joint_tour_id")
        .agg(pl.col("person_id").n_unique().alias("_n_persons"))
        .filter(pl.col("_n_persons") >= MIN_JOINT_TOUR_PARTICIPANTS)
        .select("joint_tour_id")
        .with_columns(pl.lit(value=True).alias("_shared"))
    )

    return (
        tours_with_joint_id.join(shared_ids, on="joint_tour_id", how="left")
        .with_columns(
            pl.when(pl.col("_shared").fill_null(value=False))
            .then(pl.col("joint_tour_id"))
            .otherwise(pl.lit(None, dtype=pl.Int64))
            .alias("joint_tour_id")
        )
        .drop("_shared")
    )
