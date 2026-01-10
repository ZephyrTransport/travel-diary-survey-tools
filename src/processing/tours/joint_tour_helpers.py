"""Joint tour identification for linking individual tours taken together.

This module identifies which individual tours are "joint" - where all trips
throughout the tour involve the same stable group of 2+ household members.
Joint tours are linked via joint_tour_id while maintaining individual tour
records in the canonical data model.

Algorithm:
----------
1. For each person's tour, collect all joint_trip_ids and extract participants
2. Check if a stable group of 2+ people exists for ALL trips in the tour
3. If so, assign joint_tour_id linking those individual tours together
4. Handle partial dropoffs: if 3 people start but 1 drops off, remaining 2
   can still form a joint tour if they stay together for the entire tour

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
"""

import logging

import polars as pl

from utils.create_ids import create_concatenated_id

logger = logging.getLogger(__name__)


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

    # Filter to only trips that are part of tours
    trips_with_tours = linked_trips.filter(pl.col("tour_id").is_not_null())

    # Count total trips per (person_id, tour_id)
    total_trips_per_tour = trips_with_tours.group_by(["person_id", "tour_id"]).agg(
        [pl.col("linked_trip_id").count().alias("total_num_trips")]
    )

    # Filter to only trips that are joint
    joint_trip_members = trips_with_tours.filter(pl.col("joint_trip_id").is_not_null())

    if len(joint_trip_members) == 0:
        logger.info("No joint trips found, skipping joint tour identification")
        # Add null joint_tour_id columns
        linked_trips = linked_trips.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("joint_tour_id")
        )
        tours = tours.with_columns(pl.lit(None, dtype=pl.Int64).alias("joint_tour_id"))
        return linked_trips, tours

    # For each (person_id, tour_id), collect all joint_trip_ids
    tour_joint_trips = joint_trip_members.group_by(["person_id", "tour_id"]).agg(
        [
            pl.col("hh_id").first(),
            pl.col("joint_trip_id").unique().alias("joint_trip_ids"),
            pl.col("linked_trip_id").count().alias("num_joint_trips"),
        ]
    )

    # Join with total trip counts to filter tours where ALL trips are joint
    tour_joint_trips_before_filter = tour_joint_trips.join(
        total_trips_per_tour, on=["person_id", "tour_id"], how="left"
    )

    tour_joint_trips = tour_joint_trips_before_filter.filter(
        # Only keep tours where all trips are joint
        pl.col("num_joint_trips") == pl.col("total_num_trips")
    )

    # Also filter out single-trip tours - they can't form meaningful joint tours
    # a tour needs at least 2 trips to establish a pattern of traveling together
    tour_joint_trips = tour_joint_trips.filter(
        pl.col("total_num_trips") >= 2  # noqa: PLR2004
    )

    if len(tour_joint_trips) == 0:
        logger.info("No tours where all trips are joint, skipping joint tour identification")
        linked_trips = linked_trips.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("joint_tour_id")
        )
        tours = tours.with_columns(pl.lit(None, dtype=pl.Int64).alias("joint_tour_id"))
        return linked_trips, tours

    # Get participants for each joint_trip_id from the joint_trips table
    # We need to extract who was on each joint trip
    joint_trip_participants = _extract_joint_trip_participants(joint_trip_members)

    # For each tour, check if there's a stable group throughout
    tour_stable_groups = _find_stable_groups_per_tour(tour_joint_trips, joint_trip_participants)

    # Filter to tours with stable groups of 2+ people
    valid_joint_tours = tour_stable_groups.filter(
        pl.col("stable_group_size") >= 2  # noqa: PLR2004
    )

    if len(valid_joint_tours) == 0:
        logger.info("No stable joint tour groups found")
        linked_trips = linked_trips.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("joint_tour_id")
        )
        tours = tours.with_columns(pl.lit(None, dtype=pl.Int64).alias("joint_tour_id"))
        return linked_trips, tours

    # Assign joint_tour_id to tours sharing the same stable group
    tours_with_joint_id = _assign_joint_tour_ids(valid_joint_tours)

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

    num_joint_tours = len(tours.filter(pl.col("joint_tour_id").is_not_null()))
    num_unique_joint_tour_ids = (
        tours.filter(pl.col("joint_tour_id").is_not_null()).select("joint_tour_id").n_unique()
    )
    logger.info(
        "Identified %d individual tours as joint (%d unique joint tour groups)",
        num_joint_tours,
        num_unique_joint_tour_ids,
    )

    return linked_trips, tours


def _extract_joint_trip_participants(
    joint_trip_members: pl.DataFrame,
) -> pl.DataFrame:
    """Extract participant list for each joint_trip_id.

    Args:
        joint_trip_members: Trips with joint_trip_id

    Returns:
        DataFrame with joint_trip_id and sorted participant person_ids
    """
    # Get all person_ids for each joint_trip_id
    participants = joint_trip_members.group_by("joint_trip_id").agg(
        [
            pl.col("person_id").unique().sort().alias("participants"),
        ]
    )

    return participants


def _find_stable_groups_per_tour(
    tour_joint_trips: pl.DataFrame,
    joint_trip_participants: pl.DataFrame,
) -> pl.DataFrame:
    """Find stable participant groups for each person's tour.

    A stable group exists if the same set of 2+ people participate in
    ALL joint trips throughout the tour.

    Args:
        tour_joint_trips: Tours with their joint_trip_ids
        joint_trip_participants: Participants for each joint_trip_id

    Returns:
        DataFrame with person_id, tour_id, stable_group (sorted list),
        stable_group_size
    """
    # Explode joint_trip_ids to one row per (tour, joint_trip_id)
    tour_trips_exploded = tour_joint_trips.explode("joint_trip_ids")

    # Join participants for each joint_trip
    tour_trips_with_participants = tour_trips_exploded.join(
        joint_trip_participants,
        left_on="joint_trip_ids",
        right_on="joint_trip_id",
        how="left",
    )

    # For each tour, find intersection of participants across all trips
    # This gives us the stable group (people present in ALL trips)
    stable_groups = (
        tour_trips_with_participants.group_by(["person_id", "tour_id", "hh_id"])
        .agg(
            [
                # Collect all participant lists
                pl.col("participants").alias("all_trip_participants"),
            ]
        )
        .with_columns(
            [
                # Find intersection: people present in ALL trips
                pl.col("all_trip_participants")
                .map_elements(
                    lambda lists: sorted(
                        set.intersection(*[set(lst) for lst in lists]) if len(lists) > 0 else set()
                    ),
                    return_dtype=pl.List(pl.Int64),
                )
                .alias("stable_group"),
            ]
        )
        .with_columns(
            [
                pl.col("stable_group").list.len().alias("stable_group_size"),
            ]
        )
    )

    return stable_groups


def _assign_joint_tour_ids(
    valid_joint_tours: pl.DataFrame,
) -> pl.DataFrame:
    """Assign joint_tour_id to tours sharing the same stable group.

    Tours with identical stable groups (same people) get the same
    joint_tour_id. IDs follow pattern: <hh_id><2-digit-sequence>

    Args:
        valid_joint_tours: Tours with stable groups of 2+ people

    Returns:
        DataFrame with person_id, tour_id, joint_tour_id
    """
    # Create a canonical group identifier (sorted participant list as string)
    # This lets us group tours with identical participants
    tours_with_group_key = valid_joint_tours.with_columns(
        [
            pl.col("stable_group")
            .map_elements(
                lambda x: "_".join(map(str, sorted(x))),
                return_dtype=pl.String,
            )
            .alias("group_key"),
        ]
    )

    # Assign sequential ID to each unique group within household
    unique_groups = (
        tours_with_group_key.select(["hh_id", "group_key"]).unique().sort(["hh_id", "group_key"])
    )

    unique_groups = unique_groups.with_columns(
        pl.col("group_key").rank(method="dense").over("hh_id").alias("joint_tour_num")
    )

    # Create standardized joint_tour_id: <hh_id> + <2 digit enumerator>
    unique_groups = create_concatenated_id(
        unique_groups,
        output_col="joint_tour_id",
        parent_id_col="hh_id",
        sequence_col="joint_tour_num",
        sequence_padding=2,
    )

    # Join back to tours
    tours_with_joint_id = tours_with_group_key.join(
        unique_groups.select(["hh_id", "group_key", "joint_tour_id"]),
        on=["hh_id", "group_key"],
        how="left",
    )

    return tours_with_joint_id.select(["person_id", "tour_id", "joint_tour_id"])
