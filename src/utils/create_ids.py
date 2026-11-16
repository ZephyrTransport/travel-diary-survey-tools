"""Helper functions for creating globally unique IDs in the pipeline.

This module provides utilities for creating composite IDs from hierarchical
identifier columns. IDs follow a hierarchical concatenation structure:
    hh_id (8 digits) -> person_id (+ 2 digits) -> person_day_id (+ 2 digits) ->
    linked_trip_id (+ 2 digits) -> etc.

Each level is created by appending zero-padded digits to the parent level
(``parent * 10**padding + sequence``), forming an integer that uniquely
identifies entities at each level of the hierarchy.

ID vs Num Columns:
    - ID columns (e.g., person_day_id, person_id): Pre-concatenated hierarchical
      identifiers that contain the full path from root (hh_id) to current level.
      Multiple rows may share the same parent ID (e.g., multiple trips per day).
    - Num columns (e.g., day_num, person_num): Sequence numbers within a parent
      scope. These reset for each new parent (e.g., each person has
      day_num 1, 2, 3).
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def create_concatenated_id(
    df: pl.DataFrame,
    output_col: str,
    parent_id_col: str,
    sequence_col: str,
    sequence_padding: int = 2,
) -> pl.DataFrame:
    """Create hierarchical ID by concatenating parent ID with padded sequence.

    This function creates IDs following a hierarchical structure where each
    level is formed by appending zero-padded sequence numbers to the parent:
        - hh_id: "23000075" [Can variable length, typically 8 digits]
        - person_id: "2300007501" [10-digits] (hh_id + 2-digit person_num)
        - person_day_id: "230000750101" [12-digits] (person_id + 2-digit day_num)
        - unlinked_trip_id: "23000075010101" [14-digits]
            (person_day_id + 2-digit trip_num)
        - linked_trip_id: "23000075010102" [14-digits]
            (person_day_id + 2-digit trip_num)
        - tour_id: "2300007501011010" [16-digits]
            (person_day_id + 4-digit tour_num * 1000 + subtour_num * 10)

    The parent_id_col may already contain the full hierarchical path
    (pre-concatenated), making this a simple append operation.

    Args:
        df: Input DataFrame
        output_col: Name for the new ID column to create
        parent_id_col: Column containing the parent ID (may be pre-concatenated)
        sequence_col: Column containing sequence number at this level
        sequence_padding: Number of digits for zero-padding sequence
            (default: 2)

    Returns:
        DataFrame with new ID column added

    Examples:
        # Create person_id from hh_id + person_num
        >>> df = create_concatenated_id(
        ...     df,
        ...     output_col="person_id",
        ...     parent_id_col="hh_id",
        ...     sequence_col="person_num",
        ...     sequence_padding=2
        ... )

        # Create person_day_id from person_id + day_num
        >>> df = create_concatenated_id(
        ...     df,
        ...     output_col="person_day_id",
        ...     parent_id_col="person_id",
        ...     sequence_col="day_num",
        ...     sequence_padding=2
        ... )

        # Create linked_trip_id from person_day_id + linked_trip_num
        >>> df = create_concatenated_id(
        ...     df,
        ...     output_col="linked_trip_id",
        ...     parent_id_col="person_day_id",
        ...     sequence_col="linked_trip_num",
        ...     sequence_padding=2
        ... )
    """
    if df.is_empty():
        logger.info(
            "Empty DataFrame: adding %s column with null values",
            output_col,
        )
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias(output_col))

    logger.info(
        "Creating %s = %s + %s (padded to %d digits)",
        output_col,
        parent_id_col,
        sequence_col,
        sequence_padding,
    )

    id_expr = pl.col(parent_id_col).cast(pl.Int64) * (10**sequence_padding) + pl.col(
        sequence_col
    ).cast(pl.Int64)

    df = df.with_columns(id_expr.alias(output_col))

    null_count = df[output_col].null_count()
    if null_count > 0:
        msg = f"Created ID column '{output_col}' contains {null_count} null values."
        raise ValueError(msg)

    return df


def create_linked_trip_id(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Create linked_trip_id by appending linked_trip_num to person_day_id.

    Assumes person_day_id is already pre-concatenated with the full hierarchy
    (hh_id + person_num + day_num).

    Args:
        linked_trips: Input DataFrame containing person_day_id and linked_trip_num

    Returns:
        DataFrame with "linked_trip_id" column added

    Example:
        >>> trips = trips.with_columns(
        ...     pl.col("new_trip_flag")
        ...       .cum_sum()
        ...       .over("person_id")
        ...       .alias("linked_trip_num")
        ... )
        >>> trips = create_linked_trip_id(trips)
    """
    return create_concatenated_id(
        linked_trips,
        output_col="linked_trip_id",
        parent_id_col="person_day_id",
        sequence_col="linked_trip_num",
        sequence_padding=2,
    )


def create_tour_ids(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Create tour_id and parent_tour_id from person_day_id and tour numbers.

    Creates hierarchical tour identifiers by combining person_day_id with tour_num
    and subtour_num. The tour_id includes both tour and subtour information,
    while parent_tour_id only includes the tour number (for linking subtours
    to their parent tour).

    Args:
        linked_trips: Input DataFrame containing person_day_id, tour_num and subtour_num

    Returns:
        DataFrame with "tour_id" and "parent_tour_id" columns added

    Example:
        >>> linked_trips = create_tour_ids(linked_trips)
    """
    linked_trips = linked_trips.with_columns(
        (pl.col("tour_num") * 1000 + pl.col("subtour_num") * 10).alias("_tour_id_suffix"),
        (pl.col("tour_num") * 1000).alias("_parent_tour_id_suffix"),
    )
    for output_col, suffix in (
        ("tour_id", "_tour_id_suffix"),
        ("parent_tour_id", "_parent_tour_id_suffix"),
    ):
        linked_trips = create_concatenated_id(
            linked_trips,
            output_col=output_col,
            parent_id_col="person_day_id",
            sequence_col=suffix,
            sequence_padding=4,
        )
    return linked_trips.drop("_tour_id_suffix", "_parent_tour_id_suffix")


def create_household_days(person_days: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build the household-day table from person-days, and put ``hh_day_id`` on each.

    A household-day is one household's travel date: every member's day on it.
    Its id is ``hh_id * 100 + day_num``, the household-level twin of the
    person-day id (``person_id * 100 + day_num``), so a household's day numbers
    must name its dates one to one. A survey whose members number the same date
    differently, or reuse a number for two dates, would have its household-days
    merged or split by the encoding, so that raises rather than guessing.

    Args:
        person_days: Person-days with ``hh_id``, ``day_num`` and ``travel_date``.

    Returns:
        The person-days with ``hh_day_id`` added, and one row per household-day
        holding ``hh_day_id``, ``hh_id`` and ``travel_date``.

    Raises:
        ValueError: If a household's day numbers and dates are not one to one.
    """
    pairs = person_days.select("hh_id", "day_num", "travel_date").unique()
    for key, other in (("day_num", "travel_date"), ("travel_date", "day_num")):
        clashes = pairs.group_by("hh_id", key).agg(pl.len().alias("_n")).filter(pl.col("_n") > 1)
        if not clashes.is_empty():
            msg = (
                f"{clashes.height} household {key}(s) map to more than one {other}, so "
                f"hh_id * 100 + day_num cannot name household-days. First: "
                f"{clashes.head(3).select('hh_id', key).to_dicts()}"
            )
            raise ValueError(msg)

    hh_day_id = (pl.col("hh_id") * 100 + pl.col("day_num")).cast(pl.Int64).alias("hh_day_id")
    household_days = pairs.select(hh_day_id, "hh_id", "travel_date").sort("hh_day_id")
    return person_days.with_columns(hh_day_id), household_days
