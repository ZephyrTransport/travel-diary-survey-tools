"""Primacy, numbering and identifiers of habitual locations.

Reported locations are numbered when the table is delivered: primary first, then
in the order supplied. Observed ones are numbered after them by the detection
step, which never renumbers a delivered row, so a location's identifier is fixed
from the moment it enters the table.
"""

import polars as pl

from utils.create_ids import create_concatenated_id

# location_num is packed into habitual_location_id as two digits, so a person
# cannot hold more than this many locations of one kind.
_MAX_LOCATION_NUM = 99

# Table columns in schema order (see HabitualLocationModel).
LOCATION_COLUMNS = [
    "habitual_location_id",
    "person_id",
    "location_type",
    "location_num",
    "is_primary",
    "lat",
    "lon",
    "source",
]


def _mint_ids(numbered: pl.DataFrame) -> pl.DataFrame:
    """Pack the kind and number onto the person: ``person * 1000 + type * 100 + num``."""
    overflow = numbered.filter(pl.col("location_num") > _MAX_LOCATION_NUM)
    if overflow.height > 0:
        msg = (
            f"{overflow.height} location(s) exceed location_num "
            f"{_MAX_LOCATION_NUM}, which habitual_location_id cannot encode."
        )
        raise ValueError(msg)
    numbered = numbered.with_columns(
        (pl.col("location_type") * 100 + pl.col("location_num")).alias("_location_suffix")
    )
    return create_concatenated_id(
        numbered,
        output_col="habitual_location_id",
        parent_id_col="person_id",
        sequence_col="_location_suffix",
        sequence_padding=3,
    ).drop("_location_suffix")


def assign_location_id(locations: pl.DataFrame) -> pl.DataFrame:
    """Number reported locations within each (person, kind) and mint their identifier.

    Primary first, then in the order supplied (``_order``). Nothing here depends
    on a threshold, so retuning the rules does not renumber anything.

    Args:
        locations: Reported locations with ``person_id``, ``location_type``,
            ``is_primary``, ``lat``, ``lon`` and ``_order``.

    Returns:
        The locations with ``location_num`` and ``habitual_location_id``.

    Raises:
        ValueError: If a person has more locations of one kind than the
            identifier can encode.
    """
    numbered = (
        locations.with_columns(
            (~pl.col("is_primary").fill_null(value=False)).cast(pl.Int8).alias("_primary_order")
        )
        .sort(
            ["person_id", "location_type", "_primary_order", "_order", "lat", "lon"],
            nulls_last=True,
        )
        .with_columns(
            (pl.int_range(0, pl.len()).over(["person_id", "location_type"]) + 1).alias(
                "location_num"
            )
        )
        .drop("_primary_order", "_order")
    )
    return _mint_ids(numbered)


def number_observed(observed: pl.DataFrame, delivered: pl.DataFrame) -> pl.DataFrame:
    """Settle primacy of observed locations and number them after the delivered ones.

    An observed workplace or school is *not* primary (``False``) when the
    person has a reported primary of the same kind, and *unknown* (``None``)
    when they have none — e.g. a multi-site worker with no single usual
    workplace. Observed homes are never primary and arrive saying so.

    Numbering continues from the highest delivered number of that person and
    kind, in the order the person was first seen at each place, coordinates
    settling a tie.

    Args:
        observed: Observed locations with ``person_id``, ``location_type``,
            ``is_primary``, ``lat``, ``lon``, ``source`` and ``_first_seen``.
        delivered: The delivered table.

    Returns:
        The observed locations as table rows.
    """
    kinds = delivered.group_by("person_id", "location_type").agg(
        pl.col("location_num").max().alias("_last_num"),
        pl.col("is_primary").any().alias("_has_primary"),
    )
    numbered = (
        observed.join(kinds, on=["person_id", "location_type"], how="left")
        .with_columns(
            pl.when(pl.col("is_primary").is_not_null())
            .then(pl.col("is_primary"))
            .when(pl.col("_has_primary").fill_null(value=False))
            .then(pl.lit(value=False))
            .otherwise(pl.lit(None, dtype=pl.Boolean))
            .alias("is_primary")
        )
        .sort(["person_id", "location_type", "_first_seen", "lat", "lon"], nulls_last=True)
        .with_columns(
            (
                pl.col("_last_num").fill_null(0)
                + pl.int_range(1, pl.len() + 1).over(["person_id", "location_type"])
            ).alias("location_num")
        )
        .drop("_last_num", "_has_primary", "_first_seen")
    )
    return _mint_ids(numbered).select(LOCATION_COLUMNS)
