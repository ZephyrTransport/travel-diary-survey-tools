"""Reported locations: the survey's own coordinates, as the delivered table.

``habitual_locations`` is delivered by survey cleaning holding only the
reported locations, and :func:`reported_habitual_locations` is how every project
builds it, so the rows and their numbering are the same whoever delivers them.
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from utils.helpers import expr_haversine

from .habitual_location_configs import DEFAULT_BUFFER_METERS
from .numbering import LOCATION_COLUMNS, assign_location_id

logger = logging.getLogger(__name__)

# The survey's reported coordinates: (location type, lat column, lon column).
# Home is the household's; work and school are the person's.
REPORTED_COLUMNS = [
    (LocationType.HOME, "home_lat", "home_lon"),
    (LocationType.WORK, "work_lat", "work_lon"),
    (LocationType.SCHOOL, "school_lat", "school_lon"),
]


def _primaries(households: pl.DataFrame, persons: pl.DataFrame) -> pl.DataFrame:
    """One primary row per person per place the survey reports."""
    people = persons.select(
        "person_id", "hh_id", "work_lat", "work_lon", "school_lat", "school_lon"
    )
    people = people.join(households.select("hh_id", "home_lat", "home_lon"), on="hh_id", how="left")
    return pl.concat(
        [
            people.filter(pl.col(lat).is_not_null() & pl.col(lon).is_not_null()).select(
                pl.col("person_id").cast(pl.Int64),
                pl.lit(kind.value, dtype=pl.Int64).alias("location_type"),
                pl.lit(value=True).alias("is_primary"),
                pl.col(lat).cast(pl.Float64).alias("lat"),
                pl.col(lon).cast(pl.Float64).alias("lon"),
            )
            for kind, lat, lon in REPORTED_COLUMNS
        ],
        how="vertical",
    )


def _drop_near_primary(
    primaries: pl.DataFrame, extra: pl.DataFrame, buffer_meters: float
) -> pl.DataFrame:
    """Drop an extra location within the buffer of its kind's primary.

    Within the buffer the two are the same place as far as any trip end can
    tell, so keeping both would only let GPS noise choose between them.
    """
    near = (
        extra.join(
            primaries.select(
                "person_id",
                "location_type",
                pl.col("lat").alias("_p_lat"),
                pl.col("lon").alias("_p_lon"),
            ),
            on=["person_id", "location_type"],
            how="inner",
        )
        .filter(
            expr_haversine(pl.col("lat"), pl.col("lon"), pl.col("_p_lat"), pl.col("_p_lon"))
            <= buffer_meters
        )
        .select("_order")
    )
    if near.height > 0:
        logger.info(
            "Dropped %d further reported location(s) within %.0f m of the primary of their kind",
            near.height,
            buffer_meters,
        )
    return extra.join(near, on="_order", how="anti")


def reported_habitual_locations(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    extra: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build the delivered ``habitual_locations`` table: every reported location.

    The survey's home (household), work and school (person) coordinates are
    each person's primaries. Any further reported locations — a vendor's
    second home — come in ``extra`` and follow them, in the order given.

    Args:
        households: Households with ``hh_id``, ``home_lat`` and ``home_lon``.
        persons: Persons with ``person_id``, ``hh_id``, ``work_lat/lon`` and
            ``school_lat/lon``.
        extra: Optional further reported locations with ``person_id``,
            ``location_type``, ``lat`` and ``lon``. They are never primary.
            One within ``DEFAULT_BUFFER_METERS`` of its kind's primary is the
            same place and is dropped.

    Returns:
        Table conforming to ``HabitualLocationModel``, all ``REPORTED``.
    """
    primaries = _primaries(households, persons)
    frames = [primaries.with_columns(pl.lit(None, dtype=pl.Int64).alias("_order"))]
    if extra is not None and extra.height > 0:
        further = (
            extra.filter(pl.col("lat").is_not_null() & pl.col("lon").is_not_null())
            .select(
                pl.col("person_id").cast(pl.Int64),
                pl.col("location_type").cast(pl.Int64),
                pl.lit(value=False).alias("is_primary"),
                pl.col("lat").cast(pl.Float64),
                pl.col("lon").cast(pl.Float64),
            )
            .with_row_index("_order")
            .with_columns(pl.col("_order").cast(pl.Int64))
        )
        frames.append(_drop_near_primary(primaries, further, DEFAULT_BUFFER_METERS))

    reported = pl.concat(frames, how="diagonal").with_columns(
        pl.lit(LocationSource.REPORTED.value, dtype=pl.Int64).alias("source")
    )
    return (
        assign_location_id(reported)
        .select(LOCATION_COLUMNS)
        .sort(["person_id", "location_type", "location_num"])
    )
