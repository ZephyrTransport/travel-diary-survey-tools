"""Utility functions for trip linking."""

import logging

import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def datetime_from_parts(
    date: pl.Series,
    hour: pl.Series,
    minute: pl.Series,
    second: pl.Series,
) -> pl.Series:
    """Construct datetime from date and time parts."""
    return pl.concat_str([
        date,
        pl.lit("T"),
        hour.cast(pl.Utf8).str.pad_start(2, "0"),
        pl.lit(":"),
        minute.cast(pl.Utf8).str.pad_start(2, "0"),
        pl.lit(":"),
        second.cast(pl.Utf8).str.pad_start(2, "0"),
    ]).str.to_datetime()

def add_time_columns(trips: pl.DataFrame) -> pl.DataFrame:
    """Add datetime columns for departure and arrival times if missing."""
    logger.info("Adding datetime columns...")

    suffixes = ["date", "hour", "minute", "seconds"]
    d_cols = [f"depart_{s}" for s in suffixes]
    a_cols = [f"arrive_{s}" for s in suffixes]

    if "depart_time" not in trips.columns:
        logger.info("Constructing depart_time...")
        trips = trips.with_columns([
            datetime_from_parts(*[pl.col(c) for c in d_cols])
            .alias("depart_time"),
        ])
    if "arrive_time" not in trips.columns:
        logger.info("Constructing arrive_time...")
        trips = trips.with_columns([
            datetime_from_parts(*[pl.col(c) for c in a_cols])
            .alias("arrive_time"),
        ])

    return trips


def expr_haversine(
    lat1: pl.Expr,
    lon1: pl.Expr,
    lat2: pl.Expr,
    lon2: pl.Expr,
    ) -> pl.Expr:
    """Return a Polars expression for Haversine distance in meters."""
    r = 6371000.0  # Earth radius (meters)
    dlat = (lat2.radians() - lat1.radians())
    dlon = (lon2.radians() - lon1.radians())
    a = (
        (dlat / 2).sin().pow(2) +
        lat1.radians().cos() * lat2.radians().cos() * (dlon / 2).sin().pow(2)
    )
    return 2 * r * a.sqrt().arcsin()
