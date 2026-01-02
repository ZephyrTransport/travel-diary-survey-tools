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
    return pl.concat_str(
        [
            date,
            pl.lit("T"),
            hour.cast(pl.Utf8).str.pad_start(2, "0"),
            pl.lit(":"),
            minute.cast(pl.Utf8).str.pad_start(2, "0"),
            pl.lit(":"),
            second.cast(pl.Utf8).str.pad_start(2, "0"),
        ]
    ).str.to_datetime()


def add_time_columns(
    trips: pl.DataFrame,
    datetime_format: str = "%Y-%m-%d %H:%M:%S",
) -> pl.DataFrame:
    """Add datetime columns for departure and arrival times if missing.

    If datetime columns exist as strings, parse them to datetime type.
    Otherwise, construct them from component columns.
    """
    logger.info("Adding datetime columns...")

    for prefix in ["depart", "arrive"]:
        col_name = f"{prefix}_time"
        comp_cols = [f"{prefix}_{s}" for s in ["date", "hour", "minute", "seconds"]]

        if col_name not in trips.columns:
            logger.info("Constructing %s...", col_name)
            trips = trips.with_columns(
                datetime_from_parts(*[pl.col(c) for c in comp_cols]).alias(col_name)
            )
        elif trips[col_name].dtype == pl.Utf8:
            logger.info("Parsing %s from string...", col_name)
            trips = trips.with_columns(
                pl.col(col_name).str.to_datetime(format=datetime_format, strict=False)
            )

            if trips[col_name].null_count() > 0:
                logger.info("Reconstructing null %s from components...", col_name)
                trips = trips.with_columns(
                    pl.when(pl.col(col_name).is_null())
                    .then(datetime_from_parts(*[pl.col(c) for c in comp_cols]))
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )

    return trips


def expr_haversine(
    lat1: pl.Expr,
    lon1: pl.Expr,
    lat2: pl.Expr,
    lon2: pl.Expr,
    units: str = "meters",
) -> pl.Expr:
    """Return a Polars expression for Haversine distance.

    Returns null if any coordinate is null (e.g., missing work/school
    locations for non-workers/non-students).
    """
    r = 6371000.0  # Earth radius (meters)

    # Check if all coordinates are non-null before calculation
    all_coords_valid = (
        lat1.is_not_null() & lon1.is_not_null() & lat2.is_not_null() & lon2.is_not_null()
    )

    # Fill nulls with dummy values to prevent trigonometry errors
    # (result will be masked out by all_coords_valid check)
    lat1_safe = lat1.fill_null(0.0)
    lon1_safe = lon1.fill_null(0.0)
    lat2_safe = lat2.fill_null(0.0)
    lon2_safe = lon2.fill_null(0.0)

    # Calculate distance
    dlat = lat2_safe.radians() - lat1_safe.radians()
    dlon = lon2_safe.radians() - lon1_safe.radians()
    a = (dlat / 2).sin().pow(2) + lat1_safe.radians().cos() * lat2_safe.radians().cos() * (
        dlon / 2
    ).sin().pow(2)

    distance = 2 * r * a.sqrt().arcsin()

    if units in ["kilometers", "km"]:
        distance = distance / 1000.0
    elif units in ["miles", "mi"]:
        distance = distance / 1609.344

    # Return null if any coordinate is null, otherwise return distance
    return pl.when(all_coords_valid).then(distance).otherwise(None)
