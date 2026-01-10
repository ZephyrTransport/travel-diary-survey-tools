"""Utility functions for trip linking and data processing."""

import logging
import re

import polars as pl

from data_canon.core.labeled_enum import LabeledEnum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_income_midpoint(income_enum: LabeledEnum) -> int:
    """Calculate the midpoint dollar value for an income category enum.

    Parses the income range from the enum label and returns the midpoint.
    For "Under" categories, uses $0 as the lower bound.
    For "or more" categories, uses 1.25x multiplier to estimate upper bound.

    Args:
        income_enum: Income category enum (IncomeDetailed or IncomeFollowup)

    Returns:
        Midpoint dollar value for the income bracket

    Raises:
        ValueError: If the label format cannot be parsed or is PNTA/Missing

    Example:
        >>> from data_canon.codebook.households import IncomeDetailed
        >>> midpoint = get_income_midpoint(IncomeDetailed.INCOME_50TO75)
        >>> midpoint
        62500
    """
    label = income_enum.label

    # Handle special cases
    if "Prefer not to answer" in label or "Missing" in label:
        msg = f"Cannot calculate midpoint for {income_enum.name}: {label}"
        raise ValueError(msg)

    # Handle "Under $X" format - use $0 as lower bound
    if label.startswith("Under"):
        match = re.search(r"\$[\d,]+", label)
        if match:
            upper = int(match.group().replace("$", "").replace(",", ""))
            return int(round(upper / 2, -3))  # Round to nearest $1000

    # Handle "$X or more" format - use 1.25x multiplier
    if "or more" in label:
        match = re.search(r"\$[\d,]+", label)
        if match:
            lower = int(match.group().replace("$", "").replace(",", ""))
            # Use 1.25x multiplier to estimate upper bound
            estimated_upper = int(lower * 1.25)
            # Round to nearest $1000
            return int(round((lower + estimated_upper) / 2, -3))

    # Handle "$X-$Y" range format
    matches = re.findall(r"\$[\d,]+", label)
    if len(matches) == 2:  # noqa: PLR2004
        lower = int(matches[0].replace("$", "").replace(",", ""))
        upper = int(matches[1].replace("$", "").replace(",", ""))
        return int(round((lower + upper) / 2, -3))  # Round to nearest $1000

    # If we can't parse it, raise an error
    msg = f"Cannot parse income range from label: {label}"
    raise ValueError(msg)


def datetime_from_parts(
    date: pl.Expr,
    hour: pl.Expr,
    minute: pl.Expr,
    second: pl.Expr,
) -> pl.Expr:
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


def get_age_midpoint(age_enum: LabeledEnum) -> int:
    """Calculate the midpoint age value for an age category enum.

    Parses the age range from the enum label and returns the midpoint.
    For "Under" categories, uses 0 as the lower bound.
    For "and up" categories, estimates upper bound using the category span.

    Args:
        age_enum: Age category enum (e.g., AgeCategory)

    Returns:
        Midpoint age value for the age bracket

    Raises:
        ValueError: If the label format cannot be parsed

    Example:
        >>> from data_canon.codebook.persons import AgeCategory
        >>> midpoint = get_age_midpoint(AgeCategory.AGE_35_TO_44)
        >>> midpoint
        39
    """
    label = age_enum.label

    # Handle "Under X" format - use 0 as lower bound
    if label.startswith("Under"):
        match = re.search(r"\d+", label)
        if match:
            upper = int(match.group())
            return upper // 2

    # Handle "X and up" format - estimate upper bound
    if "and up" in label.lower():
        match = re.search(r"\d+", label)
        if match:
            lower = int(match.group())
            # For 85+, estimate midpoint at 87 (assumes typical lifespan)
            return lower + 2

    # Handle "X to Y" range format
    matches = re.findall(r"\d+", label)
    if len(matches) == 2:  # noqa: PLR2004
        lower = int(matches[0])
        upper = int(matches[1])
        return (lower + upper) // 2

    # Handle single age (e.g., "16 to 17" returns 16)
    if len(matches) == 1:
        return int(matches[0])

    # If we can't parse it, raise an error
    msg = f"Cannot parse age range from label: {label}"
    raise ValueError(msg)
