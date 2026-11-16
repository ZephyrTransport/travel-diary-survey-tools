"""Household formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import DaysimPersonType
from processing.formatting.income_bins import income_bin_midpoints

from .mappings import (
    RENTOWN_MAP,
    RESTYPE_MAP,
)

logger = logging.getLogger(__name__)


def format_households(
    households: pl.DataFrame,
    persons_daysim: pl.DataFrame,
) -> pl.DataFrame:
    """Format household data to DaySim specification.

    Calculates household composition from person data and fills income from
    the income bin where no income is given.

    Key Transformations:

    - **Household Composition**: Aggregate person types within household (full-time workers,
      part-time workers, retirees, non-working adults, university students, high school
      students, children by age)
    - **Income**: The household's income, else the midpoint of its income bin, else -1
    - **Size**: Household size, vehicles and workers as reported
    - **Coordinates**: Home location coordinates and TAZ/MAZ assignment

    Household composition fields:

    - `hhftw`: Full-time workers
    - `hhptw`: Part-time workers
    - `hhret`: Retirees (non-working seniors)
    - `hhoad`: Other adults (non-working < 65)
    - `hhuni`: University students
    - `hhhsc`: High school students 16+
    - `hh515`: Children 5-15
    - `hhcu5`: Children 0-4

    Args:
        households: DataFrame with canonical household fields
        persons_daysim: DataFrame with formatted DaySim person fields

    Returns:
        DataFrame with DaySim household fields
    """
    logger.info("Formatting household data")

    # Calculate household composition from persons_daysim
    hh_composition = persons_daysim.group_by("hhno").agg(
        hhftw=(pl.col("pptyp") == DaysimPersonType.FULL_TIME_WORKER.value).sum(),
        hhptw=(pl.col("pptyp") == DaysimPersonType.PART_TIME_WORKER.value).sum(),
        hhret=(pl.col("pptyp") == DaysimPersonType.RETIRED.value).sum(),
        hhoad=(pl.col("pptyp") == DaysimPersonType.NON_WORKER.value).sum(),
        hhuni=(pl.col("pptyp") == DaysimPersonType.UNIVERSITY_STUDENT.value).sum(),
        hhhsc=(pl.col("pptyp") == DaysimPersonType.CHILD_DRIVING_AGE.value).sum(),
        hh515=(pl.col("pptyp") == DaysimPersonType.CHILD_NON_DRIVING_AGE.value).sum(),
        hhcu5=(pl.col("pptyp") == DaysimPersonType.CHILD_UNDER_5.value).sum(),
    )

    # Rename columns to DaySim naming convention
    households_daysim = households.rename(
        {
            "hh_id": "hhno",
            "home_maz": "hhparcel",
            "home_taz": "hhtaz",
            "home_lon": "hhxco",
            "home_lat": "hhyco",
            "num_people": "hhsize",
            "num_vehicles": "hhvehs",
            "num_workers": "hhwkrs",
            "hh_weight": "hhexpfac",
        }
    )

    # If there is no weight column at all, create one with default value 1.0
    if "hhexpfac" not in households_daysim.columns:
        households_daysim = households_daysim.with_columns(pl.lit(1.0).alias("hhexpfac"))

    # Map non-income fields
    households_daysim = households_daysim.with_columns(
        pl.col("hhexpfac").fill_null(0),
        hownrent=pl.col("residence_rent_own").replace(RENTOWN_MAP),
        hrestype=pl.col("residence_type").replace(RESTYPE_MAP),
    )

    # Derive hhincome: use pre-computed income if available, otherwise midpoint of income_bin
    income_bin_expr = (
        pl.col("income_bin").cast(pl.Int64).replace_strict(income_bin_midpoints(), default=-1)
    )
    if "income" in households_daysim.columns:
        households_daysim = households_daysim.with_columns(
            hhincome=pl.coalesce(pl.col("income"), income_bin_expr).fill_null(-1)
        )
    else:
        households_daysim = households_daysim.with_columns(hhincome=income_bin_expr)

    # Join household composition and add default fields
    households_daysim = households_daysim.join(hh_composition, on="hhno", how="left").with_columns(
        samptype=pl.lit(0),
    )

    # Select DaySim household fields
    hh_cols = [
        "hhno",
        "hhsize",
        "hhvehs",
        "hhwkrs",
        "hhftw",
        "hhptw",
        "hhret",
        "hhoad",
        "hhuni",
        "hhhsc",
        "hh515",
        "hhcu5",
        "hhincome",
        "hownrent",
        "hrestype",
        "hhparcel",
        "hhtaz",
        "hhxco",
        "hhyco",
        "hhexpfac",
        "samptype",
    ]

    logger.info("Formatted %d households", len(households_daysim))
    return households_daysim.select(hh_cols).sort(by="hhno")
