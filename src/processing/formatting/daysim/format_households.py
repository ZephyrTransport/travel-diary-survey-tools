"""Household formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import DaysimPersonType

from .mappings import (
    INCOME_DETAILED_TO_MIDPOINT,
    INCOME_FOLLOWUP_TO_MIDPOINT,
    RENTOWN_MAP,
    RESTYPE_MAP,
)

logger = logging.getLogger(__name__)


def format_households(
    households: pl.DataFrame,
    persons_daysim: pl.DataFrame,
) -> pl.DataFrame:
    """Format household data to DaySim specification.

    Calculates household composition from person data and applies income
    fallback logic.

    Key Transformations:

    - **Household Composition***: Aggregate person types within household (full-time workers,
      part-time workers, retirees, non-working adults, university students, high school
      students, children by age)
    - **Income Processing**: Categorize household income into DaySim bins using midpoint
      values, handle missing values with fallback logic from detailed to followup income
    - **Size and Type**: Household size from person count, household type derived from
      composition (workers, students, children)
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

    # Map income categories to midpoint values
    # (fill null first to avoid type issues)
    households_daysim = households_daysim.with_columns(
        pl.col("income_detailed").fill_null(-1).replace(INCOME_DETAILED_TO_MIDPOINT),
        pl.col("income_followup").fill_null(-1).replace(INCOME_FOLLOWUP_TO_MIDPOINT),
        pl.col("hhexpfac").fill_null(0),
        hownrent=pl.col("residence_rent_own").replace(RENTOWN_MAP),
        hrestype=pl.col("residence_type").replace(RESTYPE_MAP),
    )

    # Use income_detailed if available, otherwise income_followup
    households_daysim = households_daysim.with_columns(
        hhincome=pl.when(pl.col("income_detailed") > 0)
        .then(pl.col("income_detailed"))
        .otherwise(pl.col("income_followup"))
    )

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
