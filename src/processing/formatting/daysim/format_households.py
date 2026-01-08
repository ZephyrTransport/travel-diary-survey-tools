"""Household formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.persons import PersonType

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

    Household composition fields:
    - hhftw: Full-time workers
    - hhptw: Part-time workers
    - hhret: Retirees (non-working seniors)
    - hhoad: Other adults (non-working < 65)
    - hhuni: University students
    - hhhsc: High school students 16+
    - hh515: Children 5-15
    - hhcu5: Children 0-4

    Args:
        households: DataFrame with canonical household fields
        persons_daysim: DataFrame with formatted DaySim person fields

    Returns:
        DataFrame with DaySim household fields
    """
    logger.info("Formatting household data")

    # Calculate household composition from persons_daysim
    hh_composition = persons_daysim.group_by("hhno").agg(
        hhftw=(pl.col("pptyp") == PersonType.FULL_TIME_WORKER.value).sum(),
        hhptw=(pl.col("pptyp") == PersonType.PART_TIME_WORKER.value).sum(),
        hhret=(pl.col("pptyp") == PersonType.RETIRED.value).sum(),
        hhoad=(pl.col("pptyp") == PersonType.NON_WORKER.value).sum(),
        hhuni=(pl.col("pptyp") == PersonType.UNIVERSITY_STUDENT.value).sum(),
        hhhsc=(pl.col("pptyp") == PersonType.CHILD_DRIVING_AGE.value).sum(),
        hh515=(pl.col("pptyp") == PersonType.CHILD_NON_DRIVING_AGE.value).sum(),
        hhcu5=(pl.col("pptyp") == PersonType.CHILD_UNDER_5.value).sum(),
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

    # Map income categories to midpoint values
    # (fill null first to avoid type issues)
    households_daysim = households_daysim.with_columns(
        pl.col("income_detailed").fill_null(-1).replace(INCOME_DETAILED_TO_MIDPOINT),
        pl.col("income_followup").fill_null(-1).replace(INCOME_FOLLOWUP_TO_MIDPOINT),
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
