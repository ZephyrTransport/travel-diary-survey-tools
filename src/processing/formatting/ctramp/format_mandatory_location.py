"""Format mandatory locations for CT-RAMP specification."""

import logging

import polars as pl

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def format_mandatory_location(
    persons_ctramp: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format mandatory locations (work/school) to CT-RAMP specification.

    Transforms person and household data to create mandatory location records
    for persons with work or school locations.

    Args:
        persons_ctramp: Formatted CT-RAMP persons DataFrame with person_id, hh_id,
            person_num, person_type (converted), age (converted), employment_category,
            student_category, work_taz, school_taz
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income,
            home_taz (for HomeTAZ field)
        config: CT-RAMP configuration with income_base_year_dollars

    Returns:
        DataFrame with CT-RAMP mandatory location fields:
        - HHID, PersonID, PersonNum
        - HomeTAZ, Income
        - PersonType, PersonAge
        - EmploymentCategory, StudentCategory
        - WorkLocation, SchoolLocation

    Notes:
        - Excludes model-only fields (walk subzones)
        - Filters to only persons with work OR school locations
        - Uses pre-computed employment_category and student_category from persons_ctramp
    """
    logger.info("Formatting mandatory location data for CT-RAMP")

    # Check if persons has work/school location columns
    # If not, return empty DataFrame (no mandatory locations)
    if (
        f"work_{config.taz_field}" not in persons_ctramp.columns
        and f"school_{config.taz_field}" not in persons_ctramp.columns
    ):
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "taz": pl.Int64,
                "WorkLocation": pl.Int64,
                "SchoolLocation": pl.Int64,
            }
        )

    # Join persons with households to get income and home TAZ
    mandatory_loc = persons_ctramp.join(
        households_ctramp.select(["hh_id", f"home_{config.taz_field}", "income"]),
        on="hh_id",
        how="left",
    )

    # Filter to only persons with work or school locations
    # Add columns as null if they don't exist
    if f"work_{config.taz_field}" not in mandatory_loc.columns:
        mandatory_loc = mandatory_loc.with_columns(
            pl.lit(None).cast(pl.Int64).alias(f"work_{config.taz_field}")
        )
    if f"school_{config.taz_field}" not in mandatory_loc.columns:
        mandatory_loc = mandatory_loc.with_columns(
            pl.lit(None).cast(pl.Int64).alias(f"school_{config.taz_field}")
        )

    mandatory_loc = mandatory_loc.filter(
        (
            pl.col(f"work_{config.taz_field}").is_not_null()
            & (pl.col(f"work_{config.taz_field}") > 0)
        )
        | (
            pl.col(f"school_{config.taz_field}").is_not_null()
            & (pl.col(f"school_{config.taz_field}") > 0)
        )
    )

    # Map to CT-RAMP column names
    mandatory_loc = mandatory_loc.select(
        [
            pl.col("hh_id").alias("HHID"),
            pl.col(f"home_{config.taz_field}").cast(pl.Int64).alias("HomeTAZ"),
            (pl.col("income") / config.income_base_year_dollars).cast(pl.Int64).alias("Income"),
            pl.col("person_id").alias("PersonID"),
            pl.col("person_num").alias("PersonNum"),
            pl.col("person_type").alias("PersonType"),
            pl.col("age").alias("PersonAge"),
            pl.col("employment_category").alias("EmploymentCategory"),
            pl.col("student_category").alias("StudentCategory"),
            pl.col(f"work_{config.taz_field}").fill_null(0).cast(pl.Int64).alias("WorkLocation"),
            pl.col(f"school_{config.taz_field}")
            .fill_null(0)
            .cast(pl.Int64)
            .alias("SchoolLocation"),
        ]
    )

    logger.info("Formatted %d mandatory location records", len(mandatory_loc))
    return mandatory_loc
