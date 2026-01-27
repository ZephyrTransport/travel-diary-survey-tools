"""Format mandatory locations for CT-RAMP specification."""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPEmploymentCategory,
    CTRAMPStudentCategory,
)
from data_canon.codebook.persons import Employment, SchoolType, Student

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)

# Employment to CT-RAMP employment category mapping
EMPLOYMENT_TO_CTRAMP = {
    Employment.EMPLOYED_FULLTIME.value: CTRAMPEmploymentCategory.FULL_TIME_EMPLOYED.value,
    Employment.EMPLOYED_PARTTIME.value: CTRAMPEmploymentCategory.PART_TIME_EMPLOYED.value,
    Employment.EMPLOYED_SELF.value: CTRAMPEmploymentCategory.FULL_TIME_EMPLOYED.value,
    Employment.EMPLOYED_UNPAID.value: CTRAMPEmploymentCategory.PART_TIME_EMPLOYED.value,
}


def format_mandatory_location(
    persons_canonical: pl.DataFrame,
    households_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format mandatory locations (work/school) to CT-RAMP specification.

    Transforms person and household data to create mandatory location records
    for persons with work or school locations.

    Args:
        persons_canonical: Canonical persons DataFrame with person_id, hh_id, person_num,
            person_type, age, employment, student, work_taz, school_taz
        households_canonical: Canonical households DataFrame with hh_id, home_taz
            (used for HomeTAZ field in output)
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
            (used for Income field in output)
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
    """
    logger.info("Formatting mandatory location data for CT-RAMP")

    # Check if persons has work/school location columns
    # If not, return empty DataFrame (no mandatory locations)
    if (
        f"work_{config.taz_field}" not in persons_canonical.columns
        and f"school_{config.taz_field}" not in persons_canonical.columns
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
    # Need home_taz from canonical and income from formatted
    mandatory_loc = persons_canonical.join(
        households_canonical.select(["hh_id", f"home_{config.taz_field}"]),
        on="hh_id",
        how="left",
    ).join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Compute employment_category from employment
    mandatory_loc = mandatory_loc.with_columns(
        pl.col("employment")
        .replace_strict(
            EMPLOYMENT_TO_CTRAMP,
            default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
        )
        .alias("employment_category")
    )

    # Compute student_category from student and school_type
    mandatory_loc = mandatory_loc.with_columns(
        pl.when(
            pl.col("student").is_in(
                [
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,
                    Student.FULLTIME_ONLINE.value,
                    Student.PARTTIME_ONLINE.value,
                ]
            )
            & pl.col("school_type").is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        .when(
            pl.col("student").is_in(
                [
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,
                    Student.FULLTIME_ONLINE.value,
                    Student.PARTTIME_ONLINE.value,
                ]
            )
            & pl.col("school_type").is_in(
                [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.MIDDLE_SCHOOL.value,
                    SchoolType.HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        .otherwise(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
        .alias("student_category")
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
