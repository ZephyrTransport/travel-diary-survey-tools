"""Module for deriving person type from person attributes."""

import polars as pl

from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    PersonType,
    SchoolType,
    Student,
)


def derive_person_type(persons: pl.DataFrame) -> pl.DataFrame:
    """Derive person_type from person attributes.

    This replicates the pptyp logic from the old pipeline's 02a-reformat
    step, converting employment/student/age data into person type categories.

    Person types (see PersonType enum)

    Args:
        persons: DataFrame with age column (categorical AgeCategory),
                 employment, student, school_type columns

    Returns:
        DataFrame with added person_type column

    Note:
        Age is a categorical variable (see AgeCategory enum):
        1=under 5, 2=5-15, 3=16-17, 4=18-24, 5=25-34, etc.
    """
    # Define age group categories
    working_age = [
        AgeCategory.AGE_25_TO_34.value,
        AgeCategory.AGE_35_TO_44.value,
        AgeCategory.AGE_45_TO_54.value,
        AgeCategory.AGE_55_TO_64.value,
    ]

    # Employment status indicators
    is_full_time = pl.col("employment").is_in(
        [
            Employment.EMPLOYED_FULLTIME.value,
            Employment.EMPLOYED_SELF.value,
            Employment.EMPLOYED_UNPAID.value,
        ]
    )
    is_part_time = pl.col("employment").is_in(
        [
            Employment.EMPLOYED_PARTTIME.value,
            Employment.EMPLOYED_SELF.value,
        ]
    )

    # Student and school status indicators
    is_student = pl.col("student").is_in(
        [
            Student.FULLTIME_INPERSON.value,
            Student.PARTTIME_INPERSON.value,
            Student.PARTTIME_ONLINE.value,
            Student.FULLTIME_ONLINE.value,
        ]
    )
    is_high_school = pl.col("school_type").is_in(
        [
            SchoolType.HOME_SCHOOL.value,
            SchoolType.HIGH_SCHOOL.value,
        ]
    )

    # Age indicators
    age = pl.col("age")
    is_under_5 = age == AgeCategory.AGE_UNDER_5.value
    is_5_to_15 = age == AgeCategory.AGE_5_TO_15.value
    is_16_to_17 = age == AgeCategory.AGE_16_TO_17.value
    is_18_to_24 = age == AgeCategory.AGE_18_TO_24.value
    is_working_age = age.is_in(working_age)

    # Build classification expression
    person_type = (
        pl.when(is_under_5)
        .then(pl.lit(PersonType.CHILD_UNDER_5))
        .when(is_5_to_15)
        .then(pl.lit(PersonType.CHILD_NON_DRIVING_AGE))
        # Teens: workers first, then students
        .when(is_16_to_17 & is_full_time)
        .then(pl.lit(PersonType.FULL_TIME_WORKER))
        .when(is_16_to_17 & is_student)
        .then(pl.lit(PersonType.CHILD_DRIVING_AGE))
        # Young adults: workers first, then HS students, then college, then PT
        .when(is_18_to_24 & is_full_time)
        .then(pl.lit(PersonType.FULL_TIME_WORKER))
        .when(is_18_to_24 & is_high_school & is_student)
        .then(pl.lit(PersonType.CHILD_DRIVING_AGE))
        .when(is_18_to_24 & is_student)
        .then(pl.lit(PersonType.UNIVERSITY_STUDENT))
        .when(is_18_to_24 & is_part_time)
        .then(pl.lit(PersonType.PART_TIME_WORKER))
        # Working age: FT workers, students, PT workers, then non-workers
        .when(is_working_age & is_full_time)
        .then(pl.lit(PersonType.FULL_TIME_WORKER))
        .when(is_working_age & is_student)
        .then(pl.lit(PersonType.UNIVERSITY_STUDENT))
        .when(is_working_age & is_part_time)
        .then(pl.lit(PersonType.PART_TIME_WORKER))
        .when(is_working_age)
        .then(pl.lit(PersonType.NON_WORKER))
        # Seniors (65+)
        .otherwise(pl.lit(PersonType.RETIRED))
        .cast(pl.Int64)
    )

    return persons.with_columns(person_type=person_type)
