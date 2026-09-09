"""Utility functions for deriving DaySim person types."""

import polars as pl

from data_canon.codebook.daysim import DaysimPersonType
from data_canon.codebook.persons import Employment, SchoolType, Student

from .mappings import AgeThreshold


def get_person_type_expr() -> pl.Expr:
    """Get Polars expression for deriving DaySim person type (pptyp).

    Person type cascading logic:
    - Age < 5: Child 0-4 (type 8)
    - Age < 16: Child 5-15 (type 7)
    - Full-time employed: Full-time worker (type 1)
    - Age 16-17 and student: High school 16+ (type 6)
    - Age 18-24 and high school: High school 16+ (type 6)
    - Age >= 18 and student: University student (type 3)
    - Part-time/self-employed: Part-time worker (type 2)
    - Age < 65: Non-working adult (type 4)
    - Age >= 65: Non-working senior (type 5)

    Returns:
        Polars expression that evaluates to DaysimPersonType values
    """
    return (
        pl.when(pl.col("pagey") < AgeThreshold.CHILD_PRESCHOOL)
        .then(pl.lit(DaysimPersonType.CHILD_UNDER_5.value))
        .when(pl.col("pagey") < AgeThreshold.CHILD_SCHOOL)
        .then(pl.lit(DaysimPersonType.CHILD_NON_DRIVING_AGE.value))
        # Age >= 16:
        .when(
            pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_SELF.value,
                    Employment.EMPLOYED_FURLOUGHED.value,
                    Employment.EMPLOYED_UNPAID.value,
                ]
            )
        )
        .then(pl.lit(DaysimPersonType.FULL_TIME_WORKER.value))
        # Age >= 16 and not full-time employed:
        .when(
            (pl.col("pagey") < AgeThreshold.YOUNG_ADULT)  # 16-17
            & (
                pl.col("student").is_in(
                    [
                        Student.FULLTIME_INPERSON.value,
                        Student.PARTTIME_INPERSON.value,
                        Student.PARTTIME_ONLINE.value,
                        Student.FULLTIME_ONLINE.value,
                    ]
                )
            )
        )
        .then(pl.lit(DaysimPersonType.CHILD_DRIVING_AGE.value))
        .when(
            (pl.col("pagey") < AgeThreshold.ADULT)  # 18-24
            & (
                pl.col("school_type").is_in(
                    [
                        SchoolType.HOME_SCHOOL.value,
                        SchoolType.HIGH_SCHOOL.value,
                    ]
                )
            )
            & (
                pl.col("student").is_in(
                    [
                        Student.FULLTIME_INPERSON.value,
                        Student.PARTTIME_INPERSON.value,
                        Student.PARTTIME_ONLINE.value,
                        Student.FULLTIME_ONLINE.value,
                    ]
                )
            )
        )
        .then(pl.lit(DaysimPersonType.CHILD_DRIVING_AGE.value))
        # Age >= 18:
        .when(
            pl.col("student").is_in(
                [
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,
                    Student.PARTTIME_ONLINE.value,
                    Student.FULLTIME_ONLINE.value,
                ]
            )
        )
        .then(pl.lit(DaysimPersonType.UNIVERSITY_STUDENT.value))
        .when(
            pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                    Employment.EMPLOYED_UNPAID.value,
                ]
            )
        )
        .then(pl.lit(DaysimPersonType.PART_TIME_WORKER.value))
        .when(pl.col("pagey") < AgeThreshold.SENIOR)
        .then(pl.lit(DaysimPersonType.NON_WORKER.value))
        .otherwise(pl.lit(DaysimPersonType.RETIRED.value))
    )
