"""CT-RAMP student-category mappings and validation diagnostics."""

import logging

import polars as pl

from data_canon.codebook.ctramp import CTRAMPPersonType, CTRAMPStudentCategory
from data_canon.codebook.persons import AgeCategory, SchoolType, Student

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def ctramp_student_category_expression(
    school_taz_col: str = "school_taz",
    age_col: str = "age",
    student_col: str = "student",
    school_type_col: str = "school_type",
) -> pl.Expr:
    """Create expression to derive student category from person attributes.

    Derives CT-RAMP student category using a three-tier classification system:
    1. Valid student status + valid school type → map by school level
    2. Missing student OR missing school type → age-based fallback
    3. Non-student with any school type → NOT_STUDENT

    Classification Rules:
        Tier 1 - Valid Data (student status present + school type present):
            - ELEMENTARY/MIDDLE_SCHOOL/HIGH_SCHOOL/HOME_SCHOOL → GRADE_OR_HIGH_SCHOOL
            - COLLEGE_2YEAR/COLLEGE_4YEAR/GRADUATE_SCHOOL/VOCATIONAL → COLLEGE_OR_HIGHER
            - DAYCARE/PRESCHOOL/ATHOME → NOT_STUDENT (early childhood)

        Tier 2 - Age-Based Fallback (student=MISSING OR school_type=MISSING/PNTA/OTHER):
            - Age 5-15 (AGE_5_TO_15) → GRADE_OR_HIGH_SCHOOL (compulsory education age)
            - Age 16-17 (AGE_16_TO_17) → GRADE_OR_HIGH_SCHOOL (still in school likely)
            - All other ages → NOT_STUDENT (under 5 or adult)

        Tier 3 - Catch-all:
            - NONSTUDENT with any school type → NOT_STUDENT

    Edge Case Handling:
        - Children age 5-17 with missing student/school data are assumed to be
          in school, preventing incorrect NOT_STUDENT classification
        - Adults 18+ with missing data are assumed NOT_STUDENT (most adults)
        - Early childhood programs (daycare/preschool) are classified as NOT_STUDENT
          since CT-RAMP doesn't model school travel for ages <5

    Args:
        school_taz_col: Name of school location TAZ column
            (used to detect if person has a school location)
        age_col: Name of age column (AgeCategory enum values)
        student_col: Name of student column (Student enum)
        school_type_col: Name of school_type column (SchoolType enum)

    Returns:
        Polars expression that evaluates to CTRAMPStudentCategory enum value

    See Also:
        log_student_category_warnings: Function to detect and log data quality issues

    Note:
        Age should be AgeCategory enum values (e.g., AGE_5_TO_15=2, AGE_16_TO_17=3).
        Applied before age category conversion to continuous in format_persons.py.

    Examples:
        >>> df = df.with_columns(
        ...     ctramp_student_category_expression().alias("student_category")
        ... )
    """
    # Define valid student statuses (active students)
    is_student = pl.col(student_col).is_in(
        [
            Student.FULLTIME_INPERSON.value,
            Student.PARTTIME_INPERSON.value,
            Student.FULLTIME_ONLINE.value,
            Student.PARTTIME_ONLINE.value,
        ]
    )

    # Define missing indicators - only truly missing, NOT explicit non-students
    is_student_missing = (pl.col(student_col) == Student.MISSING.value) | pl.col(
        student_col
    ).is_null()

    is_school_type_missing = (
        pl.col(school_type_col).is_in(
            [SchoolType.MISSING.value, SchoolType.PNTA.value, SchoolType.OTHER.value]
        )
        | pl.col(school_type_col).is_null()
    )

    # Define school type categories
    is_grade_or_high_school = pl.col(school_type_col).is_in(
        [
            SchoolType.ELEMENTARY.value,
            SchoolType.MIDDLE_SCHOOL.value,
            SchoolType.HIGH_SCHOOL.value,
            SchoolType.HOME_SCHOOL.value,
        ]
    )

    is_college = pl.col(school_type_col).is_in(
        [
            SchoolType.COLLEGE_2YEAR.value,
            SchoolType.COLLEGE_4YEAR.value,
            SchoolType.GRADUATE_SCHOOL.value,
            SchoolType.VOCATIONAL.value,
        ]
    )

    is_early_childhood = pl.col(school_type_col).is_in(
        [
            SchoolType.DAYCARE.value,
            SchoolType.PRESCHOOL.value,
            SchoolType.ATHOME.value,
        ]
    )

    # Age-based fallback categories
    # Use AgeCategory enum values (school age = 5-15 and 16-17)
    age = pl.col(age_col)
    is_school_age = age.is_in(
        [
            AgeCategory.AGE_5_TO_15.value,
            AgeCategory.AGE_16_TO_17.value,
        ]
    )

    # SCHOOL LOCATION INFERENCE:
    # If person has a school location, they ARE a student (location is strongest signal)
    # EXCEPT: Under 5 are always NOT_STUDENT (we don't model early childhood school travel)
    # Priority: 1) Under 5 → NOT_STUDENT
    #          2) If school age (5-17) → grade school (overrides school_type)
    #          3) Otherwise use school_type if valid, 4) Fall back to age if missing

    # Check if person has a school location (TAZ-based, non-zero/non-null)
    has_school_location = pl.col(school_taz_col).is_not_null() & (pl.col(school_taz_col) > 0)

    # HIGHEST PRIORITY: School age (5-17) with school location → GRADE_OR_HIGH_SCHOOL
    # (overrides any school_type including DAYCARE/PRESCHOOL which may be data errors)
    inferred_grade_by_age = has_school_location & is_school_age

    # For non-school-age: School location + valid school type → use school type
    inferred_college_by_school_type = has_school_location & ~is_school_age & is_college
    inferred_grade_by_school_type = has_school_location & ~is_school_age & is_grade_or_high_school
    inferred_early_childhood_by_school_type = (
        has_school_location & ~is_school_age & is_early_childhood
    )

    # For non-school-age: School location + missing school type → infer by age
    inferred_college_by_age = (
        has_school_location
        & ~is_school_age
        & is_school_type_missing
        & ~(pl.col(age_col) == AgeCategory.AGE_UNDER_5.value)
    )

    # Build classification expression
    _expr = (
        # HIGHEST PRIORITY: Under 5 with school location → NOT_STUDENT
        # (We don't model school travel for children under 5, even if they have a location)
        pl.when(has_school_location & (pl.col(age_col) == AgeCategory.AGE_UNDER_5.value))
        .then(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
        # School location + school age (5-17) → GRADE_OR_HIGH_SCHOOL
        # (Overrides any school_type including DAYCARE/PRESCHOOL which are likely errors)
        .when(inferred_grade_by_age)
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        # School location + non-school-age (18+) + valid school type → use school type
        .when(inferred_grade_by_school_type)
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        .when(inferred_college_by_school_type)
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        .when(inferred_early_childhood_by_school_type)
        .then(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
        # School location + non-school-age (18+) + missing school type → infer college by age
        .when(inferred_college_by_age)
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        # Active student status + valid school type → use school type
        .when(is_student & is_grade_or_high_school)
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        .when(is_student & is_college)
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        .when(is_student & is_early_childhood)
        .then(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
        # Active student 18+ with missing school_type → assume college
        # (Aligns with person_type_expression which assumes university for 18+ students)
        .when(
            is_student
            & is_school_type_missing
            & ~is_school_age
            & ~(pl.col(age_col) == AgeCategory.AGE_UNDER_5.value)
        )
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        # Missing student/school data + school age + no location → assume student
        .when((is_student_missing | is_school_type_missing) & is_school_age & ~has_school_location)
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        # NONSTUDENT + school age + no location → contradictory data, use age fallback
        # (If the data says non-student but they're school age, assume data quality issue)
        .when(
            (pl.col(student_col) == Student.NONSTUDENT.value) & is_school_age & ~has_school_location
        )
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        # Everything else → not student
        .otherwise(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
    )

    return _expr


def log_student_category_warnings(df: pl.DataFrame, config: CTRAMPConfig) -> dict[str, int]:
    """Detect and log problematic student/school type combinations.

    Identifies data quality issues where student status, school type, and age
    are inconsistent or missing. These combinations may result in incorrect
    student_category classification or reliance on age-based fallbacks.

    Warning Categories:
        - missing_data_used_fallback: Students age 5-17 with missing student
          status or school type who received GRADE_OR_HIGH_SCHOOL via age fallback
        - preschool_students: Children under 5 with active student status (impossible)
        - age_inappropriate_school_types: Mismatched age/school_type combinations
          (e.g., teens in elementary, children in college, adults in HOME_SCHOOL)
        - nonstudents_with_school_location: Persons classified as NOT_STUDENT but
          with a school location TAZ (contradictory data)
        - fulltime_workers_no_work_location: Full-time workers without work location,
          including those with school location (college students misclassified as workers)

    Args:
        df: DataFrame with person_id, age, student, school_type, student_category,
            person_type, work_taz, and school_taz columns
        config: CTRAMPConfig object containing taz_field name for school/work locations

    Returns:
        Dictionary mapping warning category names to counts of affected persons

    See Also:
        ctramp_student_category_expression: Expression that derives student_category
        log_person_type_warnings: Similar validation for person_type

    Note:
        This function logs warnings but does not modify the DataFrame. It should
        be called after student_category and person_type have been derived to audit
        data quality.
    """
    warnings = {}

    # Missing data that triggered age-based fallback
    # Children 5-17 with GRADE_OR_HIGH_SCHOOL but missing student/school data
    missing_data_fallback = df.filter(
        (
            pl.col("age").is_in(
                [
                    AgeCategory.AGE_5_TO_15.value,
                    AgeCategory.AGE_16_TO_17.value,
                ]
            )
        )
        & (pl.col("student_category") == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value)
        & (
            (pl.col("student") == Student.MISSING.value)
            | pl.col("student").is_null()
            | pl.col("school_type").is_in(
                [SchoolType.MISSING.value, SchoolType.PNTA.value, SchoolType.OTHER.value]
            )
            | pl.col("school_type").is_null()
        )
    ).select("person_id", "age", "student", "school_type", "student_category")

    if len(missing_data_fallback) > 0:
        warnings["missing_data_used_fallback"] = len(missing_data_fallback)
        logger.warning(
            "Applied age-based fallback for student_category to %d persons age 5-17 "
            "with missing student status or school type. Sample rows:\n%s",
            len(missing_data_fallback),
            missing_data_fallback.unique(subset=["age", "student", "school_type"]).head(5),
        )

    # Preschool students (impossible)
    preschool_students = df.filter(
        (pl.col("age") == AgeCategory.AGE_UNDER_5.value)
        & pl.col("student").is_in(
            [
                Student.FULLTIME_INPERSON.value,
                Student.PARTTIME_INPERSON.value,
                Student.FULLTIME_ONLINE.value,
                Student.PARTTIME_ONLINE.value,
            ]
        )
    ).select("person_id", "age", "student", "school_type")

    if len(preschool_students) > 0:
        warnings["preschool_students"] = len(preschool_students)
        logger.warning(
            "Found %d children under 5 with active student status (impossible). "
            "Will be classified as NOT_STUDENT. Sample rows:\n%s",
            len(preschool_students),
            preschool_students.unique(subset=["age", "student"]).head(5),
        )

    # Age-inappropriate school types
    inappropriate_school = df.filter(
        # Teens 16-24 in elementary/middle school
        (
            pl.col("age").is_in(
                [
                    AgeCategory.AGE_16_TO_17.value,
                    AgeCategory.AGE_18_TO_24.value,
                ]
            )
            & pl.col("school_type").is_in(
                [SchoolType.ELEMENTARY.value, SchoolType.MIDDLE_SCHOOL.value]
            )
        )
        # Children under 16 in college
        | (
            pl.col("age").is_in([AgeCategory.AGE_UNDER_5.value, AgeCategory.AGE_5_TO_15.value])
            & pl.col("school_type").is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                ]
            )
        )
        # Adults (25+) with HOME_SCHOOL
        | (
            pl.col("age").is_in(
                [
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_35_TO_44.value,
                    AgeCategory.AGE_45_TO_54.value,
                    AgeCategory.AGE_55_TO_64.value,
                    AgeCategory.AGE_65_TO_74.value,
                    AgeCategory.AGE_75_TO_84.value,
                    AgeCategory.AGE_85_AND_UP.value,
                ]
            )
            & (pl.col("school_type") == SchoolType.HOME_SCHOOL.value)
        )
    ).select("person_id", "age", "school_type", "student_category")

    if len(inappropriate_school) > 0:
        warnings["age_inappropriate_school_types"] = len(inappropriate_school)
        logger.warning(
            "Found %d persons with age-inappropriate school types "
            "(e.g., teens in elementary, children in college, adults with HOME_SCHOOL). "
            "Classification may be questionable. Sample rows:\n%s",
            len(inappropriate_school),
            inappropriate_school.unique(subset=["age", "school_type"]).head(5),
        )

    # Check for for non-students with school locations
    nonstudents_with_school = df.filter(
        (pl.col("student_category") == CTRAMPStudentCategory.NOT_STUDENT.value)
        & pl.col(f"school_{config.taz_field}").is_not_null()
        & (pl.col(f"school_{config.taz_field}") > 0)
    ).select("person_id", "age", "school_type", "student_category", f"school_{config.taz_field}")

    if len(nonstudents_with_school) > 0:
        warnings["nonstudents_with_school_location"] = len(nonstudents_with_school)
        logger.warning(
            "Found %d persons classified as NOT_STUDENT but with a school location. "
            "This may indicate data quality issues. Sample rows:\n%s",
            len(nonstudents_with_school),
            nonstudents_with_school.unique(subset=[f"school_{config.taz_field}"]).head(5),
        )

    # Check for college students with school location but full-time worker status
    # These people are classified as FULL_TIME_WORKER (employment beats student status)
    # but they only have school_taz, not work_taz - problematic for mandatory location modeling
    if f"work_{config.taz_field}" in df.columns and "person_type" in df.columns:
        has_work_taz = pl.col(f"work_{config.taz_field}").is_not_null() & (
            pl.col(f"work_{config.taz_field}") > 0
        )
        has_school_taz = pl.col(f"school_{config.taz_field}").is_not_null() & (
            pl.col(f"school_{config.taz_field}") > 0
        )

        # Full-time workers without work location
        fulltime_no_work_location = df.filter(
            (pl.col("person_type") == CTRAMPPersonType.FULL_TIME_WORKER.value)
            & (pl.col("student_category") != CTRAMPStudentCategory.NOT_STUDENT.value)
            & ~has_work_taz
        ).select(
            "person_id",
            "age",
            "student_category",
            "person_type",
            f"work_{config.taz_field}",
            f"school_{config.taz_field}",
        )

        if len(fulltime_no_work_location) > 0:
            warnings["fulltime_workers_no_work_location"] = len(fulltime_no_work_location)

            # Count how many have school location (dual status - full-time worker + college student)
            dual_status = fulltime_no_work_location.filter(has_school_taz)
            if len(dual_status) > 0:
                logger.warning(
                    "Found %d full-time workers that are ALSO students "
                    "without work location (%d also have school location). "
                    "CT-RAMP expects mandatory work locations for full-time workers. "
                    "These may be college students misclassified as workers. Sample rows:\n%s",
                    len(fulltime_no_work_location),
                    len(dual_status),
                    dual_status.head(5),
                )
            else:
                logger.warning(
                    "Found %d full-time workers without work location. "
                    "CT-RAMP expects mandatory work locations for full-time workers. "
                    "Sample rows:\n%s",
                    len(fulltime_no_work_location),
                    fulltime_no_work_location.head(5),
                )

    return warnings
