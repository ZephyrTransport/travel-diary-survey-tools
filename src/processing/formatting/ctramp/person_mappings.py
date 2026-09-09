"""CT-RAMP person-type, employment, gender, and industry mappings."""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPEmploymentCategory,
    CTRAMPGender,
    CTRAMPIndustry,
    CTRAMPPersonType,
    CTRAMPStudentCategory,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    Industry,
    SchoolType,
    Student,
)

logger = logging.getLogger(__name__)

GENDER_MAP = {
    Gender.MALE.value: CTRAMPGender.MALE.value,
    Gender.FEMALE.value: CTRAMPGender.FEMALE.value,
    # Only 2 genders coded in CT-RAMP. All else get mapped to default.
    # Gender.NON_BINARY.value: ?...,
    # Gender.OTHER.value: ?...,
    # Gender.PNTA.value: ?...,
    # -1: ?...,
}

# Employment to CT-RAMP employment category mapping (for mandatory locations)
EMPLOYMENT_TO_CTRAMP = {
    Employment.EMPLOYED_FULLTIME.value: CTRAMPEmploymentCategory.FULL_TIME_EMPLOYED.value,
    Employment.EMPLOYED_PARTTIME.value: CTRAMPEmploymentCategory.PART_TIME_EMPLOYED.value,
    Employment.EMPLOYED_SELF.value: CTRAMPEmploymentCategory.FULL_TIME_EMPLOYED.value,
    Employment.EMPLOYED_UNPAID.value: CTRAMPEmploymentCategory.PART_TIME_EMPLOYED.value,
}


# PurposeCategory to Joint Tour Frequency (JTF) group mapping
# Maps canonical tour purposes to JTF category strings used for joint tour classification
# This is an internal mapping used in CTRAMP processing to get joint tour frequencies
# based on tour purpose categories.
# S = Shopping
# M = Maintenance/errands
# E = Eating out
# V = Visiting/social/recreational
# D = Discretionary


def log_person_type_warnings(df: pl.DataFrame) -> dict[str, int]:
    """Log warnings for logically impossible person attribute combinations.

    Checks for combinations that are logically impossible in the real world
    (e.g., 10-year-old full-time workers, 4-year-old college students) and
    logs warnings to help identify data quality issues.

    Args:
        df: DataFrame with age, employment, student, school_type columns

    Returns:
        Dictionary with warning counts by category

    Example:
        >>> warnings = log_person_type_warnings(persons_df)
        >>> if sum(warnings.values()) > 0:
        ...     logger.warning(f"Found {sum(warnings.values())} impossible combinations")
    """
    logger = logging.getLogger(__name__)

    warnings = {
        "child_fulltime_workers": 0,
        "preschool_students": 0,
        "young_college_students": 0,
        "young_retirees": 0,
        "missing_employment_status": 0,
        "missing_student_status": 0,
        "age_inappropriate_school_types": 0,
    }

    # Children < 16 with full-time employment
    # Note: EMPLOYED_UNPAID is treated as part-time (consistent with EMPLOYMENT_TO_CTRAMP)
    child_workers = df.filter(
        (pl.col("age").is_in([AgeCategory.AGE_UNDER_5.value, AgeCategory.AGE_5_TO_15.value]))
        & pl.col("employment").is_in(
            [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_SELF.value,
            ]
        )
    ).select("person_id", "age", "employment")
    if len(child_workers) > 0:
        warnings["child_fulltime_workers"] = len(child_workers)
        logger.warning(
            "Found %d children under 16 with full-time employment. "
            "Age-based classification will override employment status. Sample rows:\n%s",
            len(child_workers),
            child_workers.unique(subset=["age", "employment"]).head(5),
        )

    # Children < 5 with student status
    preschool_students = df.filter(
        (pl.col("age") == AgeCategory.AGE_UNDER_5.value)
        & pl.col("student").is_in(
            [
                Student.FULLTIME_INPERSON.value,
                Student.FULLTIME_ONLINE.value,
                Student.PARTTIME_INPERSON.value,
            ]
        )
    ).select("person_id", "age", "student")
    if len(preschool_students) > 0:
        warnings["preschool_students"] = len(preschool_students)
        logger.warning(
            "Found %d children under 5 with student status. "
            "These will be classified as CHILD_UNDER_5. Sample rows:\n%s",
            len(preschool_students),
            preschool_students.unique(subset=["age", "student"]).head(5),
        )

    # College students < 16 years old
    young_college = df.filter(
        pl.col("age").is_in([AgeCategory.AGE_UNDER_5.value, AgeCategory.AGE_5_TO_15.value])
        & pl.col("school_type").is_in(
            [SchoolType.COLLEGE_2YEAR.value, SchoolType.COLLEGE_4YEAR.value]
        )
    ).select("person_id", "age", "school_type")
    if len(young_college) > 0:
        warnings["young_college_students"] = len(young_college)
        logger.warning(
            "Found %d children under 16 listed as college students. "
            "Age-based classification will override school type. Sample rows:\n%s",
            len(young_college),
            young_college.unique(subset=["age", "school_type"]).head(5),
        )

    # Check if anyone < 65 and >16 have no employment code.
    # (This shouldn't happen with correct logic, but log if it does)
    # Note: We can only check if the input data suggests retirement, not the output
    # The classification logic itself prevents young retirees

    # MISSING employment status
    missing_employment = df.filter(
        (pl.col("employment") == Employment.MISSING.value)
        & ~pl.col("age").is_in(
            [
                AgeCategory.AGE_UNDER_5.value,
                AgeCategory.AGE_5_TO_15.value,
                AgeCategory.AGE_65_TO_74.value,
                AgeCategory.AGE_75_TO_84.value,
                AgeCategory.AGE_85_AND_UP.value,
            ]
        )
    ).select("person_id", "age", "employment")
    if len(missing_employment) > 0:
        warnings["missing_employment_status"] = len(missing_employment)
        logger.warning(
            "Found %d persons with MISSING employment status. "
            "Classification will use age-based defaults. Sample rows:\n%s",
            len(missing_employment),
            missing_employment.unique(subset=["age", "employment"]).head(5),
        )

    # Check if anyone <18 has missing student status. Over 18 we expect them to be non-students.
    # MISSING student status when they have a school type
    missing_student = df.filter(
        pl.col("age").is_in(
            [
                AgeCategory.AGE_UNDER_5.value,
                AgeCategory.AGE_5_TO_15.value,
                AgeCategory.AGE_16_TO_17.value,
            ]
        )
        & (pl.col("student") == Student.MISSING.value)
        & pl.col("school_type").is_in(
            [
                SchoolType.ELEMENTARY.value,
                SchoolType.MIDDLE_SCHOOL.value,
                SchoolType.HIGH_SCHOOL.value,
                SchoolType.COLLEGE_2YEAR.value,
                SchoolType.COLLEGE_4YEAR.value,
                SchoolType.GRADUATE_SCHOOL.value,
            ]
        )
    ).select("person_id", "age", "student", "school_type")
    if len(missing_student) > 0:
        warnings["missing_student_status"] = len(missing_student)
        # Get a sample of unique offenders
        logger.warning(
            "Found %d persons with school_type but MISSING student status. "
            "Classification may be incorrect. Sample rows:\n%s",
            len(missing_student),
            missing_student.unique(subset=["age", "student", "school_type"]).head(5),
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
        # College-age (18+) with HOME_SCHOOL
        | (
            pl.col("age").is_in(
                [
                    AgeCategory.AGE_18_TO_24.value,
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
    ).select("person_id", "age", "school_type")
    if len(inappropriate_school) > 0:
        warnings["age_inappropriate_school_types"] = len(inappropriate_school)
        logger.warning(
            "Found %d persons with age-inappropriate school types "
            "(e.g., teens in elementary, children in college, adults with HOME_SCHOOL). "
            "Sample rows:\n%s",
            len(inappropriate_school),
            inappropriate_school.unique(subset=["age", "school_type"]).head(5),
        )

    return warnings


def ctramp_person_type_expression(
    age_col: str = "age",
    employment_col: str = "employment",
    student_col: str = "student",
    school_type_col: str = "school_type",
    employment_category_col: str | None = None,
    student_category_col: str | None = None,
) -> pl.Expr:
    """Create expression to derive person category from person attributes.

    This replicates the pptyp logic from the old pipeline's 02a-reformat
    step, converting employment/student/age data into person type categories.

    Classification Precedence Rules (highest to lowest):
        1. Age < 5 → CHILD_UNDER_5 (Type 8) - overrides all other attributes
        2. Age 5-15 → STUDENT_NON_DRIVING_AGE (Type 7) - cannot be workers
        3. Grade/high school student (16-17) → STUDENT_DRIVING_AGE (Type 6)
           - Children stay children regardless of employment
        4. Full-time employment → FULL_TIME_WORKER (Type 1)
           - Beats student status even for full-time college students
        5. Student status + school type → determines child/university types
        6. Part-time employment → PART_TIME_WORKER (Type 2)
           - Unless also a college student → UNIVERSITY_STUDENT (Type 3)
        7. Age 65+ without employment → RETIRED (Type 5)
        8. Default by age group → NON_WORKER (Type 4) or child types

    Edge Case Handling:
        - Children with impossible employment (e.g., 10-year-old FT worker) are
          classified by age rules
        - Young adults (16-24) who are neither students nor employed are
          classified as NON_WORKER (Type 4), except 16-17 → STUDENT_DRIVING_AGE
        - Seniors 65+ with employment are classified as workers, not RETIRED
        - Impossible combinations (e.g., 4-year-old college student) should be
          logged using log_person_type_warnings() before classification

    Args:
        age_col: Name of age column (categorical AgeCategory)
        employment_col: Name of employment column (raw Employment enum)
        student_col: Name of student column (raw Student enum)
        school_type_col: Name of school_type column (raw SchoolType enum)
        employment_category_col: Optional name of pre-derived employment category
            column (CTRAMPEmploymentCategory values). If provided, used instead
            of raw employment for FT/PT classification.
        student_category_col: Optional name of pre-derived student category column
            (CTRAMPStudentCategory values). If provided, used instead of raw
            school_type for college/grade school classification.

    Returns:
        Polars expression that evaluates to CTRAMPPersonType enum value

    See Also:
        log_person_type_warnings: Function to detect and log impossible combinations

    Note:
        Age is a categorical variable (see AgeCategory enum):
        1=under 5, 2=5-15, 3=16-17, 4=18-24, 5=25-34, etc.

        When employment_category_col and student_category_col are provided,
        classification uses the pre-derived categories for consistency with
        the rest of the CT-RAMP pipeline. EMPLOYED_UNPAID is treated as
        part-time via the EMPLOYMENT_TO_CTRAMP mapping.
    """
    # Define age group categories
    senior_age = [
        AgeCategory.AGE_65_TO_74.value,
        AgeCategory.AGE_75_TO_84.value,
        AgeCategory.AGE_85_AND_UP.value,
    ]
    working_age = [
        AgeCategory.AGE_25_TO_34.value,
        AgeCategory.AGE_35_TO_44.value,
        AgeCategory.AGE_45_TO_54.value,
        AgeCategory.AGE_55_TO_64.value,
    ]

    # Employment status indicators
    # When employment_category_col is provided, use the pre-derived category
    # (which maps EMPLOYED_UNPAID → Part-time). Otherwise, use raw employment.
    if employment_category_col:
        is_full_time = (
            pl.col(employment_category_col) == CTRAMPEmploymentCategory.FULL_TIME_EMPLOYED.value
        )
        is_part_time = (
            pl.col(employment_category_col) == CTRAMPEmploymentCategory.PART_TIME_EMPLOYED.value
        )
    else:
        # Fall back to raw employment column
        # Note: EMPLOYED_SELF is always classified as full-time
        # EMPLOYED_UNPAID is classified as part-time (consistent with EMPLOYMENT_TO_CTRAMP)
        is_full_time = pl.col(employment_col).is_in(
            [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_SELF.value,
            ]
        )
        is_part_time = pl.col(employment_col).is_in(
            [
                Employment.EMPLOYED_PARTTIME.value,
                Employment.EMPLOYED_UNPAID.value,
            ]
        )

    # Student and school status indicators
    # When student_category_col is provided, use the pre-derived category.
    # Otherwise, use raw student/school_type columns.
    if student_category_col:
        is_college = pl.col(student_category_col) == CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value
        is_high_school = (
            pl.col(student_category_col) == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        )
        is_student_of_any_kind = is_college | is_high_school
    else:
        is_college = pl.col(school_type_col).is_in(
            [
                SchoolType.COLLEGE_2YEAR.value,
                SchoolType.COLLEGE_4YEAR.value,
                SchoolType.GRADUATE_SCHOOL.value,
                SchoolType.VOCATIONAL.value,
            ]
        )
        is_high_school = pl.col(school_type_col).is_in(
            [
                SchoolType.HOME_SCHOOL.value,
                SchoolType.HIGH_SCHOOL.value,
            ]
        )
        _is_student_raw = pl.col(student_col).is_in(
            [
                Student.FULLTIME_INPERSON.value,
                Student.PARTTIME_INPERSON.value,
                Student.PARTTIME_ONLINE.value,
                Student.FULLTIME_ONLINE.value,
            ]
        )
        is_college = is_college & _is_student_raw
        is_high_school = is_high_school & _is_student_raw
        is_student_of_any_kind = _is_student_raw

    # Students 18+ with MISSING school_type are assumed to be university students
    # (only used in fallback mode without student_category_col)
    if not student_category_col:
        is_student_no_school_type = is_student_of_any_kind & (
            pl.col(school_type_col) == SchoolType.MISSING.value
        )
    else:
        # When student_category is pre-derived, MISSING school_type cases are
        # already resolved to COLLEGE_OR_HIGHER by student_category_expression
        is_student_no_school_type = pl.lit(value=False)

    # Age indicators
    age = pl.col(age_col)
    is_under_5 = age == AgeCategory.AGE_UNDER_5.value
    is_5_to_15 = age == AgeCategory.AGE_5_TO_15.value
    is_16_to_17 = age == AgeCategory.AGE_16_TO_17.value
    is_18_to_24 = age == AgeCategory.AGE_18_TO_24.value
    is_working_age = age.is_in(working_age)
    is_senior = age.is_in(senior_age)

    # Must have these categories to match CT-RAMP person types:
    # FULL_TIME_WORKER = 1, "Full-time worker"
    # PART_TIME_WORKER = 2, "Part-time worker"
    # UNIVERSITY_STUDENT = 3, "University student"
    # NON_WORKER = 4, "Non-worker"
    # RETIRED = 5, "Retired"
    # STUDENT_DRIVING_AGE = 6, "Student of driving age"
    # STUDENT_NON_DRIVING_AGE = 7, "Student of non-driving age"
    # CHILD_UNDER_5 = 8, "Child too young for school"

    # Build classification expression
    _expr = (
        pl.when(is_under_5)
        .then(pl.lit(CTRAMPPersonType.CHILD_UNDER_5))
        .when(is_5_to_15)
        .then(pl.lit(CTRAMPPersonType.STUDENT_NON_DRIVING_AGE))
        # Teens: grade/high school students are children regardless of employment
        .when(is_16_to_17 & is_high_school)
        .then(pl.lit(CTRAMPPersonType.STUDENT_DRIVING_AGE))
        .when(is_16_to_17 & is_full_time)
        .then(pl.lit(CTRAMPPersonType.FULL_TIME_WORKER))
        .when(is_16_to_17 & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_16_to_17)
        .then(pl.lit(CTRAMPPersonType.STUDENT_DRIVING_AGE))
        # Young adults: full-time employment beats student status
        .when(is_18_to_24 & is_full_time)
        .then(pl.lit(CTRAMPPersonType.FULL_TIME_WORKER))
        .when(is_18_to_24 & is_high_school)
        .then(pl.lit(CTRAMPPersonType.STUDENT_DRIVING_AGE))
        # Part-time workers who are college students -> prioritize student status
        .when(is_18_to_24 & is_part_time & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_18_to_24 & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        # Students 18+ with MISSING school_type -> assume university
        .when(is_18_to_24 & is_student_no_school_type)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_18_to_24 & is_part_time)
        .then(pl.lit(CTRAMPPersonType.PART_TIME_WORKER))
        .when(is_18_to_24)
        .then(pl.lit(CTRAMPPersonType.NON_WORKER))
        # Working age: full-time employment beats student status
        .when(is_working_age & is_full_time)
        .then(pl.lit(CTRAMPPersonType.FULL_TIME_WORKER))
        # Part-time workers who are college students -> prioritize student status
        .when(is_working_age & is_part_time & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_working_age & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        # Students with MISSING school_type -> assume university
        .when(is_working_age & is_student_no_school_type)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_working_age & is_part_time)
        .then(pl.lit(CTRAMPPersonType.PART_TIME_WORKER))
        .when(is_working_age)
        .then(pl.lit(CTRAMPPersonType.NON_WORKER))
        # Seniors (65+): employment overrides RETIRED
        .when(is_senior & is_full_time)
        .then(pl.lit(CTRAMPPersonType.FULL_TIME_WORKER))
        .when(is_senior & is_part_time & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_senior & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_senior & is_part_time)
        .then(pl.lit(CTRAMPPersonType.PART_TIME_WORKER))
        .when(is_senior)
        .then(pl.lit(CTRAMPPersonType.RETIRED))
        # Catch-all (shouldn't be reached with valid AgeCategory values)
        .otherwise(pl.lit(CTRAMPPersonType.NON_WORKER))
    )

    return _expr


INDUSTRY_TO_EMPSIX = {
    Industry.AGRICULTURE.value: CTRAMPIndustry.AGREMPN.value,
    Industry.MINING.value: CTRAMPIndustry.AGREMPN.value,
    Industry.UTILITIES.value: CTRAMPIndustry.MWTEMPN.value,
    Industry.CONSTRUCTION.value: CTRAMPIndustry.OTHEMPN.value,
    Industry.MANUFACTURING.value: CTRAMPIndustry.MWTEMPN.value,
    Industry.WHOLESALE_TRADE.value: CTRAMPIndustry.MWTEMPN.value,
    Industry.RETAIL_TRADE.value: CTRAMPIndustry.RETEMPN.value,
    Industry.TRANSPORTATION.value: CTRAMPIndustry.MWTEMPN.value,
    Industry.INFORMATION.value: CTRAMPIndustry.OTHEMPN.value,
    Industry.FINANCE_AND_INSURANCE.value: CTRAMPIndustry.FPSEMPN.value,
    Industry.REALESTATE.value: CTRAMPIndustry.FPSEMPN.value,
    Industry.PROFESSIONAL.value: CTRAMPIndustry.FPSEMPN.value,
    Industry.MANAGEMENT.value: CTRAMPIndustry.FPSEMPN.value,
    Industry.ADMINISTRATIVE.value: CTRAMPIndustry.FPSEMPN.value,
    Industry.EDUCATIONAL.value: CTRAMPIndustry.HEREMPN.value,
    Industry.HEALTH_AND_SOCIAL.value: CTRAMPIndustry.HEREMPN.value,
    Industry.ARTS_AND_RECREATION.value: CTRAMPIndustry.HEREMPN.value,
    Industry.ACCOMMODATION.value: CTRAMPIndustry.HEREMPN.value,
    Industry.OTHER.value: CTRAMPIndustry.OTHEMPN.value,
    Industry.PUBLIC_ADMINISTRATION.value: CTRAMPIndustry.OTHEMPN.value,
}

# Keyword fallback for the free-text "Other, please specify" industry response.
# Only used to fill empsix that the structured `industry` code left null.
INDUSTRY_OTHER_KEYWORD_TO_EMPSIX = {
    # Financial and professional services
    "technology": CTRAMPIndustry.FPSEMPN.value,
    "biotechnology": CTRAMPIndustry.FPSEMPN.value,
    "biotech": CTRAMPIndustry.FPSEMPN.value,
    "biomedical": CTRAMPIndustry.FPSEMPN.value,
    "tech": CTRAMPIndustry.FPSEMPN.value,
    "software": CTRAMPIndustry.FPSEMPN.value,
    "security": CTRAMPIndustry.FPSEMPN.value,
    "legal": CTRAMPIndustry.FPSEMPN.value,
    "law": CTRAMPIndustry.FPSEMPN.value,
    "attorney": CTRAMPIndustry.FPSEMPN.value,
    "marketing": CTRAMPIndustry.FPSEMPN.value,
    # Other employment
    "government": CTRAMPIndustry.OTHEMPN.value,
    "judicial": CTRAMPIndustry.OTHEMPN.value,
    "national park service": CTRAMPIndustry.OTHEMPN.value,
    "law enforcement": CTRAMPIndustry.OTHEMPN.value,
    "military": CTRAMPIndustry.OTHEMPN.value,
    "library": CTRAMPIndustry.OTHEMPN.value,
    # Manufacturing, wholesale and transportation
    "automotive": CTRAMPIndustry.MWTEMPN.value,
    # Health, educational and recreational services
    "nonprofit": CTRAMPIndustry.HEREMPN.value,
    "non-profit": CTRAMPIndustry.HEREMPN.value,
    "non profit": CTRAMPIndustry.HEREMPN.value,
    "philanthropy": CTRAMPIndustry.HEREMPN.value,
    "childcare": CTRAMPIndustry.HEREMPN.value,
    "health": CTRAMPIndustry.HEREMPN.value,
    "fitness": CTRAMPIndustry.HEREMPN.value,
    "school": CTRAMPIndustry.HEREMPN.value,
    "hospitality": CTRAMPIndustry.HEREMPN.value,
    "hotel": CTRAMPIndustry.HEREMPN.value,
    # Retail trade
    "e-commerce": CTRAMPIndustry.RETEMPN.value,
    "ecommerce": CTRAMPIndustry.RETEMPN.value,
}


def add_industry_empsix(persons: pl.DataFrame) -> pl.DataFrame:
    """Add an `industry_empsix` column derived from canonical `industry`.

    Uses the structured NAICS `industry` code first, then fills any remaining
    nulls from keyword matches in the free-text `industry_other` when that column
    is present. Both inputs are read from the canonical persons frame; nothing is
    required upstream.

    Args:
        persons: Canonical persons frame (must have `industry`; `industry_other`
            is used when present).

    Returns:
        persons with an added `industry_empsix` column.
    """
    if "industry" not in persons.columns:
        logger.warning("'industry' column not found; industry_empsix will be null for all persons")
        return persons.with_columns(pl.lit(None).cast(pl.String).alias("industry_empsix"))

    persons = persons.with_columns(
        pl.col("industry").replace_strict(INDUSTRY_TO_EMPSIX, default=None).alias("industry_empsix")
    )

    if "industry_other" in persons.columns:
        # Fill nulls from the free-text response. Materialise industry_empsix each
        # iteration so pl.col("industry_empsix") stays a cheap column reference;
        # accumulating it into a Python expression instead would build an
        # exponentially large expression tree.
        persons = persons.with_columns(
            pl.col("industry_other").str.to_lowercase().str.strip_chars().alias("_industry_other")
        )
        for term, sector in INDUSTRY_OTHER_KEYWORD_TO_EMPSIX.items():
            persons = persons.with_columns(
                pl.when(
                    pl.col("industry_empsix").is_null()
                    & pl.col("_industry_other").str.contains(term, literal=True)
                )
                .then(pl.lit(sector))
                .otherwise(pl.col("industry_empsix"))
                .alias("industry_empsix")
            )
        persons = persons.drop("_industry_other")

    return persons


def bad_person_type_combinations() -> pl.DataFrame:
    """Return a lookup table of known bad person_type / attribute combinations.

    Each row describes a (person_type, student_category, employment_category,
    age_bin) combo that should not occur, along with the expected correct
    person_type classification.  Used by ``debug_ptype`` to flag rows in the
    frequency distribution table.

    Bad combos (from BATS 2023 analysis):
    │ Full-time worker           ┆ College or higher    ┆ Part-time employed  ┆ 18-64  │ → university student
    │ Full-time worker           ┆ Grade or high school ┆ Full-time employed  ┆ 5-17   │ → child of non-driving age
    │ Full-time worker           ┆ Grade or high school ┆ Part-time employed  ┆ 5-17   │ → child type by age
    │ Full-time worker           ┆ Not a student        ┆ Part-time employed  ┆ 18-64  │ → part-time worker
    │ Nonworker                  ┆ College or higher    ┆ Not employed        ┆ 18-64  │ → university student
    │ Part-time worker           ┆ College or higher    ┆ Part-time employed  ┆ 18-64  │ → university student
    │ Retired                    ┆ College or higher    ┆ Full-time employed  ┆ 65+    │ → full-time worker
    │ Retired                    ┆ College or higher    ┆ Part-time employed  ┆ 65+    │ → part-time worker
    │ Retired                    ┆ Not a student        ┆ Full-time employed  ┆ 65+    │ → full-time worker
    │ Retired                    ┆ Not a student        ┆ Part-time employed  ┆ 65+    │ → part-time worker
    │ University student         ┆ College or higher    ┆ Full-time employed  ┆ 18-64  │ → full-time worker
    │ University student         ┆ Not a student        ┆ Not employed        ┆ 18-64  │ → nonworker
    │ University student         ┆ Not a student        ┆ Part-time employed  ┆ 18-64  │ → part-time worker
    │ Child of driving age       ┆ Not a student        ┆ Not employed        ┆ 18-64  │ → nonworker

    Note: rows 2-3 share the same join key (driving vs non-driving depends on
    exact age within 5-17); consolidated as "child type by age" in the rules.

    Returns:
        DataFrame with columns (person_type, student_category,
        employment_category, age_bin, expected).
    """  # noqa: E501
    pt = CTRAMPPersonType
    ec = CTRAMPEmploymentCategory
    sc = CTRAMPStudentCategory

    # fmt: off
    # (person_type_label, student_category, employment_category, age_bin, expected)
    rows = [
        (pt.FULL_TIME_WORKER.label,   sc.COLLEGE_OR_HIGHER.value,    ec.PART_TIME_EMPLOYED.value, "18-64", "university student"),  # noqa: E501
        (pt.FULL_TIME_WORKER.label,   sc.GRADE_OR_HIGH_SCHOOL.value, ec.FULL_TIME_EMPLOYED.value, "5-17",  "child type by age"),  # noqa: E501
        (pt.FULL_TIME_WORKER.label,   sc.GRADE_OR_HIGH_SCHOOL.value, ec.PART_TIME_EMPLOYED.value, "5-17",  "child type by age"),  # noqa: E501
        (pt.FULL_TIME_WORKER.label,   sc.NOT_STUDENT.value,          ec.PART_TIME_EMPLOYED.value, "18-64", "part-time worker"),  # noqa: E501
        (pt.NON_WORKER.label,         sc.COLLEGE_OR_HIGHER.value,    ec.NOT_EMPLOYED.value,       "18-64", "university student"),  # noqa: E501
        (pt.PART_TIME_WORKER.label,   sc.COLLEGE_OR_HIGHER.value,    ec.PART_TIME_EMPLOYED.value, "18-64", "university student"),  # noqa: E501
        (pt.RETIRED.label,            sc.COLLEGE_OR_HIGHER.value,    ec.FULL_TIME_EMPLOYED.value, "65+",   "full-time worker"),  # noqa: E501
        (pt.RETIRED.label,            sc.COLLEGE_OR_HIGHER.value,    ec.PART_TIME_EMPLOYED.value, "65+",   "part-time worker"),  # noqa: E501
        (pt.RETIRED.label,            sc.NOT_STUDENT.value,          ec.FULL_TIME_EMPLOYED.value, "65+",   "full-time worker"),  # noqa: E501
        (pt.RETIRED.label,            sc.NOT_STUDENT.value,          ec.PART_TIME_EMPLOYED.value, "65+",   "part-time worker"),  # noqa: E501
        (pt.UNIVERSITY_STUDENT.label, sc.COLLEGE_OR_HIGHER.value,    ec.FULL_TIME_EMPLOYED.value, "18-64", "full-time worker"),  # noqa: E501
        (pt.UNIVERSITY_STUDENT.label, sc.NOT_STUDENT.value,          ec.NOT_EMPLOYED.value,       "18-64", "nonworker"),  # noqa: E501
        (pt.UNIVERSITY_STUDENT.label, sc.NOT_STUDENT.value,          ec.PART_TIME_EMPLOYED.value, "18-64", "part-time worker"),  # noqa: E501
        (pt.STUDENT_DRIVING_AGE.label,  sc.NOT_STUDENT.value,          ec.NOT_EMPLOYED.value,       "18-64", "nonworker"),  # noqa: E501
    ]
    # fmt: on

    return pl.DataFrame(
        rows,
        schema=["person_type", "student_category", "employment_category", "age_bin", "expected"],
        orient="row",
    )
