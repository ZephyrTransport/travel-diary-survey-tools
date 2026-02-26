"""Mapping dictionaries for CT-RAMP formatting.

This module contains lookup tables and mappings to transform canonical
survey data into CT-RAMP model format.
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPEmploymentCategory,
    CTRAMPGender,
    CTRAMPModeType,
    CTRAMPPersonType,
    CTRAMPPurpose,
    CTRAMPStudentCategory,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import AccessEgressMode, ModeType, PurposeCategory

from .ctramp_config import CTRAMPConfig

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
PURPOSECATEGORY_TO_JTF_GROUP = {
    # Shopping
    PurposeCategory.SHOP.value: "S",
    # Maintenance/errands
    PurposeCategory.ERRAND.value: "M",
    # Eating out
    PurposeCategory.MEAL.value: "E",
    # Visiting/social/recreational
    PurposeCategory.SOCIALREC.value: "V",
    # Discretionary - Work/School (typically not joint, but possible)
    PurposeCategory.WORK.value: "D",
    PurposeCategory.WORK_RELATED.value: "D",
    PurposeCategory.SCHOOL.value: "D",
    PurposeCategory.SCHOOL_RELATED.value: "D",
    # Discretionary - Escort
    PurposeCategory.ESCORT.value: "D",
    # Discretionary - Other activities
    PurposeCategory.OTHER.value: "D",
    # Discretionary - Home/overnight (not typical joint tour destinations)
    PurposeCategory.HOME.value: "D",
    PurposeCategory.OVERNIGHT.value: "D",
    # Discretionary - Mode change (transfer point, not a tour purpose)
    PurposeCategory.CHANGE_MODE.value: "D",
    # Discretionary - Data quality issues
    PurposeCategory.MISSING.value: "D",
    PurposeCategory.PNTA.value: "D",
    PurposeCategory.NOT_IMPUTABLE.value: "D",
}


def ctramp_purpose_category_expression(
    purpose_category: pl.Expr,
    income: pl.Expr,
    school_type: pl.Expr,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
) -> pl.Expr:
    """Map canonical PurposeCategory to CTRAMP purpose string.

    CTRAMP requires detailed purpose strings that distinguish work income
    levels (low/med/high/very high) and school types (grade/high/university).

    Args:
        purpose_category: Polars expression for canonical purpose category
            (from trips.PurposeCategory enum)
        income: Polars expression for household income (absolute dollars)
        school_type: Polars expression for school type
            (from persons.SchoolType enum)
        income_low_threshold: Income threshold for low bracket
        income_med_threshold: Income threshold for med bracket
        income_high_threshold: Income threshold for high bracket

    Returns:
        Polars expression resolving to CTRAMP purpose string
    """
    # Compute student category from school_type enum
    student_category = (
        pl.when(
            school_type.is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        .when(
            school_type.is_in(
                [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.MIDDLE_SCHOOL.value,
                    SchoolType.HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        .otherwise(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
    )

    # Home purpose
    home_expr = pl.when(purpose_category == PurposeCategory.HOME.value).then(
        pl.lit(CTRAMPPurpose.HOME.value)
    )

    # Work purposes - segmented by income
    work_income_segmentation = (
        pl.when(income < income_low_threshold)
        .then(pl.lit(CTRAMPPurpose.WORK_LOW.value))
        .when(income < income_med_threshold)
        .then(pl.lit(CTRAMPPurpose.WORK_MED.value))
        .when(income < income_high_threshold)
        .then(pl.lit(CTRAMPPurpose.WORK_HIGH.value))
        .otherwise(pl.lit(CTRAMPPurpose.WORK_VERY_HIGH.value))
    )
    work_expr = home_expr.when(
        purpose_category.is_in([PurposeCategory.WORK.value, PurposeCategory.WORK_RELATED.value])
    ).then(work_income_segmentation)

    # School purposes - segmented by student type
    school_segmentation_expr = (
        pl.when(student_category == CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value)
        .then(pl.lit(CTRAMPPurpose.UNIVERSITY.value))
        .when(student_category == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value)
        .then(pl.lit(CTRAMPPurpose.SCHOOL_HIGH.value))
        .otherwise(pl.lit(CTRAMPPurpose.SCHOOL_GRADE.value))
    )
    school_expr = work_expr.when(
        purpose_category.is_in([PurposeCategory.SCHOOL.value, PurposeCategory.SCHOOL_RELATED.value])
    ).then(school_segmentation_expr)

    # At-work sub-tour (work-related)
    atwork_expr = school_expr.when(purpose_category == PurposeCategory.WORK_RELATED.value).then(
        pl.lit(CTRAMPPurpose.ATWORK_BUSINESS.value)
    )

    # Eating out
    eatout_expr = atwork_expr.when(purpose_category == PurposeCategory.MEAL.value).then(
        pl.lit(CTRAMPPurpose.EATOUT.value)
    )

    # Escort
    escort_segmentation_expr = (
        pl.when(
            student_category.is_in(
                [
                    CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value,
                    CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPPurpose.ESCORT_KIDS.value))
        .otherwise(pl.lit(CTRAMPPurpose.ESCORT_NO_KIDS.value))
    )
    escort_expr = eatout_expr.when(purpose_category == PurposeCategory.ESCORT.value).then(
        escort_segmentation_expr
    )

    # Shopping
    shopping_expr = escort_expr.when(purpose_category == PurposeCategory.SHOP.value).then(
        pl.lit(CTRAMPPurpose.SHOPPING.value)
    )

    # Social/recreation
    social_expr = shopping_expr.when(purpose_category == PurposeCategory.SOCIALREC.value).then(
        pl.lit(CTRAMPPurpose.SOCIAL.value)
    )

    # Maintenance/errands
    maintenance_expr = social_expr.when(purpose_category == PurposeCategory.ERRAND.value).then(
        pl.lit(CTRAMPPurpose.OTHMAINT.value)
    )

    # Discretionary - all others
    return maintenance_expr.otherwise(pl.lit(CTRAMPPurpose.OTHDISCR.value))


def ctramp_mode_expression(
    mode_type: pl.Expr,
    num_travelers: pl.Expr,
    access_mode: pl.Expr | None = None,
    egress_mode: pl.Expr | None = None,
) -> pl.Expr:
    """Map canonical mode_type to CTRAMP mode integer code.

    Args:
        mode_type: Polars expression for canonical mode_type
            (from ModeType enum)
        num_travelers: Polars expression for number of travelers in vehicle
        access_mode: Optional polars expression for access mode (AccessEgressMode enum)
        egress_mode: Optional polars expression for egress mode (AccessEgressMode enum)

    Returns:
        Polars expression resolving to CTRAMPModeType integer code (21 codes)

    Notes:
        - Walk=7, Bike=8
        - Transit: WLK_LOC_WLK=9 (walk-to-transit) or DRV_LOC_WLK=14 (drive-to-transit)
          Uses access_mode/egress_mode to detect drive-to-transit
        - Personal vehicle by occupancy: DA=1, SR2=3, SR3=5 (non-toll)
        - TNC: Single passenger=20, Shared=21
        - Taxi=19
        - School bus treated as SR3=5
        - Unknown modes default to DA=1
    """
    # Walk mode
    walk_expr = pl.when(mode_type == ModeType.WALK.value).then(pl.lit(CTRAMPModeType.WALK.value))

    # Bike and micromobility modes
    bike_modes = [
        ModeType.BIKE.value,
        ModeType.BIKESHARE.value,
        ModeType.SCOOTERSHARE.value,
    ]
    bike_expr = walk_expr.when(mode_type.is_in(bike_modes)).then(pl.lit(CTRAMPModeType.BIKE.value))

    # Transit modes - check for drive-to-transit via access/egress modes
    # Default to walk-local bus-walk (WLK_LOC_WLK=9)
    # If drove to transit (access or egress by car), use DRV_LOC_WLK=14
    transit_modes = [
        ModeType.TRANSIT.value,
        ModeType.FERRY.value,
        ModeType.SHUTTLE.value,
    ]
    # Define drive access/egress modes from canonical AccessEgressMode enum
    drove_access_egress = [
        AccessEgressMode.TNC.value,
        AccessEgressMode.CAR_HOUSEHOLD.value,
        AccessEgressMode.CAR_OTHER.value,
        AccessEgressMode.DROPOFF_HOUSEHOLD.value,
        AccessEgressMode.DROPOFF_OTHER.value,
    ]

    if access_mode is not None and egress_mode is not None:
        # Check if either access or egress involved driving
        drove_to_transit = access_mode.is_in(drove_access_egress) | egress_mode.is_in(
            drove_access_egress
        )
        transit_mode_code = (
            pl.when(drove_to_transit)
            .then(pl.lit(CTRAMPModeType.DRV_LOC_WLK.value))
            .otherwise(pl.lit(CTRAMPModeType.WLK_LOC_WLK.value))
        )
    else:
        # No access/egress info available, default to walk-to-transit
        transit_mode_code = pl.lit(CTRAMPModeType.WLK_LOC_WLK.value)

    transit_expr = bike_expr.when(mode_type.is_in(transit_modes)).then(transit_mode_code)

    # School bus - treat as SR3
    school_bus_expr = transit_expr.when(mode_type == ModeType.SCHOOL_BUS.value).then(
        pl.lit(CTRAMPModeType.SR3.value)
    )

    # Taxi - specific code
    taxi_expr = school_bus_expr.when(mode_type == ModeType.TAXI.value).then(
        pl.lit(CTRAMPModeType.TAXI.value)
    )

    # TNC - distinguish between single (TNC=20) and shared (TNC2=21)
    tnc_occupancy = (
        pl.when(num_travelers == 1)
        .then(pl.lit(CTRAMPModeType.TNC.value))
        .otherwise(pl.lit(CTRAMPModeType.TNC2.value))
    )
    tnc_expr = taxi_expr.when(mode_type == ModeType.TNC.value).then(tnc_occupancy)

    # Personal vehicle (CAR, CARSHARE) - distinguish by occupancy (non-toll)
    auto_modes = [
        ModeType.CAR.value,
        ModeType.CARSHARE.value,
    ]
    auto_occupancy_segmentation = (
        pl.when(num_travelers == 1)
        .then(pl.lit(CTRAMPModeType.DA.value))
        .when(num_travelers == 2)  # noqa: PLR2004
        .then(pl.lit(CTRAMPModeType.SR2.value))
        .otherwise(pl.lit(CTRAMPModeType.SR3.value))
    )
    auto_expr = tnc_expr.when(mode_type.is_in(auto_modes)).then(auto_occupancy_segmentation)

    # Default to drive alone (DA=1) for OTHER, LONG_DISTANCE, MISSING, and any unknown modes
    return auto_expr.otherwise(pl.lit(CTRAMPModeType.DA.value))


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
        2. Age 5-15 → CHILD_NON_DRIVING_AGE (Type 6) - cannot be workers
        3. Grade/high school student (16-17) → CHILD_DRIVING_AGE (Type 7)
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
          classified as NON_WORKER (Type 4), except 16-17 → CHILD_DRIVING_AGE
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
    # NON_WORKER = 4, "Nonworker"
    # RETIRED = 5, "Retired"
    # CHILD_NON_DRIVING_AGE = 6, "Child of non-driving age"
    # CHILD_DRIVING_AGE = 7, "Child of driving age"
    # CHILD_UNDER_5 = 8, "Child too young for school"

    # Build classification expression
    _expr = (
        pl.when(is_under_5)
        .then(pl.lit(CTRAMPPersonType.CHILD_UNDER_5))
        .when(is_5_to_15)
        .then(pl.lit(CTRAMPPersonType.CHILD_NON_DRIVING_AGE))
        # Teens: grade/high school students are children regardless of employment
        .when(is_16_to_17 & is_high_school)
        .then(pl.lit(CTRAMPPersonType.CHILD_DRIVING_AGE))
        .when(is_16_to_17 & is_full_time)
        .then(pl.lit(CTRAMPPersonType.FULL_TIME_WORKER))
        .when(is_16_to_17 & is_college)
        .then(pl.lit(CTRAMPPersonType.UNIVERSITY_STUDENT))
        .when(is_16_to_17)
        .then(pl.lit(CTRAMPPersonType.CHILD_DRIVING_AGE))
        # Young adults: full-time employment beats student status
        .when(is_18_to_24 & is_full_time)
        .then(pl.lit(CTRAMPPersonType.FULL_TIME_WORKER))
        .when(is_18_to_24 & is_high_school)
        .then(pl.lit(CTRAMPPersonType.CHILD_DRIVING_AGE))
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


# Validate mapping completeness at module load time
_all_purpose_categories = {pc.value for pc in PurposeCategory}
_mapped_categories = set(PURPOSECATEGORY_TO_JTF_GROUP.keys())
_missing_categories = _all_purpose_categories - _mapped_categories
if _missing_categories:
    msg = f"Missing PurposeCategory mappings in PURPOSECATEGORY_TO_JTF_GROUP: {_missing_categories}"
    raise ValueError(msg)
_duplicate_check = len(PURPOSECATEGORY_TO_JTF_GROUP)
if _duplicate_check != len(_all_purpose_categories):
    msg = "Duplicate keys found in PURPOSECATEGORY_TO_JTF_GROUP mapping"
    raise ValueError(msg)
