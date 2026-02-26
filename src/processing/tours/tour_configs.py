"""Configuration models for tour building parameters.

ALGORITHM DESIGN NOTES:
========================

This module implements a flexible tour extraction algorithm with configurable
behavior for different use cases. Key design decisions:

1. LOCATION CLASSIFICATION:
   Uses hybrid strategy: matches location if EITHER purpose code OR distance
   indicates the location. This handles both precise GPS and imprecise cases.

2. TOUR COMPLETENESS:
   Incomplete tours are allowed (e.g., missing return home), but are flagged
   and categorized accordingly in output so they can be handled downstream.

3. PURPOSE PRIORITY:
   Configurable hierarchies by person type. By default, work_related has same
   priority as work. Duration tie-breaking can be enabled via config.

4. MODE HIERARCHY:
   Ordered list where later = higher priority. Single tour mode by default,
   half-tour modes can be enabled via config.

5. WORK-BASED SUBTOURS:
   By default detects all work departures/returns. Can restrict to usual
   workplace only via config.detect_usual_workplace.

OUTPUT FORMAT:
--------------
Returns tour-level and trip-level DataFrames with enum types and minute-based
times. Downstream formatters can transform to Daysim format (integer codes,
HHMM times, tour reordering, etc.) as needed.
"""

import polars as pl
from pydantic import BaseModel, Field

from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import AgeCategory, Employment, SchoolType, Student
from data_canon.codebook.tours import PersonCategory
from data_canon.codebook.trips import ModeType, PurposeCategory


class TourConfig(BaseModel):
    """Configuration model for tour building parameters.

    This config uses Pydantic for validation and provides type-safe access
    to tour building parameters including distance thresholds, mode
    hierarchies, and purpose priorities.
    """

    # Distance thresholds for location matching (in meters)
    distance_thresholds: dict[LocationType, float] = Field(
        default={
            LocationType.HOME: 100.0,
            LocationType.WORK: 100.0,
            LocationType.SCHOOL: 100.0,
        },
        description=(
            "Distance thresholds in meters for matching trip ends "
            "to known locations (also used to identify multiple visits "
            "to primary destination)"
        ),
    )

    # Mode hierarchy: position in list determines priority
    # (later in list = higher priority for tour mode assignment)
    # NOTE: This eventually should be replaced to support more complex
    # multi-modal tours (e.g., drive-transit-walk, etc.)
    mode_hierarchy: list[ModeType] = Field(
        default=[
            ModeType.WALK,
            ModeType.BIKE,
            ModeType.BIKESHARE,
            ModeType.SCOOTERSHARE,
            ModeType.CAR,
            ModeType.CARSHARE,
            ModeType.TAXI,
            ModeType.TNC,
            ModeType.SHUTTLE,
            ModeType.SCHOOL_BUS,
            ModeType.FERRY,
            ModeType.TRANSIT,
            ModeType.LONG_DISTANCE,
        ],
        description=("Ordered list of mode types by priority - later in list = higher priority"),
    )

    # Purpose priority by person category: lower number = higher priority
    # All non-HOME purposes must be explicitly mapped
    purpose_priority_by_personcat: dict[str, dict[PurposeCategory, int]] = Field(
        default={
            PersonCategory.WORKER: {
                PurposeCategory.WORK: 1,
                PurposeCategory.WORK_RELATED: 1,
                PurposeCategory.SCHOOL: 2,
                PurposeCategory.SCHOOL_RELATED: 2,
                PurposeCategory.ESCORT: 3,
                PurposeCategory.SHOP: 4,
                PurposeCategory.MEAL: 4,
                PurposeCategory.SOCIALREC: 4,
                PurposeCategory.ERRAND: 4,
                PurposeCategory.CHANGE_MODE: 5,
                PurposeCategory.OVERNIGHT: 5,
                PurposeCategory.OTHER: 5,
                PurposeCategory.MISSING: 5,
                PurposeCategory.PNTA: 5,
                PurposeCategory.NOT_IMPUTABLE: 5,
            },
            PersonCategory.STUDENT: {
                PurposeCategory.SCHOOL: 1,
                PurposeCategory.SCHOOL_RELATED: 1,
                PurposeCategory.WORK: 2,
                PurposeCategory.WORK_RELATED: 2,
                PurposeCategory.ESCORT: 3,
                PurposeCategory.SHOP: 4,
                PurposeCategory.MEAL: 4,
                PurposeCategory.SOCIALREC: 4,
                PurposeCategory.ERRAND: 4,
                PurposeCategory.CHANGE_MODE: 5,
                PurposeCategory.OVERNIGHT: 5,
                PurposeCategory.OTHER: 5,
                PurposeCategory.MISSING: 5,
                PurposeCategory.PNTA: 5,
                PurposeCategory.NOT_IMPUTABLE: 5,
            },
            PersonCategory.OTHER: {
                PurposeCategory.WORK: 1,
                PurposeCategory.WORK_RELATED: 1,
                PurposeCategory.SCHOOL: 2,
                PurposeCategory.SCHOOL_RELATED: 2,
                PurposeCategory.ESCORT: 3,
                PurposeCategory.SHOP: 4,
                PurposeCategory.MEAL: 4,
                PurposeCategory.SOCIALREC: 4,
                PurposeCategory.ERRAND: 4,
                PurposeCategory.CHANGE_MODE: 5,
                PurposeCategory.OVERNIGHT: 5,
                PurposeCategory.OTHER: 5,
                PurposeCategory.MISSING: 5,
                PurposeCategory.PNTA: 5,
                PurposeCategory.NOT_IMPUTABLE: 5,
            },
        },
        description=(
            "Priority order for determining tour purpose by person "
            "category (lower = higher priority). All non-HOME purposes "
            "must be explicitly defined."
        ),
    )

    # ===================================================================
    # TOUR EXTRACTION BEHAVIOR
    # ===================================================================

    check_multiday_gaps: bool = Field(
        default=False,
        description=("If True, reset tour boundaries when day_gap > 1 between trips."),
    )

    detect_usual_workplace: bool = Field(
        default=False,
        description=(
            "If True, only detect work-based subtours from 'usual workplace' "
            "(coordinate match to work_lat/work_lon)."
        ),
    )

    default_activity_duration_minutes: float = Field(
        default=240.0,
        description=(
            "Default activity duration in minutes for last trip of day "
            "when calculating duration tie-breaker (legacy: 4 hours)."
        ),
    )

    def person_category_expression(
        self,
        age_col: str = "age",
        employment_col: str = "employment",
        student_col: str = "student",
        school_type_col: str = "school_type",
    ) -> pl.Expr:
        """Create expression to derive person category from person attributes.

        This replicates the pptyp logic from the old pipeline's 02a-reformat
        step, converting employment/student/age data into person type categories.

        Args:
            age_col: Name of age column (categorical AgeCategory)
            employment_col: Name of employment column
            student_col: Name of student column
            school_type_col: Name of school_type column

        Returns:
            Polars expression that evaluates to PersonCategory enum value

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
        is_full_time = pl.col(employment_col).is_in(
            [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_SELF.value,
                Employment.EMPLOYED_UNPAID.value,
            ]
        )
        is_part_time = pl.col(employment_col).is_in(
            [
                Employment.EMPLOYED_PARTTIME.value,
                Employment.EMPLOYED_SELF.value,
            ]
        )

        # Student and school status indicators
        is_student = pl.col(student_col).is_in(
            [
                Student.FULLTIME_INPERSON.value,
                Student.PARTTIME_INPERSON.value,
                Student.PARTTIME_ONLINE.value,
                Student.FULLTIME_ONLINE.value,
            ]
        )
        is_high_school = pl.col(school_type_col).is_in(
            [
                SchoolType.HOME_SCHOOL.value,
                SchoolType.HIGH_SCHOOL.value,
            ]
        )

        # Age indicators
        age = pl.col(age_col)
        is_under_5 = age == AgeCategory.AGE_UNDER_5.value
        is_5_to_15 = age == AgeCategory.AGE_5_TO_15.value
        is_16_to_17 = age == AgeCategory.AGE_16_TO_17.value
        is_18_to_24 = age == AgeCategory.AGE_18_TO_24.value
        is_working_age = age.is_in(working_age)

        # Build classification expression
        _expr = (
            pl.when(is_under_5)
            .then(pl.lit(PersonCategory.OTHER))
            .when(is_5_to_15)
            .then(pl.lit(PersonCategory.STUDENT))
            .when(is_16_to_17 & is_full_time)
            .then(pl.lit(PersonCategory.WORKER))
            .when(is_16_to_17 & is_student)
            .then(pl.lit(PersonCategory.STUDENT))
            .when(is_18_to_24 & is_full_time)
            .then(pl.lit(PersonCategory.WORKER))
            .when(is_18_to_24 & is_high_school & is_student)
            .then(pl.lit(PersonCategory.STUDENT))
            .when(is_18_to_24 & is_student)
            .then(pl.lit(PersonCategory.STUDENT))
            .when(is_18_to_24 & is_part_time)
            .then(pl.lit(PersonCategory.WORKER))
            .when(is_working_age & is_full_time)
            .then(pl.lit(PersonCategory.WORKER))
            .when(is_working_age & is_student)
            .then(pl.lit(PersonCategory.STUDENT))
            .when(is_working_age & is_part_time)
            .then(pl.lit(PersonCategory.WORKER))
            .when(is_working_age)
            .then(pl.lit(PersonCategory.OTHER))
            .otherwise(pl.lit(PersonCategory.OTHER))
            .alias("person_category")
        )

        return _expr

    class ConfigDict:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True  # Allow enum types
