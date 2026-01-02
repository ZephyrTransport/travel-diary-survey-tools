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

from pydantic import BaseModel, Field

from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import PersonType
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
    purpose_priority_by_persontype: dict[str, dict[PurposeCategory, int]] = Field(
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

    # Map detailed person types to simplified categories for priority lookup
    person_type_mapping: dict[PersonType, str] = Field(
        default={
            PersonType.FULL_TIME_WORKER: PersonCategory.WORKER,
            PersonType.PART_TIME_WORKER: PersonCategory.WORKER,
            PersonType.RETIRED: PersonCategory.OTHER,
            PersonType.NON_WORKER: PersonCategory.OTHER,
            PersonType.UNIVERSITY_STUDENT: PersonCategory.STUDENT,
            PersonType.HIGH_SCHOOL_STUDENT: PersonCategory.STUDENT,
            PersonType.CHILD_5_15: PersonCategory.STUDENT,
            PersonType.CHILD_UNDER_5: PersonCategory.OTHER,
        },
        description=("Maps detailed person types to simplified categories for tour logic"),
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

    class ConfigDict:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True  # Allow enum types
