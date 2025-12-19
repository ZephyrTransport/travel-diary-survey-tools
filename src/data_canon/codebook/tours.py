"""Tour building step for processing travel diary data."""

from data_canon.core.labeled_enum import LabeledEnum


class TourType(LabeledEnum):
    """d_tour_type value labels."""

    HOME_BASED = (1, "Home-based tour")
    WORK_BASED = (2, "Work-based tour")


class PersonCategory:
    """Simplified person categories for tour purpose prioritization."""

    WORKER = "worker"
    STUDENT = "student"
    OTHER = "other"


class TourCategory(LabeledEnum):
    """Tour boundary types."""

    COMPLETE = (1, "Start at home, end at home")
    PARTIAL_END = (2, "Start at home, end not at home")
    PARTIAL_START = (3, "Start not at home, end at home")
    PARTIAL_BOTH = (4, "Start not at home, end not at home")


class TourDirection(LabeledEnum):
    """Half-tour classification."""

    OUTBOUND = (1, "Outbound half-tour")
    INBOUND = (2, "Inbound half-tour")
    SUBTOUR = (3, "Subtour")


class TourDataQuality(LabeledEnum):
    """Tour data quality classification for validation and filtering."""

    VALID = (0, "Valid tour")
    SINGLE_TRIP = (1, "Single-trip tour")
    LOOP_TRIP = (2, "Home-based loop trip")
    MISSING_HOME_ANCHOR = (3, "No home anchor at either end of tour")
    INDETERMINATE = (4, "Invalid tour, cause unknown")
    CHANGE_MODE = (5, "Change mode as primary purpose (linking failure)")
