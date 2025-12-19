"""Generic codebook enums used across multiple data canon modules."""

from data_canon.core.labeled_enum import LabeledEnum


class Select(LabeledEnum):
    """Generic selected/not selected value labels."""

    SELECTED = (1, "Selected")
    NOT_SELECTED = (0, "Not selected")
    MISSING = (995, "Missing Response")


class YesNoMissing(LabeledEnum):
    """Generic yes/no/missing value labels."""

    YES = (1, "Yes")
    NO = (2, "No")
    MISSING = (995, "Missing Response")


class BooleanYesNo(LabeledEnum):
    """Generic boolean yes/no value labels."""

    YES = (1, "Yes")
    NO = (0, "No")


class LocationType(LabeledEnum):
    """Generic location type labels."""

    HOME = (1, "Home")
    WORK = (2, "Work")
    SCHOOL = (3, "School")
    OTHER = (4, "Other")
