"""Codebook enumerations for DaySim model format.

DaySim is an activity-based travel demand model that requires specific
coding schemes. These enums define the output codes expected by DaySim.
"""

from data_canon.core.labeled_enum import LabeledEnum


class DaysimMode(LabeledEnum):
    """DaySim mode codes.

    Mode hierarchy used by DaySim activity-based model.
    """

    OTHER = (0, "Other")
    WALK = (1, "Walk")
    BIKE = (2, "Bike")
    SOV = (3, "Drive alone (SOV)")
    HOV2 = (4, "Shared ride 2 (HOV2)")
    HOV3 = (5, "Shared ride 3+ (HOV3+)")
    WALK_TRANSIT = (6, "Walk to transit")
    DRIVE_TRANSIT = (7, "Drive to transit")
    SCHOOL_BUS = (8, "School bus")
    TNC = (9, "TNC (Uber/Lyft)")


class DaysimPathType(LabeledEnum):
    """DaySim path type codes.

    Network and transit service type indicators.
    """

    NONE = (0, "None")
    FULL_NETWORK = (1, "Full network")
    NO_TOLL = (2, "No-toll network")
    BUS = (3, "Bus")
    LRT = (4, "Light rail")
    PREMIUM = (5, "Premium (commuter rail/express bus)")
    BART = (6, "BART")
    FERRY = (7, "Ferry")


class DaysimDriverPassenger(LabeledEnum):
    """DaySim driver/passenger/occupancy codes.

    Indicates role in vehicle and TNC occupancy.
    """

    DRIVER = (1, "Driver")
    PASSENGER = (2, "Passenger")
    NA = (3, "N/A (non-auto mode)")
    MISSING = (9, "Missing (auto mode, unknown role)")
    TNC_ALONE = (11, "TNC alone")
    TNC_2 = (12, "TNC with 2 passengers")
    TNC_3PLUS = (13, "TNC with 3+ passengers")


class DaysimPurpose(LabeledEnum):
    """DaySim purpose codes.

    Activity purpose codes for trip ends.
    """

    HOME = (0, "Home")
    WORK = (1, "Work")
    SCHOOL = (2, "School")
    ESCORT = (3, "Escort")
    PERSONAL_BUSINESS = (4, "Personal business")
    SHOP = (5, "Shop")
    MEAL = (6, "Meal")
    SOCIAL_REC = (7, "Social/recreation")
    CHANGE_MODE = (8, "Change mode")
    OTHER = (9, "Other")


class DaysimGender(LabeledEnum):
    """DaySim gender codes."""

    MALE = (1, "Male")
    FEMALE = (2, "Female")
    OTHER = (3, "Other/non-binary")
    MISSING = (9, "Missing")


class DaysimStudentType(LabeledEnum):
    """DaySim student type codes."""

    NOT_STUDENT = (0, "Not a student")
    FULL_TIME = (1, "Full-time student")
    PART_TIME = (2, "Part-time student")
    MISSING = (-1, "Missing")


class DaysimWorkerType(LabeledEnum):
    """DaySim worker type codes.

    Employment status classification for persons.
    """

    NON_WORKER = (0, "Not a worker")
    FULL_TIME_WORKER = (1, "Full-time worker")
    PART_TIME_WORKER = (2, "Part-time worker")


class DaysimPaidParking(LabeledEnum):
    """DaySim paid parking at work codes."""

    FREE = (0, "Free parking")
    PAID = (1, "Paid parking")
    MISSING = (-1, "Missing/not applicable")


class DaysimResidenceOwnership(LabeledEnum):
    """DaySim residence ownership codes."""

    OWN = (1, "Own")
    RENT = (2, "Rent")
    OTHER = (3, "Other")
    MISSING = (-1, "Missing")


class DaysimResidenceType(LabeledEnum):
    """DaySim residence type codes."""

    SINGLE_FAMILY = (1, "Single-family detached")
    DUPLEX_TOWNHOUSE = (2, "Duplex/triplex/townhouse")
    APARTMENT = (3, "Apartment/condo")
    MOBILE_HOME = (4, "Mobile home/trailer")
    DORM = (5, "Dorm/group quarters")
    OTHER = (6, "Other")
    MISSING = (-1, "Missing")


class VehicleOccupancy(LabeledEnum):
    """Vehicle occupancy thresholds for mode classification.

    Used to classify auto trips into SOV, HOV2, and HOV3+ categories.
    """

    SOV = (1, "Single occupant vehicle (1 person)")
    HOV2 = (2, "High occupancy vehicle 2 (2 people)")
    HOV3_MIN = (2, "Minimum occupancy for HOV3+ (>2 people)")


class DaysimPersonType(LabeledEnum):
    """DaySim person type codes.

    Person type classification based on employment, student status, and age.
    """

    FULL_TIME_WORKER = (1, "Full-time worker")
    PART_TIME_WORKER = (2, "Part-time worker")
    RETIRED = (3, "Retired (65+)")
    NON_WORKER = (4, "Non-working adult")
    UNIVERSITY_STUDENT = (5, "University student")
    HIGH_SCHOOL_STUDENT = (6, "High school student (16+)")
    CHILD_5_15 = (7, "Child age 5-15")
    CHILD_UNDER_5 = (8, "Child age 0-4")
