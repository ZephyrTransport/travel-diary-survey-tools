"""Codebook enumerations for day table."""

from data_canon.core.labeled_enum import LabeledEnum


class TravelDow(LabeledEnum):
    """travel_dow value labels."""

    field_description = "Day of the week enumeration"

    MONDAY = (1, "Monday")
    TUESDAY = (2, "Tuesday")
    WEDNESDAY = (3, "Wednesday")
    THURSDAY = (4, "Thursday")
    FRIDAY = (5, "Friday")
    SATURDAY = (6, "Saturday")
    SUNDAY = (7, "Sunday")


class AttendSchool(LabeledEnum):
    """attend_school value labels."""

    field_description = "Whether the person attends school on the survey day"

    YES_USUAL = (1, "Yes, attend school at usual location")
    NO_ANOTHER = (2, "Yes, attend school at another location")
    NO = (3, "No, do not attend school")
    DONT_KNOW = (998, "Don't know")
    PNTA = (999, "Prefer not to answer")
    MISSING = (995, "Missing Response")


class NoSchoolReason(LabeledEnum):
    """no_school_reason value labels."""

    field_description = "Reason for not attending school"

    SICK = (1, "Sick")
    ONLINE_HOME = (2, "Online / at home")
    ONLINE_OTHER = (3, "Online / at other location")
    VACATION = (4, "Vacation")
    CLOSED_SCHEDULED = (5, "Scheduled school closure (e.g., holiday)")
    CLOSED_UNSCHEDULED = (6, "Unscheduled school closure (e.g., weather)")
    OTHER = (7, "Other")
    MISSING = (995, "Missing Response")
    DONT_KNOW = (998, "Don't know")
    PNTA = (999, "Prefer not to answer")


class BeginEndDay(LabeledEnum):
    """begin_day and end_day value labels."""

    field_description = "Location at the beginning or end of the day"

    HOME = (1, "Home")
    SOMEONE_ELSES_HOME = (2, "Someone else's home")
    WORK = (3, "Work")
    OTHER_HOME = (4, "Your/Their other home (e.g., other parent, second home)")
    TRAVELING = (5, "Traveling (e.g., red-eye flight)")
    TEMPORARY = (7, "Temporary lodging (e.g., hotel, vacation rental)")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")


class Delivery(LabeledEnum):
    """delivery value labels."""

    field_description = "Type of delivery received"

    TAKEOUT = (1, "Take-out / prepared food delivery")
    SERVICES = (2, "Someone came to provide a service (e.g., cleaning, repair)")
    GROCERIES = (3, "Groceries / other goods delivery")
    PACKAGE_HOME = (4, "Postal package delivery (e.g., USPS, FedEx, UPS)")
    PACKAGE_OTHER = (
        5,
        "Postal package delivery other location (e.g., Amazon locker)",
    )
    PACKAGE_WORK = (6, "Postal package delivery work location")
    OTHER_PACKAGE = (7, "Other item delivery (e.g., furniture, appliance)")
    OTHER_PACKAGE_WORK = (8, "Other item delivery work location")
    NONE_OF_THE_ABOVE = (9, "None of the above")


class MadeTravel(LabeledEnum):
    """made_travel value labels."""

    field_description = "Whether the person made trips on the survey day"

    YES = (1, "Yes, made trips")
    NO = (2, "No, did not go anywhere or make trips")
    MISSING = (995, "Missing Response")
    DONT_KNOW = (998, "Don't know")
    PNTA = (999, "Prefer not to answer")


class NoTravelReason(LabeledEnum):
    """no_travel_reason value labels."""

    field_description = "Reason for not making trips"

    DID_TRAVEL = (0, "I did make trips")
    NOWORK = (1, "No work/school, took day off")
    WFH = (2, "Worked from home (telework)")
    HANGOUT = (3, "Just hung out at home")
    HOLIDAY = (4, "Scheduled school/work holiday")
    NO_TRANSPORT = (5, "No transportation available")
    SICK = (6, "Sick or caring for sick household member")
    DELIVERY = (7, "Waiting for a delivery or service at home")
    HOMESCHOOL = (8, "Remote learning / homeschooling")
    WEATHER = (9, "Bad weather (e.g., snowstorm)")
    DONT_KNOW = (998, "Person made trips but don't know when or where")
    OTHER = (997, "Other")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")
