"""Codebook enumerations for PUMS (Public Use Microdata Sample) variables.

Standard Census / ACS variable definitions used in PUMS microdata.
These are the **raw codes** from the Census Bureau data dictionaries.

See Also:
    https://www.census.gov/programs-surveys/acs/microdata/documentation.html
"""

from enum import nonmember

from data_canon.core.labeled_enum import LabeledEnum

# ── Person-level variables ────────────────────────────────────────────────


class PumsSex(LabeledEnum):
    """SEX — Sex of the person."""

    canonical_field_name = "SEX"

    MALE = (1, "Male")
    FEMALE = (2, "Female")


class PumsEsr(LabeledEnum):
    """ESR — Employment Status Recode.

    Universe: persons 16 years and older.
    """

    canonical_field_name = "ESR"

    EMPLOYED_CIVILIAN_AT_WORK = (1, "Civilian employed, at work")
    EMPLOYED_CIVILIAN_NOT_AT_WORK = (2, "Civilian employed, with a job but not at work")
    UNEMPLOYED = (3, "Unemployed")
    ARMED_FORCES_AT_WORK = (4, "Armed forces, at work")
    ARMED_FORCES_NOT_AT_WORK = (5, "Armed forces, with a job but not at work")
    NOT_IN_LABOR_FORCE = (6, "Not in labor force")

    # Convenience groups (not enum members)
    EMPLOYED = nonmember([1, 2, 4, 5])
    NOT_EMPLOYED = nonmember([3, 6])


class PumsSchg(LabeledEnum):
    """SCHG — Grade Level Attending.

    Universe: persons currently attending school.
    0 = not attending.
    """

    canonical_field_name = "SCHG"

    NOT_ATTENDING = (0, "Not attending school")
    NURSERY = (1, "Nursery school/preschool")
    KINDERGARTEN = (2, "Kindergarten")
    GRADE_1 = (3, "Grade 1")
    GRADE_2 = (4, "Grade 2")
    GRADE_3 = (5, "Grade 3")
    GRADE_4 = (6, "Grade 4")
    GRADE_5 = (7, "Grade 5")
    GRADE_6 = (8, "Grade 6")
    GRADE_7 = (9, "Grade 7")
    GRADE_8 = (10, "Grade 8")
    GRADE_9 = (11, "Grade 9")
    GRADE_10 = (12, "Grade 10")
    GRADE_11 = (13, "Grade 11")
    GRADE_12 = (14, "Grade 12")
    COLLEGE_UNDERGRAD = (15, "College undergraduate")
    GRADUATE_PROFESSIONAL = (16, "Graduate or professional school")

    # Convenience groups
    K12 = nonmember(list(range(1, 15)))
    COLLEGE = nonmember([15, 16])


class PumsSchl(LabeledEnum):
    """SCHL — Educational Attainment.

    Universe: persons 3 years and older.
    """

    canonical_field_name = "SCHL"

    NO_SCHOOLING = (1, "No schooling completed")
    NURSERY = (2, "Nursery school, preschool")
    KINDERGARTEN = (3, "Kindergarten")
    GRADE_1 = (4, "Grade 1")
    GRADE_2 = (5, "Grade 2")
    GRADE_3 = (6, "Grade 3")
    GRADE_4 = (7, "Grade 4")
    GRADE_5 = (8, "Grade 5")
    GRADE_6 = (9, "Grade 6")
    GRADE_7 = (10, "Grade 7")
    GRADE_8 = (11, "Grade 8")
    GRADE_9 = (12, "Grade 9")
    GRADE_10 = (13, "Grade 10")
    GRADE_11 = (14, "Grade 11")
    GRADE_12_NO_DIPLOMA = (15, "12th grade - no diploma")
    HS_DIPLOMA = (16, "Regular high school diploma")
    GED = (17, "GED or alternative credential")
    SOME_COLLEGE_LT_1YR = (18, "Some college, but less than 1 year")
    SOME_COLLEGE_GE_1YR = (19, "Some college, 1 or more years, no degree")
    ASSOCIATE = (20, "Associate's degree")
    BACHELORS = (21, "Bachelor's degree")
    MASTERS = (22, "Master's degree")
    PROFESSIONAL = (23, "Professional degree beyond a bachelor's")
    DOCTORATE = (24, "Doctorate degree")

    # Convenience groups for mapping to Education LabeledEnum
    LESS_THAN_HS = nonmember(list(range(1, 16)))
    HIGH_SCHOOL = nonmember([16, 17])
    SOME_COLLEGE = nonmember([18, 19])
    GRADUATE = nonmember([22, 23, 24])


class PumsRac1p(LabeledEnum):
    """RAC1P — Recoded Detailed Race Code."""

    canonical_field_name = "RAC1P"

    WHITE = (1, "White alone")
    BLACK = (2, "Black or African American alone")
    AIAN = (3, "American Indian alone")
    ALASKA_NATIVE = (4, "Alaska Native alone")
    AIAN_BOTH = (5, "American Indian and Alaska Native tribes specified, or not specified")
    ASIAN = (6, "Asian alone")
    NHPI = (7, "Native Hawaiian and Other Pacific Islander alone")
    OTHER = (8, "Some Other Race alone")
    TWO_OR_MORE = (9, "Two or More Races")


class PumsHisp(LabeledEnum):
    """HISP — Recoded Detailed Hispanic Origin.

    Simplified to the broad categories used in weighting.  The full
    HISP variable has 24 codes; we keep only the top-level splits.
    """

    canonical_field_name = "HISP"

    NOT_HISPANIC = (1, "Not Spanish/Hispanic/Latino")
    MEXICAN = (2, "Mexican")
    PUERTO_RICAN = (3, "Puerto Rican")
    CUBAN = (4, "Cuban")
    # Codes 5-24 are all "Other Hispanic" subtypes


class PumsJwtrns(LabeledEnum):
    """JWTRNS — Means of Transportation to Work.

    Universe: workers 16 years and older.
    """

    canonical_field_name = "JWTRNS"

    NA = (
        0,
        "N/A (not a worker-not in the labor force, including persons "
        "under 16 years; unemployed; employed, with a job but not at "
        "work; Armed Forces, with a job but not at work",
    )
    CAR_TRUCK_VAN = (1, "Car, truck, or van")
    BUS = (2, "Bus")
    STREETCAR = (3, "Streetcar, trolley car, or cable car")
    SUBWAY = (4, "Subway or elevated rail")
    RAILROAD = (5, "Railroad or ferryboat")
    FERRYBOAT = (6, "Ferryboat")
    TAXICAB = (7, "Taxicab")
    MOTORCYCLE = (8, "Motorcycle")
    BICYCLE = (9, "Bicycle")
    WALKED = (10, "Walked")
    WORKED_AT_HOME = (11, "Worked from home")
    OTHER = (12, "Other method")

    # Convenience groups
    TRANSIT = nonmember([2, 3, 4, 5, 6])


# ── Household-level variables ─────────────────────────────────────────────

# NP (number of persons), VEH (vehicles), HINCP (household income),
# WGTP (household weight), AGEP (person age), WKHP (hours worked),
# PWGTP (person weight), and JWRIP (vehicle occupancy) are continuous
# integer fields — no enum needed.  Constants/thresholds below.


class PumsThresholds:
    """Named thresholds for continuous PUMS variables.

    Centralises numeric cut-points so they are never hard-coded in
    expression logic.
    """

    # AGEP — child age ceiling for h_children aggregation
    CHILD_MAX_AGE: int = 17

    # WKHP — weekly hours threshold below which an employed person
    # is classified as part-time
    PART_TIME_HOURS: int = 35

    # JWRIP — vehicle occupancy at or above which a driver is in a carpool
    CARPOOL_MIN_OCCUPANCY: int = 2
