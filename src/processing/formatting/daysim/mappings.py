"""DaySim Formatting Mappings and Custom Steps."""

import logging
from enum import IntEnum

import polars as pl

from data_canon.codebook.daysim import (
    DaysimGender,
    DaysimMode,
    DaysimPaidParking,
    DaysimPathType,
    DaysimPurpose,
    DaysimResidenceOwnership,
    DaysimResidenceType,
    DaysimStudentType,
)
from data_canon.codebook.households import (
    IncomeDetailed,
    IncomeFollowup,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Gender,
    Student,
    WorkParking,
)
from data_canon.codebook.trips import (
    AccessEgressMode,
    Mode,
    ModeType,
    PurposeCategory,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Age Thresholds for Person Type Classification
# =============================================================================


class AgeThreshold(IntEnum):
    """Age thresholds for person type classification."""

    CHILD_PRESCHOOL = 5  # Age < 5: child 0-4
    CHILD_SCHOOL = 16  # Age < 16: child 5-15
    YOUNG_ADULT = 18  # Age < 18: high school age
    ADULT = 25  # Age < 25: university age
    SENIOR = 65  # Age < 65: working age adult


# =============================================================================
# Enum-to-Enum and Enum-to-Value Mappings
# =============================================================================

# Age category to midpoint age for DaySim
AGE_TO_MIDPOINT = {
    AgeCategory.AGE_UNDER_5: 3,
    AgeCategory.AGE_5_TO_15: 10,
    AgeCategory.AGE_16_TO_17: 16,
    AgeCategory.AGE_18_TO_24: 21,
    AgeCategory.AGE_25_TO_34: 30,
    AgeCategory.AGE_35_TO_44: 40,
    AgeCategory.AGE_45_TO_54: 50,
    AgeCategory.AGE_55_TO_64: 60,
    AgeCategory.AGE_65_TO_74: 70,
    AgeCategory.AGE_75_TO_84: 80,
    AgeCategory.AGE_85_AND_UP: 90,
}

# Gender to DaySim gender codes
GENDER_TO_DAYSIM = {
    Gender.FEMALE: DaysimGender.FEMALE,
    Gender.MALE: DaysimGender.MALE,
    Gender.NON_BINARY: DaysimGender.OTHER,
    Gender.OTHER: DaysimGender.OTHER,
    Gender.MISSING: DaysimGender.MISSING,
    Gender.PNTA: DaysimGender.MISSING,
}

# Student status to DaySim student type
STUDENT_TO_DAYSIM = {
    Student.FULLTIME_INPERSON: DaysimStudentType.FULL_TIME,
    Student.PARTTIME_INPERSON: DaysimStudentType.PART_TIME,
    Student.NONSTUDENT: DaysimStudentType.NOT_STUDENT,
    Student.PARTTIME_ONLINE: DaysimStudentType.PART_TIME,
    Student.FULLTIME_ONLINE: DaysimStudentType.FULL_TIME,
    Student.MISSING: DaysimStudentType.MISSING,
}

# Work parking to DaySim paid parking (simplified from survey to binary)
WORK_PARK_TO_DAYSIM = {
    WorkParking.FREE: DaysimPaidParking.FREE,
    WorkParking.EMPLOYER_PAYS_ALL: DaysimPaidParking.FREE,
    WorkParking.EMPLOYER_DISCOUNT: DaysimPaidParking.PAID,
    WorkParking.PERSONAL_PAY: DaysimPaidParking.PAID,
    WorkParking.MISSING: DaysimPaidParking.MISSING,
    WorkParking.NOT_APPLICABLE: DaysimPaidParking.MISSING,
    WorkParking.DONT_KNOW: DaysimPaidParking.MISSING,
}

# Residence ownership to DaySim codes
RENTOWN_TO_DAYSIM = {
    ResidenceRentOwn.OWN: DaysimResidenceOwnership.OWN,
    ResidenceRentOwn.RENT: DaysimResidenceOwnership.RENT,
    ResidenceRentOwn.NOPAYMENT_EMPLOYER: DaysimResidenceOwnership.OTHER,
    ResidenceRentOwn.NOPAYMENT_OTHER: DaysimResidenceOwnership.OTHER,
    ResidenceRentOwn.OTHER: DaysimResidenceOwnership.OTHER,
    ResidenceRentOwn.MISSING: DaysimResidenceOwnership.MISSING,
    ResidenceRentOwn.PNTA: DaysimResidenceOwnership.MISSING,
}

# Residence type to DaySim codes
RESIDENCE_TYPE_TO_DAYSIM = {
    ResidenceType.SFH: DaysimResidenceType.SINGLE_FAMILY,
    ResidenceType.TOWNHOUSE: DaysimResidenceType.DUPLEX_TOWNHOUSE,
    ResidenceType.MULTIFAMILY: DaysimResidenceType.APARTMENT,
    ResidenceType.CONDO_5TO50_UNITS: DaysimResidenceType.APARTMENT,
    ResidenceType.CONDO_50PLUS_UNITS: DaysimResidenceType.APARTMENT,
    ResidenceType.SENIOR: DaysimResidenceType.APARTMENT,
    ResidenceType.MANUFACTURED: DaysimResidenceType.MOBILE_HOME,
    ResidenceType.GROUP_QUARTERS: DaysimResidenceType.DORM,
    ResidenceType.MISSING: DaysimResidenceType.MISSING,
    ResidenceType.BOAT_RV: DaysimResidenceType.OTHER,
}

# Income categories to midpoint values (detailed from survey)
INCOME_DETAILED_TO_MIDPOINT = {
    IncomeDetailed.INCOME_UNDER15.value: 7500,
    IncomeDetailed.INCOME_15TO25.value: 20000,
    IncomeDetailed.INCOME_25TO35.value: 30000,
    IncomeDetailed.INCOME_35TO50.value: 42500,
    IncomeDetailed.INCOME_50TO75.value: 62500,
    IncomeDetailed.INCOME_75TO100.value: 87500,
    IncomeDetailed.INCOME_100TO150.value: 125000,
    IncomeDetailed.INCOME_150TO200.value: 175000,
    IncomeDetailed.INCOME_200TO250.value: 225000,
    IncomeDetailed.INCOME_250_OR_MORE.value: 350000,
    IncomeDetailed.PNTA.value: -1,
}

# Income followup categories to midpoint values
INCOME_FOLLOWUP_TO_MIDPOINT = {
    IncomeFollowup.INCOME_UNDER25.value: 12500,
    IncomeFollowup.INCOME_25TO50.value: 37500,
    IncomeFollowup.INCOME_50TO75.value: 62500,
    IncomeFollowup.INCOME_75TO100.value: 87500,
    IncomeFollowup.INCOME_100TO200.value: 150000,
    IncomeFollowup.INCOME_200_OR_MORE.value: 250000,
    IncomeFollowup.MISSING.value: -1,
    IncomeFollowup.PNTA.value: -1,
}

# Purpose category to DaySim purpose codes
PURPOSE_TO_DAYSIM = {
    PurposeCategory.HOME: DaysimPurpose.HOME,
    PurposeCategory.WORK: DaysimPurpose.WORK,
    PurposeCategory.WORK_RELATED: DaysimPurpose.PERSONAL_BUSINESS,
    PurposeCategory.SCHOOL: DaysimPurpose.SCHOOL,
    PurposeCategory.SCHOOL_RELATED: DaysimPurpose.SCHOOL,
    PurposeCategory.ESCORT: DaysimPurpose.ESCORT,
    PurposeCategory.SHOP: DaysimPurpose.SHOP,
    PurposeCategory.MEAL: DaysimPurpose.MEAL,
    PurposeCategory.SOCIALREC: DaysimPurpose.SOCIAL_REC,
    PurposeCategory.ERRAND: DaysimPurpose.PERSONAL_BUSINESS,
    PurposeCategory.CHANGE_MODE: DaysimPurpose.CHANGE_MODE,
    PurposeCategory.OVERNIGHT: DaysimPurpose.OTHER,
    PurposeCategory.OTHER: DaysimPurpose.OTHER,
    PurposeCategory.MISSING: DaysimPurpose.OTHER,  # Map to OTHER rather than -1
    PurposeCategory.PNTA: DaysimPurpose.OTHER,
    PurposeCategory.NOT_IMPUTABLE: DaysimPurpose.OTHER,
}

# Transit mode to DaySim path type mapping
TRANSIT_MODE_TO_PATH_TYPE = {
    Mode.FERRY: DaysimPathType.FERRY,
    Mode.BART: DaysimPathType.BART,
    Mode.RAIL_INTERCITY: DaysimPathType.PREMIUM,
    Mode.RAIL_OTHER: DaysimPathType.PREMIUM,
    Mode.BUS_EXPRESS: DaysimPathType.PREMIUM,
    Mode.MUNI_METRO: DaysimPathType.LRT,
    Mode.RAIL: DaysimPathType.LRT,
    Mode.STREETCAR: DaysimPathType.LRT,
}

# Mode to DaySim mode codes
MODE_TYPE_TO_DAYSIM = {
    ModeType.WALK: DaysimMode.WALK,
    ModeType.BIKE: DaysimMode.BIKE,
    ModeType.BIKESHARE: DaysimMode.BIKE,
    ModeType.SCOOTERSHARE: DaysimMode.BIKE,
    ModeType.CAR: DaysimMode.SOV,  # Will be refined to HOV2/HOV3 based on occ
    ModeType.CARSHARE: DaysimMode.SOV,
    ModeType.TNC: DaysimMode.TNC,
    ModeType.TAXI: DaysimMode.TNC,
    ModeType.FERRY: DaysimMode.WALK_TRANSIT,
    ModeType.TRANSIT: DaysimMode.WALK_TRANSIT,  # Will be refined based on access/egress mode  # noqa: E501
    ModeType.SCHOOL_BUS: DaysimMode.SCHOOL_BUS,
    ModeType.SHUTTLE: DaysimMode.OTHER,
    ModeType.LONG_DISTANCE: DaysimMode.OTHER,
    ModeType.OTHER: DaysimMode.OTHER,
    ModeType.MISSING: DaysimMode.OTHER,
}

# ModeType to AccessEgressMode mapping
MODE_TYPE_TO_ACCESS_EGRESS = {
    ModeType.WALK: AccessEgressMode.WALK,
    ModeType.BIKE: AccessEgressMode.BICYCLE,
    ModeType.BIKESHARE: AccessEgressMode.BICYCLE,
    ModeType.SCOOTERSHARE: AccessEgressMode.MICROMOBILITY,
    ModeType.TAXI: AccessEgressMode.TNC,
    ModeType.TNC: AccessEgressMode.TNC,
    ModeType.CAR: AccessEgressMode.CAR_HOUSEHOLD,
    ModeType.CARSHARE: AccessEgressMode.CAR_OTHER,
    ModeType.SCHOOL_BUS: AccessEgressMode.TRANSFER_BUS,
    ModeType.SHUTTLE: AccessEgressMode.TRANSFER_BUS,
    ModeType.FERRY: AccessEgressMode.TRANSFER_OTHER,
    ModeType.TRANSIT: AccessEgressMode.TRANSFER_OTHER,
    ModeType.LONG_DISTANCE: AccessEgressMode.TRANSFER_OTHER,
    ModeType.OTHER: AccessEgressMode.OTHER,
    ModeType.MISSING: AccessEgressMode.MISSING,
}

# Access/egress mode codes that indicate drove to transit
DROVE_ACCESS_EGRESS = [
    AccessEgressMode.TNC.value,
    AccessEgressMode.CAR_HOUSEHOLD.value,
    AccessEgressMode.CAR_OTHER.value,
    AccessEgressMode.DROPOFF_HOUSEHOLD.value,
    AccessEgressMode.DROPOFF_OTHER.value,
]


# =============================================================================
# Convert Enum Mappings to Integer Dictionaries for Polars
# =============================================================================

# Polars replace() requires integer keys, so convert enum mappings
AGE_MAP = {k.value: v for k, v in AGE_TO_MIDPOINT.items()}
GENDER_MAP = {k.value: v.value for k, v in GENDER_TO_DAYSIM.items()}
STUDENT_MAP = {k.value: v.value for k, v in STUDENT_TO_DAYSIM.items()}
WORK_PARK_MAP = {k.value: v.value for k, v in WORK_PARK_TO_DAYSIM.items()}
PURPOSE_MAP = {k.value: v.value for k, v in PURPOSE_TO_DAYSIM.items()}
MODE_TYPE_MAP = {k.value: v.value for k, v in MODE_TYPE_TO_DAYSIM.items()}
MODE_TYPE_TO_ACCESS_EGRESS_MAP = {k.value: v.value for k, v in MODE_TYPE_TO_ACCESS_EGRESS.items()}
RENTOWN_MAP = {k.value: v.value for k, v in RENTOWN_TO_DAYSIM.items()}
RESTYPE_MAP = {k.value: v.value for k, v in RESIDENCE_TYPE_TO_DAYSIM.items()}


# =============================================================================
# Custom Step Functions
# =============================================================================
def determine_tour_mode(tours: pl.DataFrame, linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Determine DaySim tour mode from mode_type, passengers, and access mode.

    Args:
        tours: DataFrame with tour_mode (ModeType) column
        linked_trips: DataFrame with trip-level details including num_travelers

    Returns:
        DataFrame with tmodetp column containing DaySim mode codes
    """
    # Get HOV status from linked trips - check max occupancy for car trips
    hov_status = (
        linked_trips.filter(pl.col("mode_type") == ModeType.CAR.value)
        .group_by("tour_id")
        .agg(pl.col("num_travelers").max().alias("max_occupancy"))
    )

    # Get transit access/egress mode - check if drove to transit
    transit_access = (
        linked_trips.filter(pl.col("mode_type") == ModeType.TRANSIT.value)
        .group_by("tour_id")
        .agg(
            (
                pl.col("access_mode").is_in(DROVE_ACCESS_EGRESS)
                | pl.col("egress_mode").is_in(DROVE_ACCESS_EGRESS)
            )
            .any()
            .alias("drove_to_transit")
        )
    )

    # Join aggregations to tours
    tours = tours.join(hov_status, on="tour_id", how="left").join(
        transit_access, on="tour_id", how="left"
    )

    # Determine DaySim mode
    tours = tours.with_columns(
        pl.when(pl.col("tour_mode") == ModeType.CAR.value)
        .then(
            pl.when(pl.col("max_occupancy") >= 3)  # noqa: PLR2004
            .then(pl.lit(DaysimMode.HOV3.value))
            .when(pl.col("max_occupancy") == 2)  # noqa: PLR2004
            .then(pl.lit(DaysimMode.HOV2.value))
            .otherwise(pl.lit(DaysimMode.SOV.value))
        )
        .when(pl.col("tour_mode") == ModeType.TRANSIT.value)
        .then(
            pl.when(pl.col("drove_to_transit").fill_null(value=False))
            .then(pl.lit(DaysimMode.DRIVE_TRANSIT.value))
            .otherwise(pl.lit(DaysimMode.WALK_TRANSIT.value))
        )
        .otherwise(pl.col("tour_mode").replace_strict(MODE_TYPE_MAP))
        .alias("tmodetp")
    )

    return tours
