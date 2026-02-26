"""Person formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import (
    DaysimPersonType,
    DaysimStudentType,
    DaysimWorkerType,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.persons import Employment
from data_canon.codebook.trips import ModeType

from .mappings import (
    AGE_MAP,
    GENDER_MAP,
    MODE_TYPE_MAP,
    STUDENT_MAP,
    WORK_PARK_MAP,
)
from .person_type_utils import get_person_type_expr

logger = logging.getLogger(__name__)


MODE_TO_MODE_TYPE_MAP = {k.value: v.value for k, v in ModeType.from_mode().items()}


def compute_day_completeness(days: pl.DataFrame) -> pl.DataFrame:
    """Compute day completeness indicators for survey weighting.

    Creates person-level completeness indicators by day of week and calculates
    total complete days for different weekday periods (3-day, 4-day, 5-day).

    Args:
        days: DataFrame with columns [person_id, is_complete, travel_dow]

    Returns:
        DataFrame with columns:
        - hhno, pno: Household and person identifiers
        - mon_complete through sun_complete: Binary indicators (0/1)
        - num_days_complete_3dayweekday: Sum of Tue+Wed+Thu
        - num_days_complete_4dayweekday: Sum of Mon+Tue+Wed+Thu
        - num_days_complete_5dayweekday: Sum of Mon+Tue+Wed+Thu+Fri
    """
    logger.info("Computing day completeness indicators")

    # Pivot days by person and day of week
    pivoted = (
        days.select(["person_id", "is_complete", "travel_dow"])
        .pivot(index="person_id", on="travel_dow", values="is_complete")
        .fill_null(0)
    )

    # Ensure all 7 day columns exist (Mon=1 through Sun=7)
    for day in range(1, 8):
        if str(day) not in pivoted.columns:
            pivoted = pivoted.with_columns(pl.lit(0).alias(str(day)))

    result = (
        pivoted.with_columns(
            # Extract hhno and pno from person_id (person_id = hhno*100 + pno)
            hhno=(pl.col("person_id") // 100),
            pno=(pl.col("person_id") % 100),
            # Compute weekday aggregates
            num_days_complete_3dayweekday=pl.sum_horizontal(
                ["2", "3", "4"]  # Tue, Wed, Thu
            ),
            num_days_complete_4dayweekday=pl.sum_horizontal(
                ["1", "2", "3", "4"]  # Mon, Tue, Wed, Thu
            ),
            num_days_complete_5dayweekday=pl.sum_horizontal(
                ["1", "2", "3", "4", "5"]  # Mon, Tue, Wed, Thu, Fri
            ),
        )
        .select(
            [
                "hhno",
                "pno",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "num_days_complete_3dayweekday",
                "num_days_complete_4dayweekday",
                "num_days_complete_5dayweekday",
            ]
        )
        .rename(
            {
                "1": "mon_complete",
                "2": "tue_complete",
                "3": "wed_complete",
                "4": "thu_complete",
                "5": "fri_complete",
                "6": "sat_complete",
                "7": "sun_complete",
            }
        )
    )
    logger.info("Computed day completeness for %d persons", len(result))
    return result.sort(by=["hhno", "pno"])


def format_persons(persons: pl.DataFrame, days: pl.DataFrame) -> pl.DataFrame:
    """Format person data to DaySim specification.

    Applies mapping dictionaries and derives person type (pptyp) and worker
    type (pwtyp) based on age, employment, and student status.

    Person type (pptyp) cascading logic:
    - Age < 5: Child 0-4 (type 8)
    - Age < 16: Child 5-15 (type 7)
    - Full-time employed: Full-time worker (type 1)
    - Age 16-17 and student: High school 16+ (type 6)
    - Age 18-24 and high school: High school 16+ (type 6)
    - Age >= 18 and student: University student (type 5)
    - Part-time/self-employed: Part-time worker (type 2)
    - Age < 65: Non-working adult (type 4)
    - Age >= 65: Non-working senior (type 3)

    Args:
        persons: DataFrame with canonical person fields
        days: Optional DataFrame with day completeness indicators

    Returns:
        DataFrame with DaySim person fields
    """
    logger.info("Formatting person data")

    # Compute day completeness if days data provided
    day_completeness = compute_day_completeness(days)

    # Rename columns to DaySim naming convention
    persons_daysim = persons.rename(
        {
            "hh_id": "hhno",
            "person_num": "pno",
            "work_lon": "pwxco",
            "work_lat": "pwyco",
            "school_lon": "psxco",
            "school_lat": "psyco",
            "work_taz": "pwtaz",
            "work_maz": "pwpcl",
            "school_taz": "pstaz",
            "school_maz": "pspcl",
        }
    )

    # Apply basic field mappings and transformations
    persons_daysim = persons_daysim.with_columns(
        # Fill null coordinates with -1
        pl.col(["pwtaz", "pwpcl", "pstaz", "pspcl"]).fill_null(-1),
        # Map age categories to midpoint ages
        pagey=pl.col("age").replace(AGE_MAP),
        # Map gender codes
        pgend=pl.col("gender").replace(GENDER_MAP),
        # Map student status
        pstyp=pl.col("student").replace(STUDENT_MAP).fill_null(DaysimStudentType.NOT_STUDENT.value),
        # Map work parking (use work_park from canonical data)
        ppaidprk=pl.col("work_park").replace_strict(WORK_PARK_MAP),
    )

    # Derive person type (pptyp) using reusable expression
    persons_daysim = persons_daysim.with_columns(pptyp=get_person_type_expr())

    # Derive worker type (pwtyp) from person type and employment
    persons_daysim = persons_daysim.with_columns(
        pwtyp=pl.when(
            pl.col("pptyp").is_in(
                [
                    DaysimPersonType.FULL_TIME_WORKER.value,
                    DaysimPersonType.PART_TIME_WORKER.value,
                ]
            )
        )
        .then(pl.col("pptyp"))  # direct mapping for workers
        .when(
            pl.col("pptyp").is_in(
                [
                    DaysimPersonType.UNIVERSITY_STUDENT.value,
                    DaysimPersonType.CHILD_DRIVING_AGE.value,
                ]
            )
            & pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                ]
            )
        )
        .then(pl.lit(DaysimPersonType.PART_TIME_WORKER.value))
        .otherwise(pl.lit(0))  # non-worker
    )

    # Set work/school locations to None (coords) or -1 (taz) if person is not worker/student
    persons_daysim = persons_daysim.with_columns(
        pwtaz=pl.when(pl.col("pwtyp") != DaysimWorkerType.NON_WORKER.value)
        .then(pl.col("pwtaz"))
        .otherwise(pl.lit(-1)),
        pwpcl=pl.when(pl.col("pwtyp") != DaysimWorkerType.NON_WORKER.value)
        .then(pl.col("pwpcl"))
        .otherwise(pl.lit(-1)),
        pwxco=pl.when(pl.col("pwtyp") != DaysimWorkerType.NON_WORKER.value)
        .then(pl.col("pwxco"))
        .otherwise(pl.lit(None)),
        pwyco=pl.when(pl.col("pwtyp") != DaysimWorkerType.NON_WORKER.value)
        .then(pl.col("pwyco"))
        .otherwise(pl.lit(None)),
        pstaz=pl.when(pl.col("pstyp") != DaysimStudentType.NOT_STUDENT.value)
        .then(pl.col("pstaz"))
        .otherwise(pl.lit(-1)),
        pspcl=pl.when(pl.col("pstyp") != DaysimStudentType.NOT_STUDENT.value)
        .then(pl.col("pspcl"))
        .otherwise(pl.lit(-1)),
        psxco=pl.when(pl.col("pstyp") != DaysimStudentType.NOT_STUDENT.value)
        .then(pl.col("psxco"))
        .otherwise(pl.lit(None)),
        psyco=pl.when(pl.col("pstyp") != DaysimStudentType.NOT_STUDENT.value)
        .then(pl.col("psyco"))
        .otherwise(pl.lit(None)),
    )

    # If person weight is in data, use that, else default to 1.0
    if "person_weight" not in persons_daysim.columns:
        persons_daysim = persons_daysim.with_columns(pl.lit(1.0).alias("person_weight"))

    persons_daysim = persons_daysim.with_columns(
        pl.col("person_weight").fill_null(0).alias("psexpfac"),
        pwautime=pl.lit(-1),  # auto time to work (not available)
        pwaudist=pl.lit(-1),  # auto distance to work (not available)
        psautime=pl.lit(-1),  # auto time to school (not available)
        psaudist=pl.lit(-1),  # auto distance to school (not available)
        # Map work_mode: Mode --> ModeType --> DaysimMode
        puwmode=pl.col("work_mode")
        .replace_strict(MODE_TO_MODE_TYPE_MAP)
        .replace_strict(MODE_TYPE_MAP),
        puwarrp=pl.lit(-1),  # usual work arrival period (not available)
        puwdepp=pl.lit(-1),  # usual work departure period (not available)
        # transit pass
        ptpass=pl.when(pl.col("transit_pass") == BooleanYesNo.YES.value).then(1).otherwise(0),
        # proxy respondent
        pproxy=pl.when(pl.col("is_proxy") == BooleanYesNo.YES.value).then(1).otherwise(0),
        # has diary day
        pdiary=pl.when(pl.col("num_days_complete") > 0).then(1).otherwise(0),
    )

    # Join day completeness if available
    if day_completeness is not None:
        persons_daysim = persons_daysim.join(day_completeness, on=["hhno", "pno"], how="left")

    # Select DaySim person fields
    person_cols = [
        "hhno",
        "pno",
        "pptyp",
        "pagey",
        "pgend",
        "pwtyp",
        "pwpcl",
        "pwtaz",
        "pstyp",
        "pspcl",
        "pstaz",
        "ppaidprk",
        "pwautime",
        "pwaudist",
        "psautime",
        "psaudist",
        "puwmode",
        "puwarrp",
        "puwdepp",
        "ptpass",
        "pproxy",
        "pdiary",
        "pwxco",
        "pwyco",
        "psxco",
        "psyco",
        "psexpfac",
    ]

    # Add day completeness columns if available
    if day_completeness is not None:
        person_cols.extend(
            [
                "mon_complete",
                "tue_complete",
                "wed_complete",
                "thu_complete",
                "fri_complete",
                "sat_complete",
                "sun_complete",
                "num_days_complete_3dayweekday",
                "num_days_complete_4dayweekday",
                "num_days_complete_5dayweekday",
            ]
        )

    logger.info("Formatted %d persons", len(persons_daysim))
    return persons_daysim.select(person_cols).sort(by=["hhno", "pno"])
