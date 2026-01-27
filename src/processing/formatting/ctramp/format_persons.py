"""Person formatting for CT-RAMP.

Transforms canonical person data into CT-RAMP model format, including:
- Person type classification based on age, employment, and student status
- Activity pattern determination (Mandatory/Non-mandatory/Home)
- Free parking choice based on commute subsidies
- Tour frequency placeholder values

Note: Some fields like activity_pattern, imf_choice, and inmf_choice require
tour data and are set to placeholder values. These would be populated from
actual tour extraction results in a full pipeline.
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    _INMF_MAXES,
    _INMF_REVERSE_LOOKUP,
    CTRAMPPersonType,
    CTRAMPPurpose,
    FreeParkingChoice,
    IMFChoice,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.persons import AgeCategory, Employment, JobType
from utils.helpers import get_age_midpoint

from .ctramp_config import CTRAMPConfig
from .mappings import (
    EMPLOYMENT_MAP,
    GENDER_MAP,
    SCHOOL_TYPE_MAP,
    STUDENT_MAP,
)

logger = logging.getLogger(__name__)


# Individual Mandatory/Non-Mandatory Frequency Mapping ------------------------
def get_imf_choice_from_counts(work_count: int, school_count: int) -> int | None:
    """Map mandatory tour counts to IMFChoice enum value.

    Maps work and school tour counts to the appropriate IMFChoice enum value
    using ceiling semantics: counts >= 2 are capped at 2.

    Args:
        work_count: Number of work tours (0, 1, 2+)
        school_count: Number of school tours (0, 1, 2+)

    Returns:
        IMFChoice enum value (1-5), or None if both counts are 0

    Mapping logic:
        - 1 work, 0 school -> ONE_WORK (1)
        - 2+ work, 0 school -> TWO_WORK (2)
        - 0 work, 1 school -> ONE_SCHOOL (3)
        - 0 work, 2+ school -> TWO_SCHOOL (4)
        - 1+ work, 1+ school -> ONE_WORK_ONE_SCHOOL (5)
        - 0 work, 0 school -> None
    """
    # No mandatory tours
    if work_count == 0 and school_count == 0:
        return None

    # Both work and school tours present
    if work_count >= 1 and school_count >= 1:
        return IMFChoice.ONE_WORK_ONE_SCHOOL.value

    # Only work tours
    if work_count >= 1 and school_count == 0:
        if work_count == 1:
            return IMFChoice.ONE_WORK.value
        # work_count >= 2
        return IMFChoice.TWO_WORK.value

    # Only school tours (work_count == 0)
    if school_count == 1:
        return IMFChoice.ONE_SCHOOL.value
    # school_count >= 2
    return IMFChoice.TWO_SCHOOL.value


def get_inmf_code_from_counts(
    escort: int,
    shopping: int,
    othmaint: int,
    othdiscr: int,
    eatout: int,
    social: int,
) -> int:
    """Map per-purpose non-mandatory tour counts to INMF alternative code.

    Maps individual non-mandatory tour counts to the CT-RAMP alternative code
    (1-96) using ceiling semantics: counts exceeding the codebook maximum are
    capped to the maximum before lookup.

    Args:
        escort: Number of escort tours (0..2+, capped to 2)
        shopping: Number of shopping tours (0..1+, capped to 1)
        othmaint: Number of other maintenance tours (0..1+, capped to 1)
        othdiscr: Number of other discretionary tours (0..1+, capped to 1)
        eatout: Number of eating out tours (0..1+, capped to 1)
        social: Number of social tours (0..1+, capped to 1)

    Returns:
        Alternative code (1-96), or 0 if all counts are 0
    """
    # Special case: no non-mandatory tours
    if (
        escort == 0
        and shopping == 0
        and othmaint == 0
        and othdiscr == 0
        and eatout == 0
        and social == 0
    ):
        return 0

    # Cap each count to the codebook maximum (ceiling semantics)
    capped_escort = min(escort, _INMF_MAXES["escort"])
    capped_shopping = min(shopping, _INMF_MAXES["shopping"])
    capped_othmaint = min(othmaint, _INMF_MAXES["othmaint"])
    capped_othdiscr = min(othdiscr, _INMF_MAXES["othdiscr"])
    capped_eatout = min(eatout, _INMF_MAXES["eatout"])
    capped_social = min(social, _INMF_MAXES["social"])

    # Lookup the code
    key = (
        capped_escort,
        capped_shopping,
        capped_othmaint,
        capped_othdiscr,
        capped_eatout,
        capped_social,
    )
    code = _INMF_REVERSE_LOOKUP.get(key)

    if code is None:
        # Defensive fallback (shouldn't happen if capping is correct)
        return 0

    return code


def aggregate_tour_statistics(
    tours: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate tour statistics by person for activity patterns.

    Computes:
    - activity_pattern: M (mandatory), N (non-mandatory), H (no tours)
    - imf_choice: IMF alternative code (1-5) or 0 if no mandatory tours
    - inmf_choice: INMF alternative code (1-96) or 0 if no non-mandatory tours

    Args:
        tours: DataFrame with tour_purpose field (CTRAMP formatted)

    Returns:
        DataFrame with person_id and aggregated statistics
    """
    # Handle empty tours DataFrame
    if len(tours) == 0:
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "imf_choice": pl.Int64,
                "inmf_choice": pl.Int64,
                "activity_pattern": pl.String,
            }
        )

    # Define purpose categories for aggregation
    work_purposes = [
        CTRAMPPurpose.WORK_LOW.value,
        CTRAMPPurpose.WORK_MED.value,
        CTRAMPPurpose.WORK_HIGH.value,
        CTRAMPPurpose.WORK_VERY_HIGH.value,
    ]
    school_purposes = [
        CTRAMPPurpose.SCHOOL_GRADE.value,
        CTRAMPPurpose.SCHOOL_HIGH.value,
        CTRAMPPurpose.UNIVERSITY.value,
    ]

    # Define escort purposes (includes both segmented variants)
    escort_purposes = [
        CTRAMPPurpose.ESCORT_KIDS.value,
        CTRAMPPurpose.ESCORT_NO_KIDS.value,
    ]

    # Classify each tour into purpose-specific flags
    tour_stats = tours.with_columns(
        [
            pl.col("tour_purpose").is_in(work_purposes).alias("is_work"),
            pl.col("tour_purpose").is_in(school_purposes).alias("is_school"),
            pl.col("tour_purpose").is_in(escort_purposes).alias("is_escort"),
            pl.col("tour_purpose").eq("shopping").alias("is_shopping"),
            pl.col("tour_purpose").eq("othmaint").alias("is_othmaint"),
            pl.col("tour_purpose").eq("othdiscr").alias("is_othdiscr"),
            pl.col("tour_purpose").eq("eatout").alias("is_eatout"),
            pl.col("tour_purpose").eq("social").alias("is_social"),
        ]
    )

    # Aggregate counts by person
    person_stats = tour_stats.group_by("person_id").agg(
        [
            pl.col("is_work").sum().alias("work_count"),
            pl.col("is_school").sum().alias("school_count"),
            pl.col("is_escort").sum().alias("escort_count"),
            pl.col("is_shopping").sum().alias("shopping_count"),
            pl.col("is_othmaint").sum().alias("othmaint_count"),
            pl.col("is_othdiscr").sum().alias("othdiscr_count"),
            pl.col("is_eatout").sum().alias("eatout_count"),
            pl.col("is_social").sum().alias("social_count"),
        ]
    )

    # Map counts to IMF and INMF alternative codes
    person_stats = person_stats.with_columns(
        [
            pl.struct(["work_count", "school_count"])
            .map_elements(
                lambda row: get_imf_choice_from_counts(row["work_count"], row["school_count"]) or 0,
                return_dtype=pl.Int64,
            )
            .alias("imf_choice"),
            pl.struct(
                [
                    "escort_count",
                    "shopping_count",
                    "othmaint_count",
                    "othdiscr_count",
                    "eatout_count",
                    "social_count",
                ]
            )
            .map_elements(
                lambda row: get_inmf_code_from_counts(
                    escort=row["escort_count"],
                    shopping=row["shopping_count"],
                    othmaint=row["othmaint_count"],
                    othdiscr=row["othdiscr_count"],
                    eatout=row["eatout_count"],
                    social=row["social_count"],
                ),
                return_dtype=pl.Int64,
            )
            .alias("inmf_choice"),
        ]
    )

    # Determine activity pattern based on presence of mandatory/non-mandatory tours
    person_stats = person_stats.with_columns(
        pl.when(pl.col("imf_choice") > 0)
        .then(pl.lit("M"))  # Mandatory
        .when(pl.col("inmf_choice") > 0)
        .then(pl.lit("N"))  # Non-mandatory
        .otherwise(pl.lit("H"))  # Home (no tours)
        .alias("activity_pattern")
    )

    return person_stats.select(
        [
            "person_id",
            "activity_pattern",
            "imf_choice",
            "inmf_choice",
        ]
    )


def classify_person_type(
    age: int,
    employment: int,
    student: int,
    school_type: int,
) -> int:
    """Classify person type based on age, employment, and student status.

    CT-RAMP person type classification:
    1. Full-time worker
    2. Part-time worker
    3. University student
    4. Nonworker
    5. Retired
    6. Child of non-driving age
    7. Child of driving age
    8. Child too young for school

    Args:
        age: AgeCategory enum value (1-11)
        employment: Employment status code
        student: Student status code
        school_type: School type code

    Returns:
        int: CT-RAMP person type code

    Note:
        Age is an AgeCategory enum:
        1=UNDER_5, 2=5_TO_15, 3=16_TO_17, 4=18_TO_24, 5=25_TO_34,
        6=35_TO_44, 7=45_TO_54, 8=55_TO_64, 9=65_TO_74, 10=75_TO_84,
        11=85_AND_UP
    """
    # Map to intermediate categories
    emp_status = EMPLOYMENT_MAP.get(employment, "not_employed")
    is_student = STUDENT_MAP.get(student, "not_student") == "student"
    school_cat = SCHOOL_TYPE_MAP.get(school_type, "not_student")

    # Classification based on AgeCategory enum
    if age == AgeCategory.AGE_UNDER_5.value:
        person_type = CTRAMPPersonType.CHILD_UNDER_5.value
    elif age == AgeCategory.AGE_5_TO_15.value:
        person_type = (
            CTRAMPPersonType.CHILD_NON_DRIVING_AGE.value
            if is_student
            else CTRAMPPersonType.NON_WORKER.value
        )
    elif age == AgeCategory.AGE_16_TO_17.value:
        if is_student:
            person_type = CTRAMPPersonType.CHILD_DRIVING_AGE.value
        else:
            person_type = CTRAMPPersonType.NON_WORKER.value
    elif emp_status == "full_time":
        person_type = CTRAMPPersonType.FULL_TIME_WORKER.value
    elif emp_status == "part_time":
        person_type = CTRAMPPersonType.PART_TIME_WORKER.value
    elif is_student and school_cat in ("grade_school", "high_school"):
        person_type = CTRAMPPersonType.CHILD_DRIVING_AGE.value
    elif age >= AgeCategory.AGE_65_TO_74.value:  # Retired
        person_type = CTRAMPPersonType.RETIRED.value
    elif school_cat == "college":
        person_type = CTRAMPPersonType.UNIVERSITY_STUDENT.value
    else:
        person_type = CTRAMPPersonType.NON_WORKER.value

    return person_type


def format_persons(
    persons_canonical: pl.DataFrame,
    tours_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format person data to CT-RAMP specification.

    Transforms person data from canonical format to CT-RAMP format.
    Key transformations:
    - Classify person type based on age, employment, student status
    - Map gender to m/f format
    - Determine free parking eligibility
    - Aggregate activity patterns and tour frequencies from tour data

    Args:
        persons_canonical: Canonical persons DataFrame with person_id, hh_id, person_num,
            age, gender, employment, student, school_type, commute_subsidy_use_3
            (free parking), commute_subsidy_use_4 (discounted parking), value_of_time
        tours_ctramp: Formatted CT-RAMP tours DataFrame with person_id and tour_purpose
            (CTRAMP-formatted purpose strings like 'work_low', 'school_grade', etc.)
        config: CT-RAMP configuration with age thresholds

    Returns:
        DataFrame with CT-RAMP person fields:
        - hh_id: Household ID
        - person_id: Person ID
        - person_num: Person number
        - age: Person age
        - gender: Gender (m/f)
        - type: Person type (1-8)
        - value_of_time: Value of time ($/hour)
        - fp_choice: Free parking choice (1/2)
        - activity_pattern: Daily activity pattern (M/N/H)
        - imf_choice: Individual mandatory tour frequency
        - inmf_choice: Individual non-mandatory tour frequency
        - wfh_choice: Work from home choice (0/1)

    Notes:
        - activity_pattern: M=mandatory tours, N=non-mandatory only, H=no tours
        - imf_choice: Count of mandatory tours (work/school)
        - inmf_choice: Count of non-mandatory tours
        - wfh_choice: Work from home indicator (currently always 0)
    """
    logger.info("Formatting person data for CT-RAMP")

    # Apply person type classification
    persons_ctramp = persons_canonical.with_columns(
        pl.struct(["age", "employment", "student", "school_type"])
        .map_elements(
            lambda x: classify_person_type(
                x["age"],
                x["employment"],
                x["student"],
                x["school_type"],
            ),
            return_dtype=pl.Int64,
        )
        .alias("type_code")
    )

    # Convert person type code to string label (raises if invalid)
    persons_ctramp = persons_ctramp.with_columns(
        pl.col("type_code")
        .map_elements(
            lambda code: CTRAMPPersonType.from_value(code).label,
            return_dtype=pl.String,
        )
        .alias("type")
    )

    # Convert age category to continuous midpoint
    persons_ctramp = persons_ctramp.with_columns(
        pl.col("age")
        .map_elements(
            lambda code: (get_age_midpoint(ac) if (ac := AgeCategory.from_value(code)) else code),
            return_dtype=pl.Int64,
        )
        .alias("age")
    )

    # Map gender (convert int enum to string "m"/"f")
    persons_ctramp = persons_ctramp.with_columns(
        pl.col("gender")
        .fill_null(-1)
        .replace_strict(GENDER_MAP, default=config.gender_default_for_missing)
        .alias("gender")
    )

    # Determine free parking eligibility
    # Person can park for free if they use free or discounted parking
    # (commute_subsidy_use_3 or commute_subsidy_use_4 == 1)
    persons_ctramp = persons_ctramp.with_columns(
        pl.when(
            (pl.col("commute_subsidy_use_3") == BooleanYesNo.YES.value)
            | (pl.col("commute_subsidy_use_4") == BooleanYesNo.YES.value)
        )
        .then(pl.lit(FreeParkingChoice.PARK_FOR_FREE.value))
        .otherwise(pl.lit(FreeParkingChoice.PAY_TO_PARK.value))
        .alias("fp_choice")
    )

    # Aggregate tour statistics from tour data
    tour_stats = aggregate_tour_statistics(tours_ctramp)

    # Join tour statistics, filling with defaults for persons with no tours
    persons_ctramp = persons_ctramp.join(tour_stats, on="person_id", how="left").with_columns(
        [
            pl.col("activity_pattern").fill_null("H"),  # Home if no tours
            pl.col("imf_choice").fill_null(0),  # 0 mandatory tours
            pl.col("inmf_choice").fill_null(0),  # 0 non-mandatory tours
        ]
    )

    # Derive wfh_choice from job_type and employment status
    # WFH = 1 only for employed workers (full-time, part-time, self-employed) AND job_type = WFH
    persons_ctramp = persons_ctramp.with_columns(
        pl.when(
            pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                ]
            )
            & (pl.col("job_type") == JobType.WFH.value)
        )
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("wfh_choice")
    )

    # Note: value_of_time is model output, not survey data
    # If it exists in the input, keep it; otherwise it will be null

    output_cols = [
        "hh_id",
        "person_id",
        "person_num",
        "age",
        "gender",
        "type",
        "value_of_time",
        "fp_choice",
        "activity_pattern",
        "imf_choice",
        "inmf_choice",
        "wfh_choice",
    ]

    # if value_of_time is not in input, drop
    if "value_of_time" not in persons_ctramp.columns:
        output_cols.remove("value_of_time")

    # Add weight and sampleRate if person_weight exists
    if "person_weight" in persons_ctramp.columns:
        persons_ctramp = persons_ctramp.with_columns(
            pl.when(pl.col("person_weight") > 0)
            .then(pl.col("person_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate")
        )
        output_cols.extend(["person_weight", "sampleRate"])

    # Select final columns in CT-RAMP order
    persons_ctramp = persons_ctramp.select(output_cols)

    logger.info("Formatted %d persons for CT-RAMP", len(persons_ctramp))

    return persons_ctramp
