"""Person formatting for CT-RAMP.

Transforms canonical person data into CT-RAMP model format, including:

- **Person Type Classification**: Derives CT-RAMP person type from age category,
  employment status, and student status
- **Gender Mapping**: Converts to binary m/f format (configurable default for
  non-binary/missing)
- **Free Parking**: Determines eligibility based on commute subsidies
- **Tour Frequency Fields**: Derives imf_choice (mandatory tour frequency),
  inmf_choice (non-mandatory tour frequency), and activity_pattern from
  tour data when available
- **Work-from-Home**: Sets wfh_choice based on work tours vs work days
- **Value of Time**: Calculates based on employment type and household income
  bracket
- **Placeholders**: Uses sensible defaults when tour data is unavailable

Note: Some fields like activity_pattern, imf_choice, and inmf_choice require
tour data and are set to placeholder values. These would be populated from
actual tour extraction results in a full pipeline.
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    _INMF_MAXES,
    _INMF_REVERSE_LOOKUP,
    CTRAMPEmploymentCategory,
    CTRAMPPersonType,
    CTRAMPPurpose,
    CTRAMPStudentCategory,
    FreeParkingChoice,
    IMFChoice,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.persons import AgeCategory, Employment, JobType
from utils.helpers import get_age_midpoint

from .ctramp_config import CTRAMPConfig
from .mappings import (
    EMPLOYMENT_TO_CTRAMP,
    GENDER_MAP,
    ctramp_person_type_expression,
    ctramp_student_category_expression,
    log_person_type_warnings,
    log_student_category_warnings,
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


def enrich_persons_with_person_type(
    persons_canonical: pl.DataFrame,
) -> pl.DataFrame:
    """Enrich persons DataFrame with person_type and type fields.

    Derives person_type (integer code) and type (string label) based on age,
    employment, and student status using the ctramp_person_type_expression.

    If student_category and employment_category columns are present, they will
    be used as inputs to person_type classification for consistency. Otherwise,
    classification falls back to raw employment/student/school_type columns.

    Args:
        persons_canonical: Canonical persons DataFrame

    Returns:
        DataFrame with added person_type and type fields
    """
    # Validate age column using AgeCategory enum from canonical model
    # This is not a perfect validation since it's young ages could erroneously match an enum,
    # but it provides a reasonable check that age codes are within expected categories.
    invalid_ages = set(persons_canonical["age"]) - {x.value for x in AgeCategory}
    if invalid_ages:
        msg = (
            f"Found invalid age codes in input data: {invalid_ages}. "
            f"Expected values are: {[x.value for x in AgeCategory]}"
        )
        raise ValueError(msg)

    # Check if person_type and type columns already exist and are valid
    # If they are invalid, drop them to be re-derived
    # in case some other person_type vestige was present in the data.
    if "person_type" in persons_canonical.columns and "type" in persons_canonical.columns:
        valid_person_types = set(CTRAMPPersonType.to_dict().keys())
        existing_person_types = set(persons_canonical["person_type"].drop_nulls().unique())
        if not existing_person_types.issubset(valid_person_types):
            logger.warning(
                "Existing person_type column contains invalid values: %s. "
                "Dropping person_type and type columns to re-derive from attributes.",
                existing_person_types - valid_person_types,
            )
            persons_canonical = persons_canonical.drop(["person_type", "type"])

    # If person_type or type is missing, or does not match expected values, re-derive it
    if "person_type" not in persons_canonical.columns or "type" not in persons_canonical.columns:
        logger.info("person_type or type column missing, deriving person type from attributes")

        # Determine whether pre-derived categories are available for consistency
        has_employment_category = "employment_category" in persons_canonical.columns
        has_student_category = "student_category" in persons_canonical.columns

        expr_kwargs = {}
        if has_employment_category:
            expr_kwargs["employment_category_col"] = "employment_category"
        if has_student_category:
            expr_kwargs["student_category_col"] = "student_category"

        persons_with_type = persons_canonical.with_columns(
            # Integer person_type code
            ctramp_person_type_expression(**expr_kwargs).alias("person_type"),
            # String type (e.g. "full_time_worker"), derived from person_type code
            ctramp_person_type_expression(**expr_kwargs)
            .replace_strict(CTRAMPPersonType.to_dict())
            .alias("type"),
        )

        # Check for impossible input combinations and log warnings
        warnings = log_person_type_warnings(persons_with_type)
        total_warnings = sum(warnings.values())
        if total_warnings > 0:
            msg = (
                f"Found {total_warnings} problematic person attribute combinations. "
                "Will rely on age-based defaults."
            )
            for category, count in warnings.items():
                msg += f"\n  {category}: {count}"
            logger.warning(msg)
    else:
        persons_with_type = persons_canonical

    return persons_with_type


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
        persons_canonical: Canonical persons DataFrame with derived person_type field
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

    # Compute student_category BEFORE person_type derivation
    # This allows person_type_expression to use the pre-derived category for consistency.
    # Must be computed before age midpoint conversion (uses AgeCategory bins directly).
    persons_with_cats = persons_canonical.with_columns(
        ctramp_student_category_expression(school_taz_col=f"school_{config.taz_field}").alias(
            "student_category"
        )
    )

    # Compute employment_category from employment for mandatory locations
    # Computed early so person_type can use the derived category for consistency
    # (EMPLOYED_UNPAID -> Part-time in EMPLOYMENT_TO_CTRAMP, matching person_type logic)
    persons_with_cats = persons_with_cats.with_columns(
        pl.col("employment")
        .replace_strict(
            EMPLOYMENT_TO_CTRAMP,
            default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
        )
        .alias("employment_category")
    )

    # Check for problematic student/school type combinations
    student_warnings = log_student_category_warnings(persons_with_cats, config)
    total_student_warnings = sum(student_warnings.values())
    if total_student_warnings > 0:
        msg = (
            f"Found {total_student_warnings} problematic student/school type combinations. "
            "Using age-based defaults where appropriate."
        )
        for category, count in student_warnings.items():
            msg += f"\n  {category}: {count}"
        logger.warning(msg)

    # Derive/validate person_type and type fields
    # Uses pre-derived student_category and employment_category for consistency
    persons_with_type = enrich_persons_with_person_type(persons_with_cats)

    # Convert age category to continuous midpoint
    persons_ctramp = persons_with_type.with_columns(
        pl.col("age")
        .map_elements(
            lambda code: (get_age_midpoint(ac) if (ac := AgeCategory.from_value(code)) else code),
            return_dtype=pl.Int64,
        )
        .alias("age"),
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

    # Note: employment_category was already computed before person_type derivation

    # Note: value_of_time is model output, not survey data
    # If it exists in the input, keep it; otherwise it will be null
    if "value_of_time" not in persons_ctramp.columns:
        persons_ctramp = persons_ctramp.with_columns(
            pl.lit(None).cast(pl.Float64).alias("value_of_time")
        )

    # Add weight and sampleRate if person_weight exists
    if "person_weight" in persons_ctramp.columns:
        persons_ctramp = persons_ctramp.with_columns(
            pl.when(pl.col("person_weight") > 0)
            .then(pl.col("person_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate")
        )

    logger.info("Formatted %d persons for CT-RAMP", len(persons_ctramp))
    debug_ptype(persons_ctramp)

    return persons_ctramp


def debug_ptype(persons_ctramp: pl.DataFrame) -> None:
    """Log person type distributions and detect bad attribute combinations.

    Prints a frequency table of person_type by student_category by
    employment_category by age_bin for debugging. Then checks for known
    bad combinations and logs WARNING-level messages with counts.
    """
    # Make a simple age bin, <5, 5-17, 18-64, 65+ for debugging purposes
    persons_ctramp = persons_ctramp.with_columns(
        pl.when(pl.col("age") < 5)  # noqa: PLR2004
        .then(pl.lit("<5"))  # Young children (pre-school)
        .when((pl.col("age") >= 5) & (pl.col("age") <= 17))  # noqa: PLR2004
        .then(pl.lit("5-17"))  # School-aged children
        .when((pl.col("age") >= 18) & (pl.col("age") <= 64))  # noqa: PLR2004
        .then(pl.lit("18-64"))  # Working-age adults
        .otherwise(pl.lit("65+"))  # Seniors
        .alias("age_bin")
    )

    # Create freq table of person_type by student_status by employment_status by age for debugging
    freq_dist = (
        persons_ctramp.group_by("person_type", "student_category", "employment_category", "age_bin")
        .agg(pl.len())
        # Assign string person type labels for debugging
        .with_columns(pl.col("person_type").replace_strict(CTRAMPPersonType.to_dict()))
        .sort(["person_type", "student_category", "employment_category", "age_bin"])
    )

    # Flag known bad combos by joining a rules table onto the freq distribution.
    # Rows matching a rule get an 'expected' value; clean rows stay empty.
    bad_combo_rules = _bad_combo_rules()
    join_keys = ["person_type", "student_category", "employment_category", "age_bin"]
    freq_dist = freq_dist.join(bad_combo_rules, on=join_keys, how="left").with_columns(
        pl.col("expected").fill_null(pl.lit(""))
    )

    # Print full table (bad combos are visible via the 'expected' column)
    with pl.Config(tbl_rows=50):
        s = freq_dist.__repr__()
    logger.info(
        "Person type distribution by student category, employment category, and age bin:\n%s", s
    )

    # Summary warning for flagged rows
    flagged = freq_dist.filter(pl.col("expected") != "")
    if len(flagged) > 0:
        total_bad = flagged["len"].sum()
        logger.warning(
            "Detected %d persons in %d questionable person type/attribute combinations "
            "(see 'expected' column above).",
            total_bad,
            len(flagged),
        )


def _bad_combo_rules() -> pl.DataFrame:
    """Return a lookup table of known bad person_type / attribute combinations.

    Each row describes a (person_type, student_category, employment_category,
    age_bin) combo that should not occur, along with the expected correct
    person_type classification.  Used by ``debug_ptype`` to flag rows in the
    frequency distribution table.

    Bad combos (from BATS 2023 analysis):
    │ Full-time worker           ┆ College or higher    ┆ Part-time employed  ┆ 18-64  │ → university student
    │ Full-time worker           ┆ Grade or high school ┆ Full-time employed  ┆ 5-17   │ → child of non-driving age
    │ Full-time worker           ┆ Grade or high school ┆ Part-time employed  ┆ 5-17   │ → child type by age
    │ Full-time worker           ┆ Not a student        ┆ Part-time employed  ┆ 18-64  │ → part-time worker
    │ Nonworker                  ┆ College or higher    ┆ Not employed        ┆ 18-64  │ → university student
    │ Part-time worker           ┆ College or higher    ┆ Part-time employed  ┆ 18-64  │ → university student
    │ Retired                    ┆ College or higher    ┆ Full-time employed  ┆ 65+    │ → full-time worker
    │ Retired                    ┆ College or higher    ┆ Part-time employed  ┆ 65+    │ → part-time worker
    │ Retired                    ┆ Not a student        ┆ Full-time employed  ┆ 65+    │ → full-time worker
    │ Retired                    ┆ Not a student        ┆ Part-time employed  ┆ 65+    │ → part-time worker
    │ University student         ┆ College or higher    ┆ Full-time employed  ┆ 18-64  │ → full-time worker
    │ University student         ┆ Not a student        ┆ Not employed        ┆ 18-64  │ → nonworker
    │ University student         ┆ Not a student        ┆ Part-time employed  ┆ 18-64  │ → part-time worker
    │ Child of driving age       ┆ Not a student        ┆ Not employed        ┆ 18-64  │ → nonworker

    Note: rows 2-3 share the same join key (driving vs non-driving depends on
    exact age within 5-17); consolidated as "child type by age" in the rules.

    Returns:
        DataFrame with columns (person_type, student_category,
        employment_category, age_bin, expected).
    """  # noqa: E501
    pt = CTRAMPPersonType
    ec = CTRAMPEmploymentCategory
    sc = CTRAMPStudentCategory

    # fmt: off
    # (person_type_label, student_category, employment_category, age_bin, expected)
    rows = [
        (pt.FULL_TIME_WORKER.label,   sc.COLLEGE_OR_HIGHER.value,    ec.PART_TIME_EMPLOYED.value, "18-64", "university student"),  # noqa: E501
        (pt.FULL_TIME_WORKER.label,   sc.GRADE_OR_HIGH_SCHOOL.value, ec.FULL_TIME_EMPLOYED.value, "5-17",  "child type by age"),  # noqa: E501
        (pt.FULL_TIME_WORKER.label,   sc.GRADE_OR_HIGH_SCHOOL.value, ec.PART_TIME_EMPLOYED.value, "5-17",  "child type by age"),  # noqa: E501
        (pt.FULL_TIME_WORKER.label,   sc.NOT_STUDENT.value,          ec.PART_TIME_EMPLOYED.value, "18-64", "part-time worker"),  # noqa: E501
        (pt.NON_WORKER.label,         sc.COLLEGE_OR_HIGHER.value,    ec.NOT_EMPLOYED.value,       "18-64", "university student"),  # noqa: E501
        (pt.PART_TIME_WORKER.label,   sc.COLLEGE_OR_HIGHER.value,    ec.PART_TIME_EMPLOYED.value, "18-64", "university student"),  # noqa: E501
        (pt.RETIRED.label,            sc.COLLEGE_OR_HIGHER.value,    ec.FULL_TIME_EMPLOYED.value, "65+",   "full-time worker"),  # noqa: E501
        (pt.RETIRED.label,            sc.COLLEGE_OR_HIGHER.value,    ec.PART_TIME_EMPLOYED.value, "65+",   "part-time worker"),  # noqa: E501
        (pt.RETIRED.label,            sc.NOT_STUDENT.value,          ec.FULL_TIME_EMPLOYED.value, "65+",   "full-time worker"),  # noqa: E501
        (pt.RETIRED.label,            sc.NOT_STUDENT.value,          ec.PART_TIME_EMPLOYED.value, "65+",   "part-time worker"),  # noqa: E501
        (pt.UNIVERSITY_STUDENT.label, sc.COLLEGE_OR_HIGHER.value,    ec.FULL_TIME_EMPLOYED.value, "18-64", "full-time worker"),  # noqa: E501
        (pt.UNIVERSITY_STUDENT.label, sc.NOT_STUDENT.value,          ec.NOT_EMPLOYED.value,       "18-64", "nonworker"),  # noqa: E501
        (pt.UNIVERSITY_STUDENT.label, sc.NOT_STUDENT.value,          ec.PART_TIME_EMPLOYED.value, "18-64", "part-time worker"),  # noqa: E501
        (pt.CHILD_DRIVING_AGE.label,  sc.NOT_STUDENT.value,          ec.NOT_EMPLOYED.value,       "18-64", "nonworker"),  # noqa: E501
    ]
    # fmt: on

    return pl.DataFrame(
        rows,
        schema=["person_type", "student_category", "employment_category", "age_bin", "expected"],
        orient="row",
    )
