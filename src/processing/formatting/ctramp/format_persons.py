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
    CTRAMPEmploymentCategory,
    CTRAMPPersonType,
    FreeParkingChoice,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.persons import AgeCategory, Employment, JobType
from utils.helpers import get_age_midpoint

from .ctramp_config import CTRAMPConfig
from .person_mappings import (
    EMPLOYMENT_TO_CTRAMP,
    GENDER_MAP,
    add_industry_empsix,
    bad_person_type_combinations,
    ctramp_person_type_expression,
    log_person_type_warnings,
)
from .student_mappings import (
    ctramp_student_category_expression,
    log_student_category_warnings,
)
from .tour_frequency_mappings import aggregate_tour_statistics

logger = logging.getLogger(__name__)


# Individual Mandatory/Non-Mandatory Frequency Mapping ------------------------


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
            commute_subsidy_provide_free_parking (employer provides free parking),
            commute_subsidy_provide_discounted_parking (employer provides discounted parking),
            commute_subsidy_use_free_parking (respondent uses free parking),
            commute_subsidy_use_discounted_parking (respondent uses discounted parking),
            value_of_time
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
        - wfh_choice: 1 if employed, job_type=WFH, and no work tours
            (binary: WFH or commute, not both)
        - employment_category: derived from the BATS `employment` field via
          [`EMPLOYMENT_TO_CTRAMP`][processing.formatting.ctramp.person_mappings.EMPLOYMENT_TO_CTRAMP]:

          | BATS employment value | CT-RAMP EmploymentCategory |
          |---|---|
          | `EMPLOYED_FULLTIME` | Full-time worker |
          | `EMPLOYED_SELF` | Full-time worker (self-employed treated as full-time) |
          | `EMPLOYED_PARTTIME` | Part-time worker |
          | `EMPLOYED_UNPAID` | Part-time worker (unpaid work treated as part-time) |
          | all others (not employed, retired, student, etc.) | Not employed |

          Children classified as `CHILD_UNDER_5` or `STUDENT_NON_DRIVING_AGE` by person type
          are overridden to `Under age 16` regardless of reported employment.
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

    # Children under 16 get UNDER_16 employment category regardless of reported employment
    persons_with_type = persons_with_type.with_columns(
        pl.when(
            pl.col("person_type").is_in(
                [
                    CTRAMPPersonType.CHILD_UNDER_5.value,
                    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPEmploymentCategory.UNDER_16.value))
        .otherwise(pl.col("employment_category"))
        .alias("employment_category")
    )

    # Convert age category to continuous midpoint
    persons_ctramp = persons_with_type.with_columns(
        pl.col("age")
        .map_elements(
            lambda code: get_age_midpoint(ac) if (ac := AgeCategory.from_value(code)) else code,
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
    # Person parks for free if employer provides free/discounted parking
    # OR if the respondent uses free/discounted parking
    persons_ctramp = persons_ctramp.with_columns(
        pl.when(
            (pl.col("commute_subsidy_provide_free_parking") == BooleanYesNo.YES.value)
            | (pl.col("commute_subsidy_provide_discounted_parking") == BooleanYesNo.YES.value)
            | (pl.col("commute_subsidy_use_free_parking") == BooleanYesNo.YES.value)
            | (pl.col("commute_subsidy_use_discounted_parking") == BooleanYesNo.YES.value)
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
            pl.col("work_count").fill_null(0),  # 0 work tours
        ]
    )

    # Derive wfh_choice from employment status, observed telecommute time, and work tours.
    # This reflects the travel model's binary treatment: a person either WFH or commutes on a day.
    #
    # When telecommute_time is available (carried from the days table during expansion):
    #   wfh = 1 if employed AND telecommute_time > 0 AND no work tours that day.
    #   job_type is intentionally NOT used: any job type counts as long as the person
    #   actually reported telecommuting and did not make a work tour.
    #
    # Fallback when telecommute_time is absent: use job_type ∈ {WFH, HYBRID} as proxy.
    employed_cond = pl.col("employment").is_in(
        [
            Employment.EMPLOYED_FULLTIME.value,
            Employment.EMPLOYED_PARTTIME.value,
            Employment.EMPLOYED_SELF.value,
        ]
    )
    no_work_tour_cond = pl.col("work_count") == 0
    if "telecommute_time" in persons_ctramp.columns:
        wfh_cond = employed_cond & (pl.col("telecommute_time") > 0) & no_work_tour_cond
    else:
        wfh_cond = (
            employed_cond
            & pl.col("job_type").is_in([JobType.WFH.value, JobType.HYBRID.value])
            & no_work_tour_cond
        )
    persons_ctramp = persons_ctramp.with_columns(
        pl.when(wfh_cond).then(pl.lit(1)).otherwise(pl.lit(0)).alias("wfh_choice")
    )

    # Derive industry_empsix (CT-RAMP empsix employment sector) from the canonical
    # industry code, filling from free-text industry_other where available.
    persons_ctramp = persons_ctramp.join(
        add_industry_empsix(persons_canonical).select(["person_id", "industry_empsix"]),
        on="person_id",
        how="left",
    )

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

    # Reorder columns to the canonical CT-RAMP person output order.
    # Only include columns that are actually present in the DataFrame.
    column_order = [
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
        "workDCLogsum",
        "schoolDCLogsum",
        "sampleRate",
        "wfh_choice",
        "industry",
    ]
    ordered = [c for c in column_order if c in persons_ctramp.columns]
    remaining = [c for c in persons_ctramp.columns if c not in ordered]
    persons_ctramp = persons_ctramp.select(ordered + remaining)

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
    bad_combo_rules = bad_person_type_combinations()
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
