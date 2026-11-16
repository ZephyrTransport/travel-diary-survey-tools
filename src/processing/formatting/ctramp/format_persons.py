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
- **Value of Time**: Left null. It is a model output, not survey data, and is
  kept only if the input already carries it
- **Defaults**: A person with no tours gets activity_pattern 'H' and zero tour
  frequencies

activity_pattern, imf_choice and inmf_choice are derived from the person's
formatted tours.
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
from .output_columns import order_columns, sample_rate
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
from .tour_frequency_mappings import aggregate_tour_statistics, log_patterns_not_offered

logger = logging.getLogger(__name__)

# Persons under 16 by person type; their employment category is UNDER_16.
_UNDER_16_TYPES = [
    CTRAMPPersonType.CHILD_UNDER_5.value,
    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
]


def enrich_persons_with_person_type(
    persons_canonical: pl.DataFrame,
) -> pl.DataFrame:
    """Enrich persons DataFrame with person_type and type fields.

    Derives person_type (integer code) and type (string label) from age,
    ``employment_category`` and ``student_category`` using
    ``ctramp_person_type_expression``. Both categories must already be present
    (see ``add_person_type``): a caller that has not derived them is a
    programming error, not a case to guess at. An existing person_type column
    holding values outside ``CTRAMPPersonType`` is dropped and re-derived.

    The age check is not exhaustive (a young age code could match by accident),
    but it catches codes outside ``AgeCategory``.

    Args:
        persons_canonical: Canonical persons DataFrame

    Returns:
        DataFrame with added person_type and type fields
    """
    invalid_ages = set(persons_canonical["age"]) - {x.value for x in AgeCategory}
    if invalid_ages:
        msg = (
            f"Found invalid age codes in input data: {invalid_ages}. "
            f"Expected values are: {[x.value for x in AgeCategory]}"
        )
        raise ValueError(msg)

    if "person_type" in persons_canonical.columns and "type" in persons_canonical.columns:
        valid_person_types = set(CTRAMPPersonType.to_dict().keys())
        existing_person_types = set(persons_canonical["person_type"].drop_nulls().unique())
        if existing_person_types.issubset(valid_person_types):
            return persons_canonical
        logger.warning(
            "Existing person_type column contains invalid values: %s. "
            "Dropping person_type and type columns to re-derive from attributes.",
            existing_person_types - valid_person_types,
        )
        persons_canonical = persons_canonical.drop(["person_type", "type"])

    logger.info("person_type or type column missing, deriving person type from attributes")
    missing = [
        col
        for col in ("employment_category", "student_category")
        if col not in persons_canonical.columns
    ]
    if missing:
        msg = (
            f"persons is missing {missing}, which person type is derived from. "
            "Derive them first: student_category via "
            "ctramp_student_category_expression, employment_category via "
            "EMPLOYMENT_TO_CTRAMP."
        )
        raise ValueError(msg)

    persons_with_type = persons_canonical.with_columns(
        ctramp_person_type_expression().alias("person_type")
    ).with_columns(pl.col("person_type").replace_strict(CTRAMPPersonType.to_dict()).alias("type"))

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
    return persons_with_type


def add_person_type(persons: pl.DataFrame, config: CTRAMPConfig) -> pl.DataFrame:
    """Add the CT-RAMP student category, employment category and person type.

    ``student_category`` is derived while age is still an ``AgeCategory`` code,
    and ``employment_category`` from ``employment`` via ``EMPLOYMENT_TO_CTRAMP``
    (unpaid work counts as part-time); person type then reads both, so the
    rules live in one place. Anyone typed as under 16 then gets the
    ``UNDER_16`` employment category, whatever employment they reported.

    Args:
        persons: Canonical persons with age, employment, student, school_type
            and ``school_<taz_field>``.
        config: CT-RAMP settings; ``taz_field`` names the school zone column.

    Returns:
        *persons* with ``student_category``, ``employment_category``,
        ``person_type`` and ``type``.
    """
    persons = persons.with_columns(
        ctramp_student_category_expression(school_taz_col=f"school_{config.taz_field}").alias(
            "student_category"
        )
    ).with_columns(
        pl.col("employment")
        .replace_strict(EMPLOYMENT_TO_CTRAMP, default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value)
        .alias("employment_category")
    )
    return enrich_persons_with_person_type(persons).with_columns(
        pl.when(pl.col("person_type").is_in(_UNDER_16_TYPES))
        .then(pl.lit(CTRAMPEmploymentCategory.UNDER_16.value))
        .otherwise(pl.col("employment_category"))
        .alias("employment_category")
    )


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
        persons_canonical: Canonical persons DataFrame (person type is added here,
            see ``add_person_type``) with
            commute_subsidy_provide_free_parking (employer provides free parking),
            commute_subsidy_provide_discounted_parking (employer provides discounted parking),
            commute_subsidy_use_free_parking (respondent uses free parking),
            commute_subsidy_use_discounted_parking (respondent uses discounted parking),
            value_of_time
        tours_ctramp: Formatted CT-RAMP tours DataFrame with person_id and tour_purpose
            (CTRAMP-formatted purpose strings like 'work_low', 'school_grade', etc.)
        config: CT-RAMP configuration (TAZ field, gender for missing values)

    Returns:
        DataFrame with CT-RAMP person fields:

            - hh_id: Household ID
            - person_id: Person ID
            - person_num: Person number
            - age: Person age
            - gender: Gender (m/f)
            - type: Person type label (person_type holds the code, 1-8)
            - value_of_time: Value of time ($/hour); null unless supplied
            - fp_choice: Free parking choice (1/2)
            - activity_pattern: Daily activity pattern (M/N/H)
            - imf_choice: Individual mandatory tour frequency
            - inmf_choice: Individual non-mandatory tour frequency
            - wfh_choice: Work from home choice (0/1)

    Notes:
        - activity_pattern: M=mandatory tours, N=non-mandatory only, H=no tours
        - imf_choice: Count of mandatory tours (work/school)
        - inmf_choice: Count of non-mandatory tours
        - wfh_choice: 1 if employed (full-time, part-time or self-employed), made
            no work tour, and reported telecommute time that day -- or, when
            telecommute_time is absent, has job_type WFH or HYBRID. Binary, as in
            the model: a person either works from home or commutes on a day.
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

    # Must run before the age midpoint conversion below: it reads AgeCategory codes.
    persons_with_type = add_person_type(persons_canonical, config)

    student_warnings = log_student_category_warnings(persons_with_type, config)
    total_student_warnings = sum(student_warnings.values())
    if total_student_warnings > 0:
        msg = (
            f"Found {total_student_warnings} problematic student/school type combinations. "
            "Using age-based defaults where appropriate."
        )
        for category, count in student_warnings.items():
            msg += f"\n  {category}: {count}"
        logger.warning(msg)

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
            pl.col("school_count").fill_null(0),  # 0 school tours
        ]
    )
    log_patterns_not_offered(persons_ctramp)

    # wfh_choice: see the docstring. telecommute_time comes from the days table.
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

    if "person_weight" in persons_ctramp.columns:
        persons_ctramp = persons_ctramp.with_columns(sample_rate("person_weight"))

    persons_ctramp = order_columns(
        persons_ctramp,
        [
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
        ],
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
