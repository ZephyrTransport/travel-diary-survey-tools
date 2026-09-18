"""CT-RAMP mandatory and non-mandatory tour-frequency mappings."""

import polars as pl

from data_canon.codebook.ctramp import (
    _INMF_MAXES,
    _INMF_REVERSE_LOOKUP,
    CTRAMPActivityPattern,
    CTRAMPTourPurpose,
    IMFChoice,
)


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
                "work_count": pl.UInt32,
            }
        )

    # Define purpose categories for aggregation
    work_purposes = [
        CTRAMPTourPurpose.WORK_LOW.value,
        CTRAMPTourPurpose.WORK_MED.value,
        CTRAMPTourPurpose.WORK_HIGH.value,
        CTRAMPTourPurpose.WORK_VERY_HIGH.value,
    ]
    school_purposes = [
        CTRAMPTourPurpose.SCHOOL_GRADE.value,
        CTRAMPTourPurpose.SCHOOL_HIGH.value,
        CTRAMPTourPurpose.UNIVERSITY.value,
    ]

    # Define escort purposes (includes both segmented variants)
    escort_purposes = [
        CTRAMPTourPurpose.ESCORT_KIDS.value,
        CTRAMPTourPurpose.ESCORT_NO_KIDS.value,
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
        .then(pl.lit(CTRAMPActivityPattern.MANDATORY.value))
        .when(pl.col("inmf_choice") > 0)
        .then(pl.lit(CTRAMPActivityPattern.NON_MANDATORY.value))
        .otherwise(pl.lit(CTRAMPActivityPattern.HOME.value))
        .alias("activity_pattern")
    )

    return person_stats.select(
        [
            "person_id",
            "activity_pattern",
            "imf_choice",
            "inmf_choice",
            "work_count",
        ]
    )
