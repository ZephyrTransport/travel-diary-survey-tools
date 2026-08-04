"""Custom cleaning steps for the  Bay Area Travel Study (BATS) 2023 pipeline."""

import logging

import polars as pl

from data_canon.codebook.households import (
    IncomeBroad,
    IncomeDetailed,
    IncomeFollowup,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import Ethnicity, Race
from data_canon.core.labeled_enum import LabeledEnum
from pipeline.decoration import step
from utils.helpers import add_time_columns, expr_haversine, get_income_midpoint

logger = logging.getLogger(__name__)


def _income_midpoint_expr(col: str, enum: type[LabeledEnum], households: pl.DataFrame) -> pl.Expr:
    """Map an income-bracket column to its midpoint in dollars (null if unusable).

    Brackets that carry no dollar range (Missing / Prefer-not-to-answer) map to
    null so the caller can fall back to the next-best income question.
    """
    if col not in households.columns:
        return pl.lit(None, dtype=pl.Int64)
    midpoints = {}
    for member in enum:
        try:
            midpoints[member.value] = int(get_income_midpoint(member))
        except ValueError:  # PNTA / Missing carry no dollar range
            continue
    return pl.col(col).cast(pl.Int64).replace_strict(midpoints, default=None, return_dtype=pl.Int64)


@step()
def clean_2023_bats(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline."""
    # CLEANUP UNLINKED TRIPS =================================
    # Much wow...
    logger.info("Cleaning 2023 trip data")

    # Rename columns to match expected names
    unlinked_trips = unlinked_trips.rename(
        {
            "arrive_second": "arrive_seconds",
            "trip_id": "unlinked_trip_id",
        }
    )

    # Add time columns if missing
    unlinked_trips = add_time_columns(unlinked_trips)

    # "Correct" trips when depart_time > arrive_time, flip them
    # including the separate hours, minutes, seconds columns
    # Create a swap condition to reuse
    swap_condition = pl.col("depart_time") > pl.col("arrive_time")
    # Swap depart/arrive columns when depart_time > arrive_time
    swap_cols = [
        ("depart_time", "arrive_time"),
        ("depart_hour", "arrive_hour"),
        ("depart_minute", "arrive_minute"),
        ("depart_seconds", "arrive_seconds"),
    ]

    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.when(swap_condition).then(pl.col(b)).otherwise(pl.col(a)).alias(a)
            for a, b in swap_cols
        ]
        + [
            pl.when(swap_condition).then(pl.col(a)).otherwise(pl.col(b)).alias(b)
            for a, b in swap_cols
        ]
    )

    # Replace any -1 value in *_purpose columns with missing code
    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.when(pl.col(col_name) == -1).then(996).otherwise(pl.col(col_name)).alias(col_name)
            for col_name in [
                "o_purpose",
                "d_purpose",
                "o_purpose_category",
                "d_purpose_category",
            ]
        ]
    )

    # If distance is null, recalculate it from lat/lon
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("distance_meters").is_null())
        .then(
            expr_haversine(
                pl.col("o_lon"),
                pl.col("o_lat"),
                pl.col("d_lon"),
                pl.col("d_lat"),
            )
        )
        .otherwise(pl.col("distance_meters"))
        .alias("distance_meters")
    )

    # If duration_minutes is null, recalculate it from depart/arrive times
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("duration_minutes").is_null())
        .then((pl.col("arrive_time") - pl.col("depart_time")).dt.total_minutes())
        .otherwise(pl.col("duration_minutes"))
        .alias("duration_minutes")
    )

    # Drop the split time fields (hours, minutes, seconds)
    unlinked_trips = unlinked_trips.drop(
        [
            "depart_date",
            "depart_hour",
            "depart_minute",
            "depart_seconds",
            "arrive_date",
            "arrive_hour",
            "arrive_minute",
            "arrive_seconds",
        ]
    )

    # ADD DAYS FOR PERSONS WITHOUT DAYS =================================
    # Find surveyable persons without days. Unsurveyable persons (unrelated
    # household members, e.g. roommates) get no day rows at all, matching the
    # vendor's day table: they are kept for weighting at the person level but
    # contribute no travel days, and a synthesized all-null day would wrongly
    # veto the household-day completeness reduction (ALL members complete).
    persons_without_days = persons.filter(
        ~pl.col("person_id").is_in(days["person_id"].unique().implode())
        & (pl.col("surveyable") == 1)
    )

    logger.info(
        "Creating dummy days for %d persons without days",
        len(persons_without_days),
    )

    # Get travel_dow from other household members' days
    days_for_dow = (
        days.select(["hh_id", "travel_dow", "travel_date", "day_num"])
        .filter(pl.col("hh_id").is_in(persons_without_days["hh_id"].unique().implode()))
        .unique()
    )

    # Which day columns to include
    day_cols = ["hh_id", "person_id", "day_id", "travel_dow", "travel_date", "day_num"]

    # Create a default day for each person without days
    dummy_days = (
        persons_without_days.join(days_for_dow, on="hh_id", how="left")
        .with_columns(
            # Construct default day_id (person_id * 100 + day_num)
            (pl.col("person_id") * 100 + pl.col("day_num")).alias("day_id")
        )
        .select(day_cols)
    )
    # Add dummy days to days dataframe
    _days = days.clone()
    days = pl.concat([_days, dummy_days], how="diagonal")

    # CLEANUP HOUSEHOLD ATTRIBUTES =================================
    # Move residence type and residence rent/own from persons to households
    # Extract household-level attributes from persons table
    # Only one person reports residence_rent_own and residence_type
    hh_attributes = persons.group_by("hh_id").agg(
        pl.col("residence_rent_own")
        .filter(
            ~pl.col("residence_rent_own").is_in(
                [ResidenceRentOwn.MISSING.value, ResidenceRentOwn.PNTA.value]
            )
        )
        .mode()
        # mode() can return several tied values in arbitrary order; sort so the
        # tie-break is deterministic (smallest code) rather than hash-dependent.
        .sort()
        .first()
        .fill_null(995),
        pl.col("residence_type")
        .filter(pl.col("residence_type") != ResidenceType.MISSING.value)
        .mode()
        .sort()
        .first()
        .fill_null(995),
    )
    # Join to households
    households = households.join(hh_attributes, on="hh_id", how="left")

    # RECODE INCOME =========================================
    # Vendor column "income_broad" already uses IncomeBroad-compatible codes
    # (1-6, 995, 999).  Rename to canonical "income_bin" and validate.
    if "income_broad" in households.columns:
        households = households.rename({"income_broad": "income_bin"})
    elif "income_bin" not in households.columns:
        logger.warning("No income column found — creating income_bin as MISSING")
        households = households.with_columns(pl.lit(IncomeBroad.MISSING.value).alias("income_bin"))

    # Clamp any unrecognised codes to MISSING
    valid_codes = {m.value for m in IncomeBroad}
    households = households.with_columns(
        pl.when(pl.col("income_bin").is_in(valid_codes))
        .then(pl.col("income_bin"))
        .otherwise(IncomeBroad.MISSING.value)
        .alias("income_bin")
    )

    # Populate the canonical continuous `income` (dollars) so DaySim `hhincome`
    # keeps the survey's full granularity. The survey asks a broad bin and then
    # a detailed follow-up that not every respondent answers, so fall back:
    #   income_detailed (10 brackets) -> income_followup (6) -> null
    # A null falls through to the income_bin midpoint in format_households,
    # which is also what the imputed households get.
    households = households.with_columns(
        pl.coalesce(
            _income_midpoint_expr("income_detailed", IncomeDetailed, households),
            _income_midpoint_expr("income_followup", IncomeFollowup, households),
        ).alias("income")
    )
    n_income = households["income"].drop_nulls().len()
    logger.info(
        "Derived continuous income for %d / %d households (%.1f%%); "
        "the rest fall back to the income_bin midpoint",
        n_income,
        len(households),
        100 * n_income / len(households),
    )

    # CLEANUP PERSON ATTRIBUTES =================================
    eth_cols = ["ethnicity_1", "ethnicity_2", "ethnicity_3", "ethnicity_4", "ethnicity_997"]
    race_cols = ["race_1", "race_2", "race_3", "race_4", "race_5", "race_997"]

    # Fill nulls and 995 with 0 for all race columns
    for col in [*race_cols, *eth_cols, "race_999", "ethnicity_999"]:
        if col in persons.columns:
            persons = persons.with_columns(
                pl.when(pl.col(col) == 995)  # noqa: PLR2004
                .then(0)
                .otherwise(pl.col(col))
                .alias(col)
            ).with_columns(pl.col(col).fill_null(0))

    # Need to create a single "race" columns instead of the multiple binary columns
    # race_1 - Race: African American or Black
    # race_2 - Race: American Indian or Alaska Native
    # race_3 - Race: Asian
    # race_4 - Race: Native Hawaiian or other Pacific Islander
    # race_5 - Race: White
    # race_997 - Race: Other race
    # race_999 - Race: Prefer not to answer
    persons = persons.with_columns(
        pl.when(pl.col("race_1") == 1)
        .then(pl.lit(Race.AFAM.value))
        .when(pl.col("race_2") == 1)
        .then(pl.lit(Race.NATIVE.value))
        .when(pl.col("race_3") == 1)
        .then(pl.lit(Race.ASIAN.value))
        .when(pl.col("race_4") == 1)
        .then(pl.lit(Race.PACIFIC.value))
        .when(pl.col("race_5") == 1)
        .then(pl.lit(Race.WHITE.value))
        .when(pl.col("race_997") == 1)
        .then(pl.lit(Race.OTHER.value))
        .when(pl.sum_horizontal(pl.col(race_cols)) > 1)
        .then(pl.lit(Race.MULTI.value))
        .when(pl.col("race_999") == 1)
        .then(pl.lit(None))
        .otherwise(None)
        .alias("race")
    )

    # Need to create a single "ethnicity" column instead of the multiple binary columns
    # ethnicity_1 - Ethnicity: Not of Hispanic, Latino, or Spanish origin
    # ethnicity_2 - Ethnicity: Mexican, Mexican American, Chicano
    # ethnicity_3 - Ethnicity: Puerto Rican
    # ethnicity_4 - Ethnicity: Cuban
    # ethnicity_997 - Ethnicity: Another Hispanic, Latino, or Spanish origin
    # ethnicity_999 - Ethnicity: Prefer not to answer
    persons = persons.with_columns(
        pl.when(pl.col("ethnicity_1") == 1)
        .then(pl.lit(Ethnicity.NOT_HISPANIC.value))
        .when(pl.col("ethnicity_2") == 1)
        .then(pl.lit(Ethnicity.MEXICAN.value))
        .when(pl.col("ethnicity_3") == 1)
        .then(pl.lit(Ethnicity.PUERTO_RICAN.value))
        .when(pl.col("ethnicity_4") == 1)
        .then(pl.lit(Ethnicity.CUBAN.value))
        .when(pl.col("ethnicity_997") == 1)
        .then(pl.lit(Ethnicity.OTHER.value))
        .when(pl.sum_horizontal(pl.col(eth_cols)) > 1)
        .then(pl.lit(Ethnicity.OTHER.value))
        .when(pl.col("ethnicity_999") == 1)
        .then(pl.lit(None))
        .otherwise(None)
        .alias("ethnicity")
    )

    # ASSIGN COMPLETION STATUS =========================================
    # Completeness rolls up from the surveyed trip (see processing.completeness).
    # This project measures two leaf facts directly; person and household
    # completeness are derived *upward* later by the cascade_completeness step, so
    # the vendor's person/household is_complete here is only a placeholder the
    # rollup overwrites.
    #
    #   trip.complete = trip_survey_complete            (the measured leaf)
    #   day.complete  = every trip surveyed, else a *declared* no-travel day
    #                   (a no-travel reason given, or a proxy filled the day in)
    unlinked_trips = unlinked_trips.rename({"trip_survey_complete": "complete"}).with_columns(
        pl.col("complete").cast(pl.Boolean)
    )

    all_trips_surveyed = unlinked_trips.group_by("day_id").agg(
        pl.col("complete").all().alias("_all_surveyed")
    )
    days = (
        days.join(all_trips_surveyed, on="day_id", how="left")
        .with_columns(
            pl.when(pl.col("num_trips") > 0)
            .then(pl.col("_all_surveyed").fill_null(value=False))
            .otherwise((pl.col("num_reasons_no_travel") >= 1) | (pl.col("proxy_complete") == 1))
            .fill_null(value=False)
            .alias("complete")
        )
        .drop("_all_surveyed")
    )

    households = households.rename({"is_complete": "complete"}).with_columns(
        pl.col("complete").cast(pl.Boolean)
    )
    persons = persons.rename({"is_complete": "complete"}).with_columns(
        pl.col("complete").cast(pl.Boolean)
    )

    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
    }
