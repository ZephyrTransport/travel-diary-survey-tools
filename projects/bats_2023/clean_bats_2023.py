"""Custom cleaning steps for the  Bay Area Travel Study (BATS) 2023 pipeline."""

import logging
from typing import Literal

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.households import (
    IncomeBroad,
    IncomeDetailed,
    IncomeFollowup,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import Ethnicity, Race, SurveyTech
from data_canon.core.labeled_enum import LabeledEnum
from pipeline.decoration import Tables, step
from processing.habitual_locations import reported_habitual_locations
from utils.create_ids import create_household_days
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


def second_homes(persons: pl.DataFrame) -> pl.DataFrame:
    """Each person's reported second home, as a further reported location.

    BATS asks whether the respondent has a second home and geocodes it into the
    wide ``second_home_lat/lon`` person columns. Canonical data carries it as a
    row of the delivered ``habitual_locations`` table instead.
    """
    return persons.filter(
        (pl.col("second_home") == 1)
        & pl.col("second_home_lat").is_not_null()
        & pl.col("second_home_lon").is_not_null()
    ).select(
        "person_id",
        pl.lit(LocationType.HOME.value, dtype=pl.Int64).alias("location_type"),
        pl.col("second_home_lat").alias("lat"),
        pl.col("second_home_lon").alias("lon"),
    )


def survey_tech(persons: pl.DataFrame) -> pl.DataFrame:
    """Add ``survey_tech`` from the vendor's ``diary_platform``.

    A proxied person carries the platform their travel was reported through,
    which the vendor gives every member of the household. A null platform is
    MISSING; a platform not in the mapping stops the run rather than guess.

    Raises:
        ValueError: If ``diary_platform`` holds a value the mapping does not know.
    """
    techs = {
        "rmove": SurveyTech.GPS,
        "browser": SurveyTech.BROWSER,
        "call": SurveyTech.CALL_CENTER,
    }
    unknown = sorted(set(persons["diary_platform"].drop_nulls().unique()) - set(techs))
    if unknown:
        msg = f"diary_platform has values with no survey_tech: {unknown}"
        raise ValueError(msg)
    codes = {platform: tech.value for platform, tech in techs.items()}
    return persons.with_columns(
        pl.col("diary_platform")
        .replace_strict(codes, default=None, return_dtype=pl.Int64)
        .fill_null(SurveyTech.MISSING.value)
        .alias("survey_tech")
    )


def exclude_by_survey_tech(persons: pl.DataFrame, excluded: list[str]) -> pl.DataFrame:
    """Give every person collected by an excluded technology an ``exclusion_reason``.

    *excluded* names technologies as config writes them (``call_center``,
    ``form``, ``gps``, ``missing``). Every other person keeps a null reason.

    Raises:
        ValueError: If *excluded* names a technology that does not exist.
    """
    names = {tech.name.lower(): tech for tech in SurveyTech}
    unknown = sorted(set(excluded) - set(names))
    if unknown:
        msg = f"exclude_survey_tech names {unknown}, which are not one of {sorted(names)}"
        raise ValueError(msg)
    reasons = {names[n].value: f"survey_tech excluded: {n}" for n in excluded}
    return persons.with_columns(
        pl.col("survey_tech")
        .replace_strict(reasons, default=None, return_dtype=pl.String)
        .alias("exclusion_reason")
    )


def race_from_boxes() -> pl.Expr:
    """One ``race`` from the vendor's race boxes, each 1 when ticked.

    Two or more boxes is MULTI, as the census counts "two or more races"; one
    box is that race; none (or only "prefer not to answer") is null.
    """
    boxes = {
        "race_1": Race.AFAM,
        "race_2": Race.NATIVE,
        "race_3": Race.ASIAN,
        "race_4": Race.PACIFIC,
        "race_5": Race.WHITE,
        "race_997": Race.OTHER,
    }
    ticked = pl.sum_horizontal([(pl.col(box) == 1).fill_null(value=False) for box in boxes])
    expr = pl.when(ticked > 1).then(pl.lit(Race.MULTI.value))
    for box, race in boxes.items():
        expr = expr.when(pl.col(box) == 1).then(pl.lit(race.value))
    return expr.otherwise(None).alias("race")


def trip_survey_verdict(col: str) -> pl.Expr:
    """A trip's own ``survey_complete`` from the vendor's trip-survey flag.

    1 is surveyed and 0 is not. 995 is the vendor's NA: it marks the trips the
    vendor added itself (numbered after the person's other trips, with a mode and
    purpose, and almost all on days the vendor calls complete). They had no trip
    survey to answer, so they get no verdict of their own (null), and the
    cascade gives them their day's.
    """
    flag = pl.col(col)
    return pl.when(flag == 995).then(None).otherwise(flag == 1).alias(col)  # noqa: PLR2004


def day_survey_complete(
    person_days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    child_day_complete: Literal["vendor", "observed_trips"],
) -> pl.DataFrame:
    """Set ``person_days.survey_complete``: the vendor's day verdict, with a choice for children.

    The vendor decides what a complete survey day is, and an adult's day always
    takes its ``is_complete``. Children (``is_participant`` 0) do not take the
    survey. An adult files one proxy report covering a child's whole day, on one
    weekday; on the other days the child has only the trips they made with an
    adult, copied from that adult's (``copied_from_proxy``). The vendor counts a
    child's day only with the proxy report.

    ``child_day_complete`` chooses the rule for children, and is the one place this
    project departs from the vendor's verdict:

    * ``"vendor"``: the vendor's verdict, as for adults.
    * ``"observed_trips"``: complete when an adult filed the proxy report, when the
      day has trips and every one with a trip-survey verdict is surveyed, or when
      it has none and a no-travel reason was given. This counts the days seen only
      through copied trips, which show the child's travel with that adult and
      nothing else.

    Expects ``unlinked_trips.survey_complete`` from :func:`trip_survey_verdict`.
    """
    vendor = pl.col("is_complete") == 1
    if child_day_complete == "vendor":
        return person_days.with_columns(vendor.fill_null(value=False).alias("survey_complete"))
    if child_day_complete != "observed_trips":
        msg = f"child_day_complete must be 'vendor' or 'observed_trips', got {child_day_complete!r}"
        raise ValueError(msg)

    # all() skips nulls: a trip with no verdict of its own does not decide the day.
    all_surveyed = unlinked_trips.group_by("person_day_id").agg(
        pl.col("survey_complete").all().alias("_all_surveyed")
    )
    observed = (pl.col("proxy_complete") == 1) | (
        pl.when(pl.col("num_trips") > 0)
        .then(pl.col("_all_surveyed").fill_null(value=False))
        .otherwise(pl.col("num_reasons_no_travel") >= 1)
    )
    return (
        person_days.join(all_surveyed, on="person_day_id", how="left")
        .with_columns(
            pl.when(pl.col("is_participant") == 0)
            .then(observed)
            .otherwise(vendor)
            .fill_null(value=False)
            .alias("survey_complete")
        )
        .drop("_all_surveyed")
    )


@step(
    requires={"households": set(), "persons": set(), "person_days": set(), "unlinked_trips": set()}
)
def clean_2023_bats(
    tables: Tables,
    child_day_complete: Literal["vendor", "observed_trips"],
    exclude_survey_tech: list[str],
) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline.

    Args:
        tables: The delivered households, persons, person-days and trips.
        child_day_complete: How a child's day is judged complete; see
            :func:`day_survey_complete`. Required, so every config says which.
        exclude_survey_tech: Technologies whose persons are dropped from every
            usability profile; see :func:`exclude_by_survey_tech`. Required, so
            every config says which (an empty list drops no one).
    """
    households = tables["households"]
    persons = tables["persons"]
    person_days = tables["person_days"]
    unlinked_trips = tables["unlinked_trips"]

    # CLEANUP UNLINKED TRIPS =================================
    # Much wow...
    logger.info("Cleaning 2023 trip data")

    # Rename columns to match expected names. The vendor's day_id is our person_day_id.
    unlinked_trips = unlinked_trips.rename(
        {
            "arrive_second": "arrive_seconds",
            "trip_id": "unlinked_trip_id",
            "day_id": "person_day_id",
        }
    )
    person_days = person_days.rename({"day_id": "person_day_id"})

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
        ~pl.col("person_id").is_in(person_days["person_id"].unique().implode())
        & (pl.col("surveyable") == 1)
    )

    logger.info(
        "Creating dummy days for %d persons without days",
        len(persons_without_days),
    )

    # Get travel_dow from other household members' days
    days_for_dow = (
        person_days.select(["hh_id", "travel_dow", "travel_date", "day_num"])
        .filter(pl.col("hh_id").is_in(persons_without_days["hh_id"].unique().implode()))
        .unique()
    )

    # Which day columns to include
    day_cols = ["hh_id", "person_id", "person_day_id", "travel_dow", "travel_date", "day_num"]

    # Create a default day for each person without days
    dummy_days = (
        persons_without_days.join(days_for_dow, on="hh_id", how="left")
        .with_columns(
            # Construct default person_day_id (person_id * 100 + day_num)
            (pl.col("person_id") * 100 + pl.col("day_num")).alias("person_day_id")
        )
        .select(day_cols)
    )
    # Add dummy days to days dataframe
    _days = person_days.clone()
    person_days = pl.concat([_days, dummy_days], how="diagonal")

    # CLEANUP HOUSEHOLD ATTRIBUTES =================================
    # Move residence type and residence rent/own from persons to households
    # Extract household-level attributes from persons table
    # Only one person reports residence_rent_own and residence_type
    # For each column: filter out missing/PNTA values, then take the mode.
    # On ties, pick the smallest code (sort ascending before first()) so the
    # result is deterministic regardless of Polars internal hash ordering.
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

    persons = persons.with_columns(race_from_boxes())

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

    # Rename commute subsidy columns to canonical field names
    persons = persons.rename(
        {
            "commute_subsidy_3": "commute_subsidy_provide_free_parking",
            "commute_subsidy_4": "commute_subsidy_provide_discounted_parking",
            "commute_subsidy_use_3": "commute_subsidy_use_free_parking",
            "commute_subsidy_use_4": "commute_subsidy_use_discounted_parking",
        }
    )

    # SURVEY TECHNOLOGY AND EXCLUSIONS ==================================
    persons = exclude_by_survey_tech(survey_tech(persons), exclude_survey_tech)
    n_excluded = persons["exclusion_reason"].is_not_null().sum()
    logger.info("Excluded %d of %d persons by survey_tech", n_excluded, persons.height)

    # ASSIGN COMPLETION STATUS =========================================
    # Person and household completeness are rolled up later by cascade_usability,
    # so their is_complete here is only a placeholder.
    unlinked_trips = unlinked_trips.rename(
        {"trip_survey_complete": "survey_complete"}
    ).with_columns(trip_survey_verdict("survey_complete"))
    person_days = day_survey_complete(person_days, unlinked_trips, child_day_complete)

    # HOUSEHOLD DAYS ===================================================
    person_days, household_days = create_household_days(person_days)

    households = households.rename({"is_complete": "survey_complete"}).with_columns(
        pl.col("survey_complete").cast(pl.Boolean)
    )
    persons = persons.rename({"is_complete": "survey_complete"}).with_columns(
        pl.col("survey_complete").cast(pl.Boolean)
    )

    return {
        "households": households,
        "persons": persons,
        "household_days": household_days,
        "person_days": person_days,
        "unlinked_trips": unlinked_trips,
        "habitual_locations": reported_habitual_locations(
            households, persons, extra=second_homes(persons)
        ),
    }
