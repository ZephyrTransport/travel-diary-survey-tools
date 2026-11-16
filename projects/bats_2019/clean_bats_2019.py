"""Custom cleaning steps for the Bay Area Travel Study (BATS) 2019 ."""

import logging
from pathlib import Path
from types import NoneType
from typing import get_args

import polars as pl

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import Employment, Ethnicity, Gender, Race, Student, SurveyTech
from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory, TNCType
from data_canon.models.survey import HouseholdModel, PersonDayModel, PersonModel, UnlinkedTripModel
from pipeline.decoration import Tables, step
from pipeline.step_registry import get_all_required_fields
from processing.habitual_locations import reported_habitual_locations
from utils.create_ids import create_household_days
from utils.helpers import METERS_PER_MILE, expr_haversine

logger = logging.getLogger(__name__)


NEW_DAY_TIME = 3  # 3 AM``


def check_fields(df: pl.DataFrame, table_name: str, model: type) -> None:
    """Check if all fields in the model are present in the dataframe.

    Args:
        df: Input DataFrame
        table_name: Canonical table name (e.g. "unlinked_trips")
        model: Pydantic model to check against
    Raises:
        ValueError: If any fields are missing
    """
    all_required = get_all_required_fields(table_name)

    # Keep only non-optional fields that some step actually needs
    required_fields = set()
    for field_name, field_info in model.model_fields.items():
        if NoneType not in get_args(field_info.annotation) and field_name in all_required:
            required_fields.add(field_name)

    # Check for missing
    missing_fields = required_fields - set(df.columns)
    if missing_fields:
        msg = f"Missing fields in data: {missing_fields}.\nAvailable columns: {df.columns}"
        raise ValueError(msg)

    logger.info("All required fields present for %s", model.__name__)


def swap_bad_times(
    unlinked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Swap depart/arrive times if depart_time > arrive_time.

    Args:
        unlinked_trips: Unlinked trips DataFrame
    Returns:
        DataFrame with swapped times where needed
    """
    # Check if depart_time < arrive_time ever
    bad_times = unlinked_trips.filter(pl.col("depart_time") > pl.col("arrive_time")).select(
        ["unlinked_trip_id", "depart_time", "arrive_time"]
    )

    if not bad_times.is_empty():
        logger.warning(
            "Found %d trips with depart_time > arrive_time. "
            "They will be corrected. But it is not an optimal solution....",
            bad_times.height,
        )

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

    # Drop cols not in df
    for pair in swap_cols:
        if pair[0] not in unlinked_trips.columns or pair[1] not in unlinked_trips.columns:
            swap_cols.remove(pair)

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

    # Update duration_minutes column after swapping
    unlinked_trips = unlinked_trips.with_columns(
        (pl.col("arrive_time") - pl.col("depart_time")).dt.total_minutes().alias("duration_minutes")
    )

    return unlinked_trips


def clean_households(households: pl.DataFrame) -> pl.DataFrame:
    """Custom cleaning for households."""
    logger.info("Cleaning 2019 household data")
    households = households.with_columns(
        home_lat=pl.col("reported_home_lat"),
        home_lon=pl.col("reported_home_lon"),
        residence_type=pl.col("res_type"),
        residence_rent_own=pl.col("rent_own"),
    )

    # Replace -9998 with 995
    households = households.with_columns(
        pl.when(pl.col(c) == -9998).then(995).otherwise(pl.col(c)).alias(c)  # noqa: PLR2004
        for c in ["residence_type", "residence_rent_own"]
    )

    # CODEBOOK VALUES:
    # INCOME DETAILED:
    # 1	Under $15,000
    # 2	$15,000-$24,999
    # 3	$25,000-$34,999
    # 4	$35,000-$49,999
    # 5	$50,000-$74,999
    # 6	$75,000-$99,999
    # 7	$100,000-$149,999
    # 8	$150,000-$199,999
    # 9	$200,000-$249,999
    # 10	$250,000 or more
    # 999	Prefer not to answer

    # INCOME FOLLOWUP:
    # 1	Under $25,000
    # 2	$25,000-$49,999
    # 3	$50,000-$74,999
    # 4	$75,000-$99,999
    # 5	$100,000-$249,999
    # 6	$250,000 or more
    # 999	Prefer not to answer

    # RECODE TO MATCH CANONICAL INCOME GROUPS:
    # IncomeBroad enum values:
    # INCOME_UNDER25 = 1, INCOME_25TO50 = 2, INCOME_50TO75 = 3,
    # INCOME_75TO100 = 4, INCOME_100TO200 = 5, INCOME_200_OR_MORE = 6,
    # MISSING = 995, PNTA = 999

    # income_detailed (10 categories) → IncomeBroad
    _DETAILED_TO_BROAD: dict[int, int] = {  # noqa: N806
        1: IncomeBroad.INCOME_UNDER25.value,  # Under $15,000
        2: IncomeBroad.INCOME_UNDER25.value,  # $15,000-$24,999
        3: IncomeBroad.INCOME_25TO50.value,  # $25,000-$34,999
        4: IncomeBroad.INCOME_25TO50.value,  # $35,000-$49,999
        5: IncomeBroad.INCOME_50TO75.value,  # $50,000-$74,999
        6: IncomeBroad.INCOME_75TO100.value,  # $75,000-$99,999
        7: IncomeBroad.INCOME_100TO200.value,  # $100,000-$149,999
        8: IncomeBroad.INCOME_100TO200.value,  # $150,000-$199,999
        9: IncomeBroad.INCOME_200_OR_MORE.value,  # $200,000-$249,999
        10: IncomeBroad.INCOME_200_OR_MORE.value,  # $250,000 or more
        999: IncomeBroad.PNTA.value,
    }

    # income_followup (6 categories) → IncomeBroad
    _FOLLOWUP_TO_BROAD: dict[int, int] = {  # noqa: N806
        1: IncomeBroad.INCOME_UNDER25.value,  # Under $25,000
        2: IncomeBroad.INCOME_25TO50.value,  # $25,000-$49,999
        3: IncomeBroad.INCOME_50TO75.value,  # $50,000-$74,999
        4: IncomeBroad.INCOME_75TO100.value,  # $75,000-$99,999
        5: IncomeBroad.INCOME_100TO200.value,  # $100,000-$249,999 (followup lumps 100-249k)
        6: IncomeBroad.INCOME_200_OR_MORE.value,  # $250,000 or more
        999: IncomeBroad.PNTA.value,
    }

    # Prefer income_detailed; fall back to income_followup; else MISSING
    detailed_expr = pl.col("income_detailed").replace_strict(_DETAILED_TO_BROAD, default=None)
    followup_expr = pl.col("income_followup").replace_strict(_FOLLOWUP_TO_BROAD, default=None)
    households = households.with_columns(
        pl.coalesce(detailed_expr, followup_expr)
        .fill_null(IncomeBroad.MISSING.value)
        .alias("income_bin")
    )

    n_missing = households.filter(pl.col("income_bin") == IncomeBroad.MISSING.value).height
    n_pnta = households.filter(pl.col("income_bin") == IncomeBroad.PNTA.value).height
    logger.info(
        "Income recode: %d MISSING, %d PNTA out of %d households",
        n_missing,
        n_pnta,
        len(households),
    )

    return households


def most_common_mode(trips: pl.DataFrame) -> pl.DataFrame:
    """Each person's most common ``mode_type`` over *trips*, one row per person.

    A tie goes to the lowest mode code. Without that rule the winner was
    whichever tied row came first, which changes from run to run.
    """
    return (
        trips.group_by("person_id", "mode_type")
        .agg(pl.len().alias("mode_count"))
        .group_by("person_id")
        .agg(
            pl.col("mode_type")
            .sort_by(["mode_count", "mode_type"], descending=[True, False])
            .first()
        )
    )


def clean_persons(persons: pl.DataFrame, unlinked_trips: pl.DataFrame) -> pl.DataFrame:
    """Custom cleaning for persons."""
    logger.info("Cleaning 2019 person data")

    replace_cols = [
        "gender",
        "school_type",
        "job_type",
        "education",
    ]

    # Replace -9998 with 995
    persons = persons.with_columns(
        pl.when(pl.col(c) == -9998).then(995).otherwise(pl.col(c)).alias(c)  # noqa: PLR2004
        for c in replace_cols
    )

    # Recode gender
    gender_map = {
        1: Gender.MALE.value,
        2: Gender.FEMALE.value,
        995: Gender.MISSING.value,
    }
    persons = persons.with_columns(
        pl.col("gender").replace_strict(gender_map, default=Gender.MISSING.value).alias("gender")
    )

    # Update "student" code to match canonical Student enum
    student_map = {
        0: Student.NONSTUDENT.value,
        # We only have in-person instruction in 2019.
        1: Student.FULLTIME_INPERSON.value,
        2: Student.PARTTIME_INPERSON.value,
        995: Student.MISSING.value,
    }
    persons = persons.with_columns(
        pl.col("student").replace_strict(student_map, default=pl.col("student")).alias("student")
    )

    # Update employment codes to match canonical Employment enum
    # 1	Employed full-time (paid)
    # 2	Employed part-time (paid)
    # 3	Primarily self-employed
    # 6	Not currently employed
    # 7	Unpaid volunteer or intern
    employment_map = {
        1: Employment.EMPLOYED_FULLTIME.value,
        2: Employment.EMPLOYED_PARTTIME.value,
        3: Employment.EMPLOYED_SELF.value,
        6: Employment.UNEMPLOYED_NOT_LOOKING.value,
        7: Employment.EMPLOYED_UNPAID.value,
        995: Employment.MISSING.value,
    }
    persons = persons.with_columns(
        pl.col("employment").replace_strict(employment_map, default=pl.col("employment"))
    )

    # Get work mode for persons --------------------------------------
    # 2019 bats never had work_mode field! We must infer it from the data :(
    logger.info("Determining primary work mode for persons with fixed work locations")

    # Find persons that don't have a work mode but should
    missing_work_mode = persons.filter(
        pl.col("employment").is_in(
            [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_PARTTIME.value,
                Employment.EMPLOYED_SELF.value,
                Employment.EMPLOYED_UNPAID.value,
            ]
        )
    )

    # Find work trips for these people.
    work_trips = unlinked_trips.filter(
        (
            pl.col("d_purpose_category").is_in(
                [
                    PurposeCategory.WORK.value,
                    PurposeCategory.WORK_RELATED.value,
                ]
            )
        )
        & (pl.col("person_id").is_in(missing_work_mode["person_id"].to_list()))
    ).select("person_id", "mode_type", "d_lat", "d_lon")

    work_mode = most_common_mode(work_trips).rename({"mode_type": "work_mode"})

    # Join back to the original persons table
    persons = persons.join(work_mode, on="person_id", how="left")

    # Report how many of these persons have no trips at all
    n_missing_work_mode = missing_work_mode.height
    n_no_trips = missing_work_mode.filter(
        ~pl.col("person_id").is_in(unlinked_trips["person_id"].to_list())
    ).height
    n_imputed_work_mode = missing_work_mode.filter(
        pl.col("person_id").is_in(work_mode["person_id"].to_list())
    ).height
    n_no_work_trips = n_missing_work_mode - n_no_trips - n_imputed_work_mode

    logger.info(
        "Work mode derivation:\n"
        "%d persons with employment but no work mode\n"
        "%d had work trips allowing imputation of work mode.\n"
        "%d had no trips at all\n"
        "%d had trips but no work trips\n"
        "We were able to infer work mode for %d/%d (%.1f%%) of employed persons.",
        n_missing_work_mode,
        n_imputed_work_mode,
        n_no_trips,
        n_no_work_trips,
        n_imputed_work_mode,
        n_missing_work_mode,
        n_imputed_work_mode / n_missing_work_mode * 100 if n_missing_work_mode > 0 else 0,
    )

    # Derive race from the ethnicity_* boxes of 2019's one race/ethnicity question.
    # A box is 1 ticked, 0 not; 995, 999 and -9998 are no answer, so only 1 counts.
    race_flag_cols = [
        "ethnicity_af_am",
        "ethnicity_aiak",
        "ethnicity_asian",
        "ethnicity_hapi",
        "ethnicity_white",
        "ethnicity_mideast",
        "ethnicity_other",
    ]
    ticked = pl.sum_horizontal([(pl.col(c) == 1).fill_null(value=False) for c in race_flag_cols])
    persons = persons.with_columns(
        pl.when(pl.col("ethnicity_multi") == 1)
        .then(pl.lit(Race.MULTI.value))
        .when(ticked > 1)
        .then(pl.lit(Race.MULTI.value))
        .when(pl.col("ethnicity_af_am") == 1)
        .then(pl.lit(Race.AFAM.value))
        .when(pl.col("ethnicity_aiak") == 1)
        .then(pl.lit(Race.NATIVE.value))
        .when(pl.col("ethnicity_asian") == 1)
        .then(pl.lit(Race.ASIAN.value))
        .when(pl.col("ethnicity_hapi") == 1)
        .then(pl.lit(Race.PACIFIC.value))
        .when(pl.col("ethnicity_white") == 1)
        .then(pl.lit(Race.WHITE.value))
        .when(pl.col("ethnicity_mideast") == 1)
        .then(pl.lit(Race.OTHER.value))
        .when(pl.col("ethnicity_other") == 1)
        .then(pl.lit(Race.OTHER.value))
        .when(pl.col("ethnicity_no_answer") == 1)
        .then(pl.lit(None))
        .otherwise(None)
        .alias("race")
    )

    # Whether the survey could collect this person's travel at all. bats_2023
    # gets this from the vendor; 2019 records it as is_active_participant, and
    # the two agree in structure -- every non-participant has no day rows, and
    # no non-participant has any. The cascade reads it so unsurveyable members
    # neither veto a household-date nor inherit its verdict.
    persons = persons.with_columns((pl.col("is_active_participant") == 1).alias("surveyable"))

    # 2019 collected every diary through a phone app (GPS). A non-participant
    # kept no diary, so no technology collected their travel.
    persons = persons.with_columns(
        pl.when(pl.col("is_active_participant") == 1)
        .then(pl.lit(SurveyTech.GPS.value))
        .otherwise(pl.lit(SurveyTech.MISSING.value))
        .alias("survey_tech")
    )

    # Derive ethnicity from the ethnicity_hisp box; any code but 0 or 1 is no answer.
    # 2019 does not distinguish Hispanic subtypes, so map to OTHER (Hispanic or Latino)
    persons = persons.with_columns(
        pl.when(pl.col("ethnicity_no_answer") == 1)
        .then(pl.lit(None))
        .when(pl.col("ethnicity_hisp") == 1)
        .then(pl.lit(Ethnicity.OTHER.value))
        .when(pl.col("ethnicity_hisp") == 0)
        .then(pl.lit(Ethnicity.NOT_HISPANIC.value))
        .otherwise(None)
        .alias("ethnicity")
    )

    # 2019 did not ask commute frequency. Null, not inferred; p_commute_mode is
    # off for 2019 and raises on an all-null input if turned on.
    persons = persons.with_columns(pl.lit(None, dtype=pl.Int64).alias("commute_freq"))

    return persons


def clean_days(
    unlinked_trips: pl.DataFrame,
    person_days: pl.DataFrame,
    persons: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Clean day_num issues in days and unlinked trips."""
    logger.info("Cleaning 2019 day data")

    # The vendor puts a trip that departs before 3 a.m. on the day before, and
    # gives day_num 0 to one whose day before is outside the diary: no surveyed
    # day holds it, so it is dropped rather than attached to a day it is not on.
    outside = unlinked_trips.filter(pl.col("day_num") == 0)
    if not outside.is_empty():
        logger.info("Dropping %d trips on the vendor's day 0, before the diary", outside.height)
        unlinked_trips = unlinked_trips.filter(pl.col("day_num") != 0)
    person_days = person_days.with_columns(
        # to datetime
        travel_date=pl.col("travel_date").str.to_datetime("%Y-%m-%d").dt.date(),
        # day of week from travel_date
        travel_dow=pl.col("travel_date_dow"),
        # Create person_day_id as (person_id * 100 + day_num)
        person_day_id=(pl.col("person_id") * 100 + pl.col("day_num")),
        # 2019 did not ask where the day began or ended.
        begin_day=pl.lit(None, dtype=pl.Int64),
        end_day=pl.lit(None, dtype=pl.Int64),
    )

    # Add day entries for surveyable persons without any days recorded.
    #
    # Only surveyable ones. A person who never agreed to participate was never
    # going to report travel, and the vendor gives them no day rows at all --
    # 4,688 of them here, none with a real day between them. Handing each a
    # synthesised all-null day would make them veto the household-day
    # completeness reduction, which is an ALL over members, so a household with
    # one roommate could never have a complete date. That alone put 2,821 of
    # 4,540 households out of the weighting seed.
    #
    # The 97 active participants who still reported no day are a different
    # thing: that is missing data from someone who was meant to report, and it
    # should count against the household.
    persons_without_days = persons.filter(
        ~pl.col("person_id").is_in(person_days["person_id"].unique().implode())
        & (pl.col("is_active_participant") == 1)
    )

    # Get travel_dow from other household members' days
    days_for_dow = (
        person_days.select(["hh_id", "travel_dow", "travel_date", "day_num"])
        .filter(pl.col("hh_id").is_in(persons_without_days["hh_id"].unique().implode()))
        # Keep order: these rows become the filler days, so their order is the days' order.
        .unique(maintain_order=True)
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
    person_days = pl.concat([person_days, dummy_days], how="diagonal")

    # Fix day_num --------------------------------------------------------
    # day_num is erroneous for miscelleneous reasons
    # E.g., seems like there were inconsistent rules for assigning day_num for trips after midnight,
    # synthetic trips, and split/merged trips.
    # So we will create a clean new day_num based on depart_time before/after 3am

    # First we "shift" any trips that start between 12am and 3am to the previous day
    # by subtracting 3 hours from travel_date
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(
            (pl.col("depart_time").dt.hour() >= 0)
            & (pl.col("depart_time").dt.hour() < NEW_DAY_TIME)
        )
        .then((pl.col("depart_time") - pl.duration(hours=NEW_DAY_TIME)).dt.date())
        .otherwise(pl.col("depart_time").dt.date())
        .alias("travel_date")
    )

    # Recycle the day_num basis for travel dates - day_num mapping
    unique_days = (
        person_days.filter(pl.col("travel_date").is_not_null())
        .select(["hh_id", "day_num", "travel_date"])
        .unique()
    )

    # Lets see if we have any missing travel dates unique days
    unique_days = unique_days.join(
        unlinked_trips.select(["hh_id", "travel_date"]).unique(),
        on=["hh_id", "travel_date"],
        how="right",
    )

    n_missing_before = unique_days.filter(pl.col("day_num").is_null()).height

    # Sort by hh_id and travel_date and create a new day_num sequence
    unique_days = (
        unique_days.sort(["hh_id", "travel_date"])
        .with_columns(
            pl.col("day_num")
            .fill_null(pl.int_range(1, pl.count() + 1).over("hh_id"))
            .alias("day_num")
        )
        .select(["hh_id", "travel_date", "day_num"])
    )

    n_missing_after = unique_days.filter(pl.col("day_num").is_null()).height

    logger.info(
        "Fixed %d missing day_num entries after recalculating from depart_time.",
        n_missing_before - n_missing_after,
    )

    # Person-days keep the vendor's day_num: one to one with each household's
    # dates, and equal to the renumbering wherever that numbers a date. The
    # renumbering only reaches dates with trips, so it would leave no-travel days
    # without one.

    # Join to unlinked_trips to get the new day_num based on shifted travel_date
    unlinked_trips = unlinked_trips.join(
        unique_days,
        on=["hh_id", "travel_date"],
        how="left",
    )

    # Confirm that all the travel dates align with the day info
    unique_days_from_trips = unlinked_trips.select(
        ["hh_id", "day_num_right", "travel_date"]
    ).unique()

    mismatched_days = (
        unique_days.join(unique_days_from_trips, on=["hh_id", "travel_date"]).filter(
            pl.col("day_num") != pl.col("day_num_right")
        )
    ).sort("hh_id")

    if not mismatched_days.is_empty():
        msg = (
            f"Found {mismatched_days.height} mismatched day_num entries "
            "after recalculating from depart_time. Please investigate."
        )
        raise ValueError(msg)

    # Check for null day_num_right
    null_days = unlinked_trips.filter(pl.col("day_num_right").is_null())
    if not null_days.is_empty():
        msg = (
            f"Found {null_days.height} unlinked trips with null person_day_id after cleaning. "
            "Please investigate."
        )
        raise ValueError(msg)

    # Now, finally, make a new person_day_id based on the corrected day_num
    unlinked_trips = unlinked_trips.with_columns(
        person_day_id=(pl.col("person_id") * 100 + pl.col("day_num_right")).alias("person_day_id")
    ).rename(
        {
            "day_num": "day_num_old",
            "day_num_right": "day_num",
        }
    )

    # Ensure no orphaned day_ids from trips
    orphans = unlinked_trips.filter(
        ~pl.col("person_day_id").is_in(person_days["person_day_id"].unique().implode())
    ).select("day_num")

    if not orphans.is_empty():
        msg = (
            f"Found {orphans.height} unlinked trips with day_ids not in days table. "
            "Please investigate."
        )
        raise ValueError(msg)

    return unlinked_trips, person_days


def clean_trips(unlinked_trips: pl.DataFrame) -> pl.DataFrame:
    """Custom cleaning for unlinked trips."""
    logger.info("Cleaning 2019 trip data")

    # Drop trips with missing lat/lon
    unlinked_trips = unlinked_trips.filter(
        pl.col("o_lat").is_not_null()
        & pl.col("o_lon").is_not_null()
        & pl.col("d_lat").is_not_null()
        & pl.col("d_lon").is_not_null()
    )

    # Rename columns to match expected names
    unlinked_trips = unlinked_trips.rename(
        {
            "trip_id": "unlinked_trip_id",
            "distance": "distance_miles",
        }
    )

    # Time fields to datetime
    dt_format = "%Y-%m-%dT%H:%M:%SZ"
    unlinked_trips = unlinked_trips.with_columns(
        depart_time=pl.col("depart_time").str.to_datetime(dt_format),
        arrive_time=pl.col("arrive_time").str.to_datetime(dt_format),
    )

    # Update missing time component columns
    unlinked_trips = unlinked_trips.with_columns(
        # Add person_day_id to unlinked trips for joining later
        # person_day_id=(pl.col("person_id") * 100 + pl.col("day_num_12am")),
        travel_dow=pl.col("depart_time").dt.weekday().cast(pl.Int8),
        travel_date=pl.col("depart_time").dt.date(),
    )

    purpose_cols = [
        "o_purpose",
        "d_purpose",
        "o_purpose_category",
        "d_purpose_category",
    ]
    mode_cols = [
        "mode_1",
        "mode_2",
        "mode_3",
        "mode_4",
        "mode_type",
        "mode_type_imputed",
    ]

    other_cols = [
        "driver",
    ]

    # Replace -9998 with 995 for mode and purpose columns
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col(c) == -9998).then(995).otherwise(pl.col(c)).alias(c)  # noqa: PLR2004
        for c in purpose_cols + mode_cols + other_cols
    )

    # Replace 997 with OTHER purpose
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col(c) == 997).then(Purpose.OTHER.value).otherwise(pl.col(c)).alias(c)  # noqa: PLR2004
        for c in purpose_cols
    )

    # Re-map mode_type
    # From M:\Data\HomeInterview\Bay Area Travel Study 2018-2019\Data\
    #   Final Version with Imputations\
    #   Final Updated Dataset as of 10-18-2021\
    #   Consolidated_SB1_TNC_Study_Codebook_March2021.xlsx
    #  1    Walk
    #  2    Bike
    #  3    Car
    #  4    Taxi
    #  5    Transit
    #  6    Schoolbus
    #  7    Other
    #  8    Shuttle/vanpool
    #  9    TNC
    #  10   Carshare
    #  11   Bikeshare
    #  12   Scooter share
    #  13   Long-distance passenger mode
    mode_type_map = {
        1: ModeType.WALK.value,
        2: ModeType.BIKE.value,
        3: ModeType.CAR.value,
        4: ModeType.TAXI.value,
        5: ModeType.TRANSIT.value,
        6: ModeType.SCHOOL_BUS.value,
        7: ModeType.OTHER.value,
        8: ModeType.SHUTTLE.value,
        9: ModeType.TNC.value,
        10: ModeType.CARSHARE.value,
        11: ModeType.BIKESHARE.value,
        12: ModeType.SCOOTERSHARE.value,
        13: ModeType.LONG_DISTANCE.value,
        -9998: ModeType.MISSING.value,
    }
    unlinked_trips = unlinked_trips.with_columns(
        mode_type_original=pl.col("mode_type"),
        mode_type=pl.col("mode_type_imputed").replace_strict(
            mode_type_map, default=pl.col("mode_type_imputed")
        ),
    )

    # 2019 records that a trip was a TNC but never asked which service, so the
    # column 2023 carries has no counterpart here. Stating that as UNKNOWN on the
    # TNC trips -- rather than leaving the column out, or nulling it everywhere --
    # keeps null meaning what consumers already read it as: no TNC segment. A
    # missing column instead fails validation, since the field is nullable but
    # required to be present.
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("mode_type") == ModeType.TNC.value)
        .then(pl.lit(TNCType.UNKNOWN.value))
        .otherwise(None)
        .cast(pl.Int64)
        .alias("tnc_type")
    )

    # Num_travelers: replace <0 with None
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("num_travelers") < 0)
        .then(1)  # Default to 1 traveler if unknown
        .otherwise(pl.col("num_travelers"))
        .alias("num_travelers")
    )

    # Re-map purpose_category
    # 1	Home
    # 2	Work
    # 3	Work-related
    # 4	School
    # 5	Escort
    # 6	Shop
    # 7	Meal
    # 8	Social/recreation
    # 9	Errand/other
    # 10	Change mode
    # 11	Spent the night at non-home location
    # 12	Other/Missing
    # 14	School-related
    purpose_map = {
        1: PurposeCategory.HOME.value,
        2: PurposeCategory.WORK.value,
        3: PurposeCategory.WORK_RELATED.value,
        4: PurposeCategory.SCHOOL.value,
        5: PurposeCategory.ESCORT.value,
        6: PurposeCategory.SHOP.value,
        7: PurposeCategory.MEAL.value,
        8: PurposeCategory.SOCIALREC.value,
        9: PurposeCategory.ERRAND.value,
        10: PurposeCategory.CHANGE_MODE.value,
        11: PurposeCategory.OVERNIGHT.value,
        12: PurposeCategory.OTHER.value,
        14: PurposeCategory.SCHOOL_RELATED.value,
        995: PurposeCategory.MISSING.value,
    }

    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.col(col).replace_strict(purpose_map).alias(col)
            for col in ["o_purpose_category", "d_purpose_category"]
        ]
    )

    # Fix any bad times where depart_time > arrive_time
    unlinked_trips = swap_bad_times(unlinked_trips)

    # Conver distance to meters
    unlinked_trips = unlinked_trips.with_columns(
        (pl.col("distance_miles") * METERS_PER_MILE).alias("distance_meters")
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

    # Drop the split time fields (date, hours, minutes, seconds) to avoid confusion
    for col in [
        "depart_date",
        "depart_hour",
        "depart_minute",
        "depart_seconds",
        "arrive_date",
        "arrive_hour",
        "arrive_minute",
        "arrive_seconds",
    ]:
        if col in unlinked_trips.columns:
            unlinked_trips = unlinked_trips.drop(col)

    return unlinked_trips


def flag_measured_completeness(
    person_days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Set the two measured leaves: was each trip surveyed, was each day completed.

    ``survey_complete_day`` is what the vendor recorded, so this is the one
    completeness fact the project owns. Everything above it -- person,
    household-day, household, and every trip and tour hanging off a day -- is
    derived upward by the ``cascade_usability`` step, which is also where the
    usability profiles are stamped.

    This used to cascade here as well, and the two did not agree: the docstring
    described a household as complete when it had at least one date on which
    *every* member reported, while the code marked it complete if *any* member
    ever did. Deriving it in one place removes the chance of that drifting again.

    Both are facts the vendor recorded, so they are the project's to set. The
    trip leaf matters as much as the day one: tours are built from trips, and
    ``cascade_usability`` broadcasts a day's verdict onto the records that sit
    on it rather than inventing one, so a trip table with no ``survey_complete`` leaves
    tours with none either and the usability pass has nothing to stand on.

    Args:
        person_days: Person-days carrying ``survey_complete_day``.
        unlinked_trips: Trips carrying ``survey_complete_trip``.

    Returns:
        Tuple of (days, unlinked_trips), each carrying ``survey_complete``, plus the
        per-day trip counts kept for debugging.
    """
    # 995 is the vendor's NA, on trips it added without a trip survey: no verdict of
    # their own (null), so the cascade gives them their day's.
    trip_flag = pl.col("survey_complete_trip")
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(trip_flag == 995)  # noqa: PLR2004
        .then(None)
        .otherwise(trip_flag == 1)
        .alias("survey_complete")
    )

    # Add num_complete_trips column to help with debugging and weighting later
    person_days = person_days.join(
        unlinked_trips.select(["person_day_id", "survey_complete_trip"])
        .group_by("person_day_id")
        .agg(
            pl.sum("survey_complete_trip").alias("num_complete_trips"),
            pl.count("survey_complete_trip").alias("total_trips"),
        ),
        on="person_day_id",
        how="left",
    ).with_columns(
        pl.col("num_complete_trips").fill_null(0),
        pl.col("total_trips").fill_null(0),
        # The vendor's own verdict is taken as given. It already counts a
        # genuine no-travel day as complete, which is the distinction 2023 has
        # to reconstruct from trip counts and stated reasons, so it answers the
        # right question rather than merely a similar one.
        #
        # Null is not missing information here, it is the absence of a day: the
        # 33,495 rows with a null flag are the ones synthesised for persons who
        # had no day record at all, and every vendor field on them is null with
        # zero trips behind it. They are incomplete because nothing was
        # observed, which `is_null()` says and `== 1` only implied.
        pl.when(pl.col("survey_complete_day").is_null())
        .then(pl.lit(value=False))
        .otherwise(pl.col("survey_complete_day") == 1)
        .alias("survey_complete"),
    )
    return person_days, unlinked_trips


def placeholder_survey_complete(
    households: pl.DataFrame, persons: pl.DataFrame, person_days: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Give households and persons a placeholder ``survey_complete``.

    The 2019 vendor files carry completeness per day only, and the field check
    needs the column on households and persons too. cascade_usability replaces
    both with its own rollup of the day verdicts, so this only has to exist: a
    household or person with at least one complete day.
    """
    complete = person_days.filter(pl.col("survey_complete"))
    return (
        households.with_columns(
            pl.col("hh_id").is_in(complete["hh_id"].unique().implode()).alias("survey_complete")
        ),
        persons.with_columns(
            pl.col("person_id")
            .is_in(complete["person_id"].unique().implode())
            .alias("survey_complete")
        ),
    )


def exclude_as_vendor_weekday_weights(
    households: pl.DataFrame, person_days: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Give an ``exclusion_reason`` to what the vendor's weekday weights left out.

    The vendor weighted only days with a ``day_status`` of 9 or 10, and no
    household without a ``subregion``. Excluding the same keeps these weights
    backwards compatible; nothing else about these records is known to be wrong,
    so a future weighting could keep them.
    """
    households = households.with_columns(
        pl.when(pl.col("subregion").is_null())
        .then(pl.lit("no subregion"))
        .otherwise(None)
        .alias("exclusion_reason")
    )
    person_days = person_days.with_columns(
        pl.when(pl.col("day_status") < 9)  # noqa: PLR2004
        .then(pl.lit("day_status below 9"))
        .otherwise(None)
        .alias("exclusion_reason")
    )
    logger.info(
        "Excluded %d of %d households and %d of %d days as the vendor's weekday weights",
        households["exclusion_reason"].is_not_null().sum(),
        households.height,
        person_days["exclusion_reason"].is_not_null().sum(),
        person_days.height,
    )
    return households, person_days


@step(
    requires={"households": set(), "persons": set(), "person_days": set(), "unlinked_trips": set()}
)
def clean_2019_bats(
    tables: Tables,
    weights_dir: str,
    weight_col_suffix: str | None = None,
) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline."""
    households = tables["households"]
    persons = tables["persons"]
    person_days = tables["person_days"]
    unlinked_trips = tables["unlinked_trips"]

    # CLEANUP UNLINKED TRIPS =================================
    unlinked_trips = clean_trips(unlinked_trips)

    # CLEAN DAYS ==================================
    unlinked_trips, person_days = clean_days(unlinked_trips, person_days, persons)

    # CLEAN PERSONS ==================================
    persons = clean_persons(persons, unlinked_trips)

    # CLEAN HOUSEHOLDS ==================================
    households = clean_households(households)

    # FLAG COMPLETE DAYS ==================================
    # Only the measured leaf; cascade_usability derives the rest.
    person_days, unlinked_trips = flag_measured_completeness(person_days, unlinked_trips)
    households, persons = placeholder_survey_complete(households, persons, person_days)

    # EXCLUSIONS ==================================
    # Follows the vendor's weekday weighting, for backwards compatibility; comment
    # out to keep these records for future weighting.
    households, person_days = exclude_as_vendor_weekday_weights(households, person_days)

    # HOUSEHOLD DAYS ==================================
    person_days, household_days = create_household_days(person_days)

    results = {
        "households": households,
        "persons": persons,
        "household_days": household_days,
        "person_days": person_days,
        "unlinked_trips": unlinked_trips,
    }

    # PREPARE WEIGHT COLUMNS ==================================
    # Weights are assumed not to be in the data and are appended/calculated after core processing
    # Thus, extract the weights from each table and save them as separate CSVs in the input dir

    # Check if weights_dir exists if weight_suffix provided, if not create it
    Path(weights_dir).mkdir(parents=True, exist_ok=True)

    # Canonical weight naming
    weight_names = {
        "households": ("hh_id", "hh_weight"),
        "persons": ("person_id", "person_weight"),
        "person_days": ("person_day_id", "person_day_weight"),
        "unlinked_trips": ("unlinked_trip_id", "unlinked_trip_weight"),
    }
    models = {
        "households": HouseholdModel,
        "persons": PersonModel,
        "person_days": PersonDayModel,
        "unlinked_trips": UnlinkedTripModel,
    }
    for df_name, (id_col, canon_weight_col) in weight_names.items():
        # Get the dataframe by name
        df = results[df_name]

        # If weight suffix provided, find matching weight column name
        if weight_col_suffix:
            weight_col = next(col for col in df.columns if col.endswith(weight_col_suffix))
        else:
            weight_col = canon_weight_col

        # Save the weights as separate CSVs in the input directory
        weights_df = df.rename({weight_col: canon_weight_col}).select([id_col, canon_weight_col])

        # If NULLs are found, WARN, but fill them with 0 to avoid issues with missing weights later
        n_null_weights = weights_df.filter(pl.col(canon_weight_col).is_null()).height
        if n_null_weights > 0:
            logger.warning(
                "Found %d null weights in %s.\n"
                "They will be filled with 0 for now, but please investigate.",
                n_null_weights,
                df_name,
            )

        weights_df = weights_df.fill_null(0)

        weights_df.write_csv(f"{weights_dir}/{df_name}_weights.csv")
        results[df_name] = df.drop(weight_col)

        weights_df.filter(pl.col(canon_weight_col).is_null())

        # Final check
        check_fields(results[df_name], df_name, models[df_name])

    # 2019 reports home, work and school only; there are no further locations.
    results["habitual_locations"] = reported_habitual_locations(
        results["households"], results["persons"]
    )
    return results
