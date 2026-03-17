"""Custom cleaning steps for the Bay Area Travel Study (BATS) 2019 ."""

import logging
from pathlib import Path
from types import NoneType
from typing import get_args

import polars as pl

from data_canon.codebook.persons import JobType
from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory
from data_canon.models.survey import HouseholdModel, PersonDayModel, PersonModel, UnlinkedTripModel
from pipeline.decoration import step
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)


NEW_DAY_TIME = 3  # 3 AM``


def check_fields(df: pl.DataFrame, model: type) -> None:
    """Check if all fields in the model are present in the dataframe.

    Args:
        df: Input DataFrame
        model: Pydantic model to check against
    Raises:
        ValueError: If any fields are missing
    """
    # Get required fields if not optional and has any required steps
    required_fields = set()

    for field_name, field_info in model.__fields__.items():
        schema_extras = field_info.json_schema_extra or {}

        if (
            NoneType not in get_args(field_info.annotation)
            and len(schema_extras.get("required_in_steps", [])) > 0
        ):
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


def replace_with_code(
    df: pl.DataFrame,
    column: str,
    to_replace: dict[int, int],
) -> pl.DataFrame:
    """Replace values in a column based on a mapping dictionary."""
    for old_value, new_value in to_replace.items():
        df = df.with_columns(
            pl.when(pl.col(column) == old_value)
            .then(new_value)
            .otherwise(pl.col(column))
            .alias(column)
        )
    return df


def get_work_mode(
    persons: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Get the primary work mode for persons with fixed work locations."""
    logger.info("Determining primary work mode for persons with fixed work locations")
    # Filter persons with single work location
    single_work_location_persons = (
        persons.filter(pl.col("job_type") == JobType.FIXED.value)
        .select("person_id")
        .unique()
        .to_series()
        .implode()
    )

    # Filter work trips
    work_trips = unlinked_trips.filter(
        (pl.col("d_purpose_category") == PurposeCategory.WORK.value)
        & (pl.col("person_id").is_in(single_work_location_persons))
    ).select("person_id", "mode_type", "d_lat", "d_lon")

    # Find the most common mode for each person
    work_mode = (
        work_trips
        # Start by getting count of each mode used by each person
        .group_by(["person_id", "mode_type"])
        .agg(pl.count().alias("mode_count"))
        # Then select the mode with the highest count for each person
        .sort(["person_id", "mode_count"], descending=[False, True])
        .group_by("person_id")
        .agg(pl.first("mode_type").alias("work_mode"))
    )

    # Join back to persons
    persons = persons.join(work_mode, on="person_id", how="left")

    return persons


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
    households = replace_with_code(households, "residence_type", {-9998: 995})
    households = replace_with_code(households, "residence_rent_own", {-9998: 995})

    return households


def clean_persons(persons: pl.DataFrame, unlinked_trips: pl.DataFrame) -> pl.DataFrame:
    """Custom cleaning for persons."""
    logger.info("Cleaning 2019 person data")

    replace_cols = [
        "gender",
        "school_type",
        "job_type",
    ]

    # Replace -9998 with 995
    for col in replace_cols:
        persons = replace_with_code(persons, col, {-9998: 995})

    # Get work mode for persons
    persons = get_work_mode(persons, unlinked_trips)

    return persons


def clean_days(
    unlinked_trips: pl.DataFrame,
    days: pl.DataFrame,
    persons: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Clean day_num issues in days and unlinked trips."""
    logger.info("Cleaning 2019 day data")
    days = days.with_columns(
        # to datetime
        travel_date=pl.col("travel_date").str.to_datetime("%Y-%m-%d").dt.date(),
        # day of week from travel_date
        travel_dow=pl.col("travel_date_dow"),
        # Create day_id as (person_id * 100 + day_num)
        day_id=(pl.col("person_id") * 100 + pl.col("day_num")),
    )

    # Add day entries for persons without any days recorded
    # Find persons without days
    persons_without_days = persons.filter(
        ~pl.col("person_id").is_in(days["person_id"].unique().implode())
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
    days = pl.concat([days, dummy_days], how="diagonal")

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
        days.filter(pl.col("travel_date").is_not_null())
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

    # Replace the original day_num in the day table to match the new logic
    days = days.rename({"day_num": "day_num_old"}).join(
        unique_days,
        on=["hh_id", "travel_date"],
        how="left",
    )

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
            f"Found {null_days.height} unlinked trips with null day_id after cleaning. "
            "Please investigate."
        )
        raise ValueError(msg)

    # Now, finally, make a new day_id based on the corrected day_num
    unlinked_trips = unlinked_trips.with_columns(
        day_id=(pl.col("person_id") * 100 + pl.col("day_num_right")).alias("day_id")
    ).rename(
        {
            "day_num": "day_num_old",
            "day_num_right": "day_num",
        }
    )

    # Ensure no orphaned day_ids from trips
    orphans = unlinked_trips.filter(
        ~pl.col("day_id").is_in(days["day_id"].unique().implode())
    ).select("day_num")

    if not orphans.is_empty():
        msg = (
            f"Found {orphans.height} unlinked trips with day_ids not in days table. "
            "Please investigate."
        )
        raise ValueError(msg)

    return unlinked_trips, days


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
        # Add day_id to unlinked trips for joining later
        # day_id=(pl.col("person_id") * 100 + pl.col("day_num_12am")),
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
    ]

    other_cols = [
        "driver",
    ]

    # Replace -9998 with 995 for mode and purpose columns
    for col in purpose_cols + mode_cols + other_cols:
        unlinked_trips = replace_with_code(
            unlinked_trips,
            col,
            {-9998: 995},
        )

    # Replace 997 with OTHER purpose
    for col in purpose_cols:
        unlinked_trips = replace_with_code(
            unlinked_trips,
            col,
            {997: Purpose.OTHER.value},
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
    }
    unlinked_trips = unlinked_trips.with_columns(
        pl.col("mode_type")
        .replace_strict(mode_type_map, default=pl.col("mode_type"))
        .alias("mode_type")
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
        (pl.col("distance_miles") * 1609.34).alias("distance_meters")
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


@step()
def clean_2019_bats(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    weights_dir: str,
    weight_col_suffix: str | None = None,
) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline."""
    # CLEANUP UNLINKED TRIPS =================================
    unlinked_trips = clean_trips(unlinked_trips)

    # CLEAN DAYS ==================================
    unlinked_trips, days = clean_days(unlinked_trips, days, persons)

    # CLEAN PERSONS ==================================
    persons = clean_persons(persons, unlinked_trips)

    # CLEAN HOUSEHOLDS ==================================
    households = clean_households(households)

    # PREPARE WEIGHT COLUMNS ==================================
    # Weights are assumed not to be in the data and are appended/calculated after core processing
    # Thus, extract the weights from each table and save them as separate CSVs in the input dir

    # Check if weights_dir exists if weight_suffix provided, if not create it
    Path(weights_dir).mkdir(parents=True, exist_ok=True)

    # Canonical weight naming
    weight_names = {
        "households": ("hh_id", "hh_weight"),
        "persons": ("person_id", "person_weight"),
        "days": ("day_id", "day_weight"),
        "unlinked_trips": ("unlinked_trip_id", "unlinked_trip_weight"),
    }
    models = {
        "households": HouseholdModel,
        "persons": PersonModel,
        "days": PersonDayModel,
        "unlinked_trips": UnlinkedTripModel,
    }
    results = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
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
        check_fields(results[df_name], models[df_name])

    return results
