"""Custom cleaning steps for the  Bay Area Travel Study (BATS) 2023 pipeline."""

import logging

import polars as pl

from data_canon.codebook.households import ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import Ethnicity, Race
from pipeline.decoration import step
from utils.helpers import add_time_columns, expr_haversine

logger = logging.getLogger(__name__)


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
    # Find persons without days
    persons_without_days = persons.filter(
        ~pl.col("person_id").is_in(days["person_id"].unique().implode())
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
        .first()
        .fill_null(995),
        pl.col("residence_type")
        .filter(pl.col("residence_type") != ResidenceType.MISSING.value)
        .mode()
        .first()
        .fill_null(995),
    )
    # Join to households
    households = households.join(hh_attributes, on="hh_id", how="left")

    # Copy income_broad to canonical income
    households = households.with_columns(
        pl.col("income_broad").alias("income_bin"),
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

    return {
        "households": households,
        "persons": persons,
        "unlinked_trips": unlinked_trips,
        "days": days,
    }
