"""Trip Linker Module."""
import logging

import polars as pl

from .models import LinkedTripModel, TripModel
from .utils import add_time_columns, expr_haversine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Trip Linker Functions --------------------------------------------------------

def link_trip_ids(
    trips: pl.DataFrame,
    change_mode_code: int,
    max_dwell_time: float = 120,
    dwell_buffer_distance: float = 100,
) -> pl.DataFrame:
    """Link trips based on purpose and mode in time sequence.

    Logic:
    For each person's day of trips:
     - Sort trips by departure time
     - If previous trip's destination purpose is 'change_mode',
       continue the linked trip; else start a new linked trip.
     - Assign linked trip IDs accordingly.
     Linked trip IDs are made globally unique by combining day_id and
     local linked trip index.

    Args:
        trips: DataFrame containing trip data
        change_mode_code: Purpose code indicating a mode change
        max_dwell_time: Maximum time gap between trips to link them (minutes)
        dwell_buffer_distance: Maximum distance between trips to link (miles)

    Returns:
        DataFrame with linked_trip_id column added

    """
    logger.info("Linking trip IDs...")

    # Step 1: Sort trips by day and departure time and arrive time.
    linked_trips = trips.sort(["day_id", "depart_time", "arrive_time"])

    # Step 2: Get previous trip purpose category within the same person
    linked_trips = linked_trips.with_columns([
        pl.col("d_purpose_category")
            .shift(fill_value=None)
            .over("person_id")
            .alias("prev_purpose"),
        pl.col("d_lon")
            .shift(fill_value=None)
            .over("person_id")
            .alias("prev_d_lon"),
        pl.col("d_lat")
            .shift(fill_value=None)
            .over("person_id")
            .alias("prev_d_lat"),
        pl.col("arrive_time")
            .shift(fill_value=None)
            .over("person_id")
            .alias("prev_arrive_time"),
    ])

    # Step 3: Is new linked trips when:
    #  - prev_purpose not change_mode_code
    #  - prev_purpose is missing
    #  - distance between prev_d_coord and o_coord > threshold
    #  - time between prev_arrive_time and depart_time > threshold
    linked_trips = linked_trips.with_columns([
        (
            (pl.col("prev_purpose") != change_mode_code) |
            pl.col("prev_purpose").is_null() |
            (
                expr_haversine(
                    pl.col("prev_d_lat"),
                    pl.col("prev_d_lon"),
                    pl.col("o_lat"),
                    pl.col("o_lon"),
                ) > dwell_buffer_distance
            ) |
            (
                (
                    (pl.col("depart_time") - pl.col("prev_arrive_time"))
                    .dt.total_minutes()
                ) > max_dwell_time
            )
        )
        .cast(pl.Int32)
        .alias("new_trip_flag"),
    ])

    # Step 4: Assign linked trip IDs using cumulative sum
    linked_trips = linked_trips.with_columns([
        pl.col("new_trip_flag")
        .cum_sum()
        .over("person_id")
        .alias("linked_trip_id"),
    ])

    # Step 5: Make linked_trip_id globally unique across days
    linked_trips = linked_trips.with_columns([
        (
            pl.col("day_id").cast(pl.Utf8) +
            pl.col("linked_trip_id").cast(pl.Utf8).str.pad_start(2, "0")
        )
        .cast(pl.Int64)
        .alias("linked_trip_id"),
    ])

    # Step 6: Clean up temporary columns
    return linked_trips.drop(["prev_purpose", "new_trip_flag"])



def aggregate_linked_trips(
    trips: pl.DataFrame,
    transit_mode_codes: list[int],
) -> pl.DataFrame:
    """Aggregate linked trips into single records, summarizing key info.

    Logic:
    For each linked trip:
     - keep the first trips depart_* and o_* fields
     - keep the last trips arrive_* and d_* fields
     - Mode is based on hierarchy. Simple case:
    If:
        transit is involved in any trip segment, use transit mode.
    Else:
        Use mode of longest duration trip segment.

    Args:
        trips: DataFrame with linked_trip_id column
        transit_mode_codes: List of mode codes that count as transit

    Returns:
        Aggregated DataFrame with one row per linked trip

    """
    logger.info("Aggregating linked trips...")

    # First, find the mode type from the longest duration trip segment
    mode_selection = (
        trips
        # Calculate trip durations
        .with_columns([
            (pl.col("arrive_time") - pl.col("depart_time"))
            .alias("trip_duration"),
        ])
        # sort so longest trip per linked_trip_id is first
        .sort(["linked_trip_id", "trip_duration"], descending=[False, True])
        .group_by("linked_trip_id")
        .agg([
        # Transit mode if present
        pl.col("mode_type")
            .filter(pl.col("mode_type").is_in(transit_mode_codes))
            .first()
            .alias("mode_transit"),
        # Longest non-transit mode (trips already sorted by duration desc)
        pl.col("mode_type")
            .filter(~pl.col("mode_type").is_in(transit_mode_codes))
            .first()
            .alias("mode_non_transit"),
        ])
        .with_columns([
        pl.when(pl.col("mode_transit").is_not_null())
        .then(pl.col("mode_transit"))
        .otherwise(pl.col("mode_non_transit"))
        .alias("mode_type"),
        ])
        .select(["linked_trip_id", "mode_type"])
    )

    # Now aggregate with proper time ordering
    linked_trips = (
        trips
        # Sort by departure time
        .sort(["linked_trip_id", "depart_time", "arrive_time"])
        .group_by(
        ["linked_trip_id", "person_id", "hh_id"],
        )
        .agg([
            # Departure information (from first trip segment)
            pl.first("depart_date"),
            pl.first("depart_hour"),
            pl.first("depart_minute"),
            pl.first("depart_seconds"),
            pl.first("depart_time"),
            pl.first("o_purpose_category"),
            pl.first("o_lat"),
            pl.first("o_lon"),

            # Arrival information (from last trip segment)
            pl.last("arrive_date"),
            pl.last("arrive_hour"),
            pl.last("arrive_minute"),
            pl.last("arrive_seconds"),
            pl.last("arrive_time"),
            pl.last("d_purpose_category"),
            pl.last("d_lat"),
            pl.last("d_lon"),

            # Trip distance (sum of segment distances)
            pl.col("distance_miles").sum(),

            # Travel duration (sum of segment durations)
            pl.col("duration_minutes").sum().alias("travel_duration_minutes"),

            # Total trip duration
            (pl.col("arrive_time").max() - pl.col("depart_time").min())
                .dt.total_minutes()
                .alias("duration_minutes"),

            # Dwell duration at change_mode locations:
            # duration_minutes - travel_duration_minutes
            (
                (pl.col("arrive_time").max() - pl.col("depart_time").min())
                .dt.total_minutes() -
                pl.col("duration_minutes").sum()
            ).alias("dwell_duration_minutes"),

            # Number of segments in linked trip
            pl.len().alias("num_segments"),

            # Linked trip weight (mean of segment weights)
            pl.col("trip_weight").mean().alias("linked_trip_weight"),
        ])
        # Join with mode selection based on longest duration
        .join(mode_selection, on="linked_trip_id", how="left")
    )

    # Join day_id back for reference
    return linked_trips.join(
        trips.select(["linked_trip_id", "day_id"]).unique(),
        on="linked_trip_id",
        how="left",
    )


def link_trips(
    trips: pl.DataFrame,
    change_mode_code: int,
    transit_mode_codes: list[int],
    max_dwell_time: float = 120,
    dwell_buffer_distance: float = 100,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Link trips and aggregate them into complete journey records.

    Args:
        trips: DataFrame containing trip data
        change_mode_code: Purpose code indicating a mode change
        transit_mode_codes: List of mode codes that count as transit
        max_dwell_time: Maximum time gap between trips to link them (minutes)
        dwell_buffer_distance: Maximum distance between trips to link (miles)

    Returns:
        Tuple of (trips with linked_trip_id, aggregated linked trips)

    """
    logger.info("Linking trips...")

    # Validate input
    trips = TripModel.validate(trips)

    # Concatenate time columns if missing
    trips = add_time_columns(trips)

    # Link trip IDs
    trips_with_ids = link_trip_ids(
        trips,
        change_mode_code,
        max_dwell_time,
        dwell_buffer_distance,
    )

    # Aggregate linked trips
    linked_trips = aggregate_linked_trips(
        trips_with_ids,
        transit_mode_codes,
    )

    # Validate output
    linked_trips = LinkedTripModel.validate(linked_trips)

    logger.info("Trip linking completed.")
    return trips_with_ids, linked_trips



# Some debugging...
if __name__ == "__main__":  # pragma: no cover
    import logging
    from pathlib import Path

    import polars as pl

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    # Not really sure where everything should live but all this is pretty sinful
    DATA_DIR = Path("E:\\Box\\Modeling and Surveys\\Surveys\\Travel Diary Survey")  # noqa: E501
    DATA_DIR = DATA_DIR / "BATS_2023\\MTC_RSG_Partner Repository\\5.Deliverables"  # noqa: E501
    DATA_DIR = DATA_DIR / "Task 10 - Weighting and Expansion Data Files"
    DATA_DIR = DATA_DIR / "WeightedDataset_12112024"
    TRIP_PATH = DATA_DIR / "trip.csv"

    CHANGE_MODE_CODE: int = 11  # Purpose code for 'change_mode'
    TRANSIT_MODES: list[str] = [12, 13, 14]


    # Load data
    trips_df = pl.read_csv(TRIP_PATH)

    # Much wow...
    trips_df = trips_df.rename({"arrive_second": "arrive_seconds"})

    # Add time columns if missing
    trips_df = add_time_columns(trips_df)

    # "Correct" trips when depart_time > arrive_time, flip them
    # including the separate hours, minutes, seconds columns
    # Create a swap condition to reuse
    swap_condition = pl.col("depart_time") > pl.col("arrive_time")
    trips_df = trips_df.with_columns(
        pl.when(swap_condition).then(pl.col("arrive_time")).otherwise(pl.col("depart_time")).alias("depart_time"),
        pl.when(swap_condition).then(pl.col("depart_time")).otherwise(pl.col("arrive_time")).alias("arrive_time"),
        pl.when(swap_condition).then(pl.col("arrive_hour")).otherwise(pl.col("depart_hour")).alias("depart_hour"),
        pl.when(swap_condition).then(pl.col("arrive_minute")).otherwise(pl.col("depart_minute")).alias("depart_minute"),
        pl.when(swap_condition).then(pl.col("arrive_seconds")).otherwise(pl.col("depart_seconds")).alias("depart_seconds"),
        pl.when(swap_condition).then(pl.col("depart_hour")).otherwise(pl.col("arrive_hour")).alias("arrive_hour"),
        pl.when(swap_condition).then(pl.col("depart_minute")).otherwise(pl.col("arrive_minute")).alias("arrive_minute"),
        pl.when(swap_condition).then(pl.col("depart_seconds")).otherwise(pl.col("arrive_seconds")).alias("arrive_seconds"),
    )

    # Link trips
    trips_with_ids, linked_trips = link_trips(
        trips_df,
        CHANGE_MODE_CODE,
        TRANSIT_MODES,
    )

