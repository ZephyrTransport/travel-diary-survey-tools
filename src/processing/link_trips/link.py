"""Trip Linker Module."""

import logging

import polars as pl

from data_canon.codebook.trips import AccessEgressMode, Driver, ModeType
from pipeline.decoration import step
from utils.create_ids import create_linked_trip_id
from utils.helpers import (
    expr_haversine,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ModeType to AccessEgressMode mapping for transit access/egress
MODE_TYPE_TO_ACCESS_EGRESS = {
    ModeType.WALK.value: AccessEgressMode.WALK.value,
    ModeType.BIKE.value: AccessEgressMode.BICYCLE.value,
    ModeType.BIKESHARE.value: AccessEgressMode.BICYCLE.value,
    ModeType.SCOOTERSHARE.value: AccessEgressMode.MICROMOBILITY.value,
    ModeType.TAXI.value: AccessEgressMode.TNC.value,
    ModeType.TNC.value: AccessEgressMode.TNC.value,
    ModeType.CAR.value: AccessEgressMode.CAR_HOUSEHOLD.value,
    ModeType.CARSHARE.value: AccessEgressMode.CAR_OTHER.value,
    ModeType.SCHOOL_BUS.value: AccessEgressMode.TRANSFER_BUS.value,
    ModeType.SHUTTLE.value: AccessEgressMode.TRANSFER_BUS.value,
    ModeType.FERRY.value: AccessEgressMode.TRANSFER_OTHER.value,
    ModeType.TRANSIT.value: AccessEgressMode.TRANSFER_OTHER.value,
    ModeType.LONG_DISTANCE.value: AccessEgressMode.TRANSFER_OTHER.value,
    ModeType.OTHER.value: AccessEgressMode.OTHER.value,
    ModeType.MISSING.value: AccessEgressMode.MISSING.value,
}


# Trip Linker Functions --------------------------------------------------------
@step()
def link_trips(
    unlinked_trips: pl.DataFrame,
    change_mode_code: int,
    transit_mode_codes: list[int],
    max_dwell_time: float = 120,
    dwell_buffer_distance: float = 100,
) -> dict[str, pl.DataFrame]:
    """Link trips and aggregate them into complete journey records.

    Args:
        unlinked_trips: DataFrame containing trip data
        change_mode_code: Purpose code indicating a mode change
        transit_mode_codes: List of mode codes that count as transit
        max_dwell_time: Maximum time gap between trips to link them (minutes)
        dwell_buffer_distance: Maximum distance between trips to link (meters)

    Returns:
        Tuple of (trips with linked_trip_id, aggregated linked trips)

    """
    logger.info("Linking trips...")

    # Link trip IDs
    unlinked_trips_with_ids = link_trip_ids(
        unlinked_trips,
        change_mode_code,
        max_dwell_time,
        dwell_buffer_distance,
    )

    # Aggregate linked trips
    linked_trips = aggregate_linked_trips(
        unlinked_trips_with_ids,
        transit_mode_codes,
    )

    logger.info("Trip linking completed.")
    return {
        "unlinked_trips": unlinked_trips_with_ids,
        "linked_trips": linked_trips,
    }


def link_trip_ids(
    unlinked_trips: pl.DataFrame,
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
        unlinked_trips: DataFrame containing trip data
        change_mode_code: Purpose code indicating a mode change
        max_dwell_time: Maximum time gap between trips to link them (minutes)
        dwell_buffer_distance: Maximum distance between trips to link (meters)

    Returns:
        DataFrame with linked_trip_id column added

    """
    logger.info("Linking trip IDs...")
    # If empty dataframe just extend the schema and return
    if unlinked_trips.is_empty():
        logger.info("No trips to link; returning empty DataFrame.")
        return unlinked_trips.with_columns(pl.lit(None).cast(pl.Utf8).alias("linked_trip_id"))

    # Step 1: Sort trips by person, day, and departure time
    unlinked_trips = unlinked_trips.sort(["person_id", "day_id", "depart_time", "arrive_time"])

    # Step 2: Get previous trip purpose category within the same person
    unlinked_trips = unlinked_trips.with_columns(
        pl.col("d_purpose_category", "d_lon", "d_lat", "arrive_time")
        .shift(fill_value=None)
        .over("person_id")
        .name.map(lambda c: f"prev_{c}")
    )

    # Step 3: Is new linked trips when:
    #  - prev_purpose not change_mode_code
    #  - prev_purpose is missing
    #  - distance between prev_d_coord and o_coord > threshold
    #  - time between prev_arrive_time and depart_time > threshold
    unlinked_trips = unlinked_trips.with_columns(
        [
            (
                (pl.col("prev_d_purpose_category") != change_mode_code)
                | pl.col("prev_d_purpose_category").is_null()
                | (
                    expr_haversine(
                        pl.col("prev_d_lat"),
                        pl.col("prev_d_lon"),
                        pl.col("o_lat"),
                        pl.col("o_lon"),
                    )
                    > dwell_buffer_distance
                )
                | (
                    ((pl.col("depart_time") - pl.col("prev_arrive_time")).dt.total_minutes())
                    > max_dwell_time
                )
            )
            .cast(pl.Int32)
            .alias("new_trip_flag"),
        ]
    )

    # Step 4: Assign linked trip IDs using cumulative sum
    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.col("new_trip_flag").cum_sum().over("person_id").alias("linked_trip_num"),
        ]
    )

    # Step 5: Create globally unique linked_trip_id
    unlinked_trips_with_id = create_linked_trip_id(unlinked_trips)

    # Step 6: Clean up temporary columns
    return unlinked_trips_with_id.drop(
        [
            "prev_d_purpose_category",
            "prev_d_lon",
            "prev_d_lat",
            "prev_arrive_time",
            "new_trip_flag",
        ]
    )


# NOTE: Consider removing from this stage and leave to downstream "formatting"
def aggregate_linked_trips(
    unlinked_trips: pl.DataFrame,
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
        unlinked_trips: DataFrame with linked_trip_id column
        transit_mode_codes: List of mode codes that count as transit

    Returns:
        Aggregated DataFrame with one row per linked trip

    """
    logger.info("Aggregating linked trips...")

    # First, find the mode type from the longest duration trip segment
    mode_selection = (
        unlinked_trips
        # Calculate trip durations
        .with_columns(
            [
                (pl.col("arrive_time") - pl.col("depart_time")).alias("trip_duration"),
            ]
        )
        # sort so longest trip per linked_trip_id is first
        .sort(["linked_trip_id", "trip_duration"], descending=[False, True])
        .group_by("linked_trip_id")
        .agg(
            [
                # Transit mode if present
                pl.col("mode_type")
                .filter(pl.col("mode_type").is_in(transit_mode_codes))
                .first()
                .alias("mode_transit"),
                # Longest non-transit mode (trips already sorted by duration)
                pl.col("mode_type")
                .filter(~pl.col("mode_type").is_in(transit_mode_codes))
                .first()
                .alias("mode_non_transit"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("mode_transit").is_not_null())
                .then(pl.col("mode_transit"))
                .otherwise(pl.col("mode_non_transit"))
                .alias("mode_type"),
            ]
        )
        .select(["linked_trip_id", "mode_type"])
    )

    # Get access and egress modes for transit trips
    # Access mode = mode_type of first segment before transit
    # Egress mode = mode_type of last segment after transit
    # These will be cast to AccessEgressMode enum values during join
    access_egress = (
        unlinked_trips.sort(["linked_trip_id", "depart_time", "arrive_time"])
        .with_columns(
            [
                pl.col("mode_type").is_in(transit_mode_codes).alias("is_transit"),
            ]
        )
        .group_by("linked_trip_id")
        .agg(
            [
                # Access mode: first non-transit mode_type before any transit
                pl.when(pl.col("is_transit").any())
                .then(pl.col("mode_type").filter(~pl.col("is_transit")).first())
                .otherwise(pl.lit(None))
                .alias("access_mode"),
                # Egress mode: last non-transit mode_type after transit
                pl.when(pl.col("is_transit").any())
                .then(pl.col("mode_type").filter(~pl.col("is_transit")).last())
                .otherwise(pl.lit(None))
                .alias("egress_mode"),
            ]
        )
    )

    # Now aggregate with proper time ordering
    linked_trips = (
        unlinked_trips
        # Sort by departure time
        .sort(["linked_trip_id", "depart_time", "arrive_time"])
        .group_by(
            ["linked_trip_id", "person_id", "hh_id"],
        )
        .agg(
            [
                # Linked trip number (from first trip segment)
                pl.first("linked_trip_num"),
                # Travel dow is from first trip. Caution for overnight trips
                pl.first("travel_dow"),
                # Departure information (from first trip segment)
                # pl.first("depart_date"),
                # pl.first("depart_hour"),
                # pl.first("depart_minute"),
                # pl.first("depart_seconds"),
                pl.first("depart_time"),
                pl.first("o_purpose_category"),
                pl.first("o_lat"),
                pl.first("o_lon"),
                # Arrival information (from last trip segment)
                # pl.last("arrive_date"),
                # pl.last("arrive_hour"),
                # pl.last("arrive_minute"),
                # pl.last("arrive_seconds"),
                pl.last("arrive_time"),
                pl.last("d_purpose_category"),
                pl.last("d_lat"),
                pl.last("d_lon"),
                # Trip distance (sum of segment distances)
                pl.col("distance_meters").sum(),
                # Travel duration (sum of segment durations)
                pl.col("duration_minutes").sum().alias("travel_duration_minutes"),
                # Total trip duration
                (pl.col("arrive_time").max() - pl.col("depart_time").min())
                .dt.total_minutes()
                .alias("duration_minutes"),
                # Dwell duration at change_mode locations:
                # duration_minutes - travel_duration_minutes
                (
                    (pl.col("arrive_time").max() - pl.col("depart_time").min()).dt.total_minutes()
                    - pl.col("duration_minutes").sum()
                ).alias("dwell_duration_minutes"),
                # Number of segments in linked trip
                pl.len().alias("num_segments"),
                # Linked trip weight (mean of segment weights)
                pl.col("trip_weight").mean().alias("linked_trip_weight"),
                # num_travelers (max of segment num_travelers)
                pl.col("num_travelers").max().alias("num_travelers"),
                # Determine driver status across segments
                pl.when(pl.col("driver").n_unique() == 1)
                .then(pl.col("driver").first())
                # If missing entirely
                .when(
                    pl.col("driver").filter(pl.col("driver") != Driver.MISSING.value).n_unique()
                    == 0
                )
                .then(pl.lit(Driver.MISSING.value))  # All missing
                # If mixed driver/passenger, set to BOTH
                .otherwise(pl.lit(Driver.BOTH.value))
                .alias("driver"),
            ]
        )
        # Join with mode selection based on longest duration
        .join(mode_selection, on="linked_trip_id", how="left")
        # Join with access/egress modes
        .join(access_egress, on="linked_trip_id", how="left")
        # Map mode_type values to AccessEgressMode enum values
        .with_columns(
            [
                pl.when(pl.col("access_mode").is_not_null())
                .then(pl.col("access_mode").replace_strict(MODE_TYPE_TO_ACCESS_EGRESS))
                .otherwise(pl.lit(None))
                .alias("access_mode"),
                pl.when(pl.col("egress_mode").is_not_null())
                .then(pl.col("egress_mode").replace_strict(MODE_TYPE_TO_ACCESS_EGRESS))
                .otherwise(pl.lit(None))
                .alias("egress_mode"),
            ]
        )
    )

    # Join day_id back for reference
    return linked_trips.join(
        unlinked_trips.select(["linked_trip_id", "day_id"]).unique(),
        on="linked_trip_id",
        how="left",
    )
