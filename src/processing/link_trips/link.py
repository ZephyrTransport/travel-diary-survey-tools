"""Trip Linker Module."""

import logging

import polars as pl

from data_canon.codebook.trips import AccessEgressMode, Driver, ModeType, PurposeCategory
from pipeline.decoration import step
from utils.create_ids import create_linked_trip_id
from utils.enum_helpers import resolve_enum_labels
from utils.helpers import expr_haversine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
"""ModeType to AccessEgressMode mapping for transit access/egress.

Maps travel mode types to access/egress mode categories used in transit trip analysis.
This mapping is used when aggregating linked trips to classify non-transit segments
as access or egress modes for transit journeys.
"""


# Trip Linker Functions --------------------------------------------------------
@step(
    requires={
        "unlinked_trips": {
            "day_id",
            "o_lon",
            "o_lat",
            "d_lon",
            "d_lat",
            "o_purpose_category",
            "d_purpose_category",
            "mode_type",
            "depart_time",
            "arrive_time",
        },
    },
    produces={
        "unlinked_trips": {"linked_trip_id"},
        "linked_trips": {"linked_trip_id", "day_id", "person_id", "hh_id", "driver"},
    },
)
def link_trips(
    unlinked_trips: pl.DataFrame,
    change_mode_enum: str,
    transit_mode_enums: list[str],
    max_dwell_time: float = 120,
    dwell_buffer_distance: float = 100,
    *,
    split_on_occupancy: bool,
) -> dict[str, pl.DataFrame]:
    """Link unlinked trip segments into complete journey records.

    Detects mode changes and aggregates trip chains by validating spatial and
    temporal continuity across consecutive trips.

    Args:
        unlinked_trips: Individual trip records with person_id, day_id,
            depart_time, arrive_time, o/d locations, o/d purposes, mode_type.
        change_mode_enum: Enum label indicating a mode change purpose.
        transit_mode_enums: List of enum labels that count as transit modes.
        max_dwell_time: Maximum time gap between trips to link them, in
            minutes (default: 120).
        dwell_buffer_distance: Maximum spatial distance between trips to link,
            in meters (default: 100).
        split_on_occupancy: Refuse to link two segments whose reported party
            size differs. A change of occupancy at a stop is evidence that
            somebody was picked up or dropped off, which makes the stop an
            activity rather than a transfer. Required, and deliberately without
            a default: either answer changes what a linked trip *is*, so the
            choice belongs to whoever configured the run rather than to this
            signature.

    Returns:
        Dictionary containing:
            - unlinked_trips: Original trips with added linked_trip_id column
            - linked_trips: Aggregated journey records with combined attributes

    Algorithm:
        # Phase 1: Link Trip IDs

        1. Sort unlinked trips by person, day, and departure time
        2. For each person-day sequence:
            - If previous trip's destination purpose is change_mode_enum,
              continue current linked trip
            - Validate spatial/temporal continuity:
                - Time gap between trips ≤ max_dwell_time minutes
                - Distance between previous destination and current origin
                  ≤ dwell_buffer_distance meters
            - Otherwise, start a new linked trip
        3. Assign globally unique linked_trip_id =
           (day_id * 1000) + sequence_number

        # Phase 2: Aggregate Linked Trips

        1. Group unlinked trips by linked_trip_id
        2. For each linked trip, aggregate:
            - Origin/Destination: First trip's origin, last trip's destination
            - Timing: First depart_time, last arrive_time
            - Distance: Sum of all trip distances
            - Duration: Sum of all trip durations (including dwell time)
            - Purposes: First origin purpose, last destination purpose
            - Mode Logic:
                - If any trip uses transit → mode_type = TRANSIT
                - Otherwise, use mode of longest distance trip
            - Transit Details: Count boarding/alighting, aggregate access/egress modes
            - Driver/Passenger: Aggregate from component trips
        3. Create trip_list array containing all component trip IDs

    Notes:
        - Links trips when travelers make intermediate stops for mode changes or transfers
        - Preserves full trip detail in unlinked_trips while creating journey-level linked_trips
        - Transit detection ensures multi-modal journeys classified correctly
        - Access/egress mode mapping converts trip modes to transit-specific codes
        - Spatial/temporal thresholds prevent false linkages across separate journeys
    """
    logger.info("Linking trips...")

    # Link trip IDs
    unlinked_trips_with_ids = link_trip_ids(
        unlinked_trips,
        change_mode_enum,
        max_dwell_time,
        dwell_buffer_distance,
        split_on_occupancy=split_on_occupancy,
    )

    # Aggregate linked trips
    linked_trips = aggregate_linked_trips(
        unlinked_trips_with_ids,
        transit_mode_enums,
    )

    logger.info("Trip linking completed.")
    return {
        "unlinked_trips": unlinked_trips_with_ids,
        "linked_trips": linked_trips,
    }


def link_trip_ids(
    unlinked_trips: pl.DataFrame,
    change_mode_enum: str,
    max_dwell_time: float = 120,
    dwell_buffer_distance: float = 100,
    *,
    split_on_occupancy: bool,
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
        change_mode_enum: Enum label indicating a mode change
        max_dwell_time: Maximum time gap between trips to link them (minutes)
        dwell_buffer_distance: Maximum distance between trips to link (meters)
        split_on_occupancy: Also break the link where the reported party size
            changes between segments. Required; see ``link_trips``.

    Returns:
        DataFrame with linked_trip_id column added

    Raises:
        ValueError: split_on_occupancy is set but the trips carry no
            num_travelers column to compare.

    """
    logger.info("Linking trip IDs...")
    # If empty dataframe just extend the schema and return
    if unlinked_trips.is_empty():
        logger.info("No trips to link; returning empty DataFrame.")
        return unlinked_trips.with_columns(pl.lit(None).cast(pl.Utf8).alias("linked_trip_id"))

    # If linked_trip_id already exists, throw warning and overwrite
    if "linked_trip_id" in unlinked_trips.columns:
        logger.warning(
            "linked_trip_id column already exists in unlinked_trips; it will be overwritten!"
        )
        unlinked_trips = unlinked_trips.drop("linked_trip_id")

    # Step 1: Sort trips by person, day, and departure time
    unlinked_trips = unlinked_trips.sort(["person_id", "day_id", "depart_time", "arrive_time"])

    if split_on_occupancy and "num_travelers" not in unlinked_trips.columns:
        msg = (
            "split_on_occupancy is enabled but unlinked_trips has no num_travelers "
            "column, so a change of party size cannot be detected."
        )
        raise ValueError(msg)

    # Step 2: Get previous trip purpose category within the same person
    shift_cols = ["d_purpose_category", "d_lon", "d_lat", "arrive_time"]
    if split_on_occupancy:
        shift_cols.append("num_travelers")
    unlinked_trips = unlinked_trips.with_columns(
        pl.col(shift_cols).shift(fill_value=None).over("person_id").name.map(lambda c: f"prev_{c}")
    )

    # Get change mode integer code value from enum label
    change_mode_code = resolve_enum_labels(
        table_name="unlinked_trips", field_name="d_purpose_category", enum_labels=[change_mode_enum]
    )[0]

    # Step 3: Is new linked trips when:
    #  - prev_purpose not change_mode_code
    #  - prev_purpose is missing
    #  - distance between prev_d_coord and o_coord > threshold
    #  - time between prev_arrive_time and depart_time > threshold
    starts_new_trip = (
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

    if split_on_occupancy:
        # Only a reported change breaks the link. An unreported party size on
        # either side is missing data, not evidence that anybody got in or out.
        starts_new_trip = starts_new_trip | (
            pl.col("num_travelers").is_not_null()
            & pl.col("prev_num_travelers").is_not_null()
            & (pl.col("num_travelers") != pl.col("prev_num_travelers"))
        )

    unlinked_trips = unlinked_trips.with_columns(
        starts_new_trip.cast(pl.Int32).alias("new_trip_flag")
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
    temp_cols = [
        "prev_d_purpose_category",
        "prev_d_lon",
        "prev_d_lat",
        "prev_arrive_time",
        "new_trip_flag",
    ]
    if split_on_occupancy:
        temp_cols.append("prev_num_travelers")
    return unlinked_trips_with_id.drop(temp_cols)


def _warn_on_occupancy_change(unlinked_trips: pl.DataFrame) -> None:
    """Report linked trips whose party size changes between segments.

    ``num_travelers`` is rolled up with a max, which silently reports the
    largest party as though it applied to the whole journey. A party that
    changes mid-journey usually means somebody was picked up or dropped off,
    making the stop an activity rather than a transfer -- so the link itself is
    suspect, not just the count. Linking with ``split_on_occupancy`` enabled
    keeps those segments apart and silences this warning.
    """
    if "num_travelers" not in unlinked_trips.columns:
        return

    varying = (
        unlinked_trips.group_by("linked_trip_id")
        .agg(pl.col("num_travelers").drop_nulls().n_unique().alias("_n_sizes"))
        .filter(pl.col("_n_sizes") > 1)
    )
    if varying.is_empty():
        return

    logger.warning(
        "%d linked trip(s) change party size between segments; num_travelers reports "
        "the largest. This usually indicates a pick-up or drop-off that was linked as "
        "a transfer -- consider link_trips(split_on_occupancy=True). Examples: %s",
        varying.height,
        varying["linked_trip_id"].head(5).to_list(),
    )


def aggregate_linked_trips(
    unlinked_trips: pl.DataFrame,
    transit_mode_enums: list[str],
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
        transit_mode_enums: List of mode enums that count as transit

    Returns:
        Aggregated DataFrame with one row per linked trip

    """
    logger.info("Aggregating linked trips...")
    _warn_on_occupancy_change(unlinked_trips)

    transit_mode_codes = resolve_enum_labels(
        table_name="unlinked_trips",
        field_name="mode_type",
        enum_labels=transit_mode_enums,  # pyright: ignore[reportArgumentType]
    )

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

    # Build aggregation list
    agg_exprs = [
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
        pl.first("o_purpose"),
        pl.first("o_purpose_category"),
        pl.first("o_lat"),
        pl.first("o_lon"),
        # Arrival information (from last trip segment)
        # pl.last("arrive_date"),
        # pl.last("arrive_hour"),
        # pl.last("arrive_minute"),
        # pl.last("arrive_seconds"),
        pl.last("arrive_time"),
        pl.last("d_purpose"),
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
        # Dwell duration: time waiting between the merged segments (e.g. a
        # transfer), summed over each gap from one segment's arrival to the
        # next segment's departure. Measured from the timestamps alone so it
        # cannot go negative and a single-segment trip is exactly zero. The
        # earlier "elapsed minus summed segment durations" form differenced two
        # independently rounded quantities -- vendor durations are whole
        # minutes, elapsed is truncated -- and so reported -1 minutes of dwell
        # on trips that had no dwell at all.
        (pl.col("depart_time").shift(-1) - pl.col("arrive_time"))
        .dt.total_seconds()
        .sum()
        .truediv(60)
        .round()
        .cast(pl.Int64)
        .alias("dwell_duration_minutes"),
        # Number of segments in linked trip
        pl.len().alias("num_segments"),
    ]

    # Conditionally add linked_trip_weight if column exists
    if "unlinked_trip_weight" in unlinked_trips.columns:
        agg_exprs.append(pl.col("unlinked_trip_weight").mean().alias("linked_trip_weight"))

    # Propagate complete: a linked trip is complete only if all segments are complete
    if "complete" in unlinked_trips.columns:
        agg_exprs.append(pl.all("complete").alias("complete"))

    # Add remaining aggregations
    agg_exprs.extend(
        [
            # num_travelers (max of segment num_travelers)
            pl.col("num_travelers").max().alias("num_travelers"),
            # Determine driver status across segments
            pl.when(pl.col("driver").n_unique() == 1)
            .then(pl.col("driver").first())
            # If missing entirely
            .when(pl.col("driver").filter(pl.col("driver") != Driver.MISSING.value).n_unique() == 0)
            .then(pl.lit(Driver.MISSING.value))  # All missing
            # If mixed driver/passenger, set to BOTH
            .otherwise(pl.lit(Driver.BOTH.value))
            .alias("driver"),
        ]
    )

    # Now aggregate with proper time ordering
    linked_trips = (
        unlinked_trips
        # Sort by departure time
        .sort(["linked_trip_id", "depart_time", "arrive_time"])
        .group_by(
            ["linked_trip_id", "person_id", "hh_id"],
        )
        .agg(agg_exprs)
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
    linked_trips = linked_trips.join(
        unlinked_trips.select(["linked_trip_id", "day_id"]).unique(),
        on="linked_trip_id",
        how="left",
    )

    # Compute d_activity_duration: whole minutes spent at the destination before the next
    # departure (i.e. the activity duration at the trip destination). Ordered
    # within each person-day. Sentinels: -1 when the destination is home; -2 for
    # the last trip of a person-day (no subsequent departure).
    linked_trips = (
        linked_trips.sort(["person_id", "day_id", "depart_time", "arrive_time"])
        .with_columns(
            pl.col("depart_time").shift(-1).over(["person_id", "day_id"]).alias("_next_depart_time")
        )
        .with_columns(
            pl.when(pl.col("d_purpose_category") == PurposeCategory.HOME.value)
            .then(pl.lit(-1))
            .when(pl.col("_next_depart_time").is_null())
            .then(pl.lit(-2))
            .otherwise((pl.col("_next_depart_time") - pl.col("arrive_time")).dt.total_minutes())
            .cast(pl.Int64)
            .alias("d_activity_duration")
        )
        .drop("_next_depart_time")
    )

    return linked_trips
