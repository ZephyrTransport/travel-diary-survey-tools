"""Trip formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    VehicleOccupancy,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
)

from .mappings import (
    DROVE_ACCESS_EGRESS,
    PURPOSE_MAP,
)

logger = logging.getLogger(__name__)


def _determine_linked_trip_mode_type(
    unlinked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Determine mode_type and transit access/egress for each linked trip.

    Uses transit-first logic: if any segment is FERRY, TRANSIT, or
    LONG_DISTANCE, use that mode_type. Otherwise, use the mode_type from
    the longest duration segment.

    For transit trips, also captures the reported transit_access and
    transit_egress modes from unlinked segments.

    Args:
        unlinked_trips: DataFrame with canonical unlinked trip fields including
            linked_trip_id, mode_type, depart_time, arrive_time,
            transit_access, transit_egress

    Returns:
        DataFrame with columns [linked_trip_id, mode_type, transit_access,
        transit_egress] for all linked trips

    Raises:
        ValueError: If any linked_trip_id has a null mode_type after aggregation
    """
    # Compute trip duration for each unlinked segment
    unlinked_with_duration = unlinked_trips.with_columns(
        trip_duration=(pl.col("arrive_time") - pl.col("depart_time"))
    )

    # Group by linked_trip_id and determine mode + transit access/egress
    mode_agg = (
        unlinked_with_duration.sort(["linked_trip_id", "trip_duration"], descending=[False, True])
        .group_by("linked_trip_id")
        .agg(
            [
                # Transit mode if any segment is transit
                pl.col("mode_type")
                .filter(
                    pl.col("mode_type").is_in(
                        [
                            ModeType.FERRY.value,
                            ModeType.TRANSIT.value,
                            ModeType.LONG_DISTANCE.value,
                        ]
                    )
                )
                .first()
                .alias("mode_transit"),
                # Longest non-transit segment (already sorted by duration)
                pl.col("mode_type")
                .filter(
                    ~pl.col("mode_type").is_in(
                        [
                            ModeType.FERRY.value,
                            ModeType.TRANSIT.value,
                            ModeType.LONG_DISTANCE.value,
                        ]
                    )
                )
                .first()
                .alias("mode_non_transit"),
                # Transit access/egress (first non-null value)
                pl.col("transit_access").drop_nulls().first().alias("transit_access"),
                pl.col("transit_egress").drop_nulls().first().alias("transit_egress"),
            ]
        )
        .with_columns(
            mode_type=pl.when(pl.col("mode_transit").is_not_null())
            .then(pl.col("mode_transit"))
            .otherwise(pl.col("mode_non_transit"))
        )
        .select(["linked_trip_id", "mode_type", "transit_access", "transit_egress"])
    )

    # Validate no nulls in mode_type
    null_count = mode_agg.filter(pl.col("mode_type").is_null()).height
    if null_count > 0:
        msg = (
            f"Missing mode_type for {null_count} linked trips. "
            "All unlinked segments must have valid mode_type values."
        )
        raise ValueError(msg)

    return mode_agg


def _aggregate_transit_path_flags(unlinked_trips: pl.DataFrame) -> pl.DataFrame:
    """Aggregate transit mode flags from unlinked segments for linked trip.

    Scans mode_1, mode_2, mode_3, mode_4 values (if present) or falls back
    to the main mode column across ALL unlinked segments within each linked
    trip to determine which specific transit modes were used.

    Why multi-segment scanning is necessary:
    - Linked trips can contain multiple unlinked segments (e.g., bus->walk)
    - Each segment has mode_1-4 slots for respondent-reported sequences
    - Respondents report at varying granularity (some fill all 4, just 1)
    - Mode.MISSING (995) indicates unreported/empty slots
    - Must scan all segments x all slots (e.g., 3 segments x 4 modes = 12)
      to find all transit modes used in the linked trip

    Fallback behavior:
    - If mode_1 through mode_4 columns are not present, uses the main mode
      column instead
    - This allows the formatter to work with simplified test data or datasets
      that don't have detailed mode sequences

    Args:
        unlinked_trips: DataFrame with canonical unlinked trip fields including
            linked_trip_id, and either mode_1/mode_2/mode_3/mode_4 or mode

    Returns:
        DataFrame with columns [linked_trip_id, has_ferry, has_bart,
        has_premium, has_lrt, has_intercity_rail]. All boolean flags are
        non-null (False for non-transit trips or when specific mode not
        present).
    """
    # Check for mode_1 through mode_4 columns
    mode_cols = [
        col for col in unlinked_trips.columns if col.startswith("mode_") and col[-1].isdigit()
    ]

    # Use mode_1-4 columns if present, otherwise fallback to main mode column
    if mode_cols:
        # Collect all mode values by unpivoting mode_1-4 columns
        all_modes = (
            unlinked_trips.select(["linked_trip_id", *mode_cols])
            .unpivot(
                index="linked_trip_id",
                on=mode_cols,
                variable_name="mode_slot",
                value_name="mode",
            )
            .filter(pl.col("mode") != Mode.MISSING.value)
        )
    else:
        # Fallback: use the main mode column directly
        all_modes = unlinked_trips.select(["linked_trip_id", "mode"]).filter(
            pl.col("mode") != Mode.MISSING.value
        )

    # Check for each specific transit mode across all linked trip segments
    transit_flags = all_modes.group_by("linked_trip_id").agg(
        [
            pl.col("mode").is_in([Mode.FERRY.value]).any().alias("has_ferry"),
            pl.col("mode").is_in([Mode.BART.value]).any().alias("has_bart"),
            pl.col("mode")
            .is_in(
                [
                    Mode.RAIL_INTERCITY.value,
                    Mode.RAIL_OTHER.value,
                    Mode.BUS_EXPRESS.value,
                ]
            )
            .any()
            .alias("has_premium"),
            pl.col("mode")
            .is_in(
                [
                    Mode.MUNI_METRO.value,
                    Mode.RAIL.value,
                    Mode.STREETCAR.value,
                ]
            )
            .any()
            .alias("has_lrt"),
            pl.col("mode").is_in([Mode.RAIL_INTERCITY.value]).any().alias("has_intercity_rail"),
        ]
    )

    # Ensure all linked_trip_ids have entries (fill missing with False)
    all_linked_trips = unlinked_trips.select("linked_trip_id").unique()

    result = all_linked_trips.join(transit_flags, on="linked_trip_id", how="left").with_columns(
        [
            pl.col("has_ferry").fill_null(value=False),
            pl.col("has_bart").fill_null(value=False),
            pl.col("has_premium").fill_null(value=False),
            pl.col("has_lrt").fill_null(value=False),
            pl.col("has_intercity_rail").fill_null(value=False),
        ]
    )

    return result


def _compute_daysim_mode_expr() -> pl.Expr:
    """Create expression to compute DaySim mode from mode_type and flags.

    Expects columns: mode_type, has_intercity_rail, num_travelers,
    transit_access, transit_egress.

    Returns:
        Polars expression mapping to DaysimMode enum values
    """
    # Step 1: Handle non-motorized modes
    walk_expr = pl.when(pl.col("mode_type") == ModeType.WALK.value).then(
        pl.lit(DaysimMode.WALK.value)
    )

    bike_expr = walk_expr.when(
        pl.col("mode_type").is_in(
            [
                ModeType.BIKE.value,
                ModeType.BIKESHARE.value,
                ModeType.SCOOTERSHARE.value,
            ]
        )
    ).then(pl.lit(DaysimMode.BIKE.value))

    # Step 2: Handle private vehicle modes with occupancy
    vehicle_occupancy_expr = (
        pl.when(pl.col("num_travelers") == VehicleOccupancy.SOV.value)
        .then(pl.lit(DaysimMode.SOV.value))
        .when(pl.col("num_travelers") == VehicleOccupancy.HOV2.value)
        .then(pl.lit(DaysimMode.HOV2.value))
        .when(pl.col("num_travelers") > VehicleOccupancy.HOV3_MIN.value)
        .then(pl.lit(DaysimMode.HOV3.value))
    )

    car_expr = bike_expr.when(
        pl.col("mode_type").is_in(
            [
                ModeType.CAR.value,
                ModeType.CARSHARE.value,
            ]
        )
    ).then(vehicle_occupancy_expr)

    # Step 3: Handle ride-hailing services
    tnc_expr = car_expr.when(
        pl.col("mode_type").is_in(
            [
                ModeType.TAXI.value,
                ModeType.TNC.value,
            ]
        )
    ).then(pl.lit(DaysimMode.TNC.value))

    # Step 4: Handle special vehicle modes
    school_bus_expr = tnc_expr.when(pl.col("mode_type") == ModeType.SCHOOL_BUS.value).then(
        pl.lit(DaysimMode.SCHOOL_BUS.value)
    )

    shuttle_expr = school_bus_expr.when(pl.col("mode_type") == ModeType.SHUTTLE.value).then(
        pl.lit(DaysimMode.HOV3.value)
    )  # shuttle/vanpool as HOV3+

    # Step 5: Handle transit modes with access/egress
    transit_condition = pl.col("mode_type").is_in(
        [
            ModeType.FERRY.value,
            ModeType.TRANSIT.value,
        ]
    ) | ((pl.col("mode_type") == ModeType.LONG_DISTANCE.value) & pl.col("has_intercity_rail"))

    transit_access_expr = (
        pl.when(
            pl.col("transit_access").is_in(DROVE_ACCESS_EGRESS)
            | pl.col("transit_egress").is_in(DROVE_ACCESS_EGRESS)
        )
        .then(pl.lit(DaysimMode.DRIVE_TRANSIT.value))
        .otherwise(pl.lit(DaysimMode.WALK_TRANSIT.value))
    )

    mode_expr = shuttle_expr.when(transit_condition).then(transit_access_expr)

    # Step 6: Handle all other modes
    mode_expr = mode_expr.otherwise(pl.lit(DaysimMode.OTHER.value))

    return mode_expr


def _compute_daysim_path_type_expr() -> pl.Expr:
    """Create expression to compute DaySim path type from mode and flags.

    Uses hierarchical logic: FERRY > BART > PREMIUM > LRT > BUS.

    Expects columns: mode_type, mode, has_ferry, has_bart, has_premium,
    has_lrt.

    Returns:
        Polars expression mapping to DaysimPathType enum values
    """
    # Non-transit modes
    non_transit_expr = (
        pl.when(pl.col("mode_type") == ModeType.CAR.value)
        .then(pl.lit(DaysimPathType.FULL_NETWORK.value))
        .otherwise(pl.lit(DaysimPathType.NONE.value))
    )

    # Transit path type hierarchy: FERRY > BART > PREMIUM > LRT > BUS
    transit_path_expr = (
        pl.when(pl.col("has_ferry"))
        .then(pl.lit(DaysimPathType.FERRY.value))
        .when(pl.col("has_bart"))
        .then(pl.lit(DaysimPathType.BART.value))
        .when(pl.col("has_premium"))
        .then(pl.lit(DaysimPathType.PREMIUM.value))
        .when(pl.col("has_lrt"))
        .then(pl.lit(DaysimPathType.LRT.value))
        .otherwise(pl.lit(DaysimPathType.BUS.value))
    )

    # Combine logic: check if transit mode, then apply appropriate path type
    path_type_expr = (
        pl.when(
            pl.col("mode").is_in(
                [
                    DaysimMode.WALK_TRANSIT.value,
                    DaysimMode.DRIVE_TRANSIT.value,
                ]
            )
        )
        .then(transit_path_expr)
        .otherwise(non_transit_expr)
    )

    return path_type_expr


def _compute_driver_passenger_expr() -> pl.Expr:
    """Create expression to compute DaySim driver/passenger code.

    Expects columns: mode, driver, num_travelers.

    Returns:
        Polars expression mapping to DaysimDriverPassenger enum values
    """
    # Handle private vehicle modes (SOV, HOV2, HOV3)
    private_vehicle_expr = (
        pl.when(
            pl.col("driver").is_in(
                [
                    Driver.DRIVER.value,
                    Driver.BOTH.value,
                ]
            )
        )
        .then(pl.lit(DaysimDriverPassenger.DRIVER.value))
        .when(pl.col("driver") == Driver.PASSENGER.value)
        .then(pl.lit(DaysimDriverPassenger.PASSENGER.value))
        .otherwise(pl.lit(DaysimDriverPassenger.MISSING.value))
    )

    # Handle TNC modes (ride-hailing services)
    tnc_expr = (
        pl.when(pl.col("num_travelers") == VehicleOccupancy.SOV.value)
        .then(pl.lit(DaysimDriverPassenger.TNC_ALONE.value))
        .when(pl.col("num_travelers") == VehicleOccupancy.HOV2.value)
        .then(pl.lit(DaysimDriverPassenger.TNC_2.value))
        .when(pl.col("num_travelers") > VehicleOccupancy.HOV3_MIN.value)
        .then(pl.lit(DaysimDriverPassenger.TNC_3PLUS.value))
    )

    # Combine logic for all mode types
    driver_passenger_exp = (
        pl.when(
            pl.col("mode").is_in(
                [
                    DaysimMode.SOV.value,
                    DaysimMode.HOV2.value,
                    DaysimMode.HOV3.value,
                ]
            )
        )
        .then(private_vehicle_expr)
        .when(pl.col("mode") == DaysimMode.TNC.value)
        .then(tnc_expr)
        .otherwise(pl.lit(DaysimDriverPassenger.NA.value))
    )
    return driver_passenger_exp


def _prepare_basic_fields(linked_trips: pl.DataFrame, persons: pl.DataFrame) -> pl.DataFrame:
    """Prepare basic DaySim fields from linked trips.

    Joins person_num, computes Daysim trip identification fields (tour, half,
    tseg, tsvid), renames columns to DaySim convention, fills null coordinates,
    formats times, and maps purposes.

    Args:
        linked_trips: DataFrame with canonical linked trip fields
        persons: DataFrame with person_id and person_num

    Returns:
        DataFrame with basic DaySim fields prepared
    """
    # Join person_num to linked trips
    trips = linked_trips.join(
        persons.select(["person_id", "person_num"]),
        on=["person_id"],
        how="left",
    )

    # Compute Daysim trip identification fields:
    # - tour: tour sequence number within person-day (from tour_num)
    # - half: half-tour direction (1=OUTBOUND, 2=INBOUND, from tour_direction)
    # - tseg: trip sequence within half-tour (ranked by departure then arrival)
    # - tsvid: travel survey trip ID (use linked_trip_id)
    # - tripno: sequential trip number per person-day (bonus field)
    trips = trips.with_columns(
        [
            # Use tour_num if it exists, otherwise create it
            pl.col("tour_num").alias("tour")
            if "tour_num" in linked_trips.columns
            else pl.lit(1).alias("tour"),
            # Map tour_direction to half (1=OUTBOUND, 2=INBOUND)
            pl.col("tour_direction").alias("half")
            if "tour_direction" in linked_trips.columns
            else pl.lit(1).alias("half"),
            # Compute trip sequence within half-tour
            # CRITICAL: rank() with method="ordinal"
            # and sort by depart_time, arrive_time
            # This ensures tseg follows temporal order and
            # handles tied departure times
            pl.col("depart_time")
            .rank(method="ordinal")
            .over(
                ["hh_id", "person_id", "day_id", "tour_num", "tour_direction"],
                order_by=["depart_time", "arrive_time"],
            )
            .alias("tseg")
            if "tour_num" in linked_trips.columns and "tour_direction" in linked_trips.columns
            else pl.col("depart_time")
            .rank(method="ordinal")
            .over(
                ["hh_id", "person_id", "day_id"],
                order_by=["depart_time", "arrive_time"],
            )
            .alias("tseg"),
            # Use linked_trip_num as travel survey ID
            pl.col("linked_trip_num").cast(pl.Int32).alias("tsvid"),
            # Bonus: sequential trip number per person-day
            pl.col("depart_time")
            .rank("ordinal")
            .over(["hh_id", "person_id", "day_id"])
            .alias("tripno"),
            # Add default address types (3 = other)
            pl.lit(3).alias("oadtyp"),
            pl.lit(3).alias("dadtyp"),
        ]
    )

    # Rename columns to DaySim naming convention
    trips = trips.rename(
        {
            "hh_id": "hhno",
            "person_num": "pno",
            "travel_dow": "day",
            "o_taz": "otaz",
            "o_maz": "opcl",
            "d_taz": "dtaz",
            "d_maz": "dpcl",
            "o_lon": "oxco",
            "o_lat": "oyco",
            "d_lon": "dxco",
            "d_lat": "dyco",
            "o_purpose_category": "opurp",
            "d_purpose_category": "dpurp",
        }
    )

    # Apply basic transformations
    trips = trips.with_columns(
        [
            # Fill null coordinates with -1
            pl.col("oxco").fill_null(value=-1),
            pl.col("oyco").fill_null(value=-1),
            pl.col("dxco").fill_null(value=-1),
            pl.col("dyco").fill_null(value=-1),
            # Convert datetime to minutes after midnight (0-1439)
            (
                pl.col("depart_time").dt.hour().cast(pl.Int16) * 60
                + pl.col("depart_time").dt.minute()
            ).alias("deptm"),
            (
                pl.col("arrive_time").dt.hour().cast(pl.Int16) * 60
                + pl.col("arrive_time").dt.minute()
            ).alias("arrtm"),
            # Compute end activity time (same as arrival for now)
            (
                pl.col("arrive_time").dt.hour().cast(pl.Int16) * 60
                + pl.col("arrive_time").dt.minute()
            ).alias("endacttm"),
            # Map purposes
            pl.col("opurp").replace(PURPOSE_MAP).alias("opurp"),
            pl.col("dpurp").replace(PURPOSE_MAP).alias("dpurp"),
        ]
    )

    return trips


def format_linked_trips(
    persons: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Format linked trip data to DaySim specification.

    Computes DaySim mode, path type, and driver/passenger codes by
    aggregating mode information from unlinked trip segments, then
    applying DaySim-specific mappings.

    This function performs mode aggregation that was previously done in
    the linking step. By moving it here, we preserve maximum granularity
    in the core linked_trips table per the pipeline design philosophy,
    deferring format-specific aggregations to output formatters.

    Args:
        persons: DataFrame with canonical person fields
        unlinked_trips: DataFrame with canonical unlinked trip fields
        linked_trips: DataFrame with canonical linked trip fields

    Returns:
        DataFrame with DaySim trip fields formatted per DaySim spec
    """
    logger.info("Formatting linked trip data for DaySim")

    # Step 1: Aggregate mode information from unlinked trip segments
    logger.info("Aggregating mode_type from unlinked segments")
    mode_agg = _determine_linked_trip_mode_type(unlinked_trips)

    logger.info("Aggregating transit path flags from unlinked segments")
    transit_flags = _aggregate_transit_path_flags(unlinked_trips)

    # Step 2: Prepare basic DaySim fields
    logger.info("Preparing basic DaySim fields")
    trips_daysim = _prepare_basic_fields(linked_trips, persons)

    # Step 3: Join aggregated mode information
    trips_daysim = trips_daysim.join(mode_agg, on="linked_trip_id", how="left")
    trips_daysim = trips_daysim.join(transit_flags, on="linked_trip_id", how="left")

    # Step 4: Compute DaySim-specific fields using expression functions
    logger.info("Computing DaySim mode, path type, and driver/passenger codes")
    trips_daysim = trips_daysim.with_columns(
        mode=_compute_daysim_mode_expr(),  # Evaluate this expression first
    ).with_columns(
        pathtype=_compute_daysim_path_type_expr(),
        dorp=_compute_driver_passenger_expr(),
        # Add default travel time, cost, dist (set to -1 for missing)
        travtime=pl.lit(-1.0),
        travcost=pl.lit(-1.0),
        travdist=pl.lit(-1.0),
    )

    # Step 5: Add trip weight from linked trips, assign 1.0 if missing
    if "trip_weight" in linked_trips.columns:
        trips_daysim = trips_daysim.join(
            linked_trips.select(["linked_trip_id", "trip_weight"]).rename(
                {"trip_weight": "trexpfac"}
            ),
            on="linked_trip_id",
            how="left",
        )
    else:
        trips_daysim = trips_daysim.with_columns(pl.lit(1.0).alias("trexpfac"))

    # Step 6: Select final DaySim fields and sort
    trip_cols = [
        "hhno",
        "pno",
        "day",
        "tour",
        "half",
        "tseg",
        "tsvid",
        "opurp",
        "dpurp",
        "oadtyp",
        "dadtyp",
        "opcl",
        "otaz",
        "oxco",
        "oyco",
        "dpcl",
        "dtaz",
        "dxco",
        "dyco",
        "mode",
        "pathtype",
        "dorp",
        "deptm",
        "arrtm",
        "endacttm",
        "travtime",
        "travcost",
        "travdist",
        "trexpfac",
        "tripno",  # Bonus field for reference
    ]

    trips_daysim = trips_daysim.select(trip_cols).sort(
        by=["hhno", "pno", "day", "tour", "half", "tseg"]
    )

    logger.info("Formatted %d linked trips", len(trips_daysim))
    return trips_daysim
