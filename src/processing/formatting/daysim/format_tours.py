"""Tour formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.tours import TourDirection, TourType
from data_canon.codebook.trips import ModeType

from .mappings import (
    daysim_purpose,
    daysim_tour_number,
    determine_tour_mode,
    minutes_after_midnight,
)

logger = logging.getLogger(__name__)


def format_tours(
    persons: pl.DataFrame,
    person_days: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> pl.DataFrame:
    """Format tour data to DaySim specification.

    Transforms canonical tour data into DaySim tour format with proper
    field mappings and time conversions.

    Key Transformations:

    - **Purpose Mapping**: Map canonical tour purposes to DaySim purpose codes (work,
      university, school, escort, shop, personal business, social, recreation, meal,
      change mode)
    - **Timing**: Convert departure and arrival times to minutes after midnight
    - **Location**: Map origin/destination to TAZ/MAZ, handle missing locations with -1
    - **Tour Mode**: Determine primary mode from trip-level modes within tour
    - **Parent Tours**: Link subtours to parent tour IDs for tour-based modeling

    Args:
        persons: DataFrame with canonical person fields
        person_days: DataFrame with canonical day fields
        linked_trips: DataFrame with canonical linked trip fields
        tours: DataFrame with canonical tour fields

    Returns:
        DataFrame with DaySim tour fields
    """
    logger.info("Formatting tour data")

    # Join person_num and travel_dow to tours for DaySim hhno, pno, day
    tours_daysim = (
        tours.join(
            persons.select(["hh_id", "person_id", "person_num"]),
            on=["hh_id", "person_id"],
            how="left",
        )
        .join(
            person_days.select(["hh_id", "person_id", "person_day_id", "travel_dow"]),
            on=["hh_id", "person_id", "person_day_id"],
            how="left",
        )
        .join(persons.select("person_id", "employment"), on="person_id", how="left")
    )

    # Number the tours within each person-day. See daysim_tour_number: the trips
    # file must derive the same number or its trips point at the wrong tour.
    tours_daysim = tours_daysim.with_columns(
        hhno=pl.col("hh_id"),
        pno=pl.col("person_num"),
        day=pl.col("travel_dow"),
        tour=daysim_tour_number(),
    )

    # Point each subtour at its parent's DaySim tour number. Home-based tours
    # self-reference in canonical data, but DaySim reserves parent=0 for "not a
    # subtour", so they are zeroed rather than pointed at themselves.
    tours_daysim = tours_daysim.join(
        tours_daysim.select(["tour_id", "tour"]).rename({"tour": "parent_tour_number"}),
        left_on="parent_tour_id",
        right_on="tour_id",
        how="left",
    ).with_columns(
        parent=pl.when(pl.col("tour_type") == TourType.HOME_BASED.value)
        .then(0)
        .otherwise(pl.col("parent_tour_number").fill_null(0))
        .cast(pl.Int16),
        pdpurp=daysim_purpose(pl.col("tour_purpose"), pl.col("employment")),
        toadtyp=pl.col("o_location_type"),
        tdadtyp=pl.col("d_location_type"),
    )
    tours_daysim = tours_daysim.with_columns(
        tlvorig=minutes_after_midnight("origin_depart_time"),
        tardest=minutes_after_midnight("dest_arrive_time"),
        tlvdest=minutes_after_midnight("dest_depart_time"),
        tarorig=minutes_after_midnight("origin_arrive_time"),
    )

    # Set location coordinates and mode
    tours_daysim = tours_daysim.with_columns(
        toxco=pl.col("o_lon"),
        toyco=pl.col("o_lat"),
        tdxco=pl.col("d_lon"),
        tdyco=pl.col("d_lat"),
    )

    # Determine tour mode (requires linked_trips for HOV and transit access)
    tours_daysim = determine_tour_mode(tours_daysim, linked_trips)

    # Aggregate auto time and distance from linked_trips
    auto_agg = (
        linked_trips.filter(
            pl.col("mode_type").is_in(
                [
                    ModeType.CAR.value,
                    ModeType.CARSHARE.value,
                    ModeType.TNC.value,
                    ModeType.TAXI.value,
                ]
            )
        )
        .group_by("tour_id")
        .agg(
            [
                pl.sum("duration_minutes").alias("tautotime"),
                pl.sum("distance_meters").alias("tautodist"),
            ]
        )
    )
    # On the canonical tour_id the trips carry, not DaySim's per-day tour number.
    tours_daysim = tours_daysim.join(auto_agg, on="tour_id", how="left")

    # Subtours per parent. A home-based tour is its own parent, so it is not counted.
    subtour_counts = (
        tours_daysim.filter(pl.col("parent_tour_id") != pl.col("tour_id"))
        .group_by("parent_tour_id")
        .agg(pl.len().alias("subtrs"))
    )
    tours_daysim = tours_daysim.join(subtour_counts, on="parent_tour_id", how="left")

    # Get taz and parcel fields from linked trips
    tours_daysim = (
        tours_daysim.join(
            linked_trips.select(["linked_trip_id", "o_taz", "o_maz"]),
            left_on="origin_linked_trip_id",
            right_on="linked_trip_id",
            how="left",
        )
        .join(
            linked_trips.select(["linked_trip_id", "d_taz", "d_maz"]),
            left_on="dest_linked_trip_id",
            right_on="linked_trip_id",
            how="left",
        )
        .rename(
            {
                "o_taz": "totaz",
                "o_maz": "topcl",
                "d_taz": "tdtaz",
                "d_maz": "tdpcl",
            }
        )
    )

    # Count number of outbound and inbound stops from linked trips
    outbound_stops = (
        linked_trips.filter(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
        .group_by("tour_id")
        .agg(pl.len().alias("num_outbound_stops"))
    )

    inbound_stops = (
        linked_trips.filter(pl.col("tour_direction") == TourDirection.INBOUND.value)
        .group_by("tour_id")
        .agg(pl.len().alias("num_inbound_stops"))
    )

    # Join stop counts to tours
    tours_daysim = tours_daysim.join(outbound_stops, on="tour_id", how="left").join(
        inbound_stops, on="tour_id", how="left"
    )

    # Calculate tour weight from linked_trips
    if "linked_trip_weight" in linked_trips.columns:
        tour_weights = linked_trips.group_by("tour_id").agg(
            pl.mean("linked_trip_weight").alias("toexpfac")
        )
        tours_daysim = tours_daysim.join(tour_weights, on="tour_id", how="left")
    else:
        tours_daysim = tours_daysim.with_columns(toexpfac=pl.lit(1.0))

    # Add DaySim-specific fields (placeholders and defaults)
    tours_daysim = tours_daysim.with_columns(
        # Tour structure fields
        jtindex=pl.lit(0),  # Joint tour index (not supported)
        subtrs=pl.col("subtrs").fill_null(0),  # Work-based subtours count
        # Travel characteristics (not available)
        tpathtp=pl.lit(1),  # Path type (default to full network)
        tautocost=pl.lit(-1.0),  # Auto cost
        tautodist=pl.col("tautodist").fill_null(-1.0),  # Auto distance
        tautotime=pl.col("tautotime").fill_null(-1.0),  # Auto time
        # Linked trips on each half tour, which is DaySim's segment count; 1 when a
        # half tour has no matched trips, for the schema's ge=1.
        tripsh1=pl.col("num_outbound_stops").fill_null(1),
        tripsh2=pl.col("num_inbound_stops").fill_null(1),
        # Half-tour indices (not used)
        phtindx1=pl.lit(0),
        phtindx2=pl.lit(0),
        fhtindx1=pl.lit(0),
        fhtindx2=pl.lit(0),
        # Expansion factor
        toexpfac=pl.col("toexpfac").fill_null(0),
    )

    # Select DaySim tour fields
    tour_cols = [
        "hhno",
        "pno",
        "day",
        "tour",
        "jtindex",
        "parent",
        "subtrs",
        "pdpurp",
        "tlvorig",
        "tardest",
        "tlvdest",
        "tarorig",
        "toadtyp",
        "tdadtyp",
        "topcl",
        "totaz",
        "tdpcl",
        "tdtaz",
        "toxco",
        "toyco",
        "tdxco",
        "tdyco",
        "tmodetp",
        "tpathtp",
        "tautotime",
        "tautocost",
        "tautodist",
        "tripsh1",
        "tripsh2",
        "phtindx1",
        "phtindx2",
        "fhtindx1",
        "fhtindx2",
        "toexpfac",
    ]

    tours_daysim = tours_daysim.select(tour_cols).sort(by=["hhno", "pno", "day", "tour"])

    logger.info("Formatted %d tours", len(tours_daysim))
    return tours_daysim
