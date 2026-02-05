"""Trip formatting for CT-RAMP.

Transforms canonical trip data into CT-RAMP model format, including:
- Individual trips (trips on individual tours)
- Joint trips (trips on joint tours)
"""

import logging

import polars as pl

from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection

from .ctramp_config import CTRAMPConfig
from .mappings import map_mode_to_ctramp, map_purpose_category_to_ctramp

logger = logging.getLogger(__name__)


def format_individual_trip(
    linked_trips_canonical: pl.DataFrame,
    tours_ctramp: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format individual trips to CT-RAMP specification.

    Transforms linked trip data (for individual tours only) to CT-RAMP format.

    Args:
        linked_trips_canonical: Canonical DataFrame with linked trip fields (tour_id,
            o_purpose_category, d_purpose_category, mode_type, o_taz, d_taz,
            tour_direction, depart_time, arrive_time, person_id, hh_id)
        tours_ctramp: Formatted CT-RAMP individual tours DataFrame (already filtered to
            individual tours only, without joint_tour_id). Contains tour_id,
            tour_purpose, tour_mode, tour_category
        persons_canonical: Canonical persons DataFrame with person_id, person_num, school_type
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP individual trip fields

    Notes:
        - Excludes trips on joint tours
        - Creates stop_id sequence starting at 1 per half-tour
        - Excludes model-only fields (parking costs, value of time, etc.)
    """
    logger.info("Formatting individual trip data for CT-RAMP")

    # Handle empty input DataFrames
    if len(tours_ctramp) == 0 or len(linked_trips_canonical) == 0:
        logger.info("No tours or trips provided")
        return pl.DataFrame()

    # Filter to trips on individual tours only
    # tours_ctramp has _tour_id_canonical for matching with canonical trip tour_ids
    individual_trips = linked_trips_canonical.filter(
        pl.col("tour_id").is_in(tours_ctramp["_tour_id_canonical"].implode())
    )

    # Get conflicting fields between tours and trips, drop from trips
    conflicting_fields = set(individual_trips.columns).intersection(
        {"tour_purpose", "tour_mode", "tour_category"}
    )
    # Join with tour context (tour fields are already CTRAMP formatted)
    # Rename canonical tour_id temporarily to avoid collision, then join
    individual_trips = (
        individual_trips.drop(conflicting_fields)
        .rename({"tour_id": "_canonical_tour_id"})
        .join(
            tours_ctramp.select(
                ["_tour_id_canonical", "tour_id", "tour_purpose", "tour_mode", "tour_category"]
            ),
            left_on="_canonical_tour_id",
            right_on="_tour_id_canonical",
            how="left",
        )
    )

    # Join with persons and households
    individual_trips = individual_trips.join(
        persons_canonical.select(["person_id", "person_num", "school_type"]),
        on="person_id",
        how="left",
    ).join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Create stop_id: sequence number within each tour direction
    # (outbound/inbound). Starting at 1 for each direction
    # Map tour_direction enum to string for easier handling
    individual_trips = (
        individual_trips.with_columns(
            pl.when(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
            .then(pl.lit("outbound"))
            .when(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .then(pl.lit("inbound"))
            .otherwise(pl.lit("subtour"))
            .alias("tour_direction_str")
        )
        .sort(["tour_id", "tour_direction_str", "depart_time"])
        .with_columns(
            pl.col("depart_time")
            .rank("dense")
            .over(["tour_id", "tour_direction_str"])
            .cast(pl.Int64)
            .alias("stop_id")
        )
    )

    # Map inbound flag (0=outbound, 1=inbound)
    individual_trips = individual_trips.with_columns(
        pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("inbound")
    )

    # Map origin and destination purposes
    individual_trips = individual_trips.with_columns(
        [
            map_purpose_category_to_ctramp(
                pl.col("o_purpose_category"),
                pl.col("income"),
                pl.col("school_type"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("orig_purpose"),
            map_purpose_category_to_ctramp(
                pl.col("d_purpose_category"),
                pl.col("income"),
                pl.col("school_type"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("dest_purpose"),
            # tour_purpose is already CTRAMP formatted from join, no need to remap
        ]
    )

    # Map trip mode (tour_mode already formatted from join)
    individual_trips = individual_trips.with_columns(
        map_mode_to_ctramp(
            pl.col("mode_type"),
            pl.col("num_travelers"),
            pl.col("access_mode"),
            pl.col("egress_mode"),
        ).alias("trip_mode")
    )

    # Convert times to minutes after midnight and extract hours
    individual_trips = individual_trips.with_columns(
        [
            (pl.col("depart_time").dt.hour() * 60 + pl.col("depart_time").dt.minute()).alias(
                "depart_minutes"
            ),
            (pl.col("arrive_time").dt.hour() * 60 + pl.col("arrive_time").dt.minute()).alias(
                "arrive_minutes"
            ),
            pl.col("depart_time").dt.hour().alias("depart_hour"),
        ]
    )

    # Add survey distance
    individual_trips = individual_trips.with_columns(
        (pl.col("distance_meters") / 1609.34).alias("distance_survey")
    )

    # Ensure required columns are formatted and cast to correct types
    individual_trips_ctramp = individual_trips.with_columns(
        [
            pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
            pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
            pl.lit(0).cast(pl.Int64).alias("parking_taz"),  # Default 0 (no parking)
            pl.col("depart_hour").cast(pl.Int64),
            pl.col("depart_minutes").cast(pl.Int64),
            pl.col("arrive_minutes").cast(pl.Int64),
        ]
    )

    # Add weight and sampleRate if linked_trip_weight exists
    if "linked_trip_weight" in individual_trips_ctramp.columns:
        individual_trips_ctramp = individual_trips_ctramp.with_columns(
            [
                pl.col("linked_trip_weight").alias("trip_weight"),
                pl.when(pl.col("linked_trip_weight") > 0)
                .then(pl.col("linked_trip_weight").pow(-1))
                .otherwise(None)
                .alias("sampleRate"),
            ]
        )

    logger.info("Formatted %d individual trip records", len(individual_trips_ctramp))
    return individual_trips_ctramp


def format_joint_trip(
    joint_trips_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    tours_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format joint trips to CT-RAMP specification.

    Transforms joint trip data using mean coordinates from joint_trips table.

    Args:
        joint_trips_canonical: Aggregated joint trip DataFrame with joint_trip_id, hh_id,
            num_joint_travelers (mean locations)
        linked_trips_canonical: Canonical linked trips DataFrame to bridge joint_trip_id
            to tour_id (contains joint_trip_id, tour_id, o_purpose_category,
            d_purpose_category, mode_type, depart_time, arrive_time, num_travelers,
            o_taz, d_taz)
        tours_canonical: Canonical tours DataFrame with tour_id, joint_tour_id,
            tour_purpose, tour_category, tour_mode
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP joint trip fields

    Notes:
        - Uses mean coordinates from joint_trips aggregation table
        - Deduplicates to one row per joint_trip_id
    """
    logger.info("Formatting joint trip data for CT-RAMP")

    if len(joint_trips_canonical) == 0:
        logger.info("No joint trips found")
        return pl.DataFrame()

    # Join to linked_trips to get tour_id and other trip context
    # Filter to only joint trips first to maintain proper schema
    joint_linked_trips = linked_trips_canonical.filter(pl.col("joint_trip_id").is_not_null())

    if len(joint_linked_trips) == 0:
        logger.warning("No linked trips found with joint_trip_id")
        return pl.DataFrame()

    joint_trip_context = joint_linked_trips.group_by("joint_trip_id").agg(
        [
            pl.col("tour_id").first(),
            pl.col("o_purpose_category").first(),
            pl.col("d_purpose_category").first(),
            pl.col("mode_type").first(),
            pl.col("depart_time").first(),
            pl.col("arrive_time").first(),
            pl.col("num_travelers").first(),
            pl.col(f"o_{config.taz_field}").first(),
            pl.col(f"d_{config.taz_field}").first(),
            pl.col("tour_direction").first(),
            pl.col("access_mode").first(),
            pl.col("egress_mode").first(),
        ]
    )

    joint_trips_formatted = joint_trips_canonical.join(
        joint_trip_context,
        on="joint_trip_id",
        how="left",
    )

    # Join with tour context
    joint_trips_formatted = joint_trips_formatted.join(
        tours_canonical.select(["tour_id", "joint_tour_id", "tour_purpose", "tour_mode"]),
        on="tour_id",
        how="left",
    )

    # Filter to only trips on joint tours
    joint_trips_formatted = joint_trips_formatted.filter(pl.col("joint_tour_id").is_not_null())

    # Join with households
    joint_trips_formatted = joint_trips_formatted.join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Map purposes and mode using mean locations
    joint_trips_formatted = joint_trips_formatted.with_columns(
        [
            map_purpose_category_to_ctramp(
                pl.col("o_purpose_category"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("orig_purpose"),
            map_purpose_category_to_ctramp(
                pl.col("d_purpose_category"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("dest_purpose"),
            map_purpose_category_to_ctramp(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("tour_purpose_ctramp"),
            map_mode_to_ctramp(
                pl.col("mode_type"),
                pl.col("num_travelers").fill_null(2),
                pl.col("access_mode"),
                pl.col("egress_mode"),
            ).alias("trip_mode"),
            map_mode_to_ctramp(
                pl.col("tour_mode"),
                pl.col("num_travelers").fill_null(2),
                None,  # Tour mode doesn't have access/egress
                None,
            ).alias("tour_mode_ctramp"),
        ]
    )

    # Convert times and extract hours
    joint_trips_formatted = joint_trips_formatted.with_columns(
        [
            pl.col("depart_time").dt.hour().alias("depart_hour"),
            (pl.col("arrive_time") - pl.col("depart_time"))
            .dt.total_minutes()
            .cast(pl.Int64)
            .alias("trip_time"),
        ]
    )

    # Determine inbound flag from tour_direction
    joint_trips_formatted = joint_trips_formatted.with_columns(
        pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("inbound")
    )

    # Create stop_id as sequence within tour
    joint_trips_formatted = joint_trips_formatted.sort(
        ["joint_tour_id", "depart_time"]
    ).with_columns(
        pl.col("joint_trip_id").rank("dense").over("joint_tour_id").cast(pl.Int64).alias("stop_id")
    )

    # Select final columns with snake_case names
    joint_trips_ctramp = joint_trips_formatted.select(
        [
            pl.col("hh_id"),
            pl.col("joint_tour_id").alias("tour_id"),
            pl.col("stop_id"),
            pl.col("inbound"),
            pl.col("tour_purpose_ctramp").alias("tour_purpose"),
            pl.col("orig_purpose"),
            pl.col("dest_purpose"),
            pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
            pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
            pl.lit(0).cast(pl.Int64).alias("parking_taz"),  # Default 0 (no parking)
            pl.col("trip_mode"),
            pl.col("tour_mode_ctramp").alias("tour_mode"),
            # All joint tours are just "JOINT_NON_MANDATORY" category
            pl.lit("JOINT_NON_MANDATORY").alias("tour_category"),
            pl.col("num_joint_travelers").cast(pl.Int64).alias("num_participants"),
            pl.col("depart_hour").cast(pl.Int64),
            pl.col("trip_time"),
        ]
    )

    logger.info("Formatted %d joint trip records", len(joint_trips_ctramp))
    return joint_trips_ctramp
