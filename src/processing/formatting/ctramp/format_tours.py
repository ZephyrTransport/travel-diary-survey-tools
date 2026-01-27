"""Tour formatting for CT-RAMP.

Transforms canonical tour data into CT-RAMP model format, including:
- Individual tours (non-joint tours)
- Joint tours (household member group tours)
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import CTRAMPTourCategory, TourComposition
from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.mappings import (
    PERSON_TYPE_TO_CTRAMP,
    map_mode_to_ctramp,
    map_purpose_category_to_ctramp,
)

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def format_individual_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format individual tours to CT-RAMP specification.

    Transforms tour data (excluding joint tours) to CT-RAMP format.

    Args:
        tours_canonical: Canonical tours DataFrame with tour_id, hh_id, person_id,
            person_num, tour_category, tour_purpose, o_taz, d_taz, origin_depart_time,
            origin_arrive_time, tour_mode, joint_tour_id (for filtering), parent_tour_id
            (for subtour counting)
        linked_trips_canonical: Canonical trips DataFrame with tour_id, tour_direction
            (1=outbound, 2=inbound, 3=subtour)
        persons_canonical: Canonical persons DataFrame with person_id, person_num,
            person_type, school_type
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP individual tour fields:
        - hh_id, person_id, person_num, person_type
        - tour_id, tour_category, tour_purpose
        - orig_taz, dest_taz
        - start_hour, end_hour
        - tour_mode
        - atWork_freq (subtour count)
        - num_ob_stops, num_ib_stops

    Notes:
        - Excludes joint tours (joint_tour_id IS NULL)
        - Excludes all model-only fields (random numbers, wait times, logsums)
    """
    logger.info("Formatting individual tour data for CT-RAMP")

    # Filter to individual tours only (not joint)
    individual_tours = tours_canonical.filter(pl.col("joint_tour_id").is_null())

    # Join with persons for person_type and school_type,
    # and households for income
    individual_tours = individual_tours.join(
        persons_canonical.select(["person_id", "person_num", "person_type", "school_type"]),
        on="person_id",
        how="left",
    ).join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Remap person_type to CTRAMP format (keep as integer enum value)
    individual_tours = individual_tours.with_columns(
        pl.col("person_type").replace_strict(PERSON_TYPE_TO_CTRAMP).alias("person_type_ctramp")
    )

    # Calculate subtour count (at-work tours)
    # atWork_freq represents the number of at-work subtours
    # Per CTRAMP documentation: "Number of at work sub-tours (non-zero only for work tours)"
    subtour_counts = (
        individual_tours.filter(pl.col("parent_tour_id") != pl.col("tour_id"))
        .group_by("parent_tour_id")
        .agg(pl.len().alias("atWork_freq"))
    )

    individual_tours = individual_tours.join(
        subtour_counts,
        left_on="tour_id",
        right_on="parent_tour_id",
        how="left",
    ).with_columns(pl.col("atWork_freq").fill_null(0))

    # Map tour purpose to CTRAMP format
    individual_tours = individual_tours.with_columns(
        map_purpose_category_to_ctramp(
            pl.col("tour_purpose"),
            pl.col("income"),
            pl.col("school_type"),
            config.income_low_threshold,
            config.income_med_threshold,
            config.income_high_threshold,
        ).alias("tour_purpose_ctramp")
    )

    # Map tour_category to string labels
    # Note: tour_purpose here is the canonical PurposeCategory enum, not the mapped CTRAMP string
    mandatory_purposes = [
        PurposeCategory.WORK.value,
        PurposeCategory.SCHOOL.value,
    ]

    individual_tours = individual_tours.with_columns(
        pl.when(pl.col("parent_tour_id") != pl.col("tour_id"))
        .then(pl.lit(CTRAMPTourCategory.AT_WORK.value))
        .when(pl.col("tour_purpose").is_in(mandatory_purposes))
        .then(pl.lit(CTRAMPTourCategory.MANDATORY.value))
        .otherwise(pl.lit(CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value))
        .alias("tour_category_ctramp")
    )

    # Convert times to hour integers (5am-11pm = 5-23)
    individual_tours = individual_tours.with_columns(
        [
            pl.col("origin_depart_time").dt.hour().alias("start_hour"),
            pl.col("origin_arrive_time").dt.hour().alias("end_hour"),
        ]
    )

    # Map mode to CTRAMP integer codes
    # Get max num_travelers from trips for each tour to properly map occupancy-based modes
    if len(linked_trips_canonical) > 0:
        tour_travelers = linked_trips_canonical.group_by("tour_id").agg(
            pl.col("num_travelers").max().alias("max_num_travelers")
        )
        individual_tours = individual_tours.join(tour_travelers, on="tour_id", how="left")
        # For tours with no trips (shouldn't happen), default to 1
        num_travelers_expr = pl.col("max_num_travelers").fill_null(1)
    else:
        num_travelers_expr = pl.lit(1)

    individual_tours = individual_tours.with_columns(
        map_mode_to_ctramp(
            pl.col("tour_mode"),
            num_travelers_expr,
            None,  # Tours don't have access/egress modes
            None,
        ).alias("tour_mode_ctramp")
    )

    # Calculate number of outbound and inbound stops from trips
    # Stops = trips - 1 (number of intermediate destinations, not total trips)
    # A direct home->work tour has 1 trip but 0 stops
    # A home->store->work tour has 2 trips and 1 stop
    if len(linked_trips_canonical) > 0:
        outbound_stops = (
            linked_trips_canonical.filter(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
            .group_by("tour_id")
            .agg((pl.len() - 1).alias("num_ob_stops"))
        )

        inbound_stops = (
            linked_trips_canonical.filter(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .group_by("tour_id")
            .agg((pl.len() - 1).alias("num_ib_stops"))
        )
    else:
        # Handle empty trips DataFrame - create empty aggregation results
        outbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ob_stops": []},
            schema={"tour_id": pl.Int64, "num_ob_stops": pl.UInt32},
        )
        inbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ib_stops": []},
            schema={"tour_id": pl.Int64, "num_ib_stops": pl.UInt32},
        )

    # Join stop counts to tours
    individual_tours = (
        individual_tours.join(outbound_stops, on="tour_id", how="left")
        .join(inbound_stops, on="tour_id", how="left")
        .with_columns(
            [
                pl.col("num_ob_stops").fill_null(0),
                pl.col("num_ib_stops").fill_null(0),
            ]
        )
    )

    # Validate that no tours have zero trips
    # Tours without any trip records will not appear in the trips dataframe
    # Check if any individual tours are missing from the trips data
    tours_with_trips = (
        linked_trips_canonical.select("tour_id").unique()
        if len(linked_trips_canonical) > 0
        else pl.DataFrame({"tour_id": []}, schema={"tour_id": pl.Int64})
    )
    zero_trip_tours = individual_tours.filter(
        ~pl.col("tour_id").is_in(tours_with_trips["tour_id"].implode())
        & (pl.col("subtour_num") == 0)  # Exclude subtours from this check
    )

    if len(zero_trip_tours) > 0:
        tour_ids = zero_trip_tours["tour_id"].to_list()
        msg = (
            f"Found {len(zero_trip_tours)} tours with zero trips. "
            f"Tour IDs: {tour_ids[:10]}{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
        )
        raise ValueError(msg)

    # Select and rename to CTRAMP columns
    output_cols = [
        pl.col("hh_id"),
        pl.col("person_id"),
        pl.col("person_num"),
        pl.col("person_type_ctramp").alias("person_type"),
        pl.col("tour_num").alias("tour_id"),  # CTRAMP tour_id is tour_num
        pl.col("tour_id").alias("_tour_id_canonical"),  # Temp column for joining with trips
        pl.col("tour_category_ctramp").alias("tour_category"),
        pl.col("tour_purpose_ctramp").alias("tour_purpose"),
        pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
        pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
        pl.col("start_hour").cast(pl.Int64),
        pl.col("end_hour").cast(pl.Int64),
        pl.col("tour_mode_ctramp").alias("tour_mode"),
        pl.col("atWork_freq").cast(pl.Int64),
        pl.col("num_ob_stops").cast(pl.Int64),
        pl.col("num_ib_stops").cast(pl.Int64),
    ]

    # Add weight and sampleRate if tour_weight exists
    if "tour_weight" in individual_tours.columns:
        individual_tours = individual_tours.with_columns(
            pl.when(pl.col("tour_weight") > 0)
            .then(pl.col("tour_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate")
        )
        output_cols.extend([pl.col("tour_weight"), pl.col("sampleRate")])

    individual_tours_ctramp = individual_tours.select(output_cols)

    logger.info("Formatted %d individual tour records", len(individual_tours_ctramp))
    return individual_tours_ctramp


def format_joint_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format joint tours to CT-RAMP specification.

    Transforms joint tour data with participant aggregation.

    Args:
        tours_canonical: Canonical tours DataFrame with tour_id, joint_tour_id, hh_id,
            person_id, person_num, tour_category, tour_purpose, o_taz, d_taz,
            origin_depart_time, origin_arrive_time, tour_mode, subtour_num
        linked_trips_canonical: Canonical trips DataFrame with joint_tour_id, tour_direction,
            person_id
        persons_canonical: Canonical persons DataFrame with person_id, person_num,
            age_category (for composition determination)
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds and age_adult category

    Returns:
        DataFrame with CT-RAMP joint tour fields

    Notes:
        - Filters to joint tours only (joint_tour_id IS NOT NULL)
        - Aggregates participants into space-separated string
        - Determines composition from participant ages
    """
    logger.info("Formatting joint tour data for CT-RAMP")

    # Handle empty tours DataFrame
    if len(tours_canonical) == 0:
        logger.info("No tours provided")
        return pl.DataFrame()

    # Filter to joint tours only
    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())

    if len(joint_tours) == 0:
        logger.info("No joint tours found")
        return pl.DataFrame()

    # Join person_num for sorting participants
    joint_tours = joint_tours.join(
        persons_canonical.select(["person_id", "person_num", "age"]),
        on="person_id",
        how="left",
    )

    # Group by joint_tour_id to aggregate participants
    # This assumes tours table has person_num for each participant
    participants_agg = joint_tours.group_by("joint_tour_id").agg(
        [
            pl.col("hh_id").first(),
            pl.col("person_num").sort().cast(pl.Utf8).str.join(" ").alias("Participants"),
            pl.col("tour_category").first(),
            pl.col("tour_purpose").first(),
            pl.col(f"o_{config.taz_field}").first(),
            pl.col(f"d_{config.taz_field}").first(),
            pl.col("origin_depart_time").first(),
            pl.col("origin_arrive_time").first(),
            pl.col("tour_mode").first(),
            pl.col("subtour_num").first(),
        ]
    )

    # Join with persons to determine composition
    participants_with_ages = joint_tours.join(
        persons_canonical.select(["person_id", "age"]),
        on="person_id",
        how="left",
    )

    composition_agg = (
        participants_with_ages.group_by("joint_tour_id")
        .agg(
            [
                (pl.col("age") < config.age_adult).any().alias("has_children"),
                (pl.col("age") >= config.age_adult).any().alias("has_adults"),
            ]
        )
        .with_columns(
            pl.when(pl.col("has_children") & pl.col("has_adults"))
            .then(pl.lit(TourComposition.ADULTS_AND_CHILDREN.value))
            .when(pl.col("has_children"))
            .then(pl.lit(TourComposition.CHILDREN_ONLY.value))
            .otherwise(pl.lit(TourComposition.ADULTS_ONLY.value))
            .alias("Composition")
        )
    )

    # Join aggregations
    joint_tours_formatted = participants_agg.join(
        composition_agg.select(["joint_tour_id", "Composition"]),
        on="joint_tour_id",
        how="left",
    )

    # Join with households for income
    joint_tours_formatted = joint_tours_formatted.join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Calculate all trip-based metrics in a single aggregation
    # Stops = trips - 1 (number of intermediate destinations)
    # Ensure stops never goes below 0 (when there are 0 trips in a direction)
    if len(linked_trips_canonical) > 0:
        joint_trip_stats = (
            linked_trips_canonical.filter(pl.col("joint_tour_id").is_not_null())
            .group_by("joint_tour_id")
            .agg(
                [
                    pl.col("person_id").n_unique().alias("num_travelers"),
                    (
                        pl.when(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
                        .then(1)
                        .sum()
                        - 1
                    )
                    .clip(lower_bound=0)
                    .alias("num_ob_stops"),
                    (
                        pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
                        .then(1)
                        .sum()
                        - 1
                    )
                    .clip(lower_bound=0)
                    .alias("num_ib_stops"),
                    pl.when(pl.col("tour_direction") == TourDirection.SUBTOUR.value)
                    .then(1)
                    .sum()
                    .alias("num_subtour_stops"),
                ]
            )
        )

        joint_tours_formatted = joint_tours_formatted.join(
            joint_trip_stats, on="joint_tour_id", how="left"
        ).with_columns(
            [
                pl.col("num_ob_stops").fill_null(0),
                pl.col("num_ib_stops").fill_null(0),
                pl.col("num_subtour_stops").fill_null(0),
            ]
        )
    else:
        joint_tours_formatted = joint_tours_formatted.with_columns(
            [
                pl.lit(None).cast(pl.Int64).alias("num_travelers"),
                pl.lit(0).alias("num_ob_stops"),
                pl.lit(0).alias("num_ib_stops"),
                pl.lit(0).alias("num_subtour_stops"),
            ]
        )

    # Map purpose and mode
    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            map_purpose_category_to_ctramp(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),  # Joint tours not for school
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("tour_purpose_ctramp"),
            map_mode_to_ctramp(
                pl.col("tour_mode"),
                pl.col("num_travelers"),
                None,  # Tours don't have access/egress modes
                None,
            ).alias("tour_mode_ctramp"),
        ]
    )

    # Convert times
    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            pl.col("origin_depart_time").dt.hour().alias("start_hour"),
            pl.col("origin_arrive_time").dt.hour().alias("end_hour"),
        ]
    )

    # Map tour_category to string labels (joint tours are always JOINT_NON_MANDATORY)
    joint_tours_formatted = joint_tours_formatted.with_columns(
        pl.lit(CTRAMPTourCategory.JOINT_NON_MANDATORY).alias("tour_category")
    )

    # Validate that no joint tours have zero trips
    # Check if joint_tour_id appears in trips at all, not just stop counts
    if len(linked_trips_canonical) > 0:
        joint_tours_with_trips = (
            linked_trips_canonical.filter(pl.col("joint_tour_id").is_not_null())
            .select("joint_tour_id")
            .unique()
        )
        if len(joint_tours_with_trips) > 0:
            zero_trip_tours = joint_tours_formatted.filter(
                ~pl.col("joint_tour_id").is_in(joint_tours_with_trips["joint_tour_id"].implode())
            )
            if len(zero_trip_tours) > 0:
                tour_ids = zero_trip_tours["joint_tour_id"].to_list()
                msg = (
                    f"Found {len(zero_trip_tours)} joint tours with zero trips. "
                    f"Joint tour IDs: {tour_ids[:10]}"
                    f"{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
                )
                raise ValueError(msg)

    # Select final columns with snake_case names
    joint_tours_ctramp = joint_tours_formatted.select(
        [
            pl.col("hh_id"),
            pl.col("joint_tour_id").alias("tour_id"),
            pl.col("tour_category"),
            pl.col("tour_purpose_ctramp").alias("tour_purpose"),
            pl.col("Composition").alias("tour_composition"),
            pl.col("Participants").alias("tour_participants"),
            pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
            pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
            pl.col("start_hour").cast(pl.Int64),
            pl.col("end_hour").cast(pl.Int64),
            pl.col("tour_mode_ctramp").alias("tour_mode"),
            pl.col("num_ob_stops").cast(pl.Int64),
            pl.col("num_ib_stops").cast(pl.Int64),
        ]
    )

    logger.info("Formatted %d joint tour records", len(joint_tours_ctramp))
    return joint_tours_ctramp
