"""Tour formatting for CT-RAMP.

Transforms canonical tour data into CT-RAMP model format, including:

- Individual tours (non-joint tours)
- Joint tours (household member group tours)
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    AtWorkFreq,
    CTRAMPEmploymentCategory,
    CTRAMPTourCategory,
    CTRAMPTourPurpose,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.mode_mappings import (
    aggregate_transit_submode,
    ctramp_mode_expression,
)
from processing.formatting.ctramp.person_mappings import EMPLOYMENT_TO_CTRAMP
from processing.formatting.ctramp.purpose_mappings import ctramp_purpose_category_expression
from processing.formatting.ctramp.student_mappings import ctramp_student_category_expression

from .ctramp_config import CTRAMPConfig
from .format_joint_tours import (
    format_joint_tour,
    identify_misclassified_joint_tours,
)
from .format_persons import enrich_persons_with_person_type

logger = logging.getLogger(__name__)

__all__ = ["format_individual_tour", "format_joint_tour"]

TWO_SUBTOURS = 2


def _prepare_individual_tours(
    tours: pl.DataFrame,
    persons: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Select individual tours and add person, income, purpose, and category fields."""
    tours = identify_misclassified_joint_tours(tours)
    if "person_type" not in persons.columns or "type" not in persons.columns:
        logger.info("Deriving person_type for tour formatting")
        if "student_category" not in persons.columns:
            persons = persons.with_columns(
                ctramp_student_category_expression(school_taz_col="school_taz").alias(
                    "student_category"
                )
            )
        if "employment_category" not in persons.columns:
            persons = persons.with_columns(
                pl.col("employment")
                .replace_strict(
                    EMPLOYMENT_TO_CTRAMP,
                    default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
                )
                .alias("employment_category")
            )
        persons = enrich_persons_with_person_type(persons)

    individual_tours = (
        tours.filter(pl.col("joint_tour_id").is_null())
        .join(
            persons.select(["person_id", "person_num", "person_type", "school_type"]),
            on="person_id",
            how="left",
        )
        .join(
            households_ctramp.select(["hh_id", "income"]),
            on="hh_id",
            how="left",
        )
    )

    parent_tour_purposes = individual_tours.select(
        [
            pl.col("tour_id").alias("_parent_tour_id_for_join"),
            pl.col("tour_purpose").alias("_parent_tour_purpose"),
        ]
    )
    individual_tours = individual_tours.join(
        parent_tour_purposes,
        left_on="parent_tour_id",
        right_on="_parent_tour_id_for_join",
        how="left",
    ).with_columns(
        ctramp_purpose_category_expression(
            pl.col("tour_purpose"),
            pl.col("income"),
            pl.col("school_type"),
            pl.col("person_type"),
            config.income_low_threshold,
            config.income_med_threshold,
            config.income_high_threshold,
            pl.col("_parent_tour_purpose"),
            pl.col("parent_tour_id") != pl.col("tour_id"),
        ).alias("tour_purpose_ctramp")
    )

    mandatory_purposes = [PurposeCategory.WORK.value, PurposeCategory.SCHOOL.value]
    return individual_tours.with_columns(
        pl.when(
            (pl.col("parent_tour_id") != pl.col("tour_id"))
            & (pl.col("_parent_tour_purpose") == PurposeCategory.WORK.value)
        )
        .then(pl.lit(CTRAMPTourCategory.AT_WORK.value))
        .when(pl.col("tour_purpose").is_in(mandatory_purposes))
        .then(pl.lit(CTRAMPTourCategory.MANDATORY.value))
        .otherwise(pl.lit(CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value))
        .alias("tour_category_ctramp")
    ).drop("_parent_tour_purpose")


def _add_at_work_frequency(individual_tours: pl.DataFrame) -> pl.DataFrame:
    """Add CT-RAMP's at-work subtour-frequency category."""
    subtour_counts = (
        individual_tours.filter(pl.col("parent_tour_id") != pl.col("tour_id"))
        .group_by("parent_tour_id")
        .agg(
            [
                pl.len().alias("_subtour_total"),
                (pl.col("tour_purpose_ctramp") == CTRAMPTourPurpose.ATWORK_EAT.value)
                .sum()
                .alias("_subtour_eat_count"),
                (pl.col("tour_purpose_ctramp") == CTRAMPTourPurpose.ATWORK_BUSINESS.value)
                .sum()
                .alias("_subtour_business_count"),
                (pl.col("tour_purpose_ctramp") == CTRAMPTourPurpose.ATWORK_MAINT.value)
                .sum()
                .alias("_subtour_maint_count"),
            ]
        )
    )
    return (
        individual_tours.join(
            subtour_counts,
            left_on="tour_id",
            right_on="parent_tour_id",
            how="left",
        )
        .with_columns(
            pl.when(pl.col("tour_purpose") != PurposeCategory.WORK.value)
            .then(pl.lit(AtWorkFreq.NONE_NOT_WORK.value))
            .when(pl.col("_subtour_total").fill_null(0) == 0)
            .then(pl.lit(AtWorkFreq.NO_SUBTOUR.value))
            .when((pl.col("_subtour_total") == 1) & (pl.col("_subtour_eat_count") == 1))
            .then(pl.lit(AtWorkFreq.ONE_EAT.value))
            .when((pl.col("_subtour_total") == 1) & (pl.col("_subtour_business_count") == 1))
            .then(pl.lit(AtWorkFreq.ONE_BUSINESS.value))
            .when((pl.col("_subtour_total") == 1) & (pl.col("_subtour_maint_count") == 1))
            .then(pl.lit(AtWorkFreq.ONE_MAINT.value))
            .when(
                (pl.col("_subtour_total") == TWO_SUBTOURS)
                & (pl.col("_subtour_business_count") == TWO_SUBTOURS)
            )
            .then(pl.lit(AtWorkFreq.TWO_BUSINESS.value))
            .when(
                (pl.col("_subtour_total") == TWO_SUBTOURS)
                & (pl.col("_subtour_business_count") == 1)
                & (pl.col("_subtour_eat_count") == 1)
            )
            .then(pl.lit(AtWorkFreq.ONE_EAT_ONE_BUSINESS.value))
            .otherwise(pl.lit(AtWorkFreq.OTHER.value))
            .alias("atWork_freq")
        )
        .drop(
            [
                "_subtour_total",
                "_subtour_eat_count",
                "_subtour_business_count",
                "_subtour_maint_count",
            ]
        )
    )


def _add_stop_counts_and_validate(
    individual_tours: pl.DataFrame, linked_trips: pl.DataFrame
) -> pl.DataFrame:
    """Add directional stop counts and reject base tours without trips."""
    if linked_trips.is_empty():
        outbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ob_stops": []},
            schema={"tour_id": pl.Int64, "num_ob_stops": pl.UInt32},
        )
        inbound_stops = pl.DataFrame(
            {"tour_id": [], "num_ib_stops": []},
            schema={"tour_id": pl.Int64, "num_ib_stops": pl.UInt32},
        )
        tours_with_trips = pl.DataFrame({"tour_id": []}, schema={"tour_id": pl.Int64})
    else:
        outbound_stops = (
            linked_trips.filter(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
            .group_by("tour_id")
            .agg((pl.len() - 1).alias("num_ob_stops"))
        )
        inbound_stops = (
            linked_trips.filter(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .group_by("tour_id")
            .agg((pl.len() - 1).alias("num_ib_stops"))
        )
        tours_with_trips = linked_trips.select("tour_id").unique()

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
    zero_trip_tours = individual_tours.filter(
        ~pl.col("tour_id").is_in(tours_with_trips["tour_id"].implode())
        & (pl.col("subtour_num") == 0)
    )
    if not zero_trip_tours.is_empty():
        tour_ids = zero_trip_tours["tour_id"].to_list()
        msg = (
            f"Found {len(zero_trip_tours)} tours with zero trips. "
            f"Tour IDs: {tour_ids[:10]}{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
        )
        raise ValueError(msg)
    return individual_tours


def _assign_ctramp_tour_ids(individual_tours: pl.DataFrame) -> pl.DataFrame:
    """Add CT-RAMP's per-person tour identifier to canonical tours.

    CT-RAMP numbers a person's home-based tours from zero. At-work subtours use
    a two-digit identifier whose first digit is the 1-based parent-tour number
    and whose second digit is the subtour sequence number. For example, 12 is
    the second subtour of the person's first tour, whose own identifier is 0.

    A subtour whose parent is absent after filtering is treated as a base tour,
    ensuring every surviving record receives an identifier.
    """
    is_subtour_candidate = pl.col("parent_tour_id").is_not_null() & (
        pl.col("parent_tour_id") != pl.col("tour_id")
    )
    individual_tours = individual_tours.with_columns(
        is_subtour_candidate.alias("_is_subtour_candidate")
    )

    parent_keys = individual_tours.filter(~pl.col("_is_subtour_candidate"))["tour_id"]
    individual_tours = individual_tours.with_columns(
        (
            pl.col("_is_subtour_candidate") & pl.col("parent_tour_id").is_in(parent_keys.implode())
        ).alias("_is_subtour")
    )

    parent_idx = (
        individual_tours.filter(~pl.col("_is_subtour"))
        .select(["person_id", "tour_id"])
        .with_columns(
            (pl.col("tour_id").rank("ordinal").over("person_id") - 1)
            .cast(pl.Int64)
            .alias("_ctramp_parent_idx")
        )
        .select(
            pl.col("tour_id").alias("_parent_key"),
            pl.col("_ctramp_parent_idx"),
        )
    )

    subtour_seq = (
        individual_tours.filter(pl.col("_is_subtour"))
        .select(["tour_id", "parent_tour_id"])
        .with_columns(
            pl.col("tour_id")
            .rank("ordinal")
            .over("parent_tour_id")
            .cast(pl.Int64)
            .alias("_subtour_seq")
        )
        .select(["tour_id", "_subtour_seq"])
    )

    individual_tours = (
        individual_tours.with_columns(
            pl.when(pl.col("_is_subtour"))
            .then(pl.col("parent_tour_id"))
            .otherwise(pl.col("tour_id"))
            .alias("_parent_key")
        )
        .join(parent_idx, on="_parent_key", how="left")
        .join(subtour_seq, on="tour_id", how="left")
    )

    return individual_tours.with_columns(
        pl.when(pl.col("_is_subtour"))
        .then((pl.col("_ctramp_parent_idx") + 1) * 10 + pl.col("_subtour_seq"))
        .otherwise(pl.col("_ctramp_parent_idx"))
        .cast(pl.Int64)
        .alias("_ctramp_tour_id")
    ).drop(
        [
            "_is_subtour_candidate",
            "_is_subtour",
            "_parent_key",
            "_ctramp_parent_idx",
            "_subtour_seq",
        ]
    )


def format_individual_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    unlinked_trips_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format individual tours to CT-RAMP specification.

    Transforms tour data (excluding joint tours) to CT-RAMP format.
    Key Transformations:

    - Filtering: Excludes joint tours (processes only tours with
      joint_tour_id IS NULL)
    - Purpose Mapping: Converts canonical purpose categories to CT-RAMP tour
      purposes
    - Mode Translation: Maps canonical mode types to CT-RAMP mode codes
    - Time-of-Day: Extracts start/end hours from departure/arrival times
    - Stop Counts: Computes outbound and inbound stop counts from linked trips
        - Subtour Frequency: Encodes atWork_freq using CT-RAMP categorical values
            based on work subtour pattern
    - Excludes: Model simulation fields (random numbers, wait times, logsums)

    Args:
        tours_canonical: Canonical tours DataFrame with tour_id, hh_id, person_id,
            person_num, tour_category, tour_purpose, o_taz, d_taz, origin_depart_time,
            origin_arrive_time, tour_mode, joint_tour_id (for filtering), parent_tour_id
            (for subtour counting)
        linked_trips_canonical: Canonical trips DataFrame with tour_id, tour_direction
            (1=outbound, 2=inbound, 3=subtour)
        unlinked_trips_canonical: Unlinked canonical trips used to derive transit
            submode; may be empty but must be provided
        persons_canonical: Canonical persons DataFrame with person_id, person_num,
            person_type (for mode mapping), school_type (for purpose mapping) are optional
            but re-derived if missing or invalid
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds

    Returns:
        DataFrame with CT-RAMP individual tour fields:

            - hh_id, person_id, person_num, person_type
            - tour_id, tour_category, tour_purpose
            - orig_taz, dest_taz
            - start_hour, end_hour
            - tour_mode
            - atWork_freq (subtour frequency category)
            - num_ob_stops, num_ib_stops

    Notes:
        - Excludes joint tours (joint_tour_id IS NULL)
        - Excludes all model-only fields (random numbers, wait times, logsums)
    """
    logger.info("Formatting individual tour data for CT-RAMP")

    individual_tours = _prepare_individual_tours(
        tours_canonical, persons_canonical, households_ctramp, config
    )

    individual_tours = _add_at_work_frequency(individual_tours)

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

        # Get access/egress for tour's transit trip
        # Access and egress is based on the first transit trip
        tour_access_egress = (
            linked_trips_canonical.filter(pl.col("access_mode").is_not_null())
            .sort(["tour_id", "depart_time", "arrive_time"])
            .group_by("tour_id")
            .agg(
                [
                    pl.col("access_mode").first().alias("tour_access_mode"),
                    pl.col("egress_mode").first().alias("tour_egress_mode"),
                ]
            )
        )
        individual_tours = individual_tours.join(tour_access_egress, on="tour_id", how="left")
        access_expr = pl.col("tour_access_mode")
        egress_expr = pl.col("tour_egress_mode")
    else:
        num_travelers_expr = pl.lit(1)
        access_expr = None
        egress_expr = None

    # Derive the highest transit submode per tour from detailed unlinked-trip modes
    if not unlinked_trips_canonical.is_empty():
        submode_by_indiv_tour = aggregate_transit_submode(unlinked_trips_canonical, "tour_id")
        individual_tours = individual_tours.join(submode_by_indiv_tour, on="tour_id", how="left")
        indiv_submode_expr = pl.col("transit_submode")
        tnc_type = pl.col("tnc_type")
    else:
        indiv_submode_expr = None
        tnc_type = None

    individual_tours = individual_tours.with_columns(
        ctramp_mode_expression(
            pl.col("tour_mode"),
            num_travelers_expr,
            access_expr,
            egress_expr,
            indiv_submode_expr,
            tnc_type,
        ).alias("tour_mode_ctramp")
    )

    individual_tours = _add_stop_counts_and_validate(individual_tours, linked_trips_canonical)

    # Assign CT-RAMP tour_id: 0-based per person for parent tours, with at-work
    # subtours encoded as a two-digit integer <1-based parent tour #><subtour #>.
    individual_tours = _assign_ctramp_tour_ids(individual_tours)

    # Format columns to CTRAMP specifications
    individual_tours = individual_tours.with_columns(
        [
            pl.col("_ctramp_tour_id").alias("tour_id"),  # CTRAMP 0-based per-person tour id
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
    ).drop("_ctramp_tour_id")

    # Add sampleRate if tour_weight exists
    if "tour_weight" in individual_tours.columns:
        individual_tours = individual_tours.with_columns(
            pl.when(pl.col("tour_weight") > 0)
            .then(pl.col("tour_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate")
        )

    logger.info("Formatted %d individual tour records", len(individual_tours))
    return individual_tours
