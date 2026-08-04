"""Joint-trip formatting for CT-RAMP."""

import logging

import polars as pl

from data_canon.codebook.ctramp import CTRAMPPersonType, CTRAMPTourCategory
from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import TNCType

from .ctramp_config import CTRAMPConfig
from .mode_mappings import aggregate_transit_submode, ctramp_mode_expression
from .purpose_mappings import ctramp_purpose_category_expression

logger = logging.getLogger(__name__)


def format_joint_trip(
    joint_trips_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    unlinked_trips_canonical: pl.DataFrame,
    tours_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format joint trips to CT-RAMP specification.

    Transforms joint trip data using mean coordinates from joint_trips table.
    Links trips to joint tours using aggregated joint trip data. Maintains
    participant information for each trip leg.

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
        unlinked_trips_canonical: Canonical unlinked trips DataFrame with
            linked_trip_id, mode_type, access_mode, egress_mode, transit_submode,
            tnc_type; may be empty but must be provided

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

    # Derive the transit submode per joint trip from detailed unlinked-trip modes,
    # bridging unlinked segments to joint_trip_id via linked_trip_id.
    if not unlinked_trips_canonical.is_empty():
        submode_by_linked = aggregate_transit_submode(unlinked_trips_canonical, "linked_trip_id")
        submode_by_joint = (
            joint_linked_trips.select(["joint_trip_id", "linked_trip_id"])
            .join(submode_by_linked, on="linked_trip_id", how="left")
            .group_by("joint_trip_id")
            .agg(
                pl.col("transit_submode").max().alias("transit_submode"),
                # Aggregate TNC type across segments (POOLED wins, else min).
                pl.when((pl.col("tnc_type") == TNCType.POOLED.value).any())
                .then(pl.lit(TNCType.POOLED.value))
                .otherwise(pl.col("tnc_type").min())
                .alias("tnc_type"),
            )
        )
        joint_trips_formatted = joint_trips_formatted.join(
            submode_by_joint, on="joint_trip_id", how="left"
        )
        trip_submode_expr = pl.col("transit_submode")
        tnc_type = pl.col("tnc_type")
    else:
        trip_submode_expr = None
        tnc_type = None

    # Join with tour context
    joint_trips_formatted = joint_trips_formatted.join(
        tours_canonical.select(["tour_id", "joint_tour_id", "tour_purpose", "tour_mode"]),
        on="tour_id",
        how="left",
    )

    # Derive tour-level access/egress from the joint tour's first transit trip,
    # mirroring format_joint_tour so the joint tour mode reflects access/egress.
    tour_access_egress = joint_linked_trips.group_by("tour_id").agg(
        pl.col("access_mode")
        .filter(pl.col("access_mode").is_not_null())
        .first()
        .alias("tour_access_mode"),
        pl.col("egress_mode")
        .filter(pl.col("egress_mode").is_not_null())
        .first()
        .alias("tour_egress_mode"),
    )
    joint_trips_formatted = joint_trips_formatted.join(tour_access_egress, on="tour_id", how="left")

    # Derive tour-level transit submode / TNC type per joint tour, mirroring
    # format_joint_tour so the joint tour mode uses the tour's highest submode
    # rather than a single trip leg's submode.
    if not unlinked_trips_canonical.is_empty():
        tour_submode = aggregate_transit_submode(unlinked_trips_canonical, "joint_tour_id").rename(
            {"transit_submode": "tour_transit_submode", "tnc_type": "tour_tnc_type"}
        )
        joint_trips_formatted = joint_trips_formatted.join(
            tour_submode, on="joint_tour_id", how="left"
        )
        tour_submode_expr = pl.col("tour_transit_submode")
        tour_tnc_type = pl.col("tour_tnc_type")
    else:
        tour_submode_expr = None
        tour_tnc_type = None  # Derive tour-level transit submode / TNC type per joint tour,

    # Filter to only trips on joint tours
    joint_trips_formatted = joint_trips_formatted.filter(pl.col("joint_tour_id").is_not_null())

    # Join with households
    hh_cols = ["hh_id", "income"]
    if "hh_weight" in households_ctramp.columns:
        hh_cols.append("hh_weight")
    joint_trips_formatted = joint_trips_formatted.join(
        households_ctramp.select(hh_cols),
        on="hh_id",
        how="left",
    )

    # Map purposes and mode using mean locations
    joint_trips_formatted = joint_trips_formatted.with_columns(
        [
            ctramp_purpose_category_expression(
                pl.col("o_purpose_category"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                pl.lit(CTRAMPPersonType.NON_WORKER.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
                purpose_kind="trip",
            ).alias("orig_purpose"),
            ctramp_purpose_category_expression(
                pl.col("d_purpose_category"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                pl.lit(CTRAMPPersonType.NON_WORKER.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
                purpose_kind="trip",
            ).alias("dest_purpose"),
            ctramp_purpose_category_expression(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                pl.lit(CTRAMPPersonType.NON_WORKER.value),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
            ).alias("tour_purpose_ctramp"),
            ctramp_mode_expression(
                pl.col("mode_type"),
                pl.col("num_travelers").fill_null(2),
                pl.col("access_mode"),
                pl.col("egress_mode"),
                trip_submode_expr,
                tnc_type,
            ).alias("trip_mode"),
            ctramp_mode_expression(
                pl.col("tour_mode"),
                pl.col("num_travelers").fill_null(2),
                pl.col("tour_access_mode"),
                pl.col("tour_egress_mode"),
                tour_submode_expr,
                tour_tnc_type,
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

    # Create stop_id: trips within each half-tour leg are numbered 0-based in
    # order of departure; a leg consisting of a single trip (no intermediate
    # stop) is assigned -1. Directions are split so outbound and inbound legs are
    # numbered independently.
    joint_trips_formatted = (
        joint_trips_formatted.with_columns(
            pl.when(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
            .then(pl.lit("outbound"))
            .when(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .then(pl.lit("inbound"))
            .otherwise(pl.lit("subtour"))
            .alias("tour_direction_str")
        )
        .sort(["joint_tour_id", "tour_direction_str", "depart_time", "arrive_time"])
        .with_columns(
            (
                pl.col("depart_time").rank("ordinal").over(["joint_tour_id", "tour_direction_str"])
                - 1
            )
            .cast(pl.Int64)
            .alias("_stop_seq")
        )
        .with_columns(
            # Single-trip leg (max 0-based sequence is 0) has no intermediate stop
            # and is assigned -1; otherwise keep the 0-based sequence.
            pl.when(pl.col("_stop_seq").max().over(["joint_tour_id", "tour_direction_str"]) == 0)
            .then(pl.lit(-1))
            .otherwise(pl.col("_stop_seq"))
            .cast(pl.Int64)
            .alias("stop_id")
        )
        .drop("_stop_seq")
    )

    # CT-RAMP joint tour_id is 0-based per household (0=first joint tour, ...).
    joint_trips_formatted = joint_trips_formatted.with_columns(
        (pl.col("joint_tour_id").rank("dense").over("hh_id") - 1)
        .cast(pl.Int64)
        .alias("_ctramp_joint_tour_id")
    )

    # The joint trip weight comes from the cascade on joint_trips_canonical; carry
    # it through and express it as a sample rate, as individual trips do.
    weight_cols: list[pl.Expr] = []
    if "joint_trip_weight" in joint_trips_formatted.columns:
        weight_cols = [
            pl.col("joint_trip_weight"),
            pl.when(pl.col("joint_trip_weight") > 0)
            .then(pl.col("joint_trip_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate"),
        ]

    # Select final columns with snake_case names
    select_cols = [
        pl.col("hh_id"),
        # WARNING: Canonical processing uses joint_tour_id. CT-RAMP's legacy
        # joint-trip schema calls this household-scoped joint-tour number tour_id.
        # Rename only at the output boundary to avoid conflating the identifiers.
        pl.col("_ctramp_joint_tour_id").alias("tour_id"),
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
        # All joint tours are JOINT_NON_MANDATORY.
        pl.lit(CTRAMPTourCategory.JOINT_NON_MANDATORY.value).alias("tour_category"),
        pl.col("num_joint_travelers").cast(pl.Int64).alias("num_participants"),
        pl.col("depart_hour").cast(pl.Int64),
        pl.col("trip_time"),
        *weight_cols,
    ]
    joint_trips_ctramp = joint_trips_formatted.select(select_cols)

    logger.info("Formatted %d joint trip records", len(joint_trips_ctramp))
    return joint_trips_ctramp
