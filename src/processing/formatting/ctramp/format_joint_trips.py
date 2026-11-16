"""Joint-trip formatting for CT-RAMP."""

import logging

import polars as pl

from data_canon.codebook.ctramp import CTRAMPTourCategory
from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection

from .ctramp_config import CTRAMPConfig
from .format_joint_tours import ctramp_joint_tour_numbers, joint_weight_columns
from .joint_representative import log_members_spanning_zones, select_representative_members
from .mode_mappings import aggregate_transit_submode, ctramp_mode_expression, tnc_type_agg
from .output_columns import add_inbound_and_stop_id
from .purpose_mappings import NO_EMPLOYMENT, ctramp_purpose_category_expression

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

    Writes one row per joint trip. A joint trip carries no location or time of
    its own, so one member stands for the group -- the same member across every
    leg of its joint tour, and for both the zones and the clock, so the record
    describes one real trip (see ``joint_representative``). Purposes, mode and
    direction come from the member trips; the tour fields from the joint tour.

    Args:
        joint_trips_canonical: Joint trips with joint_trip_id, hh_id and
            hh_day_id
        linked_trips_canonical: Canonical linked trips DataFrame to bridge joint_trip_id
            to tour_id (contains joint_trip_id, tour_id, o_purpose_category,
            d_purpose_category, mode_type, depart_time, arrive_time, num_travelers,
            and the ``o_``/``d_`` zone columns)
        tours_canonical: Canonical tours DataFrame with tour_id, joint_tour_id,
            tour_purpose, tour_category, tour_mode
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds
        unlinked_trips_canonical: Canonical unlinked trips DataFrame with
            linked_trip_id, mode_type, access_mode, egress_mode, transit_submode,
            tnc_type; may be empty but must be provided

    Returns:
        DataFrame with CT-RAMP joint trip fields
    """
    logger.info("Formatting joint trip data for CT-RAMP")
    if len(joint_trips_canonical) == 0:
        logger.info("No joint trips found")
        return pl.DataFrame()

    joint_linked_trips = linked_trips_canonical.filter(pl.col("joint_trip_id").is_not_null())
    if len(joint_linked_trips) == 0:
        logger.warning("No linked trips found with joint_trip_id")
        return pl.DataFrame()

    joint_trips_formatted = joint_trips_canonical.join(
        _member_trip_context(joint_linked_trips), on="joint_trip_id", how="left"
    )

    log_members_spanning_zones(joint_linked_trips, config.taz_field)
    representative = select_representative_members(joint_linked_trips).select(
        "joint_trip_id",
        f"o_{config.taz_field}",
        f"d_{config.taz_field}",
        "depart_time",
        "arrive_time",
    )
    joint_trips_formatted = joint_trips_formatted.join(
        representative, on="joint_trip_id", how="left"
    )

    joint_trips_formatted, trip_submode, trip_tnc_type = _join_trip_submode(
        joint_trips_formatted, joint_linked_trips, unlinked_trips_canonical
    )
    joint_trips_formatted, tour_submode, tour_tnc_type = _join_tour_fields(
        joint_trips_formatted, joint_linked_trips, unlinked_trips_canonical, tours_canonical
    )

    # Income only: a joint record's weight and rate come from its own columns, never
    # from hh_weight, which the cascade has since split and redistributed.
    joint_trips_formatted = joint_trips_formatted.filter(
        pl.col("joint_tour_id").is_not_null()
    ).join(households_ctramp.select(["hh_id", "income"]), on="hh_id", how="left")

    def purpose(col: str, purpose_kind: str) -> pl.Expr:
        return ctramp_purpose_category_expression(
            pl.col(col),
            pl.col("income"),
            pl.lit(SchoolType.MISSING.value),
            NO_EMPLOYMENT,
            config,
            purpose_kind=purpose_kind,
        )

    joint_trips_formatted = joint_trips_formatted.with_columns(
        purpose("o_purpose_category", "trip").alias("orig_purpose"),
        purpose("d_purpose_category", "trip").alias("dest_purpose"),
        purpose("tour_purpose", "tour").alias("tour_purpose_ctramp"),
        ctramp_mode_expression(
            pl.col("mode_type"),
            pl.col("num_travelers").fill_null(2),
            pl.col("access_mode"),
            pl.col("egress_mode"),
            trip_submode,
            trip_tnc_type,
        ).alias("trip_mode"),
        ctramp_mode_expression(
            pl.col("tour_mode"),
            pl.col("num_travelers").fill_null(2),
            pl.col("tour_access_mode"),
            pl.col("tour_egress_mode"),
            tour_submode,
            tour_tnc_type,
        ).alias("tour_mode_ctramp"),
    ).with_columns(
        pl.col("depart_time").dt.hour().alias("depart_hour"),
        (pl.col("arrive_time") - pl.col("depart_time"))
        .dt.total_minutes()
        .cast(pl.Int64)
        .alias("trip_time"),
    )

    # Outbound and inbound legs, and any subtour trips, are numbered apart.
    joint_trips_formatted = add_inbound_and_stop_id(
        joint_trips_formatted,
        "joint_tour_id",
        pl.when(pl.col("tour_direction") == TourDirection.OUTBOUND.value)
        .then(pl.lit("outbound"))
        .when(pl.col("tour_direction") == TourDirection.INBOUND.value)
        .then(pl.lit("inbound"))
        .otherwise(pl.lit("subtour")),
    )

    # Numbered from the tours, not from the joint trips present here: the two
    # files must agree on it even when one of them has lost a joint tour.
    joint_trips_formatted = joint_trips_formatted.join(
        ctramp_joint_tour_numbers(tours_canonical), on="joint_tour_id", how="left"
    )

    weight_cols = joint_weight_columns(
        joint_trips_formatted.columns, "joint_trip_weight", pl.col("_n_members")
    )
    joint_trips_ctramp = joint_trips_formatted.select(
        pl.col("hh_id"),
        # CT-RAMP calls the household-scoped joint-tour number tour_id; renamed
        # only here so it is never confused with the canonical joint_tour_id.
        pl.col("_ctramp_joint_tour_id").alias("tour_id"),
        pl.col("stop_id"),
        pl.col("inbound"),
        pl.col("tour_purpose_ctramp").alias("tour_purpose"),
        pl.col("orig_purpose"),
        pl.col("dest_purpose"),
        pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
        pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
        pl.lit(0).cast(pl.Int64).alias("parking_taz"),  # no parking zone
        pl.col("trip_mode"),
        pl.col("tour_mode_ctramp").alias("tour_mode"),
        pl.lit(CTRAMPTourCategory.JOINT_NON_MANDATORY.value).alias("tour_category"),
        # The count of weighted member trips, which CT-RAMP multiplies by to recover
        # person-trips; occupancy is already in trip_mode (SHARED2 vs SHARED3+).
        pl.col("_n_members").alias("num_participants"),
        pl.col("depart_hour").cast(pl.Int64),
        pl.col("trip_time"),
        *weight_cols,
    )

    logger.info("Formatted %d joint trip records", len(joint_trips_ctramp))
    return joint_trips_ctramp


def _member_trip_context(joint_linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Per joint trip: the member count and the first member's trip fields.

    ``_n_members`` is the member trips behind the weight -- the divisor back to a
    per-person rate, and what CT-RAMP multiplies by to recover person-trips.
    """
    return joint_linked_trips.group_by("joint_trip_id").agg(
        pl.len().cast(pl.Int64).alias("_n_members"),
        pl.col("tour_id").first(),
        pl.col("o_purpose_category").first(),
        pl.col("d_purpose_category").first(),
        pl.col("mode_type").first(),
        pl.col("num_travelers").first(),
        pl.col("tour_direction").first(),
        pl.col("access_mode").first(),
        pl.col("egress_mode").first(),
    )


def _join_trip_submode(
    joint_trips: pl.DataFrame,
    joint_linked_trips: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.Expr | None, pl.Expr | None]:
    """Join each joint trip's transit submode and TNC type, from its members' segments.

    Unlinked segments reach a joint trip through their ``linked_trip_id``.

    Returns:
        *joint_trips* with the columns joined, and the submode and TNC type
        expressions for ``ctramp_mode_expression`` (None without unlinked trips).
    """
    if unlinked_trips.is_empty():
        return joint_trips, None, None
    submode_by_linked = aggregate_transit_submode(unlinked_trips, "linked_trip_id")
    submode_by_joint = (
        joint_linked_trips.select(["joint_trip_id", "linked_trip_id"])
        .join(submode_by_linked, on="linked_trip_id", how="left")
        .group_by("joint_trip_id")
        .agg(pl.col("transit_submode").max().alias("transit_submode"), tnc_type_agg())
    )
    joint_trips = joint_trips.join(submode_by_joint, on="joint_trip_id", how="left")
    return joint_trips, pl.col("transit_submode"), pl.col("tnc_type")


def _join_tour_fields(
    joint_trips: pl.DataFrame,
    joint_linked_trips: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.Expr | None, pl.Expr | None]:
    """Join the joint tour's id, purpose and mode, and what its tour mode is coded from.

    The tour's access and egress modes are the first non-null ones among its
    member trips; its transit submode and TNC type are the tour's highest, not
    a single leg's.

    Returns:
        *joint_trips* with the columns joined, and the tour submode and TNC type
        expressions for ``ctramp_mode_expression`` (None without unlinked trips).
    """
    # Drop any carried joint_tour_id first: otherwise the join suffixes the tour
    # copy and the filter downstream reads the stale, un-reclassified value.
    joint_trips = joint_trips.drop("joint_tour_id", strict=False).join(
        tours.select(["tour_id", "joint_tour_id", "tour_purpose", "tour_mode"]),
        on="tour_id",
        how="left",
    )
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
    joint_trips = joint_trips.join(tour_access_egress, on="tour_id", how="left")

    if unlinked_trips.is_empty():
        return joint_trips, None, None
    tour_submode = aggregate_transit_submode(unlinked_trips, "joint_tour_id").rename(
        {"transit_submode": "tour_transit_submode", "tnc_type": "tour_tnc_type"}
    )
    joint_trips = joint_trips.join(tour_submode, on="joint_tour_id", how="left")
    return joint_trips, pl.col("tour_transit_submode"), pl.col("tour_tnc_type")
