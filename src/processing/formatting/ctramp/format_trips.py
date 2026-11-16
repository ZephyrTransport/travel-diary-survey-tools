"""Individual trip formatting for CT-RAMP (joint trips: ``format_joint_trips``)."""

import logging

import polars as pl

from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import Purpose
from utils.helpers import METERS_PER_MILE

from .ctramp_config import CTRAMPConfig
from .mode_mappings import aggregate_transit_submode, ctramp_mode_expression
from .output_columns import add_inbound_and_stop_id, sample_rate
from .purpose_mappings import ctramp_purpose_category_expression

logger = logging.getLogger(__name__)


def _log_purpose_mapping_counts(
    trips: pl.DataFrame, purpose_col: str, ctramp_col: str, trip_end: str
) -> None:
    """Log value counts for detailed survey ``Purpose`` vs the resulting CT-RAMP trip purpose.

    Reports, for each combination of the detailed linked-trip purpose
    (``o_purpose``/``d_purpose``) and the derived CT-RAMP trip purpose
    (``orig_purpose``/``dest_purpose``), how many trips fall into that pair.
    Useful for auditing how the detailed survey purposes collapse into
    ``CTRAMPTripPurpose``.
    """
    if trips.is_empty() or purpose_col not in trips.columns or ctramp_col not in trips.columns:
        return

    purpose_labels = {p.value: p.label for p in Purpose}
    counts = (
        trips.group_by([purpose_col, ctramp_col])
        .agg(pl.len().alias("n"))
        .sort([purpose_col, "n"], descending=[False, True])
    )

    lines = [
        f"Trip {trip_end} purpose mapping counts ({purpose_col} -> {ctramp_col}):",
        f"  {'count':>8}  {'detailed Purpose':<48}  {ctramp_col}",
    ]
    for row in counts.iter_rows(named=True):
        code = row[purpose_col]
        label = purpose_labels.get(code, "unknown")
        lines.append(f"  {row['n']:>8}  {f'{label} ({code})':<48}  {row[ctramp_col]}")
    logger.info("\n".join(lines))


def format_individual_trip(
    linked_trips_canonical: pl.DataFrame,
    unlinked_trips_canonical: pl.DataFrame,
    tours_ctramp: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format individual trips to CT-RAMP specification.

    Transforms linked trip data (for individual tours only) to CT-RAMP format.
    Links trips to individual tours via tour_id. Includes stop purpose, mode,
    location, and sequence within tour. Distinguishes outbound, inbound, and
    subtour trips.

    Args:
        linked_trips_canonical: Canonical DataFrame with linked trip fields (tour_id,
            o_purpose_category, d_purpose_category, mode_type, o_taz, d_taz,
            tour_direction, depart_time, arrive_time, person_id, hh_id)
        tours_ctramp: Formatted CT-RAMP individual tours DataFrame (already filtered to
            individual tours only, without joint_tour_id). Contains tour_id,
            tour_purpose, tour_mode, tour_category
        persons_canonical: Canonical persons DataFrame with person_id, person_num,
            school_type and employment
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds
        unlinked_trips_canonical: Canonical unlinked trips DataFrame with
            linked_trip_id, mode_type, access_mode, egress_mode, transit_submode,
            tnc_type; may be empty but must be provided

    Returns:
        DataFrame with CT-RAMP individual trip fields

    Notes:
        - Excludes trips on joint tours
        - stop_id numbers each half-tour leg's trips from 0, or -1 for a leg of
          one trip (see ``add_inbound_and_stop_id``)
        - Excludes model-only fields (parking costs, value of time, etc.)
    """
    logger.info("Formatting individual trip data for CT-RAMP")

    if len(tours_ctramp) == 0 or len(linked_trips_canonical) == 0:
        logger.info("No tours or trips provided")
        return pl.DataFrame()

    # tours_ctramp keeps the canonical tour id as _tour_id_canonical.
    individual_trips = linked_trips_canonical.filter(
        pl.col("tour_id").is_in(tours_ctramp["_tour_id_canonical"].implode())
    )

    # The tour fields come from tours_ctramp, already formatted; the canonical
    # tour_id is renamed so the CT-RAMP one can take its name.
    conflicting_fields = set(individual_trips.columns).intersection(
        {"tour_purpose", "tour_mode", "tour_category"}
    )
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

    individual_trips = individual_trips.join(
        persons_canonical.select(["person_id", "person_num", "school_type", "employment"]),
        on="person_id",
        how="left",
    ).join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Subtour trips are numbered with the outbound leg. The canonical tour id is
    # unique across persons and days, so no two tours are numbered together.
    individual_trips = add_inbound_and_stop_id(
        individual_trips,
        "_canonical_tour_id",
        pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
        .then(pl.lit("inbound"))
        .otherwise(pl.lit("outbound")),
    )

    individual_trips = individual_trips.with_columns(
        ctramp_purpose_category_expression(
            pl.col("o_purpose_category"),
            pl.col("income"),
            pl.col("school_type"),
            pl.col("employment"),
            config,
            purpose_kind="trip",
        ).alias("orig_purpose"),
        ctramp_purpose_category_expression(
            pl.col("d_purpose_category"),
            pl.col("income"),
            pl.col("school_type"),
            pl.col("employment"),
            config,
            purpose_kind="trip",
        ).alias("dest_purpose"),
    )
    _log_purpose_mapping_counts(individual_trips, "o_purpose", "orig_purpose", "origin")
    _log_purpose_mapping_counts(individual_trips, "d_purpose", "dest_purpose", "destination")

    if not unlinked_trips_canonical.is_empty():
        submode_by_trip = aggregate_transit_submode(unlinked_trips_canonical, "linked_trip_id")
        individual_trips = individual_trips.join(submode_by_trip, on="linked_trip_id", how="left")
        trip_submode_expr = pl.col("transit_submode")
        tnc_type = pl.col("tnc_type")
    else:
        trip_submode_expr = None
        tnc_type = None

    individual_trips = individual_trips.with_columns(
        ctramp_mode_expression(
            pl.col("mode_type"),
            pl.col("num_travelers"),
            pl.col("access_mode"),
            pl.col("egress_mode"),
            trip_submode_expr,
            tnc_type,
        ).alias("trip_mode")
    )

    individual_trips = individual_trips.with_columns(
        pl.col("depart_time").dt.hour().alias("depart_hour")
    ).with_columns((pl.col("distance_meters") / METERS_PER_MILE).alias("distance_survey"))

    individual_trips_ctramp = individual_trips.with_columns(
        pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
        pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
        pl.lit(0).cast(pl.Int64).alias("parking_taz"),  # no parking zone
        pl.col("depart_hour").cast(pl.Int64),
    )

    if "linked_trip_weight" in individual_trips_ctramp.columns:
        individual_trips_ctramp = individual_trips_ctramp.with_columns(
            pl.col("linked_trip_weight").alias("trip_weight"),
            sample_rate("linked_trip_weight"),
        )

    logger.info("Formatted %d individual trip records", len(individual_trips_ctramp))
    return individual_trips_ctramp
