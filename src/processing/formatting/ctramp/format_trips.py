"""Trip formatting for CT-RAMP.

Transforms canonical trip data into CT-RAMP model format, including:

- Individual trips (trips on individual tours)
- Joint trips (trips on joint tours)
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPEmploymentCategory,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import Purpose

from .ctramp_config import CTRAMPConfig
from .format_joint_trips import format_joint_trip
from .format_persons import enrich_persons_with_person_type
from .mode_mappings import (
    aggregate_transit_submode,
    ctramp_mode_expression,
)
from .person_mappings import EMPLOYMENT_TO_CTRAMP
from .purpose_mappings import ctramp_purpose_category_expression
from .student_mappings import ctramp_student_category_expression

logger = logging.getLogger(__name__)

__all__ = ["format_individual_trip", "format_joint_trip"]


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
        persons_canonical: Canonical persons DataFrame with person_id, person_num, school_type
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income
        config: CT-RAMP configuration with income thresholds
        unlinked_trips_canonical: Canonical unlinked trips DataFrame with
            linked_trip_id, mode_type, access_mode, egress_mode, transit_submode,
            tnc_type; may be empty but must be provided

    Returns:
        DataFrame with CT-RAMP individual trip fields

    Notes:
        - Excludes trips on joint tours
        - Creates stop_id sequence starting at 1 per half-tour
        - Excludes model-only fields (parking costs, value of time, etc.)
    """
    logger.info("Formatting individual trip data for CT-RAMP")

    # Derive/validate person_type before purpose mapping.
    if "person_type" not in persons_canonical.columns or "type" not in persons_canonical.columns:
        logger.info("Deriving person_type for trip formatting")
        if "student_category" not in persons_canonical.columns:
            persons_canonical = persons_canonical.with_columns(
                ctramp_student_category_expression(school_taz_col="school_taz").alias(
                    "student_category"
                )
            )
        if "employment_category" not in persons_canonical.columns:
            persons_canonical = persons_canonical.with_columns(
                pl.col("employment")
                .replace_strict(
                    EMPLOYMENT_TO_CTRAMP,
                    default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
                )
                .alias("employment_category")
            )
        persons_canonical = enrich_persons_with_person_type(persons_canonical)

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
        persons_canonical.select(["person_id", "person_num", "school_type", "person_type"]),
        on="person_id",
        how="left",
    ).join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    # Create stop_id: trips within each half-tour leg (outbound/inbound/subtour)
    # are numbered 0-based in order of departure. A leg that consists of a single
    # trip (i.e. no intermediate stop) is assigned -1.
    # Group by the globally-unique canonical tour id so tours from different
    # persons/days are never combined.
    individual_trips = (
        individual_trips.with_columns(
            pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .then(pl.lit("inbound"))
            .otherwise(pl.lit("outbound"))
            .alias("tour_direction_str")
        )
        .sort(["_canonical_tour_id", "tour_direction_str", "depart_time", "arrive_time"])
        .with_columns(
            (
                pl.col("depart_time")
                .rank("ordinal")
                .over(["_canonical_tour_id", "tour_direction_str"])
                - 1
            )
            .cast(pl.Int64)
            .alias("_stop_seq")
        )
        .with_columns(
            # Single-trip leg (max 0-based sequence is 0) has no intermediate stop
            # and is assigned -1; otherwise keep the 0-based sequence.
            pl.when(
                pl.col("_stop_seq").max().over(["_canonical_tour_id", "tour_direction_str"]) == 0
            )
            .then(pl.lit(-1))
            .otherwise(pl.col("_stop_seq"))
            .cast(pl.Int64)
            .alias("stop_id")
        )
        .drop("_stop_seq")
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
            ctramp_purpose_category_expression(
                pl.col("o_purpose_category"),
                pl.col("income"),
                pl.col("school_type"),
                pl.col("person_type"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
                purpose_kind="trip",
            ).alias("orig_purpose"),
            ctramp_purpose_category_expression(
                pl.col("d_purpose_category"),
                pl.col("income"),
                pl.col("school_type"),
                pl.col("person_type"),
                config.income_low_threshold,
                config.income_med_threshold,
                config.income_high_threshold,
                purpose_kind="trip",
            ).alias("dest_purpose"),
            # tour_purpose is already CTRAMP formatted from join, no need to remap
        ]
    )

    # Log how detailed survey purposes collapse into CT-RAMP trip purposes.
    _log_purpose_mapping_counts(individual_trips, "o_purpose", "orig_purpose", "origin")
    _log_purpose_mapping_counts(individual_trips, "d_purpose", "dest_purpose", "destination")

    # Map trip mode (tour_mode already formatted from join)
    # Derive the transit submode per linked trip from detailed unlinked-trip modes
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
