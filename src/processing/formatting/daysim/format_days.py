"""Person-day formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory
from data_canon.codebook.trips import PurposeCategory

logger = logging.getLogger(__name__)


def format_days(
    persons: pl.DataFrame,
    days: pl.DataFrame,
    tours: pl.DataFrame,
) -> pl.DataFrame:
    """Format person-day data for DaySim PersonDay file.

    Creates person-day records with tour counts by purpose, stop counts,
    begin/end at home flags, work at home duration, and location coordinates.

    Args:
        persons: Canonical person data with person_id, hh_id, work/school coords
        days: Canonical day data with day_id, person_id, travel_dow, day_weight
        tours: Canonical tour data with tour_id, day_id, tour_purpose, tour_category

    Returns:
        DataFrame with DaySim PersonDay format including:
        - hhno, pno, day: Identifiers
        - beghom, endhom: Begin/end at home flags
        - hbtours, wbtours, uwtours: Total/work-based/usual work tours
        - wktours, sctours, estours, pbtours, shtours, mltours, sotours, retours, metours:
          Tour counts by purpose
        - wkstops, scstops, esstops, pbstops, shstops, mlstops, sostops, restops, mestops:
          Stop counts by purpose
        - wkathome: Minutes worked at home
        - pwxcord, pwycord, psxcord, psycord: Work/school coordinates
        - pdexpfac: Person-day expansion factor
    """
    logger.info("Formatting person-day data for DaySim")

    # Get tour counts by purpose for each day
    tour_counts = (
        tours.group_by(["day_id", "tour_purpose"])
        .agg(pl.len().alias("count"))
        .pivot(index="day_id", on="tour_purpose", values="count")
        .fill_null(0)
    )

    # Map canonical tour purposes to DaySim purpose codes
    # This mapping needs to align with your tour purpose values
    purpose_columns = {
        "work": "wktours",
        "school": "sctours",
        "escort": "estours",
        "personal_business": "pbtours",
        "shopping": "shtours",
        "meal": "mltours",
        "social": "sotours",
        "recreation": "retours",
        "medical": "metours",
    }

    # Rename purpose columns if they exist
    rename_map = {
        canon_purpose: daysim_col
        for canon_purpose, daysim_col in purpose_columns.items()
        if canon_purpose in tour_counts.columns
    }

    if rename_map:
        tour_counts = tour_counts.rename(rename_map)

    # Ensure all required purpose columns exist
    for daysim_col in purpose_columns.values():
        if daysim_col not in tour_counts.columns:
            tour_counts = tour_counts.with_columns(pl.lit(0).alias(daysim_col))

    # Count home-based tours (complete tours)
    hb_tour_counts = (
        tours.filter(pl.col("tour_category") == TourCategory.COMPLETE.value)
        .group_by("day_id")
        .agg(pl.len().alias("hbtours"))
    )

    # Count work-based subtours (tour_type == WORK_BASED)
    wb_tour_counts = (
        tours.filter(pl.col("parent_tour_id").is_not_null())
        .group_by("day_id")
        .agg(pl.len().alias("wbtours"))
    )

    # Count usual workplace tours (work tours that start/end at home)
    # This is an approximation - you may need additional logic
    uw_tour_counts = (
        tours.filter(
            (pl.col("tour_purpose") == PurposeCategory.WORK.value)
            & (pl.col("tour_category") == TourCategory.COMPLETE.value)
        )
        .group_by("day_id")
        .agg(pl.len().alias("uwtours"))
    )

    # Start with days data and join person identifiers
    days_daysim = days.join(
        persons.select(
            [
                "person_id",
                "hh_id",
                "person_num",
                "work_lon",
                "work_lat",
                "school_lon",
                "school_lat",
            ]
        ),
        on="person_id",
        how="left",
    )

    # Join tour count aggregations
    days_daysim = (
        days_daysim.join(tour_counts, on="day_id", how="left")
        .join(hb_tour_counts, on="day_id", how="left")
        .join(wb_tour_counts, on="day_id", how="left")
        .join(uw_tour_counts, on="day_id", how="left")
    )

    # Calculate begin/end at home flags
    # Check if first/last tour starts/ends at home
    first_last_tours = (
        tours.sort("origin_depart_time")
        .group_by("day_id")
        .agg(
            [
                pl.col("o_location_type").first().alias("first_o_location"),
                pl.col("o_location_type").last().alias("last_o_location"),
            ]
        )
        .with_columns(
            [
                (pl.col("first_o_location") == PurposeCategory.HOME.value)
                .cast(pl.Int16)
                .alias("beghom"),
                (pl.col("last_o_location") == PurposeCategory.HOME.value)
                .cast(pl.Int16)
                .alias("endhom"),
            ]
        )
        .select(["day_id", "beghom", "endhom"])
    )

    days_daysim = days_daysim.join(first_last_tours, on="day_id", how="left")

    # Map to DaySim schema
    days_daysim = days_daysim.select(
        [
            pl.col("hh_id").alias("hhno"),
            pl.col("person_num").alias("pno"),
            pl.col("day_num").alias("day"),
            pl.col("beghom").fill_null(0).cast(pl.Int16),
            pl.col("endhom").fill_null(0).cast(pl.Int16),
            pl.col("hbtours").fill_null(0).cast(pl.Int16),
            pl.col("wbtours").fill_null(0).cast(pl.Int16),
            pl.col("uwtours").fill_null(0).cast(pl.Int16),
            pl.col("wktours").fill_null(0).cast(pl.Int16),
            pl.col("sctours").fill_null(0).cast(pl.Int16),
            pl.col("estours").fill_null(0).cast(pl.Int16),
            pl.col("pbtours").fill_null(0).cast(pl.Int16),
            pl.col("shtours").fill_null(0).cast(pl.Int16),
            pl.col("mltours").fill_null(0).cast(pl.Int16),
            pl.col("sotours").fill_null(0).cast(pl.Int16),
            pl.col("retours").fill_null(0).cast(pl.Int16),
            pl.col("metours").fill_null(0).cast(pl.Int16),
            # Stop counts (placeholder - needs trip-level analysis)
            pl.lit(0).cast(pl.Int16).alias("wkstops"),
            pl.lit(0).cast(pl.Int16).alias("scstops"),
            pl.lit(0).cast(pl.Int16).alias("esstops"),
            pl.lit(0).cast(pl.Int16).alias("pbstops"),
            pl.lit(0).cast(pl.Int16).alias("shstops"),
            pl.lit(0).cast(pl.Int16).alias("mlstops"),
            pl.lit(0).cast(pl.Int16).alias("sostops"),
            pl.lit(0).cast(pl.Int16).alias("restops"),
            pl.lit(0).cast(pl.Int16).alias("mestops"),
            # Work at home (placeholder)
            pl.lit(0).cast(pl.Int16).alias("wkathome"),
            # Location coordinates
            pl.col("work_lon").alias("pwxcord"),
            pl.col("work_lat").alias("pwycord"),
            pl.col("school_lon").alias("psxcord"),
            pl.col("school_lat").alias("psycord"),
            # Expansion factor
            pl.col("day_weight").alias("pdexpfac"),
        ]
    )

    logger.info("Formatted %d person-days for DaySim output", len(days_daysim))

    return days_daysim.sort(["hhno", "pno", "day"])
