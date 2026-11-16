"""Person-day formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import DaysimPurpose
from data_canon.codebook.tours import TourCategory, TourType
from data_canon.codebook.trips import PurposeCategory

from .mappings import daysim_purpose

logger = logging.getLogger(__name__)


def format_days(
    persons: pl.DataFrame,
    person_days: pl.DataFrame,
    tours: pl.DataFrame,
) -> pl.DataFrame:
    """Format person-day data for DaySim PersonDay file.

    Creates person-day records with tour counts by purpose, stop counts,
    begin/end at home flags and work at home duration.

    Key Transformations:

    - **Day-Level Summaries**: Tour count by purpose (work, school, escort, etc.)
      and stop counts by purpose
    - **Tour Categories**: Classify tours as home-based, work-based, or usual work location
    - **Activity Patterns**: Begin/end at home flags, work at home duration

    Args:
        persons: Canonical person data with person_id, person_num and employment
        person_days: Canonical day data with person_day_id, person_id, travel_dow, person_day_weight
        tours: Canonical tour data with tour_id, person_day_id, tour_purpose,
            tour_type, tour_category

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
            - pdexpfac: Person-day expansion factor
    """
    logger.info("Formatting person-day data for DaySim")

    # Get tour counts by purpose for each day
    tour_counts = (
        tours.group_by(["person_day_id", "tour_purpose"])
        .agg(pl.len().alias("count"))
        .pivot(index="person_day_id", on="tour_purpose", values="count")
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

    # Count home-based tours: complete tours anchored at home. COMPLETE alone is
    # not enough -- an at-work subtour that leaves and returns to the workplace
    # is also COMPLETE, against its own anchor -- so the type is checked too.
    is_home_based = pl.col("tour_type") == TourType.HOME_BASED.value
    is_complete = pl.col("tour_category") == TourCategory.COMPLETE.value
    hb_tour_counts = (
        tours.filter(is_home_based & is_complete)
        .group_by("person_day_id")
        .agg(pl.len().alias("hbtours"))
    )

    # Count work-based subtours: tours anchored at a workplace rather than home.
    wb_tour_counts = (
        tours.filter(pl.col("tour_type") == TourType.WORK_BASED.value)
        .group_by("person_day_id")
        .agg(pl.len().alias("wbtours"))
    )

    # Count usual workplace tours: every home-based work tour, by DaySim's own
    # purpose rule (an employed person's work-related tour is work), wherever it went.
    is_work = (
        daysim_purpose(pl.col("tour_purpose"), pl.col("employment")) == DaysimPurpose.WORK.value
    )
    uw_tour_counts = (
        tours.join(persons.select("person_id", "employment"), on="person_id", how="left")
        .filter(is_work & is_home_based & is_complete)
        .group_by("person_day_id")
        .agg(pl.len().alias("uwtours"))
    )

    # Start with days data and join person identifiers
    days_daysim = person_days.drop("person_num", strict=False).join(
        persons.select("person_num", "person_id"),
        on="person_id",
        how="left",
    )

    # Join tour count aggregations
    days_daysim = (
        days_daysim.join(tour_counts, on="person_day_id", how="left")
        .join(hb_tour_counts, on="person_day_id", how="left")
        .join(wb_tour_counts, on="person_day_id", how="left")
        .join(uw_tour_counts, on="person_day_id", how="left")
    )

    # Calculate begin/end at home flags
    # Check if first/last tour starts/ends at home
    first_last_tours = (
        tours.sort("origin_depart_time")
        .group_by("person_day_id")
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
        .select(["person_day_id", "beghom", "endhom"])
    )

    days_daysim = days_daysim.join(first_last_tours, on="person_day_id", how="left")

    # If person_day_weight not in days, set to 1
    if "person_day_weight" not in days_daysim.columns:
        days_daysim = days_daysim.with_columns(pl.lit(1).alias("person_day_weight"))

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
            # Expansion factor
            pl.col("person_day_weight").fill_null(0).alias("pdexpfac"),
        ]
    )

    logger.info("Formatted %d person-days for DaySim output", len(days_daysim))

    return days_daysim.sort(["hhno", "pno", "day"])
