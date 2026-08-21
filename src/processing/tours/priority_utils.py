"""Priority calculation utilities for tour purpose and mode selection.

This module provides pure helper functions for calculating priority values
used in tour purpose and mode aggregation. These functions are stateless
and can be used across different tour processing contexts.
"""

import polars as pl

from data_canon.codebook.trips import PurposeCategory

from .tour_configs import TourConfig


def add_purpose_priority_column(
    df: pl.DataFrame,
    config: TourConfig,
    alias: str = "purpose_priority",
) -> pl.DataFrame:
    """Add purpose priority column to dataframe.

    Maps person_category (PersonCategory string) → purpose priority value.
        Purpose priority is determined by the nested mapping in the TourConfig:
    Args:
        df: DataFrame with person_category and d_purpose_category columns
        config: TourConfig with priority mappings
        alias: Column name for the priority values

    Returns:
        DataFrame with added purpose_priority column
    """

    def _get_priority(s: dict) -> int:
        """Inner function for map_elements - receives struct as dict."""
        # person_category is a PersonCategory string (e.g., 'worker', 'student', 'other', etc.)
        # d_purpose_category is a PurposeCategory enum integer
        person_category_str = s["person_category"]
        purpose_cat = PurposeCategory(s["d_purpose_category"])

        # HOME purposes don't need priority
        if purpose_cat == PurposeCategory.HOME:
            return 999

        # Get priority from nested map
        purpose_priority_map = config.purpose_priority_by_personcat
        if person_category_str not in purpose_priority_map:
            msg = f"PersonCategory '{person_category_str}' not in purpose_priority_by_personcat"
            raise ValueError(msg)
        purpose_priorities = purpose_priority_map[person_category_str]
        if purpose_cat not in purpose_priorities:
            msg = (
                f"PurposeCategory {purpose_cat} not mapped for "
                f"PersonCategory '{person_category_str}'"
            )
            raise ValueError(msg)
        return purpose_priorities[purpose_cat]

    return df.with_columns(
        [
            pl.struct(["person_category", "d_purpose_category"])
            .map_elements(_get_priority, return_dtype=pl.Int32)
            .alias(alias)
        ]
    )


def add_mode_priority_column(
    df: pl.DataFrame,
    mode_hierarchy: list,
    alias: str = "mode_priority",
) -> pl.DataFrame:
    """Add mode priority column based on mode hierarchy.

    Maps ModeType values to priority integers based on their position
    in the mode hierarchy list (later in list = higher priority).

    Args:
        df: DataFrame with mode_type column
        mode_hierarchy: Ordered list of ModeType enums (ascending priority)
        alias: Column name for the priority values

    Returns:
        DataFrame with added mode_priority column
    """
    # Convert ModeType enums to their integer values for replacement
    mode_mapping = {
        mode.value if hasattr(mode, "value") else mode: idx
        for idx, mode in enumerate(mode_hierarchy)
    }

    mode_expr = pl.col("mode_type").replace_strict(
        old=list(mode_mapping.keys()),
        new=list(mode_mapping.values()),
        default=-1,
    )

    return df.with_columns([mode_expr.cast(pl.Int32).alias(alias)])


def add_activity_duration_column(
    df: pl.DataFrame,
    default_minutes: float = 240.0,
    alias: str = "activity_duration",
) -> pl.DataFrame:
    """Add activity duration column to dataframe.

    Calculates time spent at each destination as the time between
    arrival and the next trip's departure. For the last trip of the day,
    uses the provided default duration.

    Args:
        df: DataFrame with arrive_time, depart_time, person_id, day_id columns
        default_minutes: Default duration for last trip of day (default: 240)
        alias: Column name for the duration values

    Returns:
        DataFrame with added activity_duration column (in minutes)
    """
    return df.with_columns(
        [
            (pl.col("depart_time").shift(-1).over(["person_id", "day_id"]) - pl.col("arrive_time"))
            .dt.total_minutes()
            .fill_null(default_minutes)
            .alias(alias)
        ]
    )


def add_purpose_score_column(
    df: pl.DataFrame,
    config: TourConfig,
    purpose_col: str = "_d_purpose_effective",
    duration_col: str = "_activity_duration",
    person_category_col: str = "person_category",
    alias: str = "purpose_score",
) -> pl.DataFrame:
    """Add a duration-weighted purpose score used to pick the tour purpose.

    Each candidate activity scores ``W * x / (x + h)`` where ``x`` is its
    activity duration, ``W`` is the purpose's ceiling weight for the person's
    category (``config.purpose_score_weights``) and ``h`` is the purpose's
    half-saturation duration (``config.purpose_score_halfmax``). The score rises
    with duration toward ``W``, reaching ``W / 2`` at ``x = h``, so a long
    discretionary activity can outscore a brief mandatory one. Highest score
    wins.

    Purposes with no configured weight (e.g. HOME) get a null score and are never
    selected. The lookups are vectorized joins rather than per-row evaluation.

    Args:
        df: Trip data with the purpose, duration and person-category columns.
        config: TourConfig carrying the weight and half-max tables.
        purpose_col: Column holding the (effective) purpose category value.
        duration_col: Column holding the activity duration in minutes.
        person_category_col: Column holding the PersonCategory string.
        alias: Name for the score column to add.

    Returns:
        DataFrame with the score column added.
    """
    weights = pl.DataFrame(
        [
            {"_pcat": person_category, "_purpose": purpose.value, "_w": float(weight)}
            for person_category, purposes in config.purpose_score_weights.items()
            for purpose, weight in purposes.items()
        ],
        schema={"_pcat": pl.Utf8, "_purpose": pl.Int64, "_w": pl.Float64},
    )
    halfmax = pl.DataFrame(
        [
            {"_purpose": purpose.value, "_h": float(h)}
            for purpose, h in config.purpose_score_halfmax.items()
        ],
        schema={"_purpose": pl.Int64, "_h": pl.Float64},
    )

    scored = (
        df.with_columns(pl.col(purpose_col).cast(pl.Int64).alias("_score_purpose"))
        .join(
            weights,
            left_on=[person_category_col, "_score_purpose"],
            right_on=["_pcat", "_purpose"],
            how="left",
        )
        .join(halfmax, left_on="_score_purpose", right_on="_purpose", how="left")
        .with_columns(
            (pl.col("_w") * pl.col(duration_col) / (pl.col(duration_col) + pl.col("_h"))).alias(
                alias
            )
        )
        .drop("_score_purpose", "_w", "_h")
    )
    return scored
