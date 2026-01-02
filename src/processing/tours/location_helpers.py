"""Location classification helper functions for tour extraction.

This module contains functions for:
- Calculating haversine distances between trip endpoints and known locations
- Classifying trip origins/destinations as HOME, WORK, SCHOOL, or OTHER
- Hybrid classification strategy: matches if EITHER purpose code OR distance
  indicates a location type
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.trips import PurposeCategory
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)


def prepare_person_locations(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    person_type_mapping: dict,
) -> pl.DataFrame:
    """Prepare cached person location data with person categories.

    Args:
        persons: DataFrame with person attributes
        households: DataFrame with household attributes (home_lat, home_lon)
        person_type_mapping: Dict mapping person_type enum to PersonCategory

    Returns:
        DataFrame with person locations and categories
    """
    logger.info("Preparing person location data...")

    # Join home location from households
    persons_with_home = persons.join(
        households.select(["hh_id", "home_lat", "home_lon"]),
        on="hh_id",
        how="left",
    )

    # Select needed columns
    person_locations = persons_with_home.select(
        [
            "person_id",
            "person_type",
            "home_lat",
            "home_lon",
            "work_lat",
            "work_lon",
            "school_lat",
            "school_lon",
        ]
    )

    # Add person category mapping
    # Convert enum keys to integer values for Polars compatibility
    person_type_map = {k.value: v for k, v in person_type_mapping.items()}
    return person_locations.with_columns(
        [
            pl.col("person_type")
            .replace_strict(
                person_type_map,
                default=person_type_map[next(iter(person_type_map.keys()))],
            )
            .alias("person_category")
        ]
    )


def classify_trip_locations(
    linked_trips: pl.DataFrame,
    person_locations: pl.DataFrame,
    distance_thresholds: dict,
) -> pl.DataFrame:
    """Classify trip origins and destinations by location type.

    Uses hybrid strategy: matches location if EITHER:
    - Purpose code indicates location (e.g., purpose=HOME)
    - Distance within threshold (haversine distance <= config threshold)

    Args:
        linked_trips: Trip data with o_lat, o_lon, d_lat, d_lon
        person_locations: Person location cache with home/work/school coords
        distance_thresholds: Dict mapping LocationType to distance in meters

    Returns:
        Trips with added columns:
        - _o_is_home, _o_is_work, _o_is_school (bool flags)
        - _d_is_home, _d_is_work, _d_is_school (bool flags)
        - o_location_type, d_location_type (LocationType enum)
    """
    logger.info("Classifying trip locations...")

    # Join person locations
    linked_trips = linked_trips.join(person_locations, on="person_id", how="left")

    # Calculate distances to all known locations
    linked_trips = _add_distance_columns(linked_trips)

    # Create boolean flags for location matches
    linked_trips = _add_location_flags(linked_trips, distance_thresholds)

    # Determine primary location type for each trip end
    linked_trips = _add_location_types(linked_trips)

    # Clean up temporary columns
    # Keep location flags (_o_is_home, _d_is_work, etc.) for subtour detection
    temp_cols = [
        "home_lat",
        "home_lon",
        "work_lat",
        "work_lon",
        "school_lat",
        "school_lon",
        "person_type",
    ]
    drop_cols = [c for c in linked_trips.columns if "dist_to" in c or c in temp_cols]

    logger.info("Location classification complete")
    return linked_trips.drop(drop_cols)


def _add_distance_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate haversine distances to known locations.

    Args:
        df: DataFrame with trip coordinates and person location coords

    Returns:
        DataFrame with distance columns added
    """
    distance_cols = [
        expr_haversine(
            pl.col(f"{end}_lat"),
            pl.col(f"{end}_lon"),
            pl.col(f"{loc}_lat"),
            pl.col(f"{loc}_lon"),
        ).alias(f"{end}_dist_to_{loc}_meters")
        for loc in ["home", "work", "school"]
        for end in ["o", "d"]
    ]
    return df.with_columns(distance_cols)


def _add_location_flags(df: pl.DataFrame, distance_thresholds: dict) -> pl.DataFrame:
    """Create boolean flags for location matches.

    Uses hybrid strategy: matches if EITHER purpose code OR distance
    indicates the location type.

    Args:
        df: DataFrame with distance columns
        distance_thresholds: Dict mapping LocationType to distance in meters

    Returns:
        DataFrame with location flag columns (_o_is_home, _d_is_work, etc.)
    """
    flag_cols = []

    # Location configs: (location_type, null_check, purpose_categories)
    location_configs = {
        "home": (
            LocationType.HOME,
            None,
            [PurposeCategory.HOME],
        ),
        "work": (
            LocationType.WORK,
            "work_lat",
            [PurposeCategory.WORK, PurposeCategory.WORK_RELATED],
        ),
        "school": (
            LocationType.SCHOOL,
            "school_lat",
            [PurposeCategory.SCHOOL, PurposeCategory.SCHOOL_RELATED],
        ),
    }

    for loc, (loc_type, null_check, purpose_cats) in location_configs.items():
        for end in ["o", "d"]:
            # Distance-based check
            distance_check = pl.col(f"{end}_dist_to_{loc}_meters") <= distance_thresholds[loc_type]
            if null_check:
                distance_check = distance_check & pl.col(null_check).is_not_null()

            # Purpose-based check
            purpose_col = f"{end}_purpose_category"
            if purpose_col in df.columns:
                # Convert enum objects to integer values for Polars
                purpose_values = [p.value for p in purpose_cats]
                purpose_check = pl.col(purpose_col).is_in(purpose_values)
            else:
                purpose_check = pl.lit(value=False)

            # Match if EITHER purpose OR distance indicates location
            combined_check = purpose_check | distance_check

            flag_cols.append(combined_check.alias(f"_{end}_is_{loc}"))

    return df.with_columns(flag_cols)


def _add_location_types(df: pl.DataFrame) -> pl.DataFrame:
    """Determine primary location type based on priority.

    Priority order: HOME > WORK > SCHOOL > OTHER

    Args:
        df: DataFrame with location flag columns

    Returns:
        DataFrame with o_location_type and d_location_type columns
    """

    def build_location_expr(prefix: str) -> pl.Expr:
        """Build expression for location type with priority order."""
        expr = pl.lit(LocationType.OTHER)
        # Reverse priority order: HOME > WORK > SCHOOL > OTHER
        for loc_type in [
            LocationType.SCHOOL,
            LocationType.WORK,
            LocationType.HOME,
        ]:
            col_name = f"{prefix}_is_{loc_type.name.lower()}"
            expr = pl.when(pl.col(col_name)).then(pl.lit(loc_type)).otherwise(expr)
        return expr

    return df.with_columns(
        [
            build_location_expr("_o").alias("_o_location_type"),
            build_location_expr("_d").alias("_d_location_type"),
        ]
    )
