"""Location classification helper functions for tour extraction.

This module contains functions for:
- Calculating haversine distances between trip endpoints and known locations
- Classifying trip origins/destinations as HOME, WORK, SCHOOL, or OTHER
- Hybrid classification strategy: matches if EITHER purpose code OR distance
  indicates a location type
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import PurposeCategory
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)


def prepare_person_locations(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    person_category_expression: pl.Expr,
) -> pl.DataFrame:
    """Prepare cached person location data with person categories.

    Args:
        persons: DataFrame with person attributes
        households: DataFrame with household attributes (home_lat, home_lon)
        person_category_expression: Polars expression to classify person categories

    Returns:
        DataFrame with person locations and categories
    """
    logger.info("Preparing person location data...")

    # Classify person category to persons
    persons_with_category = persons.with_columns(person_category_expression)

    # Join home location from households
    persons_with_home = persons_with_category.join(
        households.select(["hh_id", "home_lat", "home_lon"]),
        on="hh_id",
        how="left",
    )

    # Select needed columns
    person_locations = persons_with_home.select(
        [
            "person_id",
            "person_category",
            "home_lat",
            "home_lon",
            "work_lat",
            "work_lon",
            "school_lat",
            "school_lon",
        ]
    )

    return person_locations


# Location kinds: (LocationType, purpose categories that also match).
# A trip end matches a kind if it lies within the kind's distance threshold of
# any of the person's habitual locations of that kind, OR its purpose indicates
# the kind (the hybrid strategy).
_LOCATION_CONFIGS = {
    "home": (LocationType.HOME, [PurposeCategory.HOME]),
    "work": (LocationType.WORK, [PurposeCategory.WORK, PurposeCategory.WORK_RELATED]),
    "school": (LocationType.SCHOOL, [PurposeCategory.SCHOOL, PurposeCategory.SCHOOL_RELATED]),
}


def classify_trip_locations(
    linked_trips: pl.DataFrame,
    habitual_locations: pl.DataFrame,
    distance_thresholds: dict,
) -> pl.DataFrame:
    """Classify trip origins and destinations against a person's habitual locations.

    Uses a hybrid strategy: a trip end matches a location type if EITHER its
    purpose code indicates that type OR it lies within the type's distance
    threshold of one of the person's *reported* locations of that type.

    Classification deliberately uses only the reported (survey) locations, not
    observed ones: work/school trips already tag WORK/SCHOOL via their purpose,
    so observed locations would only re-tag non-work trips (a meal or social stop
    near a worksite) spatially. Observed locations instead drive anchor detection
    (see ``add_anchor_flags``). This keeps classification equivalent to the
    reported-scalar behavior.

    Args:
        linked_trips: Trip data with o_lat, o_lon, d_lat, d_lon and purpose codes.
        habitual_locations: Habitual locations (see ``habitual_locations.py``)
            with person_id, location_type, lat, lon, source.
        distance_thresholds: Dict mapping LocationType to distance in meters.

    Returns:
        Trips with added columns:
        - _o_is_home, _o_is_work, _o_is_school (bool flags)
        - _d_is_home, _d_is_work, _d_is_school (bool flags)
        - o_location_type, d_location_type (LocationType value)
    """
    logger.info("Classifying trip locations against reported habitual locations...")

    reg = habitual_locations.filter(pl.col("source") == LocationSource.REPORTED.value).select(
        "person_id", "location_type", "lat", "lon"
    )
    for end in ["o", "d"]:
        linked_trips = linked_trips.join(
            _min_distance_to_locations(linked_trips, reg, end),
            on="linked_trip_id",
            how="left",
        )

    linked_trips = _add_location_flags(linked_trips, distance_thresholds)
    linked_trips = _add_location_types(linked_trips)

    # Keep location flags (_o_is_home, _d_is_work) for anchor/subtour detection;
    # drop only the intermediate distance columns.
    drop_cols = [c for c in linked_trips.columns if "_dist_to_" in c]
    logger.info("Location classification complete")
    return linked_trips.drop(drop_cols)


def _min_distance_to_locations(
    linked_trips: pl.DataFrame,
    reg: pl.DataFrame,
    end: str,
) -> pl.DataFrame:
    """Minimum distance from a trip end to each habitual location kind.

    Returns one row per linked_trip_id with ``{end}_dist_to_{kind}_meters``
    columns (null when the person has no location of that kind).
    """
    ends = linked_trips.select(
        "linked_trip_id",
        "person_id",
        pl.col(f"{end}_lat").alias("_elat"),
        pl.col(f"{end}_lon").alias("_elon"),
    )
    joined = ends.join(reg, on="person_id", how="left").with_columns(
        expr_haversine(pl.col("_elat"), pl.col("_elon"), pl.col("lat"), pl.col("lon")).alias("_d")
    )
    aggs = [
        pl.col("_d")
        .filter(pl.col("location_type") == loc_type.value)
        .min()
        .alias(f"{end}_dist_to_{name}_meters")
        for name, (loc_type, _purposes) in _LOCATION_CONFIGS.items()
    ]
    return joined.group_by("linked_trip_id").agg(aggs)


def _add_location_flags(df: pl.DataFrame, distance_thresholds: dict) -> pl.DataFrame:
    """Create boolean flags for location matches.

    Uses hybrid strategy: matches if EITHER purpose code OR distance to any
    habitual location of that kind indicates the location type.

    Args:
        df: DataFrame with per-kind minimum distance columns
        distance_thresholds: Dict mapping LocationType to distance in meters

    Returns:
        DataFrame with location flag columns (_o_is_home, _d_is_work, etc.)
    """
    flag_cols = []
    for name, (loc_type, purpose_cats) in _LOCATION_CONFIGS.items():
        purpose_values = [p.value for p in purpose_cats]
        for end in ["o", "d"]:
            # Distance-based check: within threshold of any location of this
            # kind. A null distance (person has no such location) is not a match.
            distance_check = (
                pl.col(f"{end}_dist_to_{name}_meters") <= distance_thresholds[loc_type]
            ).fill_null(value=False)

            # Purpose-based check
            purpose_col = f"{end}_purpose_category"
            if purpose_col in df.columns:
                purpose_check = pl.col(purpose_col).is_in(purpose_values)
            else:
                purpose_check = pl.lit(value=False)

            # Match if EITHER purpose OR distance indicates location
            flag_cols.append((purpose_check | distance_check).alias(f"_{end}_is_{name}"))

    return df.with_columns(flag_cols)


# Anchor subsets drawn from the habitual locations: (name, LocationType, LocationSource,
# primary purpose that also counts as being at that anchor).
_ANCHOR_SUBSETS = [
    ("reported_work", LocationType.WORK, LocationSource.REPORTED, PurposeCategory.WORK),
    ("observed_work", LocationType.WORK, LocationSource.OBSERVED, None),
    ("reported_school", LocationType.SCHOOL, LocationSource.REPORTED, PurposeCategory.SCHOOL),
]


def add_anchor_flags(
    linked_trips: pl.DataFrame,
    habitual_locations: pl.DataFrame,
    distance_thresholds: dict,
) -> pl.DataFrame:
    """Add per-end work/school anchor flags with per-day resolution.

    A tour anchor is the place a person is *based* for the day. The rule is
    per-day, which the person-level location table alone cannot express:

    - A trip end is at the WORK anchor if it is at one of the person's *reported*
      work locations, OR — on days they did not visit their reported work at
      all — at one of their *observed* work locations. This lets someone anchor
      at a different worksite on different days (e.g. hybrid work) while keeping
      work-related errands on an office day as subtours, not anchors.
    - A trip end is at the SCHOOL anchor if it is at a reported school location.

    Anchor flags are distance-only (physically at the location) plus the primary
    WORK/SCHOOL purpose; the "_RELATED" purposes are excluded so errands away
    from a known location read as away from the anchor.

    Args:
        linked_trips: Classified trips with person_id, day_id, o/d coords and
            purpose codes.
        habitual_locations: Habitual locations with person_id, location_type,
            lat, lon, source.
        distance_thresholds: Dict mapping LocationType to distance in meters.

    Returns:
        Trips with _o_at_work, _d_at_work, _o_at_school, _d_at_school flags.
    """
    logger.info("Computing anchor flags from habitual locations...")

    threshold = {
        "reported_work": distance_thresholds[LocationType.WORK],
        "observed_work": distance_thresholds[LocationType.WORK],
        "reported_school": distance_thresholds[LocationType.SCHOOL],
    }

    # Minimum distance from each end to each anchor subset.
    for end in ["o", "d"]:
        for name, loc_type, source, _purpose in _ANCHOR_SUBSETS:
            subset = habitual_locations.filter(
                (pl.col("location_type") == loc_type.value) & (pl.col("source") == source.value)
            ).select("person_id", "lat", "lon")
            linked_trips = linked_trips.join(
                _min_distance_to_subset(linked_trips, subset, end, name),
                on="linked_trip_id",
                how="left",
            )

    # Raw "at this subset" flags: within threshold, or the primary purpose.
    raw_flags = []
    for end in ["o", "d"]:
        for name, _loc_type, _source, purpose in _ANCHOR_SUBSETS:
            at_subset = (pl.col(f"_{end}_dist_{name}") <= threshold[name]).fill_null(value=False)
            purpose_col = f"{end}_purpose_category"
            if purpose is not None and purpose_col in linked_trips.columns:
                at_subset = at_subset | (pl.col(purpose_col) == purpose.value)
            raw_flags.append(at_subset.alias(f"_{end}_at_{name}"))
    linked_trips = linked_trips.with_columns(raw_flags)

    # Per-day: did the person visit their reported workplace at all today?
    linked_trips = linked_trips.with_columns(
        (pl.col("_o_at_reported_work") | pl.col("_d_at_reported_work"))
        .any()
        .over(["person_id", "day_id"])
        .alias("_visited_reported_work")
    )

    # Final anchor flags. Work: reported anchor always counts; observed work
    # counts only on days the reported workplace was not visited. School: reported.
    linked_trips = linked_trips.with_columns(
        (
            pl.col("_o_at_reported_work")
            | (~pl.col("_visited_reported_work") & pl.col("_o_at_observed_work"))
        ).alias("_o_at_work"),
        (
            pl.col("_d_at_reported_work")
            | (~pl.col("_visited_reported_work") & pl.col("_d_at_observed_work"))
        ).alias("_d_at_work"),
        pl.col("_o_at_reported_school").alias("_o_at_school"),
        pl.col("_d_at_reported_school").alias("_d_at_school"),
    )

    drop_cols = [
        c
        for c in linked_trips.columns
        if c.startswith(("_o_dist_", "_d_dist_"))
        or c.endswith(("_at_reported_work", "_at_observed_work", "_at_reported_school"))
        or c == "_visited_reported_work"
    ]
    return linked_trips.drop(drop_cols)


def _min_distance_to_subset(
    linked_trips: pl.DataFrame,
    subset: pl.DataFrame,
    end: str,
    name: str,
) -> pl.DataFrame:
    """Minimum distance from a trip end to a location subset (per linked_trip_id)."""
    ends = linked_trips.select(
        "linked_trip_id",
        "person_id",
        pl.col(f"{end}_lat").alias("_elat"),
        pl.col(f"{end}_lon").alias("_elon"),
    )
    joined = ends.join(subset, on="person_id", how="left").with_columns(
        expr_haversine(pl.col("_elat"), pl.col("_elon"), pl.col("lat"), pl.col("lon")).alias("_d")
    )
    return joined.group_by("linked_trip_id").agg(pl.col("_d").min().alias(f"_{end}_dist_{name}"))


# Anchor kinds: work and school can anchor tours. An anchor flag is distance-only
# (physically at a habitual location of that kind) plus the *primary* purpose
# (WORK/SCHOOL). Unlike classification it excludes the "_RELATED" purposes, so a
# work/school-related errand away from a known location reads as away from the
# anchor and can form a subtour instead of being absorbed into the anchor period.
_ANCHOR_CONFIGS = {
    "work": (LocationType.WORK, PurposeCategory.WORK),
    "school": (LocationType.SCHOOL, PurposeCategory.SCHOOL),
}


def _add_anchor_flags(df: pl.DataFrame, distance_thresholds: dict) -> pl.DataFrame:
    """Create distance-only anchor flags (_o_at_work, _d_at_school, etc.)."""
    anchor_cols = []
    for name, (loc_type, anchor_purpose) in _ANCHOR_CONFIGS.items():
        for end in ["o", "d"]:
            distance_check = (
                pl.col(f"{end}_dist_to_{name}_meters") <= distance_thresholds[loc_type]
            ).fill_null(value=False)
            purpose_col = f"{end}_purpose_category"
            if purpose_col in df.columns:
                purpose_check = pl.col(purpose_col) == anchor_purpose.value
            else:
                purpose_check = pl.lit(value=False)
            anchor_cols.append((distance_check | purpose_check).alias(f"_{end}_at_{name}"))
    return df.with_columns(anchor_cols)


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
            build_location_expr("_o").alias("o_location_type"),
            build_location_expr("_d").alias("d_location_type"),
        ]
    )
