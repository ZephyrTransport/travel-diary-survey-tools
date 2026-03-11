"""Format mandatory locations for CT-RAMP specification."""

import logging

import polars as pl

from utils.helpers import expr_haversine

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def calc_fixed_location_distances(
    mandatory_locations: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    fixed_type: str,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Calculate fixed work/school location distances for validation.

    Args:
        mandatory_locations: DataFrame with person fixed locations and home locations
        linked_trips_canonical: Canonical linked trips DataFrame with trip origins/destinations
        fixed_type: Type of fixed location to calculate distances for ("work" or "school")
        config: CT-RAMP configuration with fixed location buffer and distance unit

    Returns:
        DataFrame with person_id and calculated work/school distances
    """
    if fixed_type not in ["work", "school"]:
        msg = "fixed_type must be 'work' or 'school'"
        raise ValueError(msg)

    if config.distance_unit not in ["miles", "kilometers", "meters"]:
        msg = "distance_unit must be 'miles', 'kilometers', or 'meters'"
        raise ValueError(msg)

    if config.fixed_location_buffer_meters is None:
        msg = "fixed_location_buffer_meters must be set in config"
        raise ValueError(msg)

    # Filter persons to fixed locations for the type we are calculating
    _mandatory_locations = mandatory_locations.filter(
        pl.col(f"{fixed_type}_lat").is_not_null() & pl.col(f"{fixed_type}_lon").is_not_null()
    )

    # If nothing to calculate, return empty DataFrame
    if _mandatory_locations.height == 0:
        logger.info("No persons with fixed %s locations to calculate distances for", fixed_type)
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                f"{fixed_type}_distance_{config.distance_unit}": pl.Float64,
            }
        )

    logger.info(
        "Calculated fixed location distances for %d persons",
        _mandatory_locations.select("person_id").n_unique(),
    )

    # Join trips with person fixed locations
    trips_with_locations = linked_trips_canonical.join(
        _mandatory_locations.select(
            "person_id",
            "home_lat",
            "home_lon",
            f"{fixed_type}_lat",
            f"{fixed_type}_lon",
            f"{fixed_type}_taz",
        ),
        on="person_id",
        how="right",
    )

    # Calculate flags for origin/destination proximity
    trips_with_flags = trips_with_locations.with_columns(
        [
            (
                expr_haversine(
                    pl.col("o_lat"),
                    pl.col("o_lon"),
                    pl.col("home_lat"),
                    pl.col("home_lon"),
                    units="meters",
                )
                <= config.fixed_location_buffer_meters
            ).alias("origin_is_home"),
            (
                expr_haversine(
                    pl.col("o_lat"),
                    pl.col("o_lon"),
                    pl.col(f"{fixed_type}_lat"),
                    pl.col(f"{fixed_type}_lon"),
                    units="meters",
                )
                <= config.fixed_location_buffer_meters
            ).alias(f"origin_is_{fixed_type}"),
            (
                expr_haversine(
                    pl.col("d_lat"),
                    pl.col("d_lon"),
                    pl.col("home_lat"),
                    pl.col("home_lon"),
                    units="meters",
                )
                <= config.fixed_location_buffer_meters
            ).alias("dest_is_home"),
            (
                expr_haversine(
                    pl.col("d_lat"),
                    pl.col("d_lon"),
                    pl.col(f"{fixed_type}_lat"),
                    pl.col(f"{fixed_type}_lon"),
                    units="meters",
                )
                <= config.fixed_location_buffer_meters
            ).alias(f"dest_is_{fixed_type}"),
            # Calculate matching TAZs for origin/destination, used as fallback
            (pl.col(f"o_{config.taz_field}") == pl.col(f"{fixed_type}_taz")).alias(
                f"origin_is_{fixed_type}_taz"
            ),
            (pl.col(f"d_{config.taz_field}") == pl.col(f"{fixed_type}_taz")).alias(
                f"dest_is_{fixed_type}_taz"
            ),
        ]
    )

    # Filter for home-work or work-home trips within buffer and ignore direction
    # i.e., trips that start or end at home and the other end at the fixed location
    home_to_fixed_trips = trips_with_flags.filter(
        (
            (pl.col("origin_is_home") & pl.col(f"dest_is_{fixed_type}"))
            | (pl.col(f"origin_is_{fixed_type}") & pl.col("dest_is_home"))
        )
        # Fallback to use matching TAZs
        | (pl.col(f"origin_is_{fixed_type}_taz") & pl.col("dest_is_home"))
        | (pl.col(f"dest_is_{fixed_type}_taz") & pl.col("origin_is_home"))
    )

    # Extract and average the distance field for these trips
    distances = home_to_fixed_trips.group_by("person_id").agg(
        pl.col("distance_meters").mean().alias(f"{fixed_type}_distance_meters")
    )

    # Check and warn for any missing distances due to no matching trips
    missing_distance_persons = (
        _mandatory_locations.join(distances, on="person_id", how="left")
        .filter(pl.col(f"{fixed_type}_distance_meters").is_null())
        .select(
            "person_id",
            "home_lat",
            "home_lon",
            f"{fixed_type}_lat",
            f"{fixed_type}_lon",
        )
    )

    if missing_distance_persons.height > 0:
        logger.warning(
            (
                "Found no matching trips for %d persons with a reported fixed %s locations, "
                "leaving %s_distance_meters as null"
            ),
            missing_distance_persons.height,
            fixed_type,
            f"{fixed_type}_distance_meters",
        )

    # If unit is miles, convert from meters
    if config.distance_unit == "miles":
        distances = distances.with_columns(
            (pl.col(f"{fixed_type}_distance_meters") / 1609.34).alias(
                f"{fixed_type}_distance_miles"
            )
        ).select(["person_id", f"{fixed_type}_distance_miles"])
    elif config.distance_unit == "kilometers":
        distances = distances.with_columns(
            (pl.col(f"{fixed_type}_distance_meters") / 1000).alias(
                f"{fixed_type}_distance_kilometers"
            )
        ).select(["person_id", f"{fixed_type}_distance_kilometers"])

    return distances


def format_mandatory_location(
    persons_ctramp: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format mandatory locations (work/school) to CT-RAMP specification.

    Transforms person and household data to create mandatory location records
    for persons with work or school locations.

    Args:
        persons_ctramp: Formatted CT-RAMP persons DataFrame with person_id, hh_id,
            person_num, person_type (integer code), age (non-binned continuous),
            employment_category, student_category, work_taz, school_taz
        households_ctramp: Formatted CT-RAMP households DataFrame with hh_id, income,
            home_taz (for HomeTAZ field)
        linked_trips_canonical: Canonical linked trips DataFrame for distance calculations
        config: CT-RAMP configuration with income_base_year_dollars

    Returns:
        DataFrame with CT-RAMP mandatory location fields:
            - HHID, PersonID, PersonNum
            - HomeTAZ, Income
            - PersonType, PersonAge
            - EmploymentCategory, StudentCategory
            - WorkLocation, SchoolLocation

    Notes:
        - Excludes model-only fields (walk subzones)
        - Filters to only persons with work OR school locations
        - Uses pre-computed employment_category and student_category from persons_ctramp
    """
    logger.info("Formatting mandatory location data for CT-RAMP")

    # Join persons with households to get income and home TAZ, lat/lon
    mandatory_loc = persons_ctramp.join(
        households_ctramp.select(
            ["hh_id", "home_lat", "home_lon", f"home_{config.taz_field}", "income"]
        ),
        on="hh_id",
        how="left",
    )

    # Filter to only persons with work or school locations
    # Add columns as null if they don't exist
    if f"work_{config.taz_field}" not in mandatory_loc.columns:
        mandatory_loc = mandatory_loc.with_columns(
            pl.lit(None).cast(pl.Int64).alias(f"work_{config.taz_field}")
        )
    if f"school_{config.taz_field}" not in mandatory_loc.columns:
        mandatory_loc = mandatory_loc.with_columns(
            pl.lit(None).cast(pl.Int64).alias(f"school_{config.taz_field}")
        )

    mandatory_loc = mandatory_loc.filter(
        (
            pl.col(f"work_{config.taz_field}").is_not_null()
            & (pl.col(f"work_{config.taz_field}") > 0)
        )
        | (
            pl.col(f"school_{config.taz_field}").is_not_null()
            & (pl.col(f"school_{config.taz_field}") > 0)
        )
    )

    # Calculate fixed work/school distances for validation for persons that have fixed locations
    for fixed_type in ["work", "school"]:
        distances_df = calc_fixed_location_distances(
            mandatory_locations=mandatory_loc,
            linked_trips_canonical=linked_trips_canonical,
            fixed_type=fixed_type,
            config=config,
        )
        mandatory_loc = mandatory_loc.join(
            distances_df,
            on="person_id",
            how="left",
        ).rename({f"{fixed_type}_distance_{config.distance_unit}": f"{fixed_type}_distance_survey"})

    # Map to CT-RAMP column names
    mandatory_loc = mandatory_loc.select(
        [
            pl.col("hh_id").alias("HHID"),
            pl.col(f"home_{config.taz_field}").cast(pl.Int64).alias("HomeTAZ"),
            (pl.col("income") / config.income_base_year_dollars).cast(pl.Int64).alias("Income"),
            pl.col("person_id").alias("PersonID"),
            pl.col("person_num").alias("PersonNum"),
            pl.col("person_type").alias("PersonType"),
            pl.col("age").alias("PersonAge"),
            pl.col("employment_category").alias("EmploymentCategory"),
            pl.col("student_category").alias("StudentCategory"),
            pl.col(f"work_{config.taz_field}").fill_null(0).cast(pl.Int64).alias("WorkLocation"),
            pl.col(f"school_{config.taz_field}")
            .fill_null(0)
            .cast(pl.Int64)
            .alias("SchoolLocation"),
            "work_distance_survey",
            "school_distance_survey",
        ]
    )

    logger.info("Formatted %d mandatory location records", len(mandatory_loc))
    return mandatory_loc
