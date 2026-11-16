"""Pytest fixtures and pipeline processing helpers for canonical test data.

This module provides pytest fixtures that run scenarios through the
link_trips → extract_tours pipeline, and helper functions for processing
test data.
"""

import polars as pl

from data_canon.codebook.trips import PurposeCategory
from processing.link_trips import link_trips

from .locations import LOCATIONS
from .records import create_day, create_unlinked_trip
from .scenario_builders import (
    DEFAULT_TRANSIT_MODE_CODES,
    multi_person_household,
    simple_work_tour,
    transit_commute,
)
from .tour_pipeline import locate_and_extract_tours

# The coordinate prefixes whose zones each table carries.
_ZONED = {
    "households": ["home"],
    "persons": ["work", "school"],
    "unlinked_trips": ["o", "d"],
    "linked_trips": ["o", "d"],
    "tours": ["o", "d"],
}


def create_simple_work_tour_processed(
    hh_id: int = 1,
    person_id: int = 101,
) -> dict[str, pl.DataFrame]:
    """Create simple work tour processed through pipeline.

    Returns processed data ready for formatter testing.
    """
    households, persons, person_days, unlinked_trips = simple_work_tour(hh_id, person_id)
    return process_scenario_through_pipeline(households, persons, person_days, unlinked_trips)


def create_transit_commute_processed() -> dict[str, pl.DataFrame]:
    """Create transit commute scenario with all processing applied."""
    households, persons, person_days, unlinked_trips = transit_commute()
    return process_scenario_through_pipeline(households, persons, person_days, unlinked_trips)


def create_multi_person_household_processed(
    hh_id: int = 1,
) -> dict[str, pl.DataFrame]:
    """Create multi-person household processed through pipeline."""
    households, persons = multi_person_household(hh_id=hh_id)

    # Create simple days for all persons
    days_list = []
    unlinked_trips_list = []
    unlinked_trip_id = 1

    for person in persons.iter_rows(named=True):
        person_id = person["person_id"]
        person_num = person["person_num"]

        days_list.append(
            create_day(
                person_day_id=person_id,
                person_id=person_id,
                hh_id=hh_id,
                person_num=person_num,
                day_num=1,
            )
        )

        # Create simple home->work->home trips for workers
        if person.get("work_lat") is not None and person.get("work_lon") is not None:
            home_lat = households["home_lat"][0]
            home_lon = households["home_lon"][0]
            unlinked_trips_list.extend(
                [
                    create_unlinked_trip(
                        unlinked_trip_id=unlinked_trip_id,
                        person_id=person_id,
                        hh_id=hh_id,
                        person_day_id=person_id,
                        person_num=person_num,
                        o_purpose_category=PurposeCategory.HOME,
                        d_purpose_category=PurposeCategory.WORK,
                        o_lat=home_lat,
                        o_lon=home_lon,
                        d_lat=person["work_lat"],
                        d_lon=person["work_lon"],
                    ),
                    create_unlinked_trip(
                        unlinked_trip_id=unlinked_trip_id + 1,
                        person_id=person_id,
                        hh_id=hh_id,
                        person_day_id=person_id,
                        person_num=person_num,
                        o_purpose_category=PurposeCategory.WORK,
                        d_purpose_category=PurposeCategory.HOME,
                        o_lat=person["work_lat"],
                        o_lon=person["work_lon"],
                        d_lat=home_lat,
                        d_lon=home_lon,
                    ),
                ]
            )
            unlinked_trip_id += 2

    person_days = pl.DataFrame(days_list)
    unlinked_trips = pl.DataFrame(unlinked_trips_list) if unlinked_trips_list else pl.DataFrame()

    if not unlinked_trips.is_empty():
        return process_scenario_through_pipeline(households, persons, person_days, unlinked_trips)
    return {
        "households": households,
        "persons": persons,
        "person_days": person_days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": pl.DataFrame(),
        "tours": pl.DataFrame(),
    }


def _with_zones(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """*df* with ``<prefix>_taz`` and ``<prefix>_maz`` looked up from its coordinates."""
    lat, lon = (
        df[col] if col in df.columns else [None] * df.height
        for col in (f"{prefix}_lat", f"{prefix}_lon")
    )
    found = [LOCATIONS.get(point) for point in zip(lat, lon, strict=True)]
    return df.with_columns(
        pl.Series(f"{prefix}_taz", [loc and loc.taz for loc in found], dtype=pl.Int64),
        pl.Series(f"{prefix}_maz", [loc and loc.maz for loc in found], dtype=pl.Int64),
    )


def add_test_taz_maz_ids(**tables: pl.DataFrame | None) -> dict[str, pl.DataFrame]:
    """Stand in for the zone join: each table given, with zone ids for its known coordinates."""
    result = {}
    for name, prefixes in _ZONED.items():
        df = tables.get(name)
        if df is not None:
            for prefix in prefixes:
                df = _with_zones(df, prefix)
            result[name] = df
    return result


def process_scenario_through_pipeline(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    person_days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Process a scenario through link_trips → extract_tours pipeline.

    Utility function for tests that need to process custom scenarios through
    the full pipeline with production defaults.
    """
    # Link trips (using config.yaml defaults)
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=DEFAULT_TRANSIT_MODE_CODES,
        max_dwell_time=180,  # in minutes
        dwell_buffer_distance=100,  # in meters
        split_on_occupancy=False,
    )
    linked_trips = link_result["linked_trips"].with_columns(
        pl.lit(None).cast(pl.Int64).alias("joint_trip_id")
    )
    unlinked_trips = link_result["unlinked_trips"]  # Use updated unlinked trips with linked_trip_id

    # Extract tours (using config.yaml defaults)
    tour_result = locate_and_extract_tours(
        persons=persons,
        households=households,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
    )

    # Add TAZ/MAZ IDs via mock spatial join (simulates project-specific step)
    data_with_zones = add_test_taz_maz_ids(
        households=households,
        persons=persons,
        unlinked_trips=tour_result["unlinked_trips"],
        linked_trips=tour_result["linked_trips"],
        tours=tour_result["tours"],
    )
    data_with_zones["person_days"] = person_days  # Days do not need zone IDs

    # Stamp the usability verdict the formatters read. The real pipeline gets it
    # from cascade_usability, which needs reporting flags these synthetic
    # scenarios do not carry -- so mark every record usable, which is what these
    # fixtures mean: they exist to exercise formatting, not gating. A test about
    # gating stamps its own values.
    for name, frame in data_with_zones.items():
        if "usable_test" not in frame.columns:
            data_with_zones[name] = frame.with_columns(pl.lit(value=True).alias("usable_test"))

    return data_with_zones
