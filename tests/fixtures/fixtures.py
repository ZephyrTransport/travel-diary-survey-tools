"""Pytest fixtures and pipeline processing helpers for canonical test data.

This module provides pytest fixtures that run scenarios through the
link_trips → extract_tours pipeline, and helper functions for processing
test data.
"""

import polars as pl
import pytest

from data_canon.codebook.trips import PurposeCategory
from processing.link_trips import link_trips
from processing.tours import extract_tours

from .base_records import create_day
from .locations import lookup_location
from .scenario_builders import (
    DEFAULT_TRANSIT_MODE_CODES,
    multi_person_household,
    multi_stop_tour,
    multi_tour_day,
    simple_work_tour,
    transit_commute,
)
from .trip_records import create_unlinked_trip

# ==============================================================================
# Processed Scenario Functions
# ==============================================================================


def create_simple_work_tour_processed(
    hh_id: int = 1,
    person_id: int = 101,
) -> dict[str, pl.DataFrame]:
    """Create simple work tour processed through pipeline.

    Returns processed data ready for formatter testing.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                       linked_trips, tours
    """
    households, persons, days, unlinked_trips = simple_work_tour(hh_id, person_id)
    return process_scenario_through_pipeline(households, persons, days, unlinked_trips)


def create_transit_commute_processed() -> dict[str, pl.DataFrame]:
    """Create transit commute scenario with all processing applied.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                       linked_trips, tours
    """
    households, persons, days, unlinked_trips = transit_commute()
    return process_scenario_through_pipeline(households, persons, days, unlinked_trips)


def create_multi_person_household_processed(
    hh_id: int = 1,
) -> dict[str, pl.DataFrame]:
    """Create multi-person household processed through pipeline.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                       linked_trips, tours
    """
    households, persons = multi_person_household(hh_id=hh_id)

    # Create simple days for all persons
    days_list = []
    unlinked_trips_list = []
    trip_id = 1

    for person in persons.iter_rows(named=True):
        person_id = person["person_id"]
        person_num = person["person_num"]

        days_list.append(
            create_day(
                day_id=person_id,
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
                        trip_id=trip_id,
                        person_id=person_id,
                        hh_id=hh_id,
                        day_id=person_id,
                        person_num=person_num,
                        o_purpose_category=PurposeCategory.HOME,
                        d_purpose_category=PurposeCategory.WORK,
                        o_lat=home_lat,
                        o_lon=home_lon,
                        d_lat=person["work_lat"],
                        d_lon=person["work_lon"],
                    ),
                    create_unlinked_trip(
                        trip_id=trip_id + 1,
                        person_id=person_id,
                        hh_id=hh_id,
                        day_id=person_id,
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
            trip_id += 2

    days = pl.DataFrame(days_list)
    unlinked_trips = pl.DataFrame(unlinked_trips_list) if unlinked_trips_list else pl.DataFrame()

    if not unlinked_trips.is_empty():
        return process_scenario_through_pipeline(households, persons, days, unlinked_trips)
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": pl.DataFrame(),
        "tours": pl.DataFrame(),
    }


# ==============================================================================
# Pipeline Processing Helper
# ==============================================================================


def add_test_taz_maz_ids(
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
) -> tuple[
    pl.DataFrame | None,
    pl.DataFrame | None,
    pl.DataFrame | None,
    pl.DataFrame | None,
]:
    """Add TAZ/MAZ IDs to dataframes using mock spatial join.

    Simulates project-specific spatial join step that assigns TAZ/MAZ IDs
    based on lat/lon coordinates. Uses simple dictionary lookup from
    locations.py for test scenarios.

    Args:
        households: Households DataFrame (optional)
        persons: Persons DataFrame (optional)
        unlinked_trips: Unlinked trips DataFrame (optional)
        linked_trips: Linked trips DataFrame (optional)
        tours: Tours DataFrame (optional)

    Returns:
        Tuple of (unlinked_trips, linked_trips, tours, households) with TAZ/MAZ
    """

    def assign_zone_ids(
        df: pl.DataFrame, lat_col: str, lon_col: str, taz_col: str, maz_col: str
    ) -> pl.DataFrame:
        """Assign TAZ/MAZ to a dataframe based on lat/lon columns."""
        if df.is_empty():
            # Just add the columns if empty
            return df.with_columns(
                [
                    pl.lit(None, dtype=pl.Int64).alias(taz_col),
                    pl.lit(None, dtype=pl.Int64).alias(maz_col),
                ]
            )

        # Build lookup lists
        taz_list = []
        maz_list = []

        for row in df.iter_rows(named=True):
            lat = row.get(lat_col)
            lon = row.get(lon_col)
            location = lookup_location(lat, lon)

            if location:
                taz_list.append(location.taz)
                maz_list.append(location.maz)
            else:
                taz_list.append(None)
                maz_list.append(None)

        # Add TAZ/MAZ columns
        return df.with_columns(
            [
                pl.Series(taz_col, taz_list, dtype=pl.Int64),
                pl.Series(maz_col, maz_list, dtype=pl.Int64),
            ]
        )

    # Assign TAZ/MAZ to households (home location) if provided
    if households is not None:
        households = assign_zone_ids(households, "home_lat", "home_lon", "home_taz", "home_maz")

    # Assign TAZ/MAZ to persons (work and school locations) if provided
    if persons is not None:
        persons = assign_zone_ids(persons, "work_lat", "work_lon", "work_taz", "work_maz")
        persons = assign_zone_ids(persons, "school_lat", "school_lon", "school_taz", "school_maz")

    # Assign TAZ/MAZ to unlinked trips (origin and destination) if provided
    if unlinked_trips is not None:
        unlinked_trips = assign_zone_ids(unlinked_trips, "o_lat", "o_lon", "o_taz", "o_maz")
        unlinked_trips = assign_zone_ids(unlinked_trips, "d_lat", "d_lon", "d_taz", "d_maz")

    # Assign TAZ/MAZ to linked trips (origin and destination) if provided
    if linked_trips is not None:
        linked_trips = assign_zone_ids(linked_trips, "o_lat", "o_lon", "o_taz", "o_maz")
        linked_trips = assign_zone_ids(linked_trips, "d_lat", "d_lon", "d_taz", "d_maz")

    # Assign TAZ/MAZ to tours (origin and destination) if provided
    if tours is not None:
        tours = assign_zone_ids(tours, "o_lat", "o_lon", "o_taz", "o_maz")
        tours = assign_zone_ids(tours, "d_lat", "d_lon", "d_taz", "d_maz")

    return unlinked_trips, linked_trips, tours, households


def process_scenario_through_pipeline(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Process a scenario through link_trips → extract_tours pipeline.

    Utility function for tests that need to process custom scenarios through
    the full pipeline with production defaults.

    Args:
        households: Households DataFrame
        persons: Persons DataFrame
        days: Days DataFrame
        unlinked_trips: Unlinked trips DataFrame

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    # Link trips (using config.yaml defaults)
    link_result = link_trips(
        unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=DEFAULT_TRANSIT_MODE_CODES,
        max_dwell_time=180,  # in minutes
        dwell_buffer_distance=100,  # in meters
    )
    linked_trips = link_result["linked_trips"]
    unlinked_trips = link_result["unlinked_trips"]  # Use updated unlinked trips with linked_trip_id

    # Extract tours (using config.yaml defaults)
    tour_result = extract_tours(
        persons=persons,
        households=households,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
    )

    # Add TAZ/MAZ IDs via mock spatial join (simulates project-specific step)
    (
        unlinked_with_zones,
        linked_with_zones,
        tours_with_zones,
        households_with_zones,
    ) = add_test_taz_maz_ids(
        households=households,
        persons=persons,
        unlinked_trips=tour_result["unlinked_trips"],
        linked_trips=tour_result["linked_trips"],
        tours=tour_result["tours"],
    )

    return {
        "households": households_with_zones,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_with_zones,
        "linked_trips": linked_with_zones,
        "tours": tours_with_zones,
    }


# ==============================================================================
# Pytest Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def simple_work_tour_processed():
    """Simple work tour processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    return create_simple_work_tour_processed()


@pytest.fixture(scope="module")
def multi_stop_tour_processed():
    """Multi-stop work tour processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    households, persons, days, unlinked_trips = multi_stop_tour()
    return process_scenario_through_pipeline(households, persons, days, unlinked_trips)


@pytest.fixture(scope="module")
def multi_tour_day_processed():
    """Multi-tour day processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    households, persons, days, unlinked_trips = multi_tour_day()
    return process_scenario_through_pipeline(households, persons, days, unlinked_trips)
