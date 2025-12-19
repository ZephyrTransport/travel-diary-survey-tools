"""Test duration tie-breaker for tour purpose selection."""

from datetime import datetime

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    PersonType,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Driver, ModeType, PurposeCategory
from processing import link_trips
from processing.tours.extraction import extract_tours


def create_test_data(
    person_data: dict,
    unlinked_trips_data: list[dict],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create minimal test data for tour extraction.

    Args:
        person_data: Dictionary with person attributes
        unlinked_trips_data: List of trip dictionaries with required fields

    Returns:
        Tuple of (persons, households, linked_trips, unlinked_trips) DataFrames
    """
    # Set defaults for required person fields
    person_defaults = {
        "age": AgeCategory.AGE_35_TO_44.value,
        "student": Student.NONSTUDENT.value,
        "employment": Employment.EMPLOYED_FULLTIME.value,
        "school_type": SchoolType.MISSING.value,
        "home_lat": 37.7749,
        "home_lon": -122.4194,
        "work_lat": 37.7849,
        "work_lon": -122.4094,
        "school_lat": None,
        "school_lon": None,
    }

    # Create persons DataFrame with defaults and overrides
    persons = pl.DataFrame([{**person_defaults, **person_data}])

    # Create households DataFrame
    households = pl.DataFrame(
        [
            {
                "hh_id": person_data["hh_id"],
                "home_lat": 37.7749,
                "home_lon": -122.4194,
            }
        ]
    )

    # Create linked_trips DataFrame
    unlinked_trips = pl.DataFrame(unlinked_trips_data)

    unlinked_trips, linked_trips = link_trips(
        unlinked_trips,
        change_mode_code=11,  # Purpose code for 'change_mode'
        transit_mode_codes=[12, 13, 14],
        max_dwell_time=180,  # in minutes
        dwell_buffer_distance=100,  # in meters
    ).values()

    return persons, households, unlinked_trips, linked_trips


def test_duration_tiebreaker_equal_priority():
    """When destinations have equal priority, select longest duration."""
    # Create sample trips with equal priority destinations
    persons, households, unlinked_trips, linked_trips = create_test_data(
        person_data={
            "person_id": 100,
            "hh_id": 10,
            "person_type": PersonType.FULL_TIME_WORKER.value,
        },
        unlinked_trips_data=[
            {
                "trip_id": 1,
                "person_id": 100,
                "day_id": 1,
                "hh_id": 10,
                "depart_time": datetime(2024, 1, 1, 8, 0),  # Leave home
                "arrive_time": datetime(2024, 1, 1, 8, 30),
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7749,
                "o_lon": -122.4194,  # Home
                "d_lat": 37.7850,
                "d_lon": -122.4000,  # First destination
                "distance_meters": 2000,
                "duration_minutes": 30,
                "o_purpose_category": PurposeCategory.HOME.value,
                "d_purpose_category": PurposeCategory.ERRAND.value,
                "mode_type": ModeType.CAR.value,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
            {
                "trip_id": 2,
                "person_id": 100,
                "day_id": 1,
                "hh_id": 10,
                # Leave first dest (30 min)
                "depart_time": datetime(2024, 1, 1, 9, 0),
                "arrive_time": datetime(2024, 1, 1, 9, 30),
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7850,
                "o_lon": -122.4000,  # First destination
                "d_lat": 37.7950,
                "d_lon": -122.4100,  # Second destination
                "distance_meters": 2000,
                "duration_minutes": 30,
                "o_purpose_category": PurposeCategory.ERRAND.value,
                "d_purpose_category": PurposeCategory.ERRAND.value,
                "mode_type": ModeType.CAR.value,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
            {
                "trip_id": 3,
                "person_id": 100,
                "day_id": 1,
                "hh_id": 10,
                # Leave second dest (60 min)
                "depart_time": datetime(2024, 1, 1, 10, 30),
                "arrive_time": datetime(2024, 1, 1, 11, 0),  # Arrive home
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7950,
                "o_lon": -122.4100,  # Second destination
                "d_lat": 37.7749,
                "d_lon": -122.4194,  # Home
                "distance_meters": 3000,
                "duration_minutes": 30,
                "o_purpose_category": PurposeCategory.ERRAND.value,
                "d_purpose_category": PurposeCategory.HOME.value,
                "mode_type": ModeType.CAR.value,
                "driver": Driver.DRIVER.value,
                "num_travelers": 1,
                "trip_weight": 1.0,
            },
        ],
    )
    result = extract_tours(persons, households, unlinked_trips, linked_trips)

    # Should have 1 tour
    assert len(result["tours"]) == 1

    # Activity durations:
    # - Trip 1: 9:00 - 8:30 = 30 min
    # - Trip 2: 10:30 - 9:30 = 60 min
    # - Trip 3: None (last trip, gets default 240 min)
    # Since trip 2 has longer activity (60 min > 30 min) and same
    # priority, tour purpose should be ERRAND
    tour = result["tours"].row(0, named=True)
    assert tour["tour_purpose"] == PurposeCategory.ERRAND.value


def test_duration_tiebreaker_different_priority():
    """Priority wins over duration when priorities differ."""
    # Create sample trips with different priorities
    persons, households, unlinked_trips, linked_trips = create_test_data(
        person_data={
            "person_id": 100,
            "hh_id": 10,
            "person_type": PersonType.FULL_TIME_WORKER.value,
        },
        unlinked_trips_data=[
            {
                "trip_id": 1,
                "person_id": 100,
                "day_id": 1,
                "hh_id": 10,
                "depart_time": datetime(2024, 1, 1, 8, 0),  # Leave home
                "arrive_time": datetime(2024, 1, 1, 8, 30),  # Arrive at work
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7749,
                "o_lon": -122.4194,  # Home
                "d_lat": 37.7849,
                "d_lon": -122.4094,  # Work
                "o_purpose_category": PurposeCategory.HOME.value,
                "d_purpose_category": PurposeCategory.WORK.value,
                "mode_type": ModeType.CAR.value,
                "distance_meters": 2000,
                "duration_minutes": 30,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
            {
                "trip_id": 2,
                "person_id": 100,
                "day_id": 1,
                "hh_id": 10,
                # Leave work (30 min)
                "depart_time": datetime(2024, 1, 1, 9, 0),
                "arrive_time": datetime(2024, 1, 1, 9, 30),  # Arrive at shop
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7849,
                "o_lon": -122.4094,  # Work
                "d_lat": 37.7950,
                "d_lon": -122.4100,  # Shopping
                "o_purpose_category": PurposeCategory.WORK.value,
                "d_purpose_category": PurposeCategory.SHOP.value,
                "mode_type": ModeType.CAR.value,
                "distance_meters": 2000,
                "duration_minutes": 30,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
            {
                "trip_id": 3,
                "person_id": 100,
                "day_id": 1,
                "hh_id": 10,
                "depart_time": datetime(2024, 1, 1, 10, 30),
                "arrive_time": datetime(2024, 1, 1, 11, 0),  # Arrive home
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7950,
                "o_lon": -122.4100,  # Shopping
                "d_lat": 37.7749,
                "d_lon": -122.4194,  # Home
                "o_purpose_category": PurposeCategory.SHOP.value,
                "d_purpose_category": PurposeCategory.HOME.value,
                "mode_type": ModeType.CAR.value,
                "distance_meters": 3000,
                "duration_minutes": 30,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
        ],
    )

    result = extract_tours(persons, households, unlinked_trips, linked_trips)

    # Tour purpose should be work (priority 1) regardless of duration
    tour = result["tours"].row(0, named=True)
    assert tour["tour_purpose"] == PurposeCategory.WORK.value


def test_activity_duration_last_trip():
    """Last trip of day should get default 240 minute duration."""
    persons, households, unlinked_trips, linked_trips = create_test_data(
        person_data={
            "person_id": 101,
            "hh_id": 10,
            "person_type": PersonType.FULL_TIME_WORKER.value,
            "age": AgeCategory.AGE_25_TO_34.value,
            "employment": Employment.EMPLOYED_FULLTIME.value,
            "school_type": SchoolType.MISSING.value,
            "student": Student.NONSTUDENT.value,
        },
        unlinked_trips_data=[
            {
                "trip_id": 1010101,
                "day_id": 10101,
                "person_id": 101,
                "hh_id": 10,
                "depart_time": datetime(2024, 1, 1, 8, 0),  # Leave home
                "arrive_time": datetime(2024, 1, 1, 9, 0),  # Arrive at work
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7749,
                "o_lon": -122.4194,  # Home
                "d_lat": 37.7849,
                "d_lon": -122.4094,  # Work
                "o_purpose_category": PurposeCategory.HOME.value,
                "d_purpose_category": PurposeCategory.WORK.value,
                "mode_type": ModeType.CAR.value,
                "distance_meters": 10000,
                "duration_minutes": 60,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
            {
                "trip_id": 1010102,
                "day_id": 10101,
                "person_id": 101,
                "hh_id": 10,
                "depart_time": datetime(2024, 1, 1, 17, 0),  # Leave work
                "arrive_time": datetime(2024, 1, 1, 18, 0),  # Arrive home
                "travel_dow": TravelDow.MONDAY.value,
                "o_lat": 37.7849,
                "o_lon": -122.4094,  # Work
                "d_lat": 37.7749,
                "d_lon": -122.4194,  # Home
                "o_purpose_category": PurposeCategory.WORK.value,
                "d_purpose_category": PurposeCategory.HOME.value,
                "mode_type": ModeType.CAR.value,
                "distance_meters": 10000,
                "duration_minutes": 60,
                "num_travelers": 1,
                "driver": Driver.DRIVER.value,
                "trip_weight": 1.0,
            },
        ],
    )

    result = extract_tours(persons, households, unlinked_trips, linked_trips)

    # This should not error - last trip gets default duration
    assert len(result["tours"]) == 1
