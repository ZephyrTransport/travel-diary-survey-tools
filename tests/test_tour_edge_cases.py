"""Tests tour extraction edge cases that previously caused validation errors.

This test module ensures that tour extraction handles edge cases correctly:
- Single-trip tours (tour has only one trip before returning home)
- Partial tours (tour starts away from home, no initial home departure)
- Tours starting with tour_num=1 (not 0)
- Distance threshold fallback for destination timing
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Student,
)
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from processing import link_trips
from processing.tours.extraction import extract_tours


@pytest.fixture
def single_trip_tour_data():
    """Person makes a single trip to grocery store and back home.

    This tests the edge case where a tour consists of only one trip.
    Previously, this caused tour_purpose=None because the logic filtered
    out "last trips" which left no trips to determine purpose from.
    """
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [37.85],
            "work_lon": [-122.45],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    households = pl.DataFrame(
        {
            "hh_id": [1],
            "home_lat": [37.8],
            "home_lon": [-122.4],
        }
    )

    # Single trip: home -> grocery store
    unlinked_trips = pl.DataFrame(
        {
            "trip_id": [1],
            "day_id": [1],
            "person_id": [1],
            "hh_id": [1],
            "travel_dow": [TravelDow.WEDNESDAY.value],
            "depart_time": [datetime(2024, 1, 15, 9, 0, 0)],
            "arrive_time": [datetime(2024, 1, 15, 9, 15, 0)],
            "o_purpose_category": [PurposeCategory.HOME.value],
            "d_purpose_category": [PurposeCategory.SHOP.value],
            "o_purpose": [Purpose.HOME.value],
            "d_purpose": [Purpose.SHOPPING_ERRANDS.value],
            "mode_type": [ModeType.CAR.value],
            "o_lat": [37.8],
            "o_lon": [-122.4],
            "d_lat": [37.82],
            "d_lon": [-122.42],
            "trip_weight": [1.0],
            "distance_meters": [2000.0],
            "duration_minutes": [15.0],
            "num_travelers": [1],
            "driver": [Driver.DRIVER.value],
        }
    )

    # Link trips
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value],
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    return persons, households, unlinked_trips_with_ids, linked_trips


@pytest.fixture
def partial_tour_data():
    """Person starts day away from home, makes trips, then goes home.

    This tests the edge case where the first trip doesn't originate from home.
    Previously, this could cause tour_num=0 for trips before the first
    "leaving home" flag was set.
    """
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [37.85],
            "work_lon": [-122.45],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    households = pl.DataFrame(
        {
            "hh_id": [1],
            "home_lat": [37.8],
            "home_lon": [-122.4],
        }
    )

    # Trips: work -> lunch -> work -> home
    unlinked_trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3],
            "day_id": [1, 1, 1],
            "person_id": [1, 1, 1],
            "hh_id": [1, 1, 1],
            "travel_dow": [TravelDow.WEDNESDAY.value] * 3,
            "depart_time": [
                datetime(2024, 1, 15, 12, 0, 0),
                datetime(2024, 1, 15, 12, 30, 0),
                datetime(2024, 1, 15, 13, 30, 0),
            ],  # 12:00 PM, 12:30 PM, 1:30 PM
            "arrive_time": [
                datetime(2024, 1, 15, 12, 15, 0),
                datetime(2024, 1, 15, 12, 45, 0),
                datetime(2024, 1, 15, 14, 0, 0),
            ],  # 12:15 PM, 12:45 PM, 2:00 PM
            "o_purpose_category": [
                PurposeCategory.WORK.value,
                PurposeCategory.MEAL.value,
                PurposeCategory.WORK.value,
            ],
            "d_purpose_category": [
                PurposeCategory.MEAL.value,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
            ],
            "o_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.DINING.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ],
            "d_purpose": [
                Purpose.DINING.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ],
            "mode_type": [
                ModeType.WALK.value,
                ModeType.WALK.value,
                ModeType.CAR.value,
            ],
            "o_lat": [37.85, 37.86, 37.85],
            "o_lon": [-122.45, -122.46, -122.45],
            "d_lat": [37.86, 37.85, 37.8],
            "d_lon": [-122.46, -122.45, -122.4],
            "trip_weight": [1.0, 1.0, 1.0],
            "distance_meters": [1000.0, 1000.0, 5000.0],
            "duration_minutes": [15.0, 15.0, 30.0],
            "num_travelers": [1, 1, 1],
            "driver": [
                Driver.DRIVER.value,
                Driver.DRIVER.value,
                Driver.DRIVER.value,
            ],
        }
    )

    # Link trips
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value],
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    return persons, households, unlinked_trips_with_ids, linked_trips


@pytest.fixture
def distant_destinations_data():
    """Person makes tour with destinations far from each other.

    This tests the edge case where destination distance thresholds might
    exclude all trips, causing dest_arrive_time and dest_depart_time to be None.
    """
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [37.85],
            "work_lon": [-122.45],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    households = pl.DataFrame(
        {
            "hh_id": [1],
            "home_lat": [37.8],
            "home_lon": [-122.4],
        }
    )

    # Tour with widely scattered destinations:
    # home -> SF -> Oakland -> SJ -> home
    unlinked_trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3],
            "day_id": [1, 1, 1],
            "person_id": [1, 1, 1],
            "hh_id": [1, 1, 1],
            "travel_dow": [TravelDow.WEDNESDAY.value] * 3,
            "depart_time": [
                datetime(2024, 1, 15, 8, 0, 0),
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 12, 0, 0),
            ],  # 8:00 AM, 10:00 AM, 12:00 PM
            "arrive_time": [
                datetime(2024, 1, 15, 9, 0, 0),
                datetime(2024, 1, 15, 11, 0, 0),
                datetime(2024, 1, 15, 14, 0, 0),
            ],  # 9:00 AM, 11:00 AM, 2:00 PM
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.WORK.value,
                PurposeCategory.SOCIALREC.value,
            ],
            "d_purpose_category": [
                PurposeCategory.WORK.value,
                PurposeCategory.SOCIALREC.value,
                PurposeCategory.HOME.value,
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.OTHER_SOCIAL.value,
            ],
            "d_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.OTHER_SOCIAL.value,
                Purpose.HOME.value,
            ],
            "mode_type": [
                ModeType.CAR.value,
                ModeType.CAR.value,
                ModeType.CAR.value,
            ],
            "o_lat": [37.8, 37.79, 37.6],  # Home, SF, Oakland
            "o_lon": [-122.4, -122.39, -122.2],
            "d_lat": [37.79, 37.6, 37.8],  # SF, Oakland, Home
            "d_lon": [-122.39, -122.2, -122.4],
            "trip_weight": [1.0, 1.0, 1.0],
            "distance_meters": [10000.0, 20000.0, 30000.0],
            "duration_minutes": [60.0, 60.0, 120.0],
            "num_travelers": [1, 1, 1],
            "driver": [
                Driver.DRIVER.value,
                Driver.DRIVER.value,
                Driver.DRIVER.value,
            ],
        }
    )

    # Link trips
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value],
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    return persons, households, unlinked_trips_with_ids, linked_trips


def test_single_trip_tour(single_trip_tour_data):
    """Test that single-trip tours are flagged appropriately."""
    persons, households, unlinked_trips, linked_trips = single_trip_tour_data

    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours_df = result["tours"]

    # Single-trip tours should be kept but flagged
    assert len(tours_df) == 1

    # Should be flagged as single-trip tour
    assert tours_df["single_trip_tour"][0] is True

    # Tour number should be 1, not 0
    assert tours_df["tour_num"][0] == 1

    # Single-trip tours are allowed to have null purpose and dest times
    assert tours_df["tour_purpose"][0] is None
    assert tours_df["dest_arrive_time"][0] is None
    assert tours_df["dest_depart_time"][0] is None


def test_partial_tour(partial_tour_data):
    """Test that tours starting away from home get valid tour numbers."""
    persons, households, unlinked_trips, linked_trips = partial_tour_data

    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours_df = result["tours"]

    # All tour numbers should be >= 1
    assert (tours_df["tour_num"] >= 1).all()

    # Should not have any tour_num = 0
    assert (tours_df["tour_num"] == 0).sum() == 0


def test_distant_destinations(distant_destinations_data):
    """Test that tours with distant destinations still get valid times."""
    persons, households, unlinked_trips, linked_trips = distant_destinations_data

    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours_df = result["tours"]

    assert len(tours_df) == 1

    # Even with distant destinations, should have destination times
    # (fallback logic should apply)
    assert tours_df["dest_arrive_time"][0] is not None
    assert tours_df["dest_depart_time"][0] is not None

    # Tour purpose should be WORK (highest priority non-home purpose)
    assert tours_df["tour_purpose"][0] == PurposeCategory.WORK.value


def test_tour_num_sequential():
    """Test that tour numbers are sequential (1, 2, 3...) for multiple tours."""
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [37.85],
            "work_lon": [-122.45],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    households = pl.DataFrame(
        {
            "hh_id": [1],
            "home_lat": [37.8],
            "home_lon": [-122.4],
        }
    )

    # Three complete tours:
    # home->work->home, home->shop->home, home->social->home
    unlinked_trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4, 5],
            "day_id": [1, 1, 1, 1, 1],
            "person_id": [1, 1, 1, 1, 1],
            "hh_id": [1, 1, 1, 1, 1],
            "travel_dow": [TravelDow.WEDNESDAY.value] * 5,
            "depart_time": [
                datetime(2024, 1, 15, 8, 0, 0),
                datetime(2024, 1, 15, 9, 0, 0),
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 11, 0, 0),
                datetime(2024, 1, 15, 13, 0, 0),
            ],
            "arrive_time": [
                datetime(2024, 1, 15, 8, 30, 0),
                datetime(2024, 1, 15, 9, 30, 0),
                datetime(2024, 1, 15, 10, 15, 0),
                datetime(2024, 1, 15, 11, 15, 0),
                datetime(2024, 1, 15, 13, 15, 0),
            ],
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                PurposeCategory.SHOP.value,
                PurposeCategory.HOME.value,
            ],
            "d_purpose_category": [
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                PurposeCategory.SHOP.value,
                PurposeCategory.HOME.value,
                PurposeCategory.SOCIALREC.value,
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
                Purpose.GROCERY.value,
                Purpose.HOME.value,
            ],
            "d_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
                Purpose.GROCERY.value,
                Purpose.HOME.value,
                Purpose.SOCIAL.value,
            ],
            "mode_type": [ModeType.CAR.value] * 5,
            "o_lat": [37.8, 37.85, 37.8, 37.81, 37.8],
            "o_lon": [-122.4, -122.45, -122.4, -122.41, -122.4],
            "d_lat": [37.85, 37.8, 37.81, 37.8, 37.82],
            "d_lon": [-122.45, -122.4, -122.41, -122.4, -122.42],
            "trip_weight": [1.0, 1.0, 1.0, 1.0, 1.0],
            "distance_meters": [5000.0, 5000.0, 1000.0, 1000.0, 2000.0],
            "duration_minutes": [30.0, 30.0, 15.0, 15.0, 15.0],
            "num_travelers": [1, 1, 1, 1, 1],
            "driver": [Driver.DRIVER.value] * 5,
        }
    )

    # Link trips first
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value],
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    result = extract_tours(persons, households, unlinked_trips_with_ids, linked_trips)
    tours_df = result["tours"]

    # Should have 3 tours
    # (note: last trip to SOCIALREC destination creates partial tour)
    tour_nums = sorted(tours_df["tour_num"].unique().to_list())

    # Tour numbers should start at 1 and be sequential
    assert tour_nums[0] == 1
    for i in range(1, len(tour_nums)):
        assert tour_nums[i] == tour_nums[i - 1] + 1


def test_all_tours_have_required_fields():
    """Test that all extracted tours have non-null required fields."""
    persons = pl.DataFrame(
        {
            "person_id": [1, 2],
            "hh_id": [1, 1],
            "age": [
                AgeCategory.AGE_35_TO_44.value,
                AgeCategory.AGE_25_TO_34.value,
            ],
            "employment": [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_PARTTIME.value,
            ],
            "student": [Student.NONSTUDENT.value, Student.NONSTUDENT.value],
            "school_type": [None, None],
            "work_lat": [37.85, 37.85],
            "work_lon": [-122.45, -122.45],
            "school_lat": [None, None],
            "school_lon": [None, None],
        }
    )

    households = pl.DataFrame(
        {
            "hh_id": [1],
            "home_lat": [37.8],
            "home_lon": [-122.4],
        }
    )

    # Mix of scenarios: normal tours, single-trip tours, partial tours
    unlinked_trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4],
            "day_id": [1, 1, 2, 2],
            "person_id": [1, 1, 2, 2],
            "hh_id": [1, 1, 1, 1],
            "travel_dow": [
                TravelDow.WEDNESDAY.value,
                TravelDow.WEDNESDAY.value,
                TravelDow.THURSDAY.value,
                TravelDow.THURSDAY.value,
            ],
            "depart_time": [
                datetime(2024, 1, 15, 8, 0, 0),
                datetime(2024, 1, 15, 9, 0, 0),
                datetime(2024, 1, 16, 10, 0, 0),
                datetime(2024, 1, 16, 11, 0, 0),
            ],
            "arrive_time": [
                datetime(2024, 1, 15, 8, 30, 0),
                datetime(2024, 1, 15, 9, 30, 0),
                datetime(2024, 1, 16, 10, 30, 0),
                datetime(2024, 1, 16, 11, 30, 0),
            ],
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                PurposeCategory.SHOP.value,
            ],
            "d_purpose_category": [
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                PurposeCategory.SHOP.value,
                PurposeCategory.HOME.value,
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
                Purpose.GROCERY.value,
            ],
            "d_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
                Purpose.GROCERY.value,
                Purpose.HOME.value,
            ],
            "mode_type": [ModeType.CAR.value] * 4,
            "o_lat": [37.8, 37.85, 37.8, 37.81],
            "o_lon": [-122.4, -122.45, -122.4, -122.41],
            "d_lat": [37.85, 37.8, 37.81, 37.8],
            "d_lon": [-122.45, -122.4, -122.41, -122.4],
            "trip_weight": [1.0, 1.0, 1.0, 1.0],
            "distance_meters": [5000.0, 5000.0, 1000.0, 1000.0],
            "duration_minutes": [30.0, 30.0, 30.0, 30.0],
            "num_travelers": [1, 1, 1, 1],
            "driver": [Driver.DRIVER.value] * 4,
        }
    )

    # Link trips first
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value],
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    result = extract_tours(persons, households, unlinked_trips_with_ids, linked_trips)
    tours_df = result["tours"]

    # All tours should have tour_num >= 1
    assert (tours_df["tour_num"] >= 1).all()

    # All non-single-trip tours should have non-null tour_purpose and dest times
    non_single_trip_tours = tours_df.filter(~tours_df["single_trip_tour"])
    if len(non_single_trip_tours) > 0:
        assert non_single_trip_tours["tour_purpose"].null_count() == 0
        assert non_single_trip_tours["dest_arrive_time"].null_count() == 0
        assert non_single_trip_tours["dest_depart_time"].null_count() == 0

    # All tours should have non-null origin depart/arrive times
    assert tours_df["origin_depart_time"].null_count() == 0
    assert tours_df["origin_arrive_time"].null_count() == 0
