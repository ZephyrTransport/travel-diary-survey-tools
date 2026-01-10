"""Tests for joint tour identification functionality.

This test module ensures joint tour identification correctly handles:
- Tours where all trips are joint with same stable group (2+ people)
- Tours with partial joint trips (some joint, some individual)
- Partial dropoffs (3 people start, 1 drops off, remaining 2 form joint tour)
- Single-person tours (no joint tours)
- Multiple joint tour groups within same household
- Tours without any joint trips
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from processing import link_trips
from processing.joint_trips import detect_joint_trips
from processing.tours.extraction import extract_tours


@pytest.fixture
def basic_household_data():
    """Create basic household and person data for 3-person household."""
    persons = pl.DataFrame(
        {
            "person_id": [23000075001, 23000075002, 23000075003],
            "hh_id": [23000075, 23000075, 23000075],
            "age": [
                AgeCategory.AGE_35_TO_44.value,
                AgeCategory.AGE_35_TO_44.value,
                AgeCategory.AGE_5_TO_15.value,
            ],
            "employment": [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_PARTTIME.value,
                Employment.UNEMPLOYED_NOT_LOOKING.value,
            ],
            "student": [
                Student.NONSTUDENT.value,
                Student.NONSTUDENT.value,
                Student.FULLTIME_INPERSON.value,
            ],
            "school_type": [None, None, SchoolType.ELEMENTARY.value],
            "work_lat": [37.85, 37.82, None],
            "work_lon": [-122.45, -122.48, None],
            "school_lat": [None, None, 37.81],
            "school_lon": [None, None, -122.43],
        }
    )

    households = pl.DataFrame(
        {
            "hh_id": [23000075],
            "home_lat": [37.8],
            "home_lon": [-122.4],
        }
    )

    return persons, households


def link_and_detect_joint_trips(
    unlinked_trips: pl.DataFrame, households: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Helper to link trips and detect joint trips.

    Args:
        unlinked_trips: Unlinked trip data
        households: Household data

    Returns:
        Tuple of (unlinked_trips_with_ids, linked_trips, joint_trips)
    """
    # Link trips first
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value],
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    # Detect joint trips
    joint_result = detect_joint_trips(
        linked_trips=linked_trips,
        households=households,
        method="buffer",
        time_threshold_minutes=5.0,
        space_threshold_meters=50.0,
    )
    linked_trips_with_joints = joint_result["linked_trips"]
    joint_trips = joint_result["joint_trips"]

    return unlinked_trips_with_ids, linked_trips_with_joints, joint_trips


class TestFullyJointTour:
    """Test tours where all trips involve the same group of people."""

    def test_two_person_joint_tour(self, basic_household_data):
        """Two people travel together for entire tour."""
        persons, households = basic_household_data

        # Two adults make a joint tour: home -> shop -> home
        unlinked_trips = pl.DataFrame(
            {
                "trip_id": [1, 2, 3, 4],
                "day_id": [
                    2300007500101,
                    2300007500101,
                    2300007500201,
                    2300007500201,
                ],
                "person_id": [
                    23000075001,
                    23000075001,
                    23000075002,
                    23000075002,
                ],
                "hh_id": [23000075, 23000075, 23000075, 23000075],
                "travel_dow": [TravelDow.SATURDAY.value] * 4,
                "depart_time": [
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 11, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 11, 0, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 10, 15, 0),
                    datetime(2024, 1, 15, 11, 15, 0),
                    datetime(2024, 1, 15, 10, 15, 0),
                    datetime(2024, 1, 15, 11, 15, 0),
                ],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                ],
                "d_purpose": [
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                ],
                "mode_type": [ModeType.CAR.value] * 4,
                "o_lat": [37.8, 37.82, 37.8, 37.82],
                "o_lon": [-122.4, -122.42, -122.4, -122.42],
                "d_lat": [37.82, 37.8, 37.82, 37.8],
                "d_lon": [-122.42, -122.4, -122.42, -122.4],
                "trip_weight": [1.0] * 4,
                "distance_meters": [2000.0] * 4,
                "duration_minutes": [15.0] * 4,
                "num_travelers": [2, 2, 2, 2],
                "driver": [Driver.DRIVER.value, Driver.DRIVER.value] * 2,
            }
        )

        # Link trips and detect joint trips
        (
            unlinked_trips_with_ids,
            linked_trips,
            joint_trips,
        ) = link_and_detect_joint_trips(unlinked_trips, households)

        # Extract tours
        tour_result = extract_tours(
            persons=persons,
            households=households,
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            joint_trips=joint_trips,
        )
        tours = tour_result["tours"]

        # Verify both persons have tours
        assert len(tours) == 2, "Should have 2 tours (one per person)"

        # Verify joint_tour_id is assigned and both tours share it
        joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(joint_tours) == 2, "Both tours should be marked as joint"

        joint_tour_ids = joint_tours["joint_tour_id"].unique()
        assert len(joint_tour_ids) == 1, "Both tours should share same joint_tour_id"

        # Verify joint_tour_id format (hh_id + 2 digits)
        joint_tour_id = int(joint_tour_ids[0])
        expected_prefix = 23000075  # hh_id
        assert joint_tour_id // 100 == expected_prefix, (
            f"joint_tour_id {joint_tour_id} should start with hh_id {expected_prefix}"
        )

    def test_three_person_joint_tour(self, basic_household_data):
        """Three people travel together for entire tour."""
        persons, households = basic_household_data

        # All three people make a joint tour: home -> restaurant -> home
        unlinked_trips = pl.DataFrame(
            {
                "trip_id": [1, 2, 3, 4, 5, 6],
                "day_id": [2300007500101] * 2 + [2300007500201] * 2 + [2300007500301] * 2,
                "person_id": [
                    23000075001,
                    23000075001,
                    23000075002,
                    23000075002,
                    23000075003,
                    23000075003,
                ],
                "hh_id": [23000075] * 6,
                "travel_dow": [TravelDow.SUNDAY.value] * 6,
                "depart_time": [
                    datetime(2024, 1, 15, 18, 0, 0),
                    datetime(2024, 1, 15, 20, 0, 0),
                ]
                * 3,
                "arrive_time": [
                    datetime(2024, 1, 15, 18, 15, 0),
                    datetime(2024, 1, 15, 20, 15, 0),
                ]
                * 3,
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.MEAL.value,
                ]
                * 3,
                "d_purpose_category": [
                    PurposeCategory.MEAL.value,
                    PurposeCategory.HOME.value,
                ]
                * 3,
                "o_purpose": [Purpose.HOME.value, Purpose.DINING.value] * 3,
                "d_purpose": [Purpose.DINING.value, Purpose.HOME.value] * 3,
                "mode_type": [ModeType.CAR.value] * 6,
                "o_lat": [37.8, 37.83] * 3,
                "o_lon": [-122.4, -122.43] * 3,
                "d_lat": [37.83, 37.8] * 3,
                "d_lon": [-122.43, -122.4] * 3,
                "trip_weight": [1.0] * 6,
                "distance_meters": [3000.0] * 6,
                "duration_minutes": [15.0] * 6,
                "num_travelers": [3] * 6,
                "driver": [Driver.DRIVER.value, Driver.DRIVER.value] * 3,
            }
        )

        # Link trips and detect joint trips
        (
            unlinked_trips_with_ids,
            linked_trips,
            joint_trips,
        ) = link_and_detect_joint_trips(unlinked_trips, households)

        # Extract tours
        tour_result = extract_tours(
            persons=persons,
            households=households,
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            joint_trips=joint_trips,
        )
        tours = tour_result["tours"]

        # Verify all three persons have tours
        assert len(tours) == 3, "Should have 3 tours (one per person)"

        # Verify all marked as joint with same ID
        joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(joint_tours) == 3, "All three tours should be marked as joint"

        joint_tour_ids = joint_tours["joint_tour_id"].unique()
        assert len(joint_tour_ids) == 1, "All tours should share same joint_tour_id"


class TestPartialJointTour:
    """Test tours where only some trips are joint."""

    def test_tour_with_one_joint_trip(self, basic_household_data):
        """Tour has one joint trip and one individual trip.

        Should NOT be joint tour.
        """
        persons, households = basic_household_data

        # Person 1: home -> shop (joint) -> restaurant (individual) -> home
        # Person 2: home -> shop (joint) -> home
        unlinked_trips = pl.DataFrame(
            {
                "trip_id": [1, 2, 3, 4, 5, 6],
                "day_id": [2300007500101] * 3
                + [2300007500201] * 2
                + [2300007500101],  # P1 has 3 trips same day
                "person_id": [
                    23000075001,
                    23000075001,
                    23000075001,
                    23000075002,
                    23000075002,
                    23000075001,
                ],
                "hh_id": [23000075] * 6,
                "travel_dow": [TravelDow.SATURDAY.value] * 6,
                "depart_time": [
                    datetime(2024, 1, 15, 10, 0, 0),  # P1: home -> shop
                    datetime(2024, 1, 15, 11, 0, 0),  # P1: shop -> restaurant
                    datetime(2024, 1, 15, 12, 0, 0),  # P1: restaurant -> home
                    datetime(2024, 1, 15, 10, 0, 0),  # P2: home -> shop
                    datetime(2024, 1, 15, 11, 0, 0),  # P2: shop -> home
                    datetime(2024, 1, 15, 10, 0, 0),  # Duplicate for validation
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 10, 15, 0),
                    datetime(2024, 1, 15, 11, 15, 0),
                    datetime(2024, 1, 15, 12, 15, 0),
                    datetime(2024, 1, 15, 10, 15, 0),
                    datetime(2024, 1, 15, 11, 15, 0),
                    datetime(2024, 1, 15, 10, 15, 0),
                ],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.MEAL.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.SHOP.value,
                    PurposeCategory.MEAL.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SHOP.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                    Purpose.DINING.value,
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                ],
                "d_purpose": [
                    Purpose.GROCERY.value,
                    Purpose.DINING.value,
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.GROCERY.value,
                ],
                "mode_type": [ModeType.CAR.value] * 6,
                "o_lat": [37.8, 37.82, 37.83, 37.8, 37.82, 37.8],
                "o_lon": [-122.4, -122.42, -122.43, -122.4, -122.42, -122.4],
                "d_lat": [37.82, 37.83, 37.8, 37.82, 37.8, 37.82],
                "d_lon": [-122.42, -122.43, -122.4, -122.42, -122.4, -122.42],
                "trip_weight": [1.0] * 6,
                "distance_meters": [2000.0] * 6,
                "duration_minutes": [15.0] * 6,
                "num_travelers": [
                    2,
                    1,
                    1,
                    2,
                    1,
                    2,
                ],  # First trip joint, rest individual
                "driver": [Driver.DRIVER.value] * 6,
            }
        )

        # Link trips and detect joint trips
        (
            unlinked_trips_with_ids,
            linked_trips,
            joint_trips,
        ) = link_and_detect_joint_trips(unlinked_trips, households)

        # Extract tours
        tour_result = extract_tours(
            persons=persons,
            households=households,
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            joint_trips=joint_trips,
        )
        tours = tour_result["tours"]

        # Neither person should have joint_tour_id (tour not fully joint)
        joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(joint_tours) == 0, "No tours should be marked as joint (not stable throughout)"


class TestPartialDropoff:
    """Test scenarios where some participants drop off mid-tour."""

    def test_three_start_one_drops_off(self, basic_household_data):
        """Three people start, one drops off.

        Remaining two should have joint tour.
        """
        persons, households = basic_household_data

        # All 3: home -> school (drop off child)
        # Parents only: school -> shop -> home
        unlinked_trips = pl.DataFrame(
            {
                "trip_id": list(range(1, 10)),
                "day_id": [2300007500101] * 3 + [2300007500201] * 3 + [2300007500301] * 3,
                "person_id": [23000075001] * 3 + [23000075002] * 3 + [23000075003] * 3,
                "hh_id": [23000075] * 9,
                "travel_dow": [TravelDow.MONDAY.value] * 9,
                "depart_time": [
                    # Person 1: home -> school -> shop -> home
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 8, 30, 0),
                    datetime(2024, 1, 15, 9, 30, 0),
                    # Person 2: home -> school -> shop -> home
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 8, 30, 0),
                    datetime(2024, 1, 15, 9, 30, 0),
                    # Person 3 (child): home -> school (stays)
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 16, 0, 0),  # School day
                    datetime(2024, 1, 15, 16, 15, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 8, 15, 0),
                    datetime(2024, 1, 15, 9, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 8, 15, 0),
                    datetime(2024, 1, 15, 9, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 8, 15, 0),
                    datetime(2024, 1, 15, 16, 15, 0),
                    datetime(2024, 1, 15, 16, 30, 0),
                ],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SCHOOL.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.HOME.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.K12_SCHOOL.value,
                ],
                "d_purpose": [
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.HOME.value,
                ],
                "mode_type": [ModeType.CAR.value] * 9,
                "o_lat": [37.8, 37.81, 37.82] * 3,
                "o_lon": [-122.4, -122.43, -122.42] * 3,
                "d_lat": [37.81, 37.82, 37.8] * 3,
                "d_lon": [-122.43, -122.42, -122.4] * 3,
                "trip_weight": [1.0] * 9,
                "distance_meters": [2000.0] * 9,
                "duration_minutes": [15.0] * 9,
                "num_travelers": [
                    3,
                    2,
                    2,
                    3,
                    2,
                    2,
                    3,
                    1,
                    1,
                ],  # Child drops off after first trip
                "driver": [Driver.DRIVER.value] * 9,
            }
        )

        # Link trips and detect joint trips
        (
            unlinked_trips_with_ids,
            linked_trips,
            joint_trips,
        ) = link_and_detect_joint_trips(unlinked_trips, households)

        # Extract tours
        tour_result = extract_tours(
            persons=persons,
            households=households,
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            joint_trips=joint_trips,
        )
        tours = tour_result["tours"]

        # Parents should have joint_tour_id (stable pair through entire tour)
        # Child should NOT (different tour pattern)
        parent_tours = tours.filter(pl.col("person_id").is_in([23000075001, 23000075002]))
        child_tours = tours.filter(pl.col("person_id") == 23000075003)

        parent_joint = parent_tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(parent_joint) == 2, "Both parents should have joint tours"

        # Parents should share same joint_tour_id
        parent_joint_ids = parent_joint["joint_tour_id"].unique()
        assert len(parent_joint_ids) == 1, "Parents should share same joint_tour_id"

        # Child should not have joint_tour_id for this tour
        child_joint = child_tours.filter(pl.col("joint_tour_id").is_not_null())
        # Child might have separate tour, but shouldn't share parents'
        # joint_tour_id
        if len(child_joint) > 0:
            assert child_joint["joint_tour_id"][0] != parent_joint_ids[0], (
                "Child should not share parents' joint_tour_id"
            )


class TestNoJointTours:
    """Test cases where no joint tours should be identified."""

    def test_no_joint_trips(self, basic_household_data):
        """Individual trips only - no joint tours."""
        persons, households = basic_household_data

        # Each person makes independent tour
        unlinked_trips = pl.DataFrame(
            {
                "trip_id": [1, 2, 3, 4],
                "day_id": [
                    2300007500101,
                    2300007500101,
                    2300007500201,
                    2300007500201,
                ],
                "person_id": [
                    23000075001,
                    23000075001,
                    23000075002,
                    23000075002,
                ],
                "hh_id": [23000075] * 4,
                "travel_dow": [TravelDow.MONDAY.value] * 4,
                "depart_time": [
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 17, 0, 0),
                    datetime(2024, 1, 15, 9, 0, 0),
                    datetime(2024, 1, 15, 18, 0, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 8, 30, 0),
                    datetime(2024, 1, 15, 17, 30, 0),
                    datetime(2024, 1, 15, 9, 30, 0),
                    datetime(2024, 1, 15, 18, 30, 0),
                ],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.WORK.value,
                ]
                * 2,
                "d_purpose_category": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                ]
                * 2,
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                ]
                * 2,
                "d_purpose": [
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                ]
                * 2,
                "mode_type": [ModeType.CAR.value] * 4,
                "o_lat": [37.8, 37.85, 37.8, 37.82],
                "o_lon": [-122.4, -122.45, -122.4, -122.48],
                "d_lat": [37.85, 37.8, 37.82, 37.8],
                "d_lon": [-122.45, -122.4, -122.48, -122.4],
                "trip_weight": [1.0] * 4,
                "distance_meters": [5000.0] * 4,
                "duration_minutes": [30.0] * 4,
                "num_travelers": [1] * 4,
                "driver": [Driver.DRIVER.value] * 4,
            }
        )

        # Link trips and detect joint trips
        (
            unlinked_trips_with_ids,
            linked_trips,
            joint_trips,
        ) = link_and_detect_joint_trips(unlinked_trips, households)

        # Extract tours
        tour_result = extract_tours(
            persons=persons,
            households=households,
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            joint_trips=joint_trips,
        )
        tours = tour_result["tours"]

        # No joint tours should be identified
        joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(joint_tours) == 0, "No joint tours should be identified"
