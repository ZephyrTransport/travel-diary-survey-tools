"""Tests for legacy tour extraction functionality from daysim_old pipeline.

This test module ensures that the new tour extraction implementation produces
functionally equivalent results to the legacy tour extraction code in terms of:
- Number of tours identified
- Tour categorization (home-based vs work-based)
- Tour purpose assignment
- Tour mode assignment
- Work-based subtour detection
- Tour timing (departure/arrival times)

The tests use synthetic data to compare outputs from both implementations.

Testing approach:
1. Each test creates synthetic person and trip data in the new format
2. Data is converted to legacy format using to_legacy_format()
3. Both legacy (_tour_extract_week_core) and new (TourExtractor) implementations
   are run on the same input data
4. Outputs are compared to ensure functional equivalence
5. Expected values provide additional validation of correctness

This approach mirrors the testing strategy used in test_legacy_link_trips.py.
"""

import importlib.util
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.daysim import DaysimPurpose
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    PersonType,
    SchoolType,
    Student,
)
from data_canon.codebook.tours import TourType
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from processing import extract_tours, link_trips
from processing.formatting.daysim.mappings import PURPOSE_MAP
from tests.fixtures.scenario_builders import (
    multi_stop_tour,
    multi_tour_day,
    simple_work_tour,
)

# Import the legacy tour extraction function dynamically
# Using the refactored version with _tour_extract_week_core
spec = importlib.util.spec_from_file_location(
    "tour_extract_module",
    Path(__file__).parent.parent
    / "archive"
    / "survey_processing"
    / "SFCTA"
    / "03a-tour_extract_week_func.py",
)
if spec is None or spec.loader is None:
    msg = "Could not load tour extraction module"
    raise ImportError(msg)
tour_extract_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tour_extract_module)
tour_extract_legacy = tour_extract_module._tour_extract_week_core

# Use standardized DaySim purpose mappings (PurposeCategory -> DaySim code)
# Create reverse mapping for test data generation (string -> PurposeCategory)
PURPOSE_MAP_NEW = {
    "home": PurposeCategory.HOME,
    "work": PurposeCategory.WORK,
    "school": PurposeCategory.SCHOOL,
    "escort": PurposeCategory.ESCORT,
    "shop": PurposeCategory.SHOP,
    "meal": PurposeCategory.MEAL,
    "social": PurposeCategory.SOCIALREC,
    "errand": PurposeCategory.ERRAND,
}

# Mode code mappings
MODE_MAP_LEGACY = {
    "walk": 1,
    "bike": 2,
    "drive": 3,
    "transit": 6,
    "school_bus": 8,
}

MODE_MAP_NEW = {
    "walk": ModeType.WALK,
    "bike": ModeType.BIKE,
    "drive": ModeType.CAR,
    "transit": ModeType.TRANSIT,
    "school_bus": ModeType.SCHOOL_BUS,
}

TRANSIT_MODE_CODES = [
    ModeType.TRANSIT.value,
    ModeType.FERRY.value,
    ModeType.LONG_DISTANCE.value,
]


def create_households_from_persons(persons_df: pl.DataFrame) -> pl.DataFrame:
    """Create minimal households DataFrame from persons data.

    Args:
        persons_df: Person data with hh_id, home_lat, home_lon

    Returns:
        Household DataFrame with required fields
    """
    return persons_df.group_by("hh_id").agg(
        [
            pl.col("home_lat").first(),
            pl.col("home_lon").first(),
        ]
    )


def to_legacy_format(
    persons_df: pl.DataFrame, trips_df: pl.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Convert new format DataFrames to legacy pandas format for comparison.

    Args:
        persons_df: Person data in new polars format
        trips_df: Trip data in new polars format

    Returns:
        Tuple of (hh, persons, trips) DataFrames in legacy format
    """
    # PURPOSE_MAP is already {PurposeCategory.value: DaysimPurpose.value}
    # No remapping needed - use it directly

    # Create mapping from new mode codes to legacy mode codes
    new_to_legacy_mode = {}
    for key, new_code in MODE_MAP_NEW.items():
        legacy_code = MODE_MAP_LEGACY[key]
        new_to_legacy_mode[new_code] = legacy_code

    # Build household DataFrame (minimal for tour extraction)
    hh_ids = persons_df["hh_id"].unique().to_list()
    hh = pd.DataFrame(
        {
            "hhno": hh_ids,
            "hhsize": [len(persons_df.filter(pl.col("hh_id") == hh_id)) for hh_id in hh_ids],
            "hxcord": [
                persons_df.filter(pl.col("hh_id") == hh_id)["home_lon"][0] for hh_id in hh_ids
            ],
            "hycord": [
                persons_df.filter(pl.col("hh_id") == hh_id)["home_lat"][0] for hh_id in hh_ids
            ],
            "hhparcel": [-1 for _ in hh_ids],  # MAZ - not used in tests
            "hhtaz": [-1 for _ in hh_ids],  # TAZ - not used in tests
            "hhvehs": [1 for _ in hh_ids],  # Number of vehicles - default to 1
            "hhincome": [50000 for _ in hh_ids],  # Income - default value
            "hownrent": [1 for _ in hh_ids],  # Housing tenure - 1=own, 2=rent
            "hrestype": [1 for _ in hh_ids],  # Residence type - 1=single family
            "hhexpfac": [1.0 for _ in hh_ids],  # Household weight
        }
    )

    # Build persons DataFrame
    persons_data = []
    for row in persons_df.iter_rows(named=True):
        person_dict = {
            "hhno": row["hh_id"],
            "pno": row["person_id"],
            "pptyp": row["person_type"],
            "pwtyp": (1 if row["person_type"] == PersonType.FULL_TIME_WORKER else 0),
            "pwtaz": -1,  # Not used in current tests
            "pstyp": 0,  # Not used in current tests
            "pstaz": -1,  # Not used in current tests
            "pwxcord": (row["work_lon"] if row["work_lon"] is not None else -1),
            "pwycord": (row["work_lat"] if row["work_lat"] is not None else -1),
            "psxcord": (row["school_lon"] if row["school_lon"] is not None else -1),
            "psycord": (row["school_lat"] if row["school_lat"] is not None else -1),
            "pwpcl": 0,  # Purpose category at work (not needed for tests)
            "pspcl": 0,  # Purpose category at school (not needed for tests)
            "pagey": 35,  # Default age
            "pgend": 1,  # Default gender
            "psexpfac": 1.0,  # Weight
            "num_days_complete_3dayweekday": 1,  # For weighting
        }
        persons_data.append(person_dict)
    persons = pd.DataFrame(persons_data)

    # Build trips DataFrame
    trips_data = []
    for row in trips_df.iter_rows(named=True):
        # Convert purpose codes using PURPOSE_MAP
        # Polars returns enum values as integers
        opurp_legacy = PURPOSE_MAP.get(row["o_purpose_category"], DaysimPurpose.HOME.value)
        dpurp_legacy = PURPOSE_MAP.get(row["d_purpose_category"], DaysimPurpose.HOME.value)

        # Convert mode codes
        # Polars returns enum values as integers, so convert to enum
        mode_enum = ModeType(row["mode_type"])
        mode_legacy = new_to_legacy_mode.get(mode_enum, MODE_MAP_LEGACY["drive"])

        # Set dorp (driver or passenger flag)
        # For auto modes (drive=3), default to driver (1)
        # For non-auto modes, set to N/A (3)
        dorp_value = 1 if mode_legacy == 3 else 3

        trip_dict = {
            "hhno": row["hh_id"],
            "pno": row["person_id"],
            "dow": row["travel_dow"],
            "tripno": row["linked_trip_id"],  # Legacy code expects this
            "lintripno": row["linked_trip_id"],
            "opurp": opurp_legacy,
            "dpurp": dpurp_legacy,
            "dorp": dorp_value,  # Driver or passenger flag
            "mode": mode_legacy,
            "mode_type": mode_legacy,
            "path": 1,  # Path hierarchy for mode selection
            "deptm": row["depart_time"].hour * 100 + row["depart_time"].minute,
            "arrtm": row["arrive_time"].hour * 100 + row["arrive_time"].minute,
            "otaz": -1,  # Not used in core logic
            "dtaz": -1,  # Not used in core logic
            "oxcord": row["o_lon"],
            "oycord": row["o_lat"],
            "dxcord": row["d_lon"],
            "dycord": row["d_lat"],
            "oact": 0,  # Origin activity duration (not used)
            "dact": 0,  # Destination activity duration (not used)
            "opcl": opurp_legacy,  # Origin purpose category
            "dpcl": dpurp_legacy,  # Destination purpose category
        }
        trips_data.append(trip_dict)
    trips = pd.DataFrame(trips_data)

    return hh, persons, trips


@pytest.fixture
def simple_work_tour_data():
    """Simple work tour: home -> work -> home.

    Expected: 1 home-based work tour, 0 work-based subtours.
    """
    households, persons, days, unlinked_trips = simple_work_tour()
    # Link trips for legacy format
    result = link_trips(
        unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value, ModeType.FERRY.value],
    )
    unlinked_trips_with_ids = result["unlinked_trips"]
    # Sort because legacy code expects trips in depart_time order
    linked_trips = result["linked_trips"].sort("depart_time")

    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips_with_ids,
        "linked_trips": linked_trips,
        "expected": {
            "num_tours": 1,
            "num_hb_tours": 1,
            "num_wb_tours": 0,
            "tour_purpose": PurposeCategory.WORK,
            "tour_mode": ModeType.CAR,
        },
    }


@pytest.fixture
def work_tour_with_subtour_data():
    """Work tour with lunch trip: home -> work -> lunch -> work -> home.

    Expected: 1 home-based work tour, 1 work-based subtour.
    """
    households, persons, days, unlinked_trips = multi_stop_tour()
    # Link trips for legacy format
    result = link_trips(
        unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value, ModeType.FERRY.value],
    )
    unlinked_trips_with_ids = result["unlinked_trips"]
    # Sort because legacy code expects trips in depart_time order
    linked_trips = result["linked_trips"].sort("depart_time")

    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips_with_ids,
        "linked_trips": linked_trips,
        "expected": {
            "num_tours": 2,  # 1 HB tour + 1 WB subtour
            "num_hb_tours": 1,
            "num_wb_tours": 1,
            "hb_tour_purpose": PurposeCategory.WORK,
            "hb_tour_mode": ModeType.CAR,
            "wb_tour_purpose": PurposeCategory.MEAL,
            "wb_tour_mode": ModeType.WALK,
        },
    }


@pytest.fixture
def multiple_tours_data():
    """Multiple tours in one day: home -> work -> home -> shop -> home.

    Expected: 2 home-based tours (work, shop), 0 work-based subtours.
    """
    households, persons, days, unlinked_trips = multi_tour_day()
    # Link trips for legacy format
    result = link_trips(
        unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=[ModeType.TRANSIT.value, ModeType.FERRY.value],
    )
    unlinked_trips_with_ids = result["unlinked_trips"]
    # Sort because legacy code expects trips in depart_time order
    linked_trips = result["linked_trips"].sort("depart_time")

    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips_with_ids,
        "linked_trips": linked_trips,
        "expected": {
            "num_tours": 2,
            "num_hb_tours": 2,
            "num_wb_tours": 0,
            "first_tour_purpose": PurposeCategory.WORK,
            "second_tour_purpose": PurposeCategory.SHOP,
        },
    }


@pytest.fixture
def mode_hierarchy_data():
    """Tour with multiple modes to test mode hierarchy.

    Trip 1: walk to transit, Trip 2: transit, Trip 3: walk from transit,
    Trip 4: drive home. Expected: tour mode should be transit (higher in
    hierarchy than walk).
    """
    # Coordinates
    home_coords = (37.70, -122.40)
    work_coords = (37.75, -122.45)

    # Person data
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "person_type": [PersonType.FULL_TIME_WORKER],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "school_type": [SchoolType.MISSING.value],
            "student": [Student.NONSTUDENT.value],
            "home_lat": [home_coords[0]],
            "home_lon": [home_coords[1]],
            "work_lat": [work_coords[0]],
            "work_lon": [work_coords[1]],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    # Trip data (using linked trips - already merged change_mode segments)
    trips = pl.DataFrame(
        {
            "trip_id": [1, 2],
            "linked_trip_id": [1, 2],
            "day_id": [2, 2],
            "travel_dow": [TravelDow.MONDAY.value, TravelDow.MONDAY.value],
            "person_id": [1, 1],
            "hh_id": [1, 1],
            "depart_time": [
                datetime(2024, 1, 1, 8, 0),  # home -> work (via transit)
                datetime(2024, 1, 1, 17, 0),  # work -> home (drive)
            ],
            "arrive_time": [
                datetime(2024, 1, 1, 9, 0),
                datetime(2024, 1, 1, 17, 30),
            ],
            "o_purpose_category": [
                PURPOSE_MAP_NEW["home"],
                PURPOSE_MAP_NEW["work"],
            ],
            "d_purpose_category": [
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["home"],
            ],
            "mode_type": [
                MODE_MAP_NEW["transit"],  # Transit higher priority
                MODE_MAP_NEW["drive"],
            ],
            "o_lat": [home_coords[0], work_coords[0]],
            "o_lon": [home_coords[1], work_coords[1]],
            "d_lat": [work_coords[0], home_coords[0]],
            "d_lon": [work_coords[1], home_coords[1]],
            "distance_meters": [10000, 10000],
            "duration_minutes": [60, 30],
            "num_travelers": [1, 1],
            "driver": [Driver.MISSING.value, Driver.DRIVER.value],
            "trip_weight": [1.0, 1.0],
        }
    )

    return {
        "persons": persons,
        "linked_trips": trips,
        "expected": {
            "num_tours": 1,
            # Should pick transit over drive
            "tour_mode": MODE_MAP_NEW["transit"],
        },
    }


# ============================================================================
# Test Functions
# ============================================================================


def test_simple_work_tour(simple_work_tour_data):
    """Test basic work tour identification comparing new vs legacy."""
    persons = simple_work_tour_data["persons"]
    unlinked_trips = simple_work_tour_data["unlinked_trips"]
    linked_trips = simple_work_tour_data["linked_trips"]
    expected = simple_work_tour_data["expected"]

    # Convert to legacy format
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, linked_trips)

    # Run legacy implementation
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours_new = result["tours"]

    # Compare tour counts
    assert len(tours_new) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours_new)}, legacy={len(tours_legacy)}"
    )
    assert len(tours_new) == expected["num_tours"], (
        f"Expected {expected['num_tours']} tours, got {len(tours_new)}"
    )

    # Check tour types
    hb_tours = tours_new.filter(pl.col("tour_category") == TourType.HOME_BASED)
    wb_tours = tours_new.filter(pl.col("tour_category") == TourType.WORK_BASED)

    assert len(hb_tours) == expected["num_hb_tours"], (
        f"Expected {expected['num_hb_tours']} home-based tours, got {len(hb_tours)}"
    )
    assert len(wb_tours) == expected["num_wb_tours"], (
        f"Expected {expected['num_wb_tours']} work-based tours, got {len(wb_tours)}"
    )

    # Check tour attributes
    tour = tours_new[0]
    assert tour["tour_purpose"][0] == expected["tour_purpose"].value, (
        f"Expected tour purpose {expected['tour_purpose'].value}, got {tour['tour_purpose'][0]}"
    )
    assert tour["tour_mode"][0] == expected["tour_mode"].value, (
        f"Expected tour mode {expected['tour_mode'].value}, got {tour['tour_mode'][0]}"
    )


def test_work_tour_with_subtour(work_tour_with_subtour_data):
    """Test work-based subtour detection comparing new vs legacy."""
    persons = work_tour_with_subtour_data["persons"]
    unlinked_trips = work_tour_with_subtour_data["unlinked_trips"]
    linked_trips = work_tour_with_subtour_data["linked_trips"]
    expected = work_tour_with_subtour_data["expected"]

    # Convert to legacy format
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, linked_trips)

    # Run legacy implementation
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours_new = result["tours"]

    # Compare tour counts
    assert len(tours_new) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours_new)}, legacy={len(tours_legacy)}"
    )
    assert len(tours_new) == expected["num_tours"], (
        f"Expected {expected['num_tours']} tours total, got {len(tours_new)}"
    )

    # Check tour types
    hb_tours = tours_new.filter(pl.col("tour_category") == TourType.HOME_BASED)
    wb_tours = tours_new.filter(pl.col("tour_category") == TourType.WORK_BASED)

    assert len(hb_tours) == expected["num_hb_tours"], (
        f"Expected {expected['num_hb_tours']} home-based tours, got {len(hb_tours)}"
    )
    assert len(wb_tours) == expected["num_wb_tours"], (
        f"Expected {expected['num_wb_tours']} work-based tours, got {len(wb_tours)}"
    )

    # Check home-based tour attributes
    hb_tour = hb_tours[0]
    assert hb_tour["tour_purpose"][0] == expected["hb_tour_purpose"].value, (
        f"Expected HB tour purpose {expected['hb_tour_purpose'].value}, "
        f"got {hb_tour['tour_purpose'][0]}"
    )
    assert hb_tour["tour_mode"][0] == expected["hb_tour_mode"].value, (
        f"Expected HB tour mode {expected['hb_tour_mode'].value}, got {hb_tour['tour_mode'][0]}"
    )

    # Check work-based subtour attributes
    wb_tour = wb_tours[0]
    assert wb_tour["tour_purpose"][0] == expected["wb_tour_purpose"].value, (
        f"Expected WB tour purpose {expected['wb_tour_purpose'].value}, "
        f"got {wb_tour['tour_purpose'][0]}"
    )
    assert wb_tour["tour_mode"][0] == expected["wb_tour_mode"].value, (
        f"Expected WB tour mode {expected['wb_tour_mode'].value}, got {wb_tour['tour_mode'][0]}"
    )

    # Verify subtour has correct parent reference
    assert wb_tour["parent_tour_id"][0] == hb_tour["tour_id"][0], (
        "Work-based subtour should reference home-based tour as parent"
    )


def test_multiple_tours_same_day(multiple_tours_data):
    """Test multiple home-based tours comparing new vs legacy."""
    persons = multiple_tours_data["persons"]
    unlinked_trips = multiple_tours_data["unlinked_trips"]
    linked_trips = multiple_tours_data["linked_trips"]
    expected = multiple_tours_data["expected"]

    # Convert to legacy format
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, linked_trips)

    # Run legacy implementation
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours = result["tours"]

    # Compare tour counts
    assert len(tours) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours)}, legacy={len(tours_legacy)}"
    )
    assert len(tours) == expected["num_tours"], (
        f"Expected {expected['num_tours']} tours, got {len(tours)}"
    )

    # Check all are home-based
    hb_tours = tours.filter(pl.col("tour_category") == TourType.HOME_BASED)
    assert len(hb_tours) == expected["num_hb_tours"], (
        f"Expected {expected['num_hb_tours']} home-based tours, got {len(hb_tours)}"
    )

    # Check tour purposes (should be in time order)
    tours_sorted = tours.sort("origin_depart_time")
    expected_first = expected["first_tour_purpose"].value
    assert tours_sorted[0, "tour_purpose"] == expected_first, (
        f"Expected first tour purpose {expected_first}, got {tours_sorted[0, 'tour_purpose']}"
    )
    expected_second = expected["second_tour_purpose"].value
    assert tours_sorted[1, "tour_purpose"] == expected_second, (
        f"Expected second tour purpose {expected_second}, got {tours_sorted[1, 'tour_purpose']}"
    )


def test_mode_hierarchy(mode_hierarchy_data):
    """Test tour mode reflects mode hierarchy comparing new vs legacy."""
    persons = mode_hierarchy_data["persons"]
    linked_trips = mode_hierarchy_data["linked_trips"]
    expected = mode_hierarchy_data["expected"]

    # Convert to legacy format
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, linked_trips)

    # Run legacy implementation
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Create dummy unlinked_trips from linked_trips for extract_tours
    # (mode_hierarchy_data fixture provides linked trips directly)
    unlinked_trips = linked_trips.with_columns(trip_id=pl.col("linked_trip_id"))

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips, linked_trips)
    tours = result["tours"]

    # Compare tour counts
    assert len(tours) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours)}, legacy={len(tours_legacy)}"
    )
    assert len(tours) == expected["num_tours"], (
        f"Expected {expected['num_tours']} tours, got {len(tours)}"
    )

    # Check tour mode selects highest priority mode
    tour = tours[0]
    expected_mode = expected["tour_mode"].value
    assert tour["tour_mode"][0] == expected_mode, (
        f"Expected tour mode {expected_mode} (transit should win), got {tour['tour_mode'][0]}"
    )


def test_tour_timing():
    """Test that tour timing is correctly computed from trip times.

    Tour pattern: home -> work -> lunch -> work -> home

    Home-based work tour timing:
    - origin_depart_time: when leaving origin (home) on first trip = 8:15
    - origin_arrive_time: when arriving back at origin (home) on last
      trip = 17:45
    - dest_arrive_time: when arriving at destination (work) on first
      trip = 9:00
    - dest_depart_time: LAST departure FROM work before returning
      home = 17:00

    Work-based subtour timing:
    - origin_depart_time: when leaving work for subtour = 12:00
    - origin_arrive_time: when returning to work = 13:15
    - dest_arrive_time: when arriving at lunch location = 12:15
    - dest_depart_time: when leaving lunch location = 13:00
    """
    # Coordinates
    home_coords = (37.70, -122.40)
    work_coords = (37.75, -122.45)
    lunch_coords = (37.76, -122.46)

    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "person_type": [PersonType.FULL_TIME_WORKER],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "school_type": [SchoolType.MISSING.value],
            "student": [Student.NONSTUDENT.value],
            "home_lat": [home_coords[0]],
            "home_lon": [home_coords[1]],
            "work_lat": [work_coords[0]],
            "work_lon": [work_coords[1]],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    # Trip times for the tour pattern
    trip1_depart = datetime(2024, 1, 1, 8, 15)  # Leave home
    trip1_arrive = datetime(2024, 1, 1, 9, 0)  # Arrive at work
    trip2_depart = datetime(2024, 1, 1, 12, 0)  # Leave work for lunch
    trip2_arrive = datetime(2024, 1, 1, 12, 15)  # Arrive at lunch
    trip3_depart = datetime(2024, 1, 1, 13, 0)  # Leave lunch
    trip3_arrive = datetime(2024, 1, 1, 13, 15)  # Arrive back at work
    trip4_depart = datetime(2024, 1, 1, 17, 0)  # Leave work
    trip4_arrive = datetime(2024, 1, 1, 17, 45)  # Arrive home

    # EXPECTED home-based tour timing (what the algorithm SHOULD produce)
    expected_hb_origin_depart = trip1_depart  # 8:15 - Leave home
    expected_hb_origin_arrive = trip4_arrive  # 17:45 - Return home
    expected_hb_dest_arrive = trip1_arrive  # 9:00 - First arrival at work
    expected_hb_dest_depart = trip4_depart  # 17:00 - LAST departure from work (currently broken)

    # EXPECTED work-based subtour timing (what the algorithm SHOULD produce)
    expected_wb_origin_depart = trip2_depart  # 12:00 - Leave work for lunch
    expected_wb_origin_arrive = trip3_arrive  # 13:15 - Return to work
    expected_wb_dest_arrive = trip2_arrive  # 12:15 - Arrive at lunch
    expected_wb_dest_depart = trip3_depart  # 13:00 - Departure from lunch (currently broken)

    trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4],
            "linked_trip_id": [1, 2, 3, 4],
            "day_id": [2, 2, 2, 2],
            "travel_dow": [TravelDow.MONDAY.value] * 4,
            "person_id": [1, 1, 1, 1],
            "hh_id": [1, 1, 1, 1],
            "depart_time": [
                trip1_depart,
                trip2_depart,
                trip3_depart,
                trip4_depart,
            ],
            "arrive_time": [
                trip1_arrive,
                trip2_arrive,
                trip3_arrive,
                trip4_arrive,
            ],
            "o_purpose_category": [
                PURPOSE_MAP_NEW["home"],
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["meal"],
                PURPOSE_MAP_NEW["work"],
            ],
            "d_purpose_category": [
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["meal"],
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["home"],
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.DINING.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ],
            "d_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.DINING.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ],
            "mode_type": [
                MODE_MAP_NEW["drive"],
                MODE_MAP_NEW["walk"],
                MODE_MAP_NEW["walk"],
                MODE_MAP_NEW["drive"],
            ],
            "o_lat": [
                home_coords[0],
                work_coords[0],
                lunch_coords[0],
                work_coords[0],
            ],
            "o_lon": [
                home_coords[1],
                work_coords[1],
                lunch_coords[1],
                work_coords[1],
            ],
            "d_lat": [
                work_coords[0],
                lunch_coords[0],
                work_coords[0],
                home_coords[0],
            ],
            "d_lon": [
                work_coords[1],
                lunch_coords[1],
                work_coords[1],
                home_coords[1],
            ],
            "distance_meters": [15000, 500, 500, 15000],
            "duration_minutes": [30, 15, 30, 30],
            "num_travelers": [1, 1, 1, 1],
            "driver": [
                Driver.DRIVER.value,
                Driver.MISSING.value,
                Driver.MISSING.value,
                Driver.DRIVER.value,
            ],
            "trip_weight": [1.0, 1.0, 1.0, 1.0],
        }
    )

    # Convert to legacy format and run legacy implementation
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, trips)
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Link trips first
    link_result = link_trips(
        unlinked_trips=trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=TRANSIT_MODE_CODES,
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips_with_ids, linked_trips)
    tours = result["tours"]

    # Compare tour counts - should find 1 HB tour + 1 WB subtour
    assert len(tours) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours)}, legacy={len(tours_legacy)}"
    )
    assert len(tours) == 2, f"Expected 2 tours (1 HB + 1 WB), got {len(tours)}"

    # Separate home-based and work-based tours
    hb_tours = tours.filter(pl.col("tour_category") == TourType.HOME_BASED)
    wb_tours = tours.filter(pl.col("tour_category") == TourType.WORK_BASED)

    assert len(hb_tours) == 1, f"Expected 1 home-based tour, got {len(hb_tours)}"
    assert len(wb_tours) == 1, f"Expected 1 work-based tour, got {len(wb_tours)}"

    # Check home-based tour timing
    hb_tour = hb_tours[0]
    assert hb_tour["origin_depart_time"][0] == expected_hb_origin_depart, (
        f"HB tour: expected origin departure at {expected_hb_origin_depart}, "
        f"got {hb_tour['origin_depart_time'][0]}"
    )
    assert hb_tour["origin_arrive_time"][0] == expected_hb_origin_arrive, (
        f"HB tour: expected origin arrival at {expected_hb_origin_arrive}, "
        f"got {hb_tour['origin_arrive_time'][0]}"
    )
    assert hb_tour["dest_arrive_time"][0] == expected_hb_dest_arrive, (
        f"HB tour: expected dest arrival at {expected_hb_dest_arrive}, "
        f"got {hb_tour['dest_arrive_time'][0]}"
    )
    assert hb_tour["dest_depart_time"][0] == expected_hb_dest_depart, (
        f"HB tour: expected dest departure at {expected_hb_dest_depart}, "
        f"got {hb_tour['dest_depart_time'][0]}"
    )

    # Check work-based subtour timing
    wb_tour = wb_tours[0]
    assert wb_tour["origin_depart_time"][0] == expected_wb_origin_depart, (
        f"WB tour: expected origin departure at {expected_wb_origin_depart}, "
        f"got {wb_tour['origin_depart_time'][0]}"
    )
    assert wb_tour["origin_arrive_time"][0] == expected_wb_origin_arrive, (
        f"WB tour: expected origin arrival at {expected_wb_origin_arrive}, "
        f"got {wb_tour['origin_arrive_time'][0]}"
    )
    assert wb_tour["dest_arrive_time"][0] == expected_wb_dest_arrive, (
        f"WB tour: expected dest arrival at {expected_wb_dest_arrive}, "
        f"got {wb_tour['dest_arrive_time'][0]}"
    )
    assert wb_tour["dest_depart_time"][0] == expected_wb_dest_depart, (
        f"WB tour: expected dest departure at {expected_wb_dest_depart}, "
        f"got {wb_tour['dest_depart_time'][0]}"
    )


def test_tour_trip_counts():
    """Test that trip counts are correctly computed for tours."""
    # Tour with intermediate stops: home -> stop1 -> stop2 -> work -> home
    home_coords = (37.70, -122.40)
    work_coords = (37.75, -122.45)

    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "person_type": [PersonType.FULL_TIME_WORKER],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "school_type": [SchoolType.MISSING.value],
            "student": [Student.NONSTUDENT.value],
            "home_lat": [home_coords[0]],
            "home_lon": [home_coords[1]],
            "work_lat": [work_coords[0]],
            "work_lon": [work_coords[1]],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4],
            "linked_trip_id": [1, 2, 3, 4],
            "day_id": [2, 2, 2, 2],
            "travel_dow": [TravelDow.MONDAY.value] * 4,
            "person_id": [1, 1, 1, 1],
            "hh_id": [1, 1, 1, 1],
            "depart_time": [
                datetime(2024, 1, 1, 8, 0),
                datetime(2024, 1, 1, 8, 30),
                datetime(2024, 1, 1, 9, 0),
                datetime(2024, 1, 1, 17, 0),
            ],
            "arrive_time": [
                datetime(2024, 1, 1, 8, 15),
                datetime(2024, 1, 1, 8, 45),
                datetime(2024, 1, 1, 9, 30),
                datetime(2024, 1, 1, 17, 30),
            ],
            "o_purpose_category": [
                PURPOSE_MAP_NEW["home"],
                PURPOSE_MAP_NEW["shop"],
                PURPOSE_MAP_NEW["errand"],
                PURPOSE_MAP_NEW["work"],
            ],
            "d_purpose_category": [
                PURPOSE_MAP_NEW["shop"],
                PURPOSE_MAP_NEW["errand"],
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["home"],
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.GROCERY.value,
                Purpose.SOCIAL.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ],
            "d_purpose": [
                Purpose.GROCERY.value,
                Purpose.SOCIAL.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ],
            "mode_type": [MODE_MAP_NEW["drive"]] * 4,
            "o_lat": [home_coords[0], 37.71, 37.72, work_coords[0]],
            "o_lon": [home_coords[1], -122.41, -122.42, work_coords[1]],
            "d_lat": [37.71, 37.72, work_coords[0], home_coords[0]],
            "d_lon": [-122.41, -122.42, work_coords[1], home_coords[1]],
            "distance_meters": [5000, 3000, 10000, 15000],
            "duration_minutes": [15, 15, 30, 30],
            "num_travelers": [1, 1, 1, 1],
            "driver": [Driver.DRIVER.value] * 4,
            "trip_weight": [1.0, 1.0, 1.0, 1.0],
        }
    )

    # Convert to legacy format and run legacy implementation
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, trips)
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Link trips first
    link_result = link_trips(
        unlinked_trips=trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=TRANSIT_MODE_CODES,
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips_with_ids, linked_trips)
    tours = result["tours"]

    # Compare tour counts
    assert len(tours) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours)}, legacy={len(tours_legacy)}"
    )

    # Check trip count
    tour = tours[0]
    assert tour["trip_count"][0] == 4, f"Expected 4 trips in tour, got {tour['trip_count'][0]}"
    assert tour["stop_count"][0] == 3, f"Expected 3 intermediate stops, got {tour['stop_count'][0]}"


def test_incomplete_tour_at_end_of_day():
    """Test handling of incomplete tours (no return home at end of day).

    Includes both complete and incomplete tours so legacy code doesn't crash.
    Legacy ignores incomplete tours, new code should handle them.
    """
    home_coords = (37.70, -122.40)
    work_coords = (37.75, -122.45)

    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "person_type": [PersonType.FULL_TIME_WORKER],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "school_type": [SchoolType.MISSING.value],
            "student": [Student.NONSTUDENT.value],
            "home_lat": [home_coords[0]],
            "home_lon": [home_coords[1]],
            "work_lat": [work_coords[0]],
            "work_lon": [work_coords[1]],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    # Day 1: Complete tour (home -> work -> home)
    # Day 2: Incomplete tour (home -> work, no return)
    trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3],
            "linked_trip_id": [1, 2, 3],
            "day_id": [1, 1, 2],
            "travel_dow": [TravelDow.MONDAY.value] * 3,
            "person_id": [1, 1, 1],
            "hh_id": [1, 1, 1],
            "depart_time": [
                datetime(2024, 1, 1, 8, 0),  # Day 1: home -> work
                datetime(2024, 1, 1, 17, 0),  # Day 1: work -> home
                datetime(2024, 1, 2, 8, 0),  # Day 2: home -> work (incomplete)
            ],
            "arrive_time": [
                datetime(2024, 1, 1, 9, 0),
                datetime(2024, 1, 1, 17, 30),
                datetime(2024, 1, 2, 9, 0),
            ],
            "o_purpose_category": [
                PURPOSE_MAP_NEW["home"],
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["home"],
            ],
            "d_purpose_category": [
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["home"],
                PURPOSE_MAP_NEW["work"],
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ],
            "d_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ],
            "mode_type": [
                MODE_MAP_NEW["drive"],
                MODE_MAP_NEW["drive"],
                MODE_MAP_NEW["drive"],
            ],
            "o_lat": [home_coords[0], work_coords[0], home_coords[0]],
            "o_lon": [home_coords[1], work_coords[1], home_coords[1]],
            "d_lat": [work_coords[0], home_coords[0], work_coords[0]],
            "d_lon": [work_coords[1], home_coords[1], work_coords[1]],
            "distance_meters": [15000, 15000, 15000],
            "duration_minutes": [60, 30, 60],
            "num_travelers": [1, 1, 1],
            "driver": [
                Driver.DRIVER.value,
                Driver.DRIVER.value,
                Driver.DRIVER.value,
            ],
            "trip_weight": [1.0, 1.0, 1.0],
        }
    )

    # Convert to legacy format and run legacy implementation
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, trips)
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Link trips first
    link_result = link_trips(
        unlinked_trips=trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=TRANSIT_MODE_CODES,
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips_with_ids, linked_trips)
    tours = result["tours"]

    # Legacy only counts complete tours (should be 1)
    assert len(tours_legacy) == 1, (
        f"Expected legacy to find 1 complete tour, got {len(tours_legacy)}"
    )

    # New implementation should find both complete and incomplete tours
    assert len(tours) == 2, (
        f"Expected new code to find 2 tours (1 complete + 1 incomplete), got {len(tours)}"
    )


def test_no_work_location():
    """Test handling of work tours when person has no defined work location."""
    home_coords = (37.70, -122.40)
    # Trip destination, but not person's usual work
    work_coords = (37.75, -122.45)

    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "person_type": [PersonType.FULL_TIME_WORKER],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "school_type": [SchoolType.MISSING.value],
            "student": [Student.NONSTUDENT.value],
            "home_lat": [home_coords[0]],
            "home_lon": [home_coords[1]],
            "work_lat": [None],  # No work location defined
            "work_lon": [None],
            "school_lat": [None],
            "school_lon": [None],
        }
    )

    trips = pl.DataFrame(
        {
            "trip_id": [1, 2],
            "linked_trip_id": [1, 2],
            "day_id": [2, 2],
            "travel_dow": [TravelDow.MONDAY.value, TravelDow.MONDAY.value],
            "person_id": [1, 1],
            "hh_id": [1, 1],
            "depart_time": [
                datetime(2024, 1, 1, 8, 0),
                datetime(2024, 1, 1, 17, 0),
            ],
            "arrive_time": [
                datetime(2024, 1, 1, 9, 0),
                datetime(2024, 1, 1, 17, 30),
            ],
            "o_purpose_category": [
                PURPOSE_MAP_NEW["home"],
                PURPOSE_MAP_NEW["work"],
            ],
            "d_purpose_category": [
                PURPOSE_MAP_NEW["work"],
                PURPOSE_MAP_NEW["home"],
            ],
            "o_purpose": [
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ],
            "d_purpose": [
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ],
            "mode_type": [MODE_MAP_NEW["drive"], MODE_MAP_NEW["drive"]],
            "o_lat": [home_coords[0], work_coords[0]],
            "o_lon": [home_coords[1], work_coords[1]],
            "d_lat": [work_coords[0], home_coords[0]],
            "d_lon": [work_coords[1], home_coords[1]],
            "distance_meters": [15000, 15000],
            "duration_minutes": [
                60,
                30,
            ],  # Add duration_minutes required by link_trips
            "num_travelers": [1, 1],
            "driver": [Driver.DRIVER.value, Driver.DRIVER.value],
            "trip_weight": [1.0, 1.0],
        }
    )

    # Convert to legacy format and run legacy implementation
    hh_legacy, persons_legacy, trips_legacy = to_legacy_format(persons, trips)
    _, _, _, tours_legacy, _ = tour_extract_legacy(
        hh_legacy, persons_legacy, trips_legacy, weighted=True
    )

    # Run new implementation
    households = create_households_from_persons(persons)

    # Link trips first
    link_result = link_trips(
        unlinked_trips=trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=TRANSIT_MODE_CODES,
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    # Extract tours using both unlinked and linked trips
    result = extract_tours(persons, households, unlinked_trips_with_ids, linked_trips)
    tours = result["tours"]

    # Compare tour counts
    assert len(tours) == len(tours_legacy), (
        f"Tour count mismatch: new={len(tours)}, legacy={len(tours_legacy)}"
    )

    # Should create a home-based work tour
    # No work-based subtours possible (no usual work location)
    assert len(tours) == 1, f"Expected 1 tour, got {len(tours)}"
    tour = tours[0]
    assert tour["tour_category"][0] == TourType.HOME_BASED.value
    assert tour["tour_purpose"][0] == PURPOSE_MAP_NEW["work"].value


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
