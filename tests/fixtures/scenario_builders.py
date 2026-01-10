"""Scenario builders for creating common test patterns.

This module provides pre-built test scenarios with households, persons,
days, and trips. Uses data-driven patterns to reduce code duplication.
"""

from datetime import UTC, datetime, time

import polars as pl

from data_canon.codebook.households import IncomeDetailed
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Mode, ModeType, Purpose, PurposeCategory

from .base_records import create_day, create_household, create_person
from .locations import (
    BART_HOME_LOCATION,
    BART_WORK_LOCATION,
    HOME_LOCATION,
    RESTAURANT_LOCATION,
    SCHOOL_COLLEGE_LOCATION,
    SCHOOL_HIGH_LOCATION,
    SHOPPING_LOCATION,
    WORK_2_LOCATION,
    WORK_3_LOCATION,
    WORK_LOCATION,
)
from .trip_records import create_unlinked_trip

# Default transit mode codes
DEFAULT_TRANSIT_MODE_CODES = [
    ModeType.TRANSIT.value,
    ModeType.FERRY.value,
    ModeType.LONG_DISTANCE.value,
]


def _create_trips_from_spec(
    trip_specs: list[dict], person_id: int, hh_id: int, day_id: int = 1
) -> list[dict]:
    """Create trips from a specification array.

    Args:
        trip_specs: List of trip specification dicts with keys:
            - trip_id: Trip ID
            - o_location: Origin Location object
            - d_location: Destination Location object
            - o_purpose_category, d_purpose_category: Purpose categories
            - mode, mode_type: Mode enums
            - depart_hour, depart_minute: Departure time
            - arrive_hour, arrive_minute: Arrival time
            - travel_time: Travel time in minutes
            - purpose: Legacy purpose field (optional)
        person_id: Person ID
        hh_id: Household ID
        day_id: Day ID (defaults to 1)

    Returns:
        List of trip dictionaries
    """
    trips = []
    for spec in trip_specs:
        # Resolve coordinates from Location objects
        o_location = spec["o_location"]
        d_location = spec["d_location"]

        # Create time objects
        depart_time = datetime.combine(
            datetime.now(tz=UTC).date(),
            time(spec["depart_hour"], spec.get("depart_minute", 0)),
        )
        arrive_time = datetime.combine(
            datetime.now(tz=UTC).date(),
            time(spec["arrive_hour"], spec.get("arrive_minute", 0)),
        )

        trip = create_unlinked_trip(
            trip_id=spec["trip_id"],
            person_id=person_id,
            hh_id=hh_id,
            day_id=day_id,
            o_lat=o_location.lat,
            o_lon=o_location.lon,
            d_lat=d_location.lat,
            d_lon=d_location.lon,
            o_purpose_category=spec["o_purpose_category"],
            d_purpose_category=spec["d_purpose_category"],
            mode_1=spec["mode"],
            mode_type=spec["mode_type"],
            depart_time=depart_time,
            arrive_time=arrive_time,
            travel_time=spec["travel_time"],
            purpose=spec.get("purpose"),
        )
        trips.append(trip)

    return trips


def simple_work_tour(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a simple home → work → home tour scenario (2 trips).

    Uses predefined HOME_LOCATION and WORK_LOCATION from locations.py.
    TAZ/MAZ IDs are assigned via mock spatial join in test pipeline.

    Args:
        hh_id: Household ID
        person_id: Person ID

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    # Create household
    household = create_household(
        hh_id=hh_id,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_workers=1,
    )

    # Create person
    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        work_taz=WORK_LOCATION.taz,
        work_maz=WORK_LOCATION.maz,
        work_lat=WORK_LOCATION.lat,
        work_lon=WORK_LOCATION.lon,
    )

    # Create day
    day = create_day(
        day_id=1,
        person_id=person_id,
        hh_id=hh_id,
        num_trips=2,
        is_complete=True,
    )

    # Create trips using specification
    trip_specs = [
        {
            "trip_id": 1,
            "o_location": HOME_LOCATION,
            "d_location": WORK_LOCATION,
            "o_purpose_category": PurposeCategory.HOME,
            "d_purpose_category": PurposeCategory.WORK,
            "purpose": Purpose.PRIMARY_WORKPLACE,
            "mode": Mode.HOUSEHOLD_VEHICLE,
            "mode_type": ModeType.CAR,
            "depart_hour": 8,
            "arrive_hour": 9,
            "travel_time": 60,
        },
        {
            "trip_id": 2,
            "o_location": WORK_LOCATION,
            "d_location": HOME_LOCATION,
            "o_purpose_category": PurposeCategory.WORK,
            "d_purpose_category": PurposeCategory.HOME,
            "purpose": Purpose.HOME,
            "mode": Mode.HOUSEHOLD_VEHICLE,
            "mode_type": ModeType.CAR,
            "depart_hour": 17,
            "arrive_hour": 18,
            "travel_time": 60,
        },
    ]

    trips = _create_trips_from_spec(trip_specs, person_id, hh_id, day_id=1)
    unlinked_trips = pl.DataFrame(trips)

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])

    return households, persons, days, unlinked_trips


def transit_commute(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create transit commute scenario with walk-BART-walk (6 trips).

    Pattern: Home → (walk) → BART → (BART) → BART → (walk) → Work → (return)
    Uses predefined HOME_LOCATION, WORK_LOCATION, BART_HOME_LOCATION, and
    BART_WORK_LOCATION from locations.py.

    Args:
        hh_id: Household ID
        person_id: Person ID

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    household = create_household(
        hh_id=hh_id,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        work_taz=WORK_LOCATION.taz,
        work_maz=WORK_LOCATION.maz,
        work_lat=WORK_LOCATION.lat,
        work_lon=WORK_LOCATION.lon,
    )

    day = create_day(
        day_id=1,
        person_id=person_id,
        hh_id=hh_id,
        num_trips=6,
        is_complete=True,
    )

    # Trip specification: Morning commute (3 trips) + Evening return (3 trips)
    trip_specs = [
        # Morning: Home → walk to BART
        {
            "trip_id": 1,
            "o_location": HOME_LOCATION,
            "d_location": BART_HOME_LOCATION,
            "o_purpose_category": PurposeCategory.HOME,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 7,
            "depart_minute": 50,
            "arrive_hour": 8,
            "arrive_minute": 0,
            "travel_time": 10,
        },
        # Morning: BART ride
        {
            "trip_id": 2,
            "o_location": BART_HOME_LOCATION,
            "d_location": BART_WORK_LOCATION,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.BART,
            "mode_type": ModeType.TRANSIT,
            "depart_hour": 8,
            "depart_minute": 5,
            "arrive_hour": 8,
            "arrive_minute": 35,
            "travel_time": 30,
        },
        # Morning: Walk to work
        {
            "trip_id": 3,
            "o_location": BART_WORK_LOCATION,
            "d_location": WORK_LOCATION,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.WORK,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 8,
            "depart_minute": 35,
            "arrive_hour": 8,
            "arrive_minute": 45,
            "travel_time": 10,
        },
        # Evening: Walk to BART
        {
            "trip_id": 4,
            "o_location": WORK_LOCATION,
            "d_location": BART_WORK_LOCATION,
            "o_purpose_category": PurposeCategory.WORK,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 17,
            "depart_minute": 0,
            "arrive_hour": 17,
            "arrive_minute": 10,
            "travel_time": 10,
        },
        # Evening: BART ride
        {
            "trip_id": 5,
            "o_location": BART_WORK_LOCATION,
            "d_location": BART_HOME_LOCATION,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.BART,
            "mode_type": ModeType.TRANSIT,
            "depart_hour": 17,
            "depart_minute": 15,
            "arrive_hour": 17,
            "arrive_minute": 45,
            "travel_time": 30,
        },
        # Evening: Walk home
        {
            "trip_id": 6,
            "o_location": BART_HOME_LOCATION,
            "d_location": HOME_LOCATION,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.HOME,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 17,
            "depart_minute": 45,
            "arrive_hour": 17,
            "arrive_minute": 55,
            "travel_time": 10,
        },
    ]

    trips = _create_trips_from_spec(trip_specs, person_id, hh_id, day_id=1)
    unlinked_trips = pl.DataFrame(trips)

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])

    return households, persons, days, unlinked_trips


def multi_stop_tour(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a work tour with intermediate stop (4 trips).

    Pattern: Home → Work → Stop (lunch/errand) → Work → Home
    Uses predefined HOME_LOCATION, WORK_LOCATION, and RESTAURANT_LOCATION.

    Args:
        hh_id: Household ID
        person_id: Person ID

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    household = create_household(
        hh_id=hh_id,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        work_taz=WORK_LOCATION.taz,
        work_maz=WORK_LOCATION.maz,
        work_lat=WORK_LOCATION.lat,
        work_lon=WORK_LOCATION.lon,
    )

    day = create_day(day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4)

    # Home → Work → Stop → Work → Home
    trips = [
        create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=HOME_LOCATION.lat,
            o_lon=HOME_LOCATION.lon,
            d_lat=WORK_LOCATION.lat,
            d_lon=WORK_LOCATION.lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(8, 0)),
        ),
        create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=WORK_LOCATION.lat,
            o_lon=WORK_LOCATION.lon,
            d_lat=RESTAURANT_LOCATION.lat,
            d_lon=RESTAURANT_LOCATION.lon,
            purpose=Purpose.DINING,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.MEAL,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(12, 0)),
        ),
        create_unlinked_trip(
            trip_id=3,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=RESTAURANT_LOCATION.lat,
            o_lon=RESTAURANT_LOCATION.lon,
            d_lat=WORK_LOCATION.lat,
            d_lon=WORK_LOCATION.lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.MEAL,
            d_purpose_category=PurposeCategory.WORK,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(13, 0)),
        ),
        create_unlinked_trip(
            trip_id=4,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=WORK_LOCATION.lat,
            o_lon=WORK_LOCATION.lon,
            d_lat=HOME_LOCATION.lat,
            d_lon=HOME_LOCATION.lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(17, 0)),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, days, unlinked_trips


def multi_tour_day(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a person with multiple tours in one day.

    Pattern: Home → Work → Home → Shopping → Home
    Uses predefined HOME_LOCATION, WORK_LOCATION, and SHOPPING_LOCATION.

    Args:
        hh_id: Household ID
        person_id: Person ID

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    household = create_household(
        hh_id=hh_id,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        work_taz=WORK_LOCATION.taz,
        work_maz=WORK_LOCATION.maz,
        work_lat=WORK_LOCATION.lat,
        work_lon=WORK_LOCATION.lon,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
    )

    day = create_day(day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4)

    # Home → Work → Home → Shop → Home
    trips = [
        create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=HOME_LOCATION.lat,
            o_lon=HOME_LOCATION.lon,
            d_lat=WORK_LOCATION.lat,
            d_lon=WORK_LOCATION.lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(8, 0)),
        ),
        create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=WORK_LOCATION.lat,
            o_lon=WORK_LOCATION.lon,
            d_lat=HOME_LOCATION.lat,
            d_lon=HOME_LOCATION.lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(17, 0)),
        ),
        create_unlinked_trip(
            trip_id=3,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=HOME_LOCATION.lat,
            o_lon=HOME_LOCATION.lon,
            d_lat=SHOPPING_LOCATION.lat,
            d_lon=SHOPPING_LOCATION.lon,
            purpose=Purpose.GROCERY,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.SHOP,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(19, 0)),
        ),
        create_unlinked_trip(
            trip_id=4,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=SHOPPING_LOCATION.lat,
            o_lon=SHOPPING_LOCATION.lon,
            d_lat=HOME_LOCATION.lat,
            d_lon=HOME_LOCATION.lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.SHOP,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(20, 0)),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, days, unlinked_trips


def work_tour_no_usual_location(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a work tour for person without usual work location defined.

    Tests edge case where worker has no work_lat/work_lon set in person record.
    The tour still goes to a work destination, but person doesn't have a
    defined usual workplace.

    Pattern: Home → Work → Home
    Uses HOME_LOCATION and WORK_LOCATION.

    Args:
        hh_id: Household ID
        person_id: Person ID

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    household = create_household(
        hh_id=hh_id,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_workers=1,
    )

    # Worker WITHOUT work location defined (work_lat/lon = None)
    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        work_lat=None,  # No usual work location
        work_lon=None,
        work_taz=None,
    )

    day = create_day(day_id=1, person_id=person_id, hh_id=hh_id, num_trips=2)

    # Home → Work → Home (work destination is ad-hoc, not usual location)
    trips = [
        create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=HOME_LOCATION.lat,
            o_lon=HOME_LOCATION.lon,
            d_lat=WORK_LOCATION.lat,
            d_lon=WORK_LOCATION.lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(8, 0)),
        ),
        create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_lat=WORK_LOCATION.lat,
            o_lon=WORK_LOCATION.lon,
            d_lat=HOME_LOCATION.lat,
            d_lon=HOME_LOCATION.lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(datetime.now(tz=UTC).date(), time(17, 0)),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, days, unlinked_trips


def multi_person_household(
    hh_id: int = 1,
    num_workers: int = 2,
    num_students: int = 1,
    num_children: int = 1,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Create a multi-person household with various person types.

    Uses data-driven approach to create persons based on type specifications.
    Uses HOME_LOCATION, WORK_LOCATION, WORK_2_LOCATION, SCHOOL_LOCATION, and
    SCHOOL_HIGH_LOCATION from locations.py.

    Args:
        hh_id: Household ID
        num_workers: Number of workers (full-time)
        num_students: Number of students (university)
        num_children: Number of children (age 5-15)

    Returns:
        Tuple of (households, persons) DataFrames (no trips/days)
    """
    total_people = num_workers + num_students + num_children

    household = create_household(
        hh_id=hh_id,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=total_people,
        num_workers=num_workers,
        num_vehicles=num_workers,
    )

    # Person type specifications
    person_specs = []
    person_num = 1
    base_person_id = hh_id * 100

    # Add workers
    work_locations = [WORK_LOCATION, WORK_2_LOCATION, WORK_3_LOCATION]
    for i in range(num_workers):
        work_loc = work_locations[i % len(work_locations)]
        person_specs.append(
            {
                "person_id": base_person_id + person_num,
                "person_num": person_num,
                "age": AgeCategory.AGE_35_TO_44,
                "employment": Employment.EMPLOYED_FULLTIME,
                "student": Student.NONSTUDENT,
                "work_taz": work_loc.taz,
                "work_maz": work_loc.maz,
                "work_lat": work_loc.lat,
                "work_lon": work_loc.lon,
            }
        )
        person_num += 1

    # Add students
    for _ in range(num_students):
        person_specs.append(
            {
                "person_id": base_person_id + person_num,
                "person_num": person_num,
                "age": AgeCategory.AGE_18_TO_24,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                "student": Student.FULLTIME_INPERSON,
                "school_taz": SCHOOL_COLLEGE_LOCATION.taz,
                "school_maz": SCHOOL_COLLEGE_LOCATION.maz,
                "school_lat": SCHOOL_COLLEGE_LOCATION.lat,
                "school_lon": SCHOOL_COLLEGE_LOCATION.lon,
            }
        )
        person_num += 1

    # Add children
    for _ in range(num_children):
        person_specs.append(
            {
                "person_id": base_person_id + person_num,
                "person_num": person_num,
                "age": AgeCategory.AGE_5_TO_15,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                "student": Student.FULLTIME_INPERSON,
                "school_taz": SCHOOL_HIGH_LOCATION.taz,
                "school_maz": SCHOOL_HIGH_LOCATION.maz,
                "school_lat": SCHOOL_HIGH_LOCATION.lat,
                "school_lon": SCHOOL_HIGH_LOCATION.lon,
            }
        )
        person_num += 1

    # Create persons from specifications
    persons = []
    for spec in person_specs:
        person = create_person(
            hh_id=hh_id,
            home_lat=HOME_LOCATION.lat,
            home_lon=HOME_LOCATION.lon,
            **spec,
        )
        persons.append(person)

    households = pl.DataFrame([household])
    persons_df = pl.DataFrame(persons)

    return households, persons_df


# ==============================================================================
# Pre-built Household Scenarios for Tests
# ==============================================================================


def create_single_adult_household():
    """Create a single full-time worker household for tests."""
    household = create_household(
        hh_id=1,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_vehicles=1,
        num_workers=1,
        income_detailed=IncomeDetailed.INCOME_75TO100,
    )

    person = create_person(
        person_id=101,
        hh_id=1,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        gender=Gender.MALE,
        employment=Employment.EMPLOYED_FULLTIME,
        student=Student.NONSTUDENT,
    )

    return pl.DataFrame([household]), pl.DataFrame([person])


def create_family_household():
    """Create household with working adults and school-age children."""
    household = create_household(
        hh_id=2,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=4,
        num_vehicles=2,
        num_workers=2,
        income_detailed=IncomeDetailed.INCOME_100TO150,
    )

    persons = [
        # Full-time worker (parent 1)
        create_person(
            person_id=201,
            hh_id=2,
            person_num=1,
            age=AgeCategory.AGE_35_TO_44,
            gender=Gender.FEMALE,
            employment=Employment.EMPLOYED_FULLTIME,
            student=Student.NONSTUDENT,
        ),
        # Part-time worker (parent 2)
        create_person(
            person_id=202,
            hh_id=2,
            person_num=2,
            age=AgeCategory.AGE_35_TO_44,
            gender=Gender.MALE,
            employment=Employment.EMPLOYED_PARTTIME,
            student=Student.NONSTUDENT,
        ),
        # High school student
        create_person(
            person_id=203,
            hh_id=2,
            person_num=3,
            age=AgeCategory.AGE_16_TO_17,
            gender=Gender.FEMALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.HIGH_SCHOOL,
        ),
        # Child under 16
        create_person(
            person_id=204,
            hh_id=2,
            person_num=4,
            age=AgeCategory.AGE_5_TO_15,
            gender=Gender.MALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.ELEMENTARY,
        ),
    ]

    return pl.DataFrame([household]), pl.DataFrame(persons)


def create_retired_household():
    """Create a household with retired persons for tests."""
    household = create_household(
        hh_id=3,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=2,
        num_vehicles=1,
        num_workers=0,
        income_detailed=IncomeDetailed.INCOME_50TO75,
    )

    persons = [
        create_person(
            person_id=301,
            hh_id=3,
            person_num=1,
            age=AgeCategory.AGE_65_TO_74,
            gender=Gender.MALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.NONSTUDENT,
        ),
        create_person(
            person_id=302,
            hh_id=3,
            person_num=2,
            age=AgeCategory.AGE_65_TO_74,
            gender=Gender.FEMALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.NONSTUDENT,
        ),
    ]

    return pl.DataFrame([household]), pl.DataFrame(persons)


def create_university_student_household():
    """Create a household with university students for tests."""
    household = create_household(
        hh_id=4,
        home_taz=HOME_LOCATION.taz,
        home_maz=HOME_LOCATION.maz,
        home_lat=HOME_LOCATION.lat,
        home_lon=HOME_LOCATION.lon,
        num_people=1,
        num_vehicles=1,
        num_workers=0,
        income_detailed=IncomeDetailed.INCOME_35TO50,
    )

    person = create_person(
        person_id=401,
        hh_id=4,
        person_num=1,
        age=AgeCategory.AGE_18_TO_24,
        gender=Gender.FEMALE,
        employment=Employment.UNEMPLOYED_NOT_LOOKING,
        student=Student.FULLTIME_INPERSON,
        school_type=SchoolType.COLLEGE_4YEAR,
    )

    return pl.DataFrame([household]), pl.DataFrame([person])
