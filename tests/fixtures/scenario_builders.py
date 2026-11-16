"""Whole small surveys: households, persons, days and unlinked trips for a named travel pattern."""

import polars as pl

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Mode, ModeType, Purpose, PurposeCategory

from .locations import (
    BART_HOME_LOCATION,
    BART_WORK_LOCATION,
    HOME_LOCATION,
    RESTAURANT_LOCATION,
    SCHOOL_COLLEGE_LOCATION,
    SCHOOL_HIGH_LOCATION,
    WORK_2_LOCATION,
    WORK_3_LOCATION,
    WORK_LOCATION,
    Location,
)
from .records import (
    create_day,
    create_household,
    create_person,
    create_unlinked_trip,
    on_diary_date,
)

DEFAULT_TRANSIT_MODE_CODES = [
    ModeType.TRANSIT.value,
    ModeType.FERRY.value,
    ModeType.LONG_DISTANCE.value,
]


def _at(prefix: str, location: Location, *, zones: bool = False) -> dict:
    """*location*'s coordinates as ``<prefix>_lat``/``_lon`` fields, with its zones if asked."""
    fields = {f"{prefix}_lat": location.lat, f"{prefix}_lon": location.lon}
    if zones:
        fields |= {f"{prefix}_taz": location.taz, f"{prefix}_maz": location.maz}
    return fields


def _create_trips_from_spec(
    trip_specs: list[dict], person_id: int, hh_id: int, person_day_id: int = 1
) -> list[dict]:
    """Create trips from a specification array."""
    trips = []
    for spec in trip_specs:
        o_location = spec["o_location"]
        d_location = spec["d_location"]

        depart_time = on_diary_date(
            spec["depart_hour"],
            spec.get("depart_minute", 0),
        )
        arrive_time = on_diary_date(
            spec["arrive_hour"],
            spec.get("arrive_minute", 0),
        )

        trip = create_unlinked_trip(
            unlinked_trip_id=spec["unlinked_trip_id"],
            person_id=person_id,
            hh_id=hh_id,
            person_day_id=person_day_id,
            **_at("o", o_location),
            **_at("d", d_location),
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
    """
    household = create_household(
        hh_id=hh_id,
        **_at("home", HOME_LOCATION, zones=True),
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        **_at("home", HOME_LOCATION),
        **_at("work", WORK_LOCATION, zones=True),
    )

    day = create_day(
        person_day_id=1,
        person_id=person_id,
        hh_id=hh_id,
        num_trips=2,
        is_complete=True,
    )

    trip_specs = [
        {
            "unlinked_trip_id": 1,
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
            "unlinked_trip_id": 2,
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

    trips = _create_trips_from_spec(trip_specs, person_id, hh_id, person_day_id=1)
    unlinked_trips = pl.DataFrame(trips)

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    person_days = pl.DataFrame([day])

    return households, persons, person_days, unlinked_trips


def transit_commute(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create transit commute scenario with walk-BART-walk (6 trips).

    Pattern: Home → (walk) → BART → (BART) → BART → (walk) → Work → (return)
    Uses predefined HOME_LOCATION, WORK_LOCATION, BART_HOME_LOCATION, and
    BART_WORK_LOCATION from locations.py.
    """
    household = create_household(
        hh_id=hh_id,
        **_at("home", HOME_LOCATION, zones=True),
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        **_at("home", HOME_LOCATION),
        **_at("work", WORK_LOCATION, zones=True),
    )

    day = create_day(
        person_day_id=1,
        person_id=person_id,
        hh_id=hh_id,
        num_trips=6,
        is_complete=True,
    )

    # Trip specification: Morning commute (3 trips) + Evening return (3 trips)
    trip_specs = [
        # Morning: Home → walk to BART
        {
            "unlinked_trip_id": 1,
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
            "unlinked_trip_id": 2,
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
            "unlinked_trip_id": 3,
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
            "unlinked_trip_id": 4,
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
            "unlinked_trip_id": 5,
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
            "unlinked_trip_id": 6,
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

    trips = _create_trips_from_spec(trip_specs, person_id, hh_id, person_day_id=1)
    unlinked_trips = pl.DataFrame(trips)

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    person_days = pl.DataFrame([day])

    return households, persons, person_days, unlinked_trips


def multi_stop_tour(
    hh_id: int = 1,
    person_id: int = 101,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a work tour with intermediate stop (4 trips).

    Pattern: Home → Work → Stop (lunch/errand) → Work → Home
    Uses predefined HOME_LOCATION, WORK_LOCATION, and RESTAURANT_LOCATION.
    """
    household = create_household(
        hh_id=hh_id,
        **_at("home", HOME_LOCATION, zones=True),
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        **_at("home", HOME_LOCATION),
        **_at("work", WORK_LOCATION, zones=True),
    )

    day = create_day(person_day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4)

    trips = [
        create_unlinked_trip(
            unlinked_trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            **_at("o", HOME_LOCATION),
            **_at("d", WORK_LOCATION),
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=on_diary_date(8, 0),
        ),
        create_unlinked_trip(
            unlinked_trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            **_at("o", WORK_LOCATION),
            **_at("d", RESTAURANT_LOCATION),
            purpose=Purpose.DINING,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.MEAL,
            mode_type=ModeType.WALK,
            depart_time=on_diary_date(12, 0),
        ),
        create_unlinked_trip(
            unlinked_trip_id=3,
            person_id=person_id,
            hh_id=hh_id,
            **_at("o", RESTAURANT_LOCATION),
            **_at("d", WORK_LOCATION),
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.MEAL,
            d_purpose_category=PurposeCategory.WORK,
            mode_type=ModeType.WALK,
            depart_time=on_diary_date(13, 0),
        ),
        create_unlinked_trip(
            unlinked_trip_id=4,
            person_id=person_id,
            hh_id=hh_id,
            **_at("o", WORK_LOCATION),
            **_at("d", HOME_LOCATION),
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=on_diary_date(17, 0),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    person_days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, person_days, unlinked_trips


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
    """
    total_people = num_workers + num_students + num_children

    household = create_household(
        hh_id=hh_id,
        **_at("home", HOME_LOCATION, zones=True),
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

    persons = []
    for spec in person_specs:
        person = create_person(
            hh_id=hh_id,
            **_at("home", HOME_LOCATION),
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
        **_at("home", HOME_LOCATION, zones=True),
        num_people=1,
        num_vehicles=1,
        num_workers=1,
        income_bin=IncomeBroad.INCOME_75TO100,
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
        **_at("home", HOME_LOCATION, zones=True),
        num_people=4,
        num_vehicles=2,
        num_workers=2,
        income_bin=IncomeBroad.INCOME_75TO100,
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
        **_at("home", HOME_LOCATION, zones=True),
        num_people=2,
        num_vehicles=1,
        num_workers=0,
        income_bin=IncomeBroad.INCOME_50TO75,
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
        **_at("home", HOME_LOCATION, zones=True),
        num_people=1,
        num_vehicles=1,
        num_workers=0,
        income_bin=IncomeBroad.INCOME_25TO50,
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
