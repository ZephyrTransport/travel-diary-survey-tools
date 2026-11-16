"""One full canonical row per call, for households, persons, days, trips and tours.

Each builder starts from its table's defaults below and applies the keyword
arguments on top; any key may be set or added. Enum members are stored as their
values, and a field left as None that has a derived default (a purpose category,
a time, a copied zone) gets it. ``usable_test`` is the usability verdict
``cascade_usability`` stamps; formatters read it rather than deriving a
criterion of their own, so a row standing in for canonical output carries it.
"""

from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    JobType,
    SchoolType,
    Student,
    WorkParking,
)
from data_canon.codebook.tours import TourCategory, TourDataQuality, TourDirection, TourType
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
    Purpose,
    PurposeCategory,
    PurposeToCategoryMap,
)

# A Monday, to match the default travel_dow.
DIARY_DATE = date(2024, 1, 15)

_EMPLOYED = {
    Employment.EMPLOYED_FULLTIME.value,
    Employment.EMPLOYED_PARTTIME.value,
    Employment.EMPLOYED_SELF.value,
}

HOUSEHOLD = {
    "hh_id": 1,
    "home_taz": None,
    "num_people": 1,
    "num_vehicles": 1,
    "num_workers": 1,
    "income": None,
    "income_bin": IncomeBroad.INCOME_75TO100,
    "hh_weight": 1.0,
    "home_lat": 37.70,
    "home_lon": -122.40,
    "home_maz": None,
    "home_walk_subzone": None,
    "residence_type": ResidenceType.SFH,
    "residence_rent_own": ResidenceRentOwn.OWN,
    "usable_test": True,
}

PERSON = {
    "person_id": 101,
    "hh_id": 1,
    "person_num": 1,
    "age": AgeCategory.AGE_35_TO_44,
    "gender": Gender.MALE,
    "employment": Employment.EMPLOYED_FULLTIME,
    "student": Student.NONSTUDENT,
    "school_type": SchoolType.MISSING,
    "commute_subsidy_provide_free_parking": BooleanYesNo.NO,
    "commute_subsidy_provide_discounted_parking": BooleanYesNo.NO,
    "commute_subsidy_use_free_parking": BooleanYesNo.NO,
    "commute_subsidy_use_discounted_parking": BooleanYesNo.NO,
    "value_of_time": 15.0,
    "person_weight": 1.0,
    "job_type": None,  # FIXED if employed, else MISSING
    "home_lat": None,
    "home_lon": None,
    "work_lat": None,
    "work_lon": None,
    "work_taz": None,
    "work_maz": None,
    "school_lat": None,
    "school_lon": None,
    "school_taz": None,
    "school_maz": None,
    "work_park": None,  # FREE with a work_taz, else NOT_APPLICABLE
    "transit_pass": BooleanYesNo.NO,
    "work_mode": Mode.MISSING,
    "is_proxy": BooleanYesNo.NO,
    "num_days_complete": 0,
    "usable_test": True,
}

DAY = {
    "person_day_id": 1,
    "person_id": 101,
    "hh_id": 1,
    "hh_day_id": None,  # hh_id * 100 + day_num
    "person_num": 1,
    "day_num": 1,
    "travel_date": DIARY_DATE,
    "travel_dow": TravelDow.MONDAY,
    "survey_complete": True,
    "num_trips": 0,
    "person_day_weight": 1.0,
    "usable_test": True,
}

UNLINKED_TRIP = {
    "unlinked_trip_id": 10001,
    "person_id": 101,
    "hh_id": 1,
    "person_day_id": 1,
    "person_num": 1,
    "day_num": 1,
    "depart_time": None,  # 8:00
    "arrive_time": None,  # travel_time after departure
    "travel_time": 30,
    "duration_minutes": None,  # travel_time
    "distance_meters": 1000.0,
    "unlinked_trip_weight": 1.0,
    "transit_access": 0,
    "transit_egress": 0,
    "travel_dow": TravelDow.MONDAY,
    "mode_type": ModeType.CAR,
    "mode_1": None,
    "mode_2": None,
    "mode_3": None,
    "mode_4": None,
    "driver": Driver.DRIVER,
    "num_travelers": 1,
    "change_mode": False,
    "o_purpose": Purpose.HOME,
    "o_purpose_category": None,
    "d_purpose": Purpose.OTHER,
    "d_purpose_category": None,
    "purpose": None,
    "purpose_category": None,
    "o_lat": None,
    "o_lon": None,
    "d_lat": None,
    "d_lon": None,
    "access_mode": None,
    "egress_mode": None,
    "usable_test": True,
}

LINKED_TRIP = {
    "linked_trip_id": 1,
    "person_id": 101,
    "hh_id": 1,
    "person_num": 1,
    "day_num": 1,
    "travel_dow": TravelDow.MONDAY,
    "linked_trip_num": 1,
    "tour_id": 1,
    "depart_time": None,  # 8:00
    "arrive_time": None,  # 30 minutes after departure
    "duration_minutes": None,
    "o_lat": 37.70,
    "o_lon": -122.40,
    "o_taz": 100,
    "o_TAZ1454": None,  # o_taz
    "o_maz": None,
    "o_purpose": Purpose.HOME,
    "o_purpose_category": None,
    "d_lat": 37.75,
    "d_lon": -122.45,
    "d_taz": 200,
    "d_TAZ1454": None,  # d_taz
    "d_maz": None,
    "d_purpose": Purpose.PRIMARY_WORKPLACE,
    "d_purpose_category": None,
    "mode_type": ModeType.CAR,
    "driver": Driver.DRIVER,
    "num_travelers": 1,
    "distance_meters": 8046.72,
    "d_activity_duration": None,
    "num_unlinked_trips": 1,
    "tour_direction": TourDirection.OUTBOUND,
    "linked_trip_weight": 1.0,
    "access_mode": None,
    "egress_mode": None,
    "joint_trip_id": None,
    "person_day_id": 1,
    "usable_test": True,
}

TOUR = {
    "tour_id": 1001,
    "person_id": 101,
    "hh_id": 1,
    "person_num": 1,
    "person_day_id": 1,
    "day_num": 1,
    "tour_num": 1,
    "tour_purpose": PurposeCategory.WORK,
    "tour_type": None,  # WORK_BASED for a subtour, else HOME_BASED
    "tour_category": TourCategory.COMPLETE,
    "o_taz": 100,
    "d_taz": 200,
    "o_TAZ1454": None,  # o_taz
    "d_TAZ1454": None,  # d_taz
    "o_lat": None,
    "o_lon": None,
    "d_lat": None,
    "d_lon": None,
    "o_location_type": 1,
    "d_location_type": 4,
    "origin_depart_time": None,  # 8:00
    "origin_arrive_time": None,  # 17:00
    "dest_depart_time": None,  # halfway between the two
    "dest_arrive_time": None,
    "travel_dow": TravelDow.MONDAY,
    "trip_count": 2,
    "stop_count": None,  # trip_count - 1
    "num_travelers": 1,
    "tour_mode": Mode.MISSING,
    "student_category": "Not student",
    "tour_data_quality": TourDataQuality.VALID,
    "tour_weight": 1.0,
    "joint_tour_id": None,
    "parent_tour_id": None,  # the tour itself
    "subtour_num": 0,
    "usable_test": True,
    "o_maz": None,
    "d_maz": None,
}


def on_diary_date(hour: int, minute: int = 0) -> datetime:
    """A time of day on DIARY_DATE."""
    return datetime.combine(DIARY_DATE, time(hour, minute))


def _fill(row: dict, **derived: Any) -> None:
    """Set each field of *row* still None to its derived value."""
    for key, value in derived.items():
        if row[key] is None:
            row[key] = value


def _finish(row: dict, *optional: str) -> dict:
    """*row* with enum members as values, less the *optional* fields left None."""
    return {
        key: value.value if isinstance(value, Enum) else value
        for key, value in row.items()
        if not (key in optional and value is None)
    }


def _category(purpose: Purpose | int) -> PurposeCategory:
    return PurposeToCategoryMap.get_category(Purpose(purpose))


def create_household(**fields: Any) -> dict:
    """A household row."""
    return _finish({**HOUSEHOLD, **fields})


def create_person(person_days: list[dict] | None = None, **fields: Any) -> dict:
    """A person row; *person_days* sets num_days_complete to its count of complete days.

    A bool for transit_pass or is_proxy becomes YES or NO.
    """
    row = {**PERSON, **fields}
    employment = Employment(row["employment"]).value
    _fill(
        row,
        job_type=JobType.FIXED.value if employment in _EMPLOYED else JobType.MISSING.value,
        work_park=WorkParking.FREE if row["work_taz"] is not None else WorkParking.NOT_APPLICABLE,
        transit_pass=BooleanYesNo.NO,
        work_mode=Mode.MISSING,
        is_proxy=BooleanYesNo.NO,
    )
    for key in ("transit_pass", "is_proxy"):
        if isinstance(row[key], bool):
            row[key] = BooleanYesNo.YES if row[key] else BooleanYesNo.NO
    if person_days is not None:
        row["num_days_complete"] = sum(1 for d in person_days if d.get("survey_complete", False))
    return _finish(row)


def create_day(**fields: Any) -> dict:
    """A person-day row."""
    row = {**DAY, **fields}
    _fill(row, hh_day_id=row["hh_id"] * 100 + row["day_num"])
    return _finish(row)


def create_unlinked_trip(**fields: Any) -> dict:
    """An unlinked trip row; purpose, its category, access and egress modes only when set."""
    row = {**UNLINKED_TRIP, **fields}
    _fill(row, depart_time=on_diary_date(8))
    _fill(
        row,
        arrive_time=row["depart_time"] + timedelta(minutes=row["travel_time"]),
        duration_minutes=float(row["travel_time"]),
        o_purpose_category=_category(row["o_purpose"]),
        d_purpose_category=_category(row["d_purpose"]),
    )
    return _finish(row, "purpose", "purpose_category", "access_mode", "egress_mode")


def create_linked_trip(**fields: Any) -> dict:
    """A linked trip row, by car from home to work; person_day_id only when set."""
    row = {**LINKED_TRIP, **fields}
    _fill(row, depart_time=on_diary_date(8))
    _fill(row, arrive_time=row["depart_time"] + timedelta(minutes=30))
    _fill(
        row,
        duration_minutes=int((row["arrive_time"] - row["depart_time"]).total_seconds() / 60),
        o_TAZ1454=row["o_taz"],
        d_TAZ1454=row["d_taz"],
        o_purpose_category=_category(row["o_purpose"]),
        d_purpose_category=_category(row["d_purpose"]),
    )
    return _finish(row, "person_day_id")


def create_tour(**fields: Any) -> dict:
    """A tour row, from home to work; o_maz and d_maz only when set.

    A tour numbered as a subtour, or whose parent is another tour, is work-based.
    """
    row = {**TOUR, **fields}
    parent = row["parent_tour_id"]
    is_subtour = row["subtour_num"] > 0 or (parent is not None and parent != row["tour_id"])
    _fill(row, origin_depart_time=on_diary_date(8), origin_arrive_time=on_diary_date(17))
    midpoint = (
        row["origin_depart_time"] + (row["origin_arrive_time"] - row["origin_depart_time"]) / 2
    )
    _fill(
        row,
        tour_type=TourType.WORK_BASED if is_subtour else TourType.HOME_BASED,
        o_TAZ1454=row["o_taz"],
        d_TAZ1454=row["d_taz"],
        dest_depart_time=midpoint,
        dest_arrive_time=midpoint,
        stop_count=max(row["trip_count"] - 1, 0),
        parent_tour_id=row["tour_id"],
    )
    return _finish(row, "o_maz", "d_maz")
