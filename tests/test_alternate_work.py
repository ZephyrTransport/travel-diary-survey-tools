"""Tests for alternate-workplace detection and work-based subtour formation.

A person's work location for a day is normally their usual workplace. When they
never visit it and instead report WORK_RELATED activity elsewhere, that
elsewhere is the day's workplace, and tours anchored there are work tours.

Covers:
- A day at an alternate workplace produces a WORK tour tagged ALTERNATE_WORK
- WORK_RELATED activity away from the workplace forms a work-based subtour
  rather than being absorbed as an intermediate stop
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import AgeCategory, Employment, Student
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from processing import link_trips
from processing.tours.extraction import extract_tours

HOME = (37.8, -122.4)
USUAL_WORK = (37.85, -122.45)
# Far from the usual workplace, so it cannot be mistaken for it
ALT_WORK = (37.95, -122.55)
# Near the usual workplace, but not at it - a work errand
WORK_ERRAND = (37.86, -122.46)


@pytest.fixture
def person_and_household():
    """One full-time worker with a usual workplace."""
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [USUAL_WORK[0]],
            "work_lon": [USUAL_WORK[1]],
            "school_lat": [None],
            "school_lon": [None],
        }
    )
    households = pl.DataFrame({"hh_id": [1], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})
    return persons, households


def _build(trips: list[dict]) -> pl.DataFrame:
    """Turn trip dicts into an unlinked_trips frame."""
    return pl.DataFrame(
        {
            "unlinked_trip_id": list(range(1, len(trips) + 1)),
            "day_id": [1] * len(trips),
            "person_id": [1] * len(trips),
            "hh_id": [1] * len(trips),
            "travel_dow": [TravelDow.WEDNESDAY.value] * len(trips),
            "depart_time": [t["depart"] for t in trips],
            "arrive_time": [t["arrive"] for t in trips],
            "o_purpose_category": [t["o_cat"] for t in trips],
            "d_purpose_category": [t["d_cat"] for t in trips],
            "o_purpose": [t["o_purp"] for t in trips],
            "d_purpose": [t["d_purp"] for t in trips],
            "mode_type": [ModeType.CAR.value] * len(trips),
            "o_lat": [t["o"][0] for t in trips],
            "o_lon": [t["o"][1] for t in trips],
            "d_lat": [t["d"][0] for t in trips],
            "d_lon": [t["d"][1] for t in trips],
            "unlinked_trip_weight": [1.0] * len(trips),
            "distance_meters": [5000.0] * len(trips),
            "duration_minutes": [30.0] * len(trips),
            "num_travelers": [1] * len(trips),
            "driver": [Driver.DRIVER.value] * len(trips),
        }
    )


def _extract(persons, households, unlinked_trips):
    """Link trips and extract tours."""
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
    )
    linked_trips = link_result["linked_trips"].with_columns(
        pl.lit(None).cast(pl.Int64).alias("joint_trip_id")
    )
    return extract_tours(
        persons,
        households,
        link_result["unlinked_trips"],
        linked_trips,
    )


def test_alternate_workplace_day_is_a_work_tour(person_and_household):
    """A day worked away from the usual workplace is still a work tour.

    The person never goes to their usual workplace; they spend the day at
    another location reported as WORK_RELATED. That location is the day's
    workplace, so the tour is WORK - not WORK_RELATED.
    """
    persons, households = person_and_household
    unlinked_trips = _build(
        [
            {
                "depart": datetime(2024, 1, 17, 8, 0),
                "arrive": datetime(2024, 1, 17, 8, 45),
                "o": HOME,
                "d": ALT_WORK,
                "o_cat": PurposeCategory.HOME.value,
                "d_cat": PurposeCategory.WORK_RELATED.value,
                "o_purp": Purpose.HOME.value,
                "d_purp": Purpose.WORK_ACTIVITY.value,
            },
            {
                "depart": datetime(2024, 1, 17, 17, 0),
                "arrive": datetime(2024, 1, 17, 17, 45),
                "o": ALT_WORK,
                "d": HOME,
                "o_cat": PurposeCategory.WORK_RELATED.value,
                "d_cat": PurposeCategory.HOME.value,
                "o_purp": Purpose.WORK_ACTIVITY.value,
                "d_purp": Purpose.HOME.value,
            },
        ]
    )

    result = _extract(persons, households, unlinked_trips)
    tours = result["tours"]
    linked_trips = result["linked_trips"]

    # The trip end at the day's workplace is tagged ALTERNATE_WORK. This is
    # asserted on the linked trip, which carries the authoritative per-end
    # classification; the tour-level d_location_type is a separate concern
    # (see the tour aggregation coherence note).
    assert (
        linked_trips.filter(pl.col("d_location_type") == LocationType.ALTERNATE_WORK.value).height
        >= 1
    ), "The alternate-workplace trip end should be classified ALTERNATE_WORK"

    assert len(tours) == 1
    tour = tours.row(0, named=True)
    assert tour["tour_purpose"] == PurposeCategory.WORK.value, (
        "A tour anchored at the day's workplace should be a WORK tour, not WORK_RELATED"
    )


def test_work_related_errand_forms_a_subtour(person_and_household):
    """A mid-day WORK_RELATED errand away from work is a work-based subtour.

    home -> work -> errand -> work -> home. The errand is away from the
    workplace, so it is not "at work" and the person leaving and returning
    forms a subtour rather than an intermediate stop.
    """
    persons, households = person_and_household
    unlinked_trips = _build(
        [
            {
                "depart": datetime(2024, 1, 17, 8, 0),
                "arrive": datetime(2024, 1, 17, 8, 30),
                "o": HOME,
                "d": USUAL_WORK,
                "o_cat": PurposeCategory.HOME.value,
                "d_cat": PurposeCategory.WORK.value,
                "o_purp": Purpose.HOME.value,
                "d_purp": Purpose.PRIMARY_WORKPLACE.value,
            },
            {
                "depart": datetime(2024, 1, 17, 12, 0),
                "arrive": datetime(2024, 1, 17, 12, 15),
                "o": USUAL_WORK,
                "d": WORK_ERRAND,
                "o_cat": PurposeCategory.WORK.value,
                "d_cat": PurposeCategory.WORK_RELATED.value,
                "o_purp": Purpose.PRIMARY_WORKPLACE.value,
                "d_purp": Purpose.WORK_ACTIVITY.value,
            },
            {
                "depart": datetime(2024, 1, 17, 13, 30),
                "arrive": datetime(2024, 1, 17, 13, 45),
                "o": WORK_ERRAND,
                "d": USUAL_WORK,
                "o_cat": PurposeCategory.WORK_RELATED.value,
                "d_cat": PurposeCategory.WORK.value,
                "o_purp": Purpose.WORK_ACTIVITY.value,
                "d_purp": Purpose.PRIMARY_WORKPLACE.value,
            },
            {
                "depart": datetime(2024, 1, 17, 17, 0),
                "arrive": datetime(2024, 1, 17, 17, 30),
                "o": USUAL_WORK,
                "d": HOME,
                "o_cat": PurposeCategory.WORK.value,
                "d_cat": PurposeCategory.HOME.value,
                "o_purp": Purpose.PRIMARY_WORKPLACE.value,
                "d_purp": Purpose.HOME.value,
            },
        ]
    )

    tours = _extract(persons, households, unlinked_trips)["tours"]

    subtours = tours.filter(pl.col("subtour_num") > 0)
    assert len(subtours) >= 1, (
        "The WORK_RELATED errand away from the workplace should form a work-based "
        "subtour, not be absorbed as an intermediate stop"
    )
