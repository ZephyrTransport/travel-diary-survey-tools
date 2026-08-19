"""Tests for observed alternate worksites and per-day work anchoring.

A person's work anchor for a day is normally their reported workplace. The
person-location registry also records observed work locations (places they spend
substantial time on work/work-related trips). On days a person does not visit
their reported workplace, an observed work location becomes the day's anchor -
so someone can be based at a different worksite on different days. Work-related
errands on a day they DID visit their reported workplace stay subtours.

Covers:
- A day worked entirely at an observed alternate site is a WORK tour anchored there
- A work-related errand on a reported-workplace day is a work-based subtour
- The anchor switches per day between the reported workplace and an observed site
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
            "day_id": [t.get("day", 1) for t in trips],
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


def _trip(day, depart, arrive, o, d, o_cat, d_cat, o_purp, d_purp):
    return {
        "day": day,
        "depart": depart,
        "arrive": arrive,
        "o": o,
        "d": d,
        "o_cat": o_cat,
        "d_cat": d_cat,
        "o_purp": o_purp,
        "d_purp": d_purp,
    }


def _extract(persons, households, unlinked_trips):
    """Link trips and extract tours."""
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
        split_on_occupancy=False,
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
    another location on a WORK_RELATED trip. That location is an observed work
    location and, since the usual workplace was not visited, the day's anchor -
    so the tour is WORK, and the trip end there is classified WORK (via purpose).
    """
    persons, households = person_and_household
    unlinked_trips = _build(
        [
            _trip(
                1,
                datetime(2024, 1, 17, 8, 0),
                datetime(2024, 1, 17, 8, 45),
                HOME,
                ALT_WORK,
                PurposeCategory.HOME.value,
                PurposeCategory.WORK_RELATED.value,
                Purpose.HOME.value,
                Purpose.WORK_ACTIVITY.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 17, 0),
                datetime(2024, 1, 17, 17, 45),
                ALT_WORK,
                HOME,
                PurposeCategory.WORK_RELATED.value,
                PurposeCategory.HOME.value,
                Purpose.WORK_ACTIVITY.value,
                Purpose.HOME.value,
            ),
        ]
    )

    result = _extract(persons, households, unlinked_trips)
    tours = result["tours"]
    linked_trips = result["linked_trips"]

    # The alternate-workplace trip end classifies WORK (via its work purpose),
    # not a distinct "alternate work" type.
    assert linked_trips.filter(pl.col("d_location_type") == LocationType.WORK.value).height >= 1, (
        "The alternate-workplace trip end should be classified WORK"
    )

    assert len(tours) == 1
    tour = tours.row(0, named=True)
    assert tour["tour_purpose"] == PurposeCategory.WORK.value, (
        "A tour anchored at the day's workplace should be a WORK tour, not WORK_RELATED"
    )


def test_work_related_errand_forms_a_subtour(person_and_household):
    """A mid-day WORK_RELATED errand on an office day is a work-based subtour.

    home -> work -> errand -> work -> home. The errand is away from the reported
    workplace (which was visited today), so it is not the anchor: leaving and
    returning forms a subtour rather than an intermediate stop.
    """
    persons, households = person_and_household
    unlinked_trips = _build(
        [
            _trip(
                1,
                datetime(2024, 1, 17, 8, 0),
                datetime(2024, 1, 17, 8, 30),
                HOME,
                USUAL_WORK,
                PurposeCategory.HOME.value,
                PurposeCategory.WORK.value,
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 12, 0),
                datetime(2024, 1, 17, 12, 15),
                USUAL_WORK,
                WORK_ERRAND,
                PurposeCategory.WORK.value,
                PurposeCategory.WORK_RELATED.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.WORK_ACTIVITY.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 13, 30),
                datetime(2024, 1, 17, 13, 45),
                WORK_ERRAND,
                USUAL_WORK,
                PurposeCategory.WORK_RELATED.value,
                PurposeCategory.WORK.value,
                Purpose.WORK_ACTIVITY.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 17, 0),
                datetime(2024, 1, 17, 17, 30),
                USUAL_WORK,
                HOME,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ),
        ]
    )

    tours = _extract(persons, households, unlinked_trips)["tours"]

    subtours = tours.filter(pl.col("subtour_num") > 0)
    assert len(subtours) >= 1, (
        "The WORK_RELATED errand away from the workplace should form a work-based "
        "subtour, not be absorbed as an intermediate stop"
    )


def test_anchor_switches_per_day_between_reported_and_observed(person_and_household):
    """The work anchor is resolved per day.

    Day 1: at the usual workplace, with a WORK_RELATED errand -> a subtour.
    Day 2: never visits the usual workplace, spends the day at an observed
    alternate worksite -> that day anchors there and is a WORK tour.
    """
    persons, households = person_and_household
    unlinked_trips = _build(
        [
            # Day 1 - usual workplace with a mid-day errand (subtour)
            _trip(
                1,
                datetime(2024, 1, 17, 8, 0),
                datetime(2024, 1, 17, 8, 30),
                HOME,
                USUAL_WORK,
                PurposeCategory.HOME.value,
                PurposeCategory.WORK.value,
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 12, 0),
                datetime(2024, 1, 17, 12, 15),
                USUAL_WORK,
                WORK_ERRAND,
                PurposeCategory.WORK.value,
                PurposeCategory.WORK_RELATED.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.WORK_ACTIVITY.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 13, 30),
                datetime(2024, 1, 17, 13, 45),
                WORK_ERRAND,
                USUAL_WORK,
                PurposeCategory.WORK_RELATED.value,
                PurposeCategory.WORK.value,
                Purpose.WORK_ACTIVITY.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ),
            _trip(
                1,
                datetime(2024, 1, 17, 17, 0),
                datetime(2024, 1, 17, 17, 30),
                USUAL_WORK,
                HOME,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ),
            # Day 2 - a full day at the alternate worksite (no usual-work visit)
            _trip(
                2,
                datetime(2024, 1, 18, 8, 0),
                datetime(2024, 1, 18, 8, 45),
                HOME,
                ALT_WORK,
                PurposeCategory.HOME.value,
                PurposeCategory.WORK_RELATED.value,
                Purpose.HOME.value,
                Purpose.WORK_ACTIVITY.value,
            ),
            _trip(
                2,
                datetime(2024, 1, 18, 17, 0),
                datetime(2024, 1, 18, 17, 45),
                ALT_WORK,
                HOME,
                PurposeCategory.WORK_RELATED.value,
                PurposeCategory.HOME.value,
                Purpose.WORK_ACTIVITY.value,
                Purpose.HOME.value,
            ),
        ]
    )

    tours = _extract(persons, households, unlinked_trips)["tours"]

    day1 = tours.filter(pl.col("day_id") == 1)
    day2 = tours.filter(pl.col("day_id") == 2)
    assert day1.filter(pl.col("subtour_num") > 0).height >= 1, (
        "Day 1 (usual workplace) should have a work-based subtour for the errand"
    )
    assert (day2["tour_purpose"] == PurposeCategory.WORK.value).any(), (
        "Day 2 (alternate worksite) should be a WORK tour anchored at the observed site"
    )
