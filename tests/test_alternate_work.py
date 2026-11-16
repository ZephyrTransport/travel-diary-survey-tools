"""Observed alternate worksites and per-day work anchoring.

A day's work anchor is the reported workplace if visited that day, otherwise an
observed workplace: somewhere the person stayed long enough for work. A
work-related errand on a reported-workplace day forms a work-based subtour.
"""

import polars as pl
import pytest

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.tours import TourType
from data_canon.codebook.trips import Purpose, PurposeCategory
from tests.fixtures.tour_pipeline import (
    HOME,
    HOUSEHOLDS,
    WORK,
    at,
    leg,
    link_and_extract_tours,
    person,
)

# Far from the usual workplace, so it cannot be mistaken for it
ALT_WORK = (37.95, -122.55)
# Near the usual workplace, but not at it
WORK_ERRAND = (37.86, -122.46)

H, W, WA = Purpose.HOME, Purpose.PRIMARY_WORKPLACE, Purpose.WORK_ACTIVITY


@pytest.mark.parametrize(
    ("leave_hour", "expected_sources"),
    [
        pytest.param(
            17,
            [LocationSource.REPORTED.value, LocationSource.OBSERVED.value],
            id="a_days_work_at_another_site_is_a_workplace",
        ),
        pytest.param(
            10,
            [LocationSource.REPORTED.value],
            id="a_brief_work_related_stop_is_not_a_workplace",
        ),
    ],
)
def test_how_long_a_work_related_stay_must_be_to_make_a_workplace(leave_hour, expected_sources):
    """A working day at a site makes it a workplace; a meeting there does not.

    Either way the tour keeps the WORK_RELATED purpose it was reported with.
    """
    result = link_and_extract_tours(
        person(),
        HOUSEHOLDS,
        [
            leg(HOME, ALT_WORK, H, WA, at(8), at(8, 45)),
            leg(ALT_WORK, HOME, WA, H, at(leave_hour), at(leave_hour, 45)),
        ],
    )
    workplaces = result["habitual_locations"].filter(
        pl.col("location_type") == LocationType.WORK.value
    )

    assert workplaces["source"].to_list() == expected_sources
    assert result["tours"]["tour_purpose"].to_list() == [PurposeCategory.WORK_RELATED.value]


def test_anchor_switches_per_day_between_reported_and_observed():
    """The work anchor is resolved per day.

    Day 1: at the usual workplace with a work-related errand away from it, which
    forms a work-based subtour. Day 2: a full day at an alternate worksite, which
    anchors that day's WORK tour.
    """
    result = link_and_extract_tours(
        person(),
        HOUSEHOLDS,
        [
            leg(HOME, WORK, H, W, at(8), at(8, 30)),
            leg(WORK, WORK_ERRAND, W, WA, at(12), at(12, 15)),
            leg(WORK_ERRAND, WORK, WA, W, at(13, 30), at(13, 45)),
            leg(WORK, HOME, W, H, at(17), at(17, 30)),
            leg(HOME, ALT_WORK, H, W, at(8, day=2), at(8, 45, day=2), person_day_id=2),
            leg(ALT_WORK, HOME, W, H, at(17, day=2), at(17, 45, day=2), person_day_id=2),
        ],
    )
    tours = result["tours"]

    observed = result["habitual_locations"].filter(
        (pl.col("location_type") == LocationType.WORK.value)
        & (pl.col("source") == LocationSource.OBSERVED.value)
    )
    assert observed.select("lat", "lon").rows() == [ALT_WORK]

    day1 = tours.filter(pl.col("person_day_id") == 1).sort("subtour_num")
    assert day1.select("subtour_num", "tour_type", "tour_purpose").rows() == [
        (0, TourType.HOME_BASED.value, PurposeCategory.WORK.value),
        (1, TourType.WORK_BASED.value, PurposeCategory.WORK_RELATED.value),
    ]

    day2 = tours.filter(pl.col("person_day_id") == 2)
    assert day2.select("subtour_num", "tour_purpose").rows() == [(0, PurposeCategory.WORK.value)]
