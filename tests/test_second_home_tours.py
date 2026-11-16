"""Second homes bound tours, and tours touching them are graded OTHER_HOME.

A person's other home is known only from the respondent: a vendor's second-home
address, or a day they said ended at home somewhere other than their reported
home. Travel alone never makes one.
"""

import polars as pl
import pytest

from data_canon.codebook.days import BeginEndDay
from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import AgeCategory, Employment
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import Purpose
from data_canon.validation.custom import check_trip_count_matches_quality
from tests.fixtures.tour_pipeline import (
    HOME,
    HOUSEHOLDS,
    SHOP,
    at,
    leg,
    link_and_extract_tours,
    person,
)

SECOND_HOME = (38.30, -122.90)
SHOP_NEAR_SECOND = (38.31, -122.91)

RETIREE = person(
    work=None,
    age=AgeCategory.AGE_65_TO_74.value,
    employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
)

H, G, OR = Purpose.HOME, Purpose.GROCERY, Purpose.OTHER_RESIDENCE

# Shop from home; drive to the second home; shop from there.
WEEKEND = [
    leg(HOME, SHOP, H, G, at(9), at(9, 30)),
    leg(SHOP, HOME, G, H, at(10, 30), at(11)),
    leg(HOME, SECOND_HOME, H, OR, at(17), at(18)),
    leg(SECOND_HOME, SHOP_NEAR_SECOND, OR, G, at(9, day=2), at(9, 30, day=2), person_day_id=2),
    leg(SHOP_NEAR_SECOND, SECOND_HOME, G, OR, at(11, day=2), at(11, 30, day=2), person_day_id=2),
]


def _days(end_day_1: BeginEndDay | None = None) -> pl.DataFrame:
    """The two diary days, with day 1's stated end."""
    return pl.DataFrame(
        {
            "person_day_id": [1, 2],
            "hh_day_id": [101, 102],
            "travel_date": [at(0).date(), at(0, day=2).date()],
            "begin_day": [None, None],
            "end_day": [end_day_1.value if end_day_1 else None, None],
        },
        schema={
            "person_day_id": pl.Int64,
            "hh_day_id": pl.Int64,
            "travel_date": pl.Date,
            "begin_day": pl.Int64,
            "end_day": pl.Int64,
        },
    )


def _vendor_second_home() -> pl.DataFrame:
    """The vendor's second-home address, as cleaning hands it to the helper."""
    return pl.DataFrame(
        {
            "person_id": [1],
            "location_type": [LocationType.HOME.value],
            "lat": [SECOND_HOME[0]],
            "lon": [SECOND_HOME[1]],
        }
    )


def _homes(result: dict) -> pl.DataFrame:
    return result["habitual_locations"].filter(pl.col("location_type") == LocationType.HOME.value)


@pytest.mark.parametrize(
    ("person_days", "supplied"),
    [
        pytest.param(_days(), _vendor_second_home(), id="vendor_second_home_address"),
        pytest.param(_days(BeginEndDay.OTHER_HOME), None, id="day_ended_at_my_other_home"),
    ],
)
def test_second_home_bounds_tours(person_days, supplied):
    """The drive between homes and the round trip from the second home are OTHER_HOME."""
    result = link_and_extract_tours(
        RETIREE, HOUSEHOLDS, WEEKEND, person_days=person_days, extra=supplied
    )
    tours = result["tours"].sort("origin_depart_time")

    assert tours["tour_category"].to_list() == [TourCategory.COMPLETE.value] * 3
    assert tours["tour_data_quality"].to_list() == [
        TourDataQuality.VALID.value,  # from the primary home: untouched
        TourDataQuality.OTHER_HOME.value,  # the drive between homes
        TourDataQuality.OTHER_HOME.value,  # a round trip from the second home
    ]
    assert _homes(result).height == 2
    # The drive between homes is closed and has no purpose; validation accepts it.
    assert check_trip_count_matches_quality(tours, result["linked_trips"]) == []


def test_validation_still_rejects_a_closed_tour_without_a_purpose_otherwise():
    """Only NO_DESTINATION or a move between homes explains a closed tour with no purpose."""
    tours = pl.DataFrame(
        {
            "tour_id": [1],
            "trip_count": [1],
            "tour_category": [TourCategory.COMPLETE.value],
            "tour_purpose": [None],
            "tour_data_quality": [TourDataQuality.VALID.value],
        },
        schema_overrides={"tour_purpose": pl.Int64},
    )
    errors = check_trip_count_matches_quality(tours, pl.DataFrame({"tour_id": [1]}))
    assert len(errors) == 1


def test_without_a_report_the_second_home_is_just_a_place():
    """Nights at "another residence" do not make a home on their own."""
    result = link_and_extract_tours(RETIREE, HOUSEHOLDS, WEEKEND, person_days=_days())
    tours = result["tours"].sort("origin_depart_time")

    assert _homes(result).height == 1
    assert tours["tour_category"].to_list() == [
        TourCategory.COMPLETE.value,
        TourCategory.PARTIAL_END.value,
        TourCategory.PARTIAL_BOTH.value,
    ]
    assert TourDataQuality.OTHER_HOME.value not in tours["tour_data_quality"].to_list()
