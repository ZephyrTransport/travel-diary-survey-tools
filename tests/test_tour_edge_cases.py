"""Tour extraction on edge-case days.

Single trips, days starting away from home, distant stops, and arrivals home
coded as an activity.
"""

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory
from tests.fixtures.tour_pipeline import (
    HOME,
    HOUSEHOLDS,
    LUNCH,
    SHOP,
    WORK,
    at,
    leg,
    link_and_extract_tours,
    person,
)

CORNER_SHOP = (37.81, -122.41)
DOWNTOWN = (37.79, -122.39)
ACROSS_THE_BAY = (37.6, -122.2)

H, W, S = Purpose.HOME, Purpose.PRIMARY_WORKPLACE, Purpose.SHOPPING_ERRANDS
SOCIAL = Purpose.OTHER_SOCIAL


def _tours(legs: list[dict], **kwargs) -> pl.DataFrame:
    """The tours extracted from the worker's legs."""
    return link_and_extract_tours(person(), HOUSEHOLDS, legs, **kwargs)["tours"]


def test_tour_destination_type_matches_destination_coords():
    """For home -> shop -> home, d_location_type and d_lat/d_lon both describe the shop."""
    tours = _tours(
        [
            leg(HOME, SHOP, H, S, at(9), at(9, 15)),
            leg(SHOP, HOME, S, H, at(11), at(11, 15)),
        ]
    )

    assert len(tours) == 1
    tour = tours.row(0, named=True)
    assert (tour["d_lat"], tour["d_lon"]) == SHOP
    assert tour["d_location_type"] == LocationType.OTHER.value


def test_single_trip_tour():
    """A lone trip out is kept as a tour numbered 1, ending at the diary edge.

    Its stop is the primary destination, so the tour takes its purpose and
    coordinates from that one trip.
    """
    tours = _tours([leg(HOME, SHOP, H, S, at(9), at(9, 15))])

    assert len(tours) == 1
    tour = tours.row(0, named=True)
    assert tour["trip_count"] == 1
    assert tour["tour_data_quality"] == TourDataQuality.PARTIAL_DIARY_EDGE.value
    assert tour["tour_num"] == 1
    assert tour["tour_purpose"] == PurposeCategory.SHOP.value
    assert (tour["d_lat"], tour["d_lon"]) == SHOP


def test_partial_tour():
    """A day that starts away from home is one tour, numbered from 1, partial at the start."""
    walk = {"distance_meters": 1000.0, "mode_type": ModeType.WALK.value}
    tours = _tours(
        [
            leg(WORK, LUNCH, W, Purpose.DINING, at(12), at(12, 15), **walk),
            leg(LUNCH, WORK, Purpose.DINING, W, at(12, 30), at(12, 45), **walk),
            leg(WORK, HOME, W, H, at(13, 30), at(14)),
        ]
    )

    assert len(tours) == 1
    tour = tours.row(0, named=True)
    assert tour["tour_num"] == 1, "Tour numbering starts at 1, never 0"
    assert tour["trip_count"] == 3
    assert (tour["o_lat"], tour["o_lon"]) == WORK, "The day opens away from home"
    assert tour["tour_category"] == TourCategory.PARTIAL_START.value
    assert tour["tour_data_quality"] == TourDataQuality.PARTIAL_DIARY_EDGE.value
    assert tour["tour_purpose"] == PurposeCategory.WORK.value


def test_distant_destinations():
    """A tour whose stops are far apart still gets destination times."""
    # Pin the hierarchy method: under scoring a 60-min social visit outranks a
    # 60-min work visit, and this test is about destination times, not purpose.
    tours = _tours(
        [
            leg(HOME, DOWNTOWN, H, W, at(8), at(9), distance_meters=10000.0),
            leg(DOWNTOWN, ACROSS_THE_BAY, W, SOCIAL, at(10), at(11), distance_meters=20000.0),
            leg(ACROSS_THE_BAY, HOME, SOCIAL, H, at(12), at(14), distance_meters=30000.0),
        ],
        tour_purpose_method="hierarchy",
    )

    assert len(tours) == 1
    assert tours["dest_arrive_time"][0] is not None
    assert tours["dest_depart_time"][0] is not None
    assert tours["tour_purpose"][0] == PurposeCategory.WORK.value


def test_tour_num_sequential():
    """Tour numbers run 1, 2, 3 over a day, and every tour has its origin times."""
    tours = _tours(
        [
            leg(HOME, WORK, H, W, at(8), at(8, 30)),
            leg(WORK, HOME, W, H, at(9), at(9, 30)),
            leg(HOME, CORNER_SHOP, H, Purpose.GROCERY, at(10), at(10, 15)),
            leg(CORNER_SHOP, HOME, Purpose.GROCERY, H, at(11), at(11, 15)),
            leg(HOME, SHOP, H, Purpose.SOCIAL, at(13), at(13, 15)),
        ]
    )

    tour_nums = sorted(tours["tour_num"].unique().to_list())
    assert tour_nums == list(range(1, len(tour_nums) + 1))
    assert tours["origin_depart_time"].null_count() == 0
    assert tours["origin_arrive_time"].null_count() == 0


def test_two_trips_out_and_back_are_two_tours_whatever_the_purpose_says():
    """Out and back twice is two tours, even when an arrival home is coded as an activity.

    Within the home's own address the purpose describes the activity, not a
    different place.
    """
    tours = link_and_extract_tours(
        person(work=None),
        HOUSEHOLDS,
        [
            leg(HOME, SHOP, H, S, at(9), at(9, 15)),
            leg(SHOP, HOME, S, Purpose.EXERCISE, at(11), at(11, 15)),
            leg(HOME, SHOP, Purpose.EXERCISE, S, at(14), at(14, 15)),
            leg(SHOP, HOME, S, H, at(16), at(16, 15)),
        ],
    )["tours"]

    assert tours.height == 2
    assert tours["tour_data_quality"].to_list() == [TourDataQuality.VALID.value] * 2
