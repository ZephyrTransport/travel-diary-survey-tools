"""Tests for tour validation helper functions."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from processing.tours.validation_helpers import validate_and_correct_tours

PRIMARY_HOME, SECOND_HOME = 1101, 1102

# Home, two stops on the way, and a point far from either: the third trip
# begins at FAR rather than at B, so the leg connecting them is missing.
GAP_HOME, GAP_A, GAP_B, GAP_FAR = (
    (37.70, -122.40),
    (37.75, -122.42),
    (37.76, -122.43),
    (38.30, -123.00),
)
TOUR_WITH_A_HOLE = {
    "purpose": PurposeCategory.WORK.value,
    "start_home": None,
    "end_home": None,
    "legs": [(GAP_HOME, GAP_A), (GAP_A, GAP_B), (GAP_FAR, GAP_HOME)],
}

C, PB, PS, PE = (
    TourCategory.COMPLETE,
    TourCategory.PARTIAL_BOTH,
    TourCategory.PARTIAL_START,
    TourCategory.PARTIAL_END,
)
Q = TourDataQuality


def with_homes(linked_trips, o_homes=None, d_homes=None):
    """Add the home each trip end matched, as ``match_trip_ends`` reports it.

    Ends default to no home. ``SECOND_HOME`` is the person's other home.
    """
    n = linked_trips.height
    o_homes = o_homes or [None] * n
    d_homes = d_homes or [None] * n
    return linked_trips.with_columns(
        pl.Series("_o_home_id", o_homes, dtype=pl.Int64),
        pl.Series("_d_home_id", d_homes, dtype=pl.Int64),
        pl.Series("_o_at_other_home", [h == SECOND_HOME for h in o_homes]),
        pl.Series("_d_at_other_home", [h == SECOND_HOME for h in d_homes]),
    )


def _frames(
    category,
    *,
    purpose=PurposeCategory.SHOP.value,
    trips=2,
    start_home=PRIMARY_HOME,
    end_home=PRIMARY_HOME,
    subtour_num=0,
    legs=None,
):
    """One tour and its trips, with no other travel around it.

    Its first origin is at *start_home* and its last destination at *end_home*
    (None for no home). By default the legs form a continuous chain, each trip
    starting where the last one ended, so the internal spatial-gap check stays
    quiet; pass *legs* as ``[(o, d), ...]`` coordinate pairs to break that.
    """
    if legs is None:
        pts = [(37.0 + 0.1 * (i % 2), -122.0 - 0.1 * (i % 2)) for i in range(trips + 1)]
        legs = [(pts[i], pts[i + 1]) for i in range(trips)]
    trips = len(legs)

    tours = pl.DataFrame(
        {
            "tour_id": ["tour_1"],
            "person_id": [1],
            "person_day_id": [1],
            "trip_count": [trips],
            "subtour_num": [subtour_num],
            "tour_num": [1],
            "tour_category": [category.value],
            "tour_purpose": [purpose],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "tour_id": ["tour_1"] * trips,
            "person_id": [1] * trips,
            "hh_id": [1] * trips,
            "person_day_id": [1] * trips,
            "tour_num": [1] * trips,
            "o_lat": [o[0] for o, _ in legs],
            "o_lon": [o[1] for o, _ in legs],
            "d_lat": [d[0] for _, d in legs],
            "d_lon": [d[1] for _, d in legs],
            "depart_time": [datetime(2023, 5, 1, 8 + i) for i in range(trips)],
            "arrive_time": [datetime(2023, 5, 1, 8 + i, 30) for i in range(trips)],
        }
    )
    o_homes = [start_home] + [None] * (trips - 1)
    d_homes = [None] * (trips - 1) + [end_home]
    return tours, with_homes(linked_trips, o_homes, d_homes)


def _two_day_frames(second_day_origin):
    """A tour ending away on day 1, and day 2 starting from *second_day_origin*."""
    tours = pl.DataFrame(
        {
            "tour_id": ["tour_1", "tour_2"],
            "person_id": [1, 1],
            "person_day_id": [1, 2],
            "trip_count": [1, 1],
            "subtour_num": [0, 0],
            "tour_num": [1, 1],
            "tour_category": [
                TourCategory.PARTIAL_END.value,
                TourCategory.PARTIAL_START.value,
            ],
            "tour_purpose": [PurposeCategory.SHOP.value] * 2,
        }
    )
    linked_trips = pl.DataFrame(
        {
            "tour_id": ["tour_1", "tour_2"],
            "person_id": [1, 1],
            "hh_id": [1, 1],
            "person_day_id": [1, 2],
            "tour_num": [1, 1],
            "o_lat": [37.0, second_day_origin[0]],
            "o_lon": [-122.0, second_day_origin[1]],
            "d_lat": [37.5, 37.0],
            "d_lon": [-122.5, -122.0],
            "depart_time": [datetime(2023, 5, 1, 20), datetime(2023, 5, 2, 8)],
            "arrive_time": [datetime(2023, 5, 1, 21), datetime(2023, 5, 2, 9)],
        }
    )
    return tours, with_homes(linked_trips)


def test_unassigned_trip_raises():
    """A trip boundary detection never placed in a tour (tour_num < 1) is fatal."""
    tours = pl.DataFrame(
        {
            "tour_id": ["tour_1"],
            "person_id": [1],
            "person_day_id": [1],
            "trip_count": [1],
            "subtour_num": [0],
            "tour_num": [0],
            "tour_category": [TourCategory.COMPLETE.value],
            "tour_purpose": [PurposeCategory.WORK.value],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "tour_id": ["tour_1"],
            "person_id": [1],
            "hh_id": [1],
            "person_day_id": [1],
            "tour_num": [0],
        }
    )

    with pytest.raises(ValueError, match="never assigned to a tour"):
        validate_and_correct_tours(tours, linked_trips)


@pytest.mark.parametrize(
    ("category", "kwargs", "expected"),
    [
        # With no trip either side of it, an open end is where the diary stops.
        pytest.param(PB, {}, Q.PARTIAL_DIARY_EDGE, id="lone_partial_both_is_a_diary_edge"),
        pytest.param(PS, {}, Q.PARTIAL_DIARY_EDGE, id="lone_partial_start_is_a_diary_edge"),
        pytest.param(PE, {}, Q.PARTIAL_DIARY_EDGE, id="lone_partial_end_is_a_diary_edge"),
        pytest.param(C, {}, Q.VALID, id="complete_tour_is_valid"),
        # Aggregation leaves the purpose null when every candidate was the return leg.
        pytest.param(
            C, {"purpose": None, "trips": 1}, Q.NO_DESTINATION, id="no_purpose_is_no_destination"
        ),
        pytest.param(
            C,
            {"start_home": SECOND_HOME, "end_home": SECOND_HOME},
            Q.OTHER_HOME,
            id="closed_at_a_second_home_is_other_home",
        ),
        pytest.param(
            C,
            {"purpose": None, "trips": 1, "end_home": SECOND_HOME},
            Q.OTHER_HOME,
            id="direct_move_between_homes_is_other_home",
        ),
        # One code per tour: an open end outranks the home it left from.
        pytest.param(
            PE,
            {"trips": 1, "start_home": SECOND_HOME, "end_home": None},
            Q.PARTIAL_DIARY_EDGE,
            id="open_tour_from_a_second_home_reports_its_open_end",
        ),
        # Subtours start and end at work or school, whatever home is nearby.
        pytest.param(
            C,
            {"start_home": SECOND_HOME, "end_home": SECOND_HOME, "subtour_num": 1},
            Q.VALID,
            id="subtour_beside_a_second_home_is_graded_on_its_own_anchor",
        ),
        pytest.param(C, TOUR_WITH_A_HOLE, Q.SPATIAL_GAP, id="internal_gap_is_a_spatial_gap"),
    ],
)
def test_quality_code(category, kwargs, expected):
    """Each tour shape lands on its own quality code."""
    tours, linked_trips = _frames(category, **kwargs)

    result = validate_and_correct_tours(tours, linked_trips)

    assert result["tour_data_quality"][0] == expected.value


def test_category_is_not_rewritten():
    """Quality is a verdict on the category; it never edits it."""
    tours, linked_trips = _frames(TourCategory.PARTIAL_END, trips=1)

    result = validate_and_correct_tours(tours, linked_trips)

    assert result["tour_category"][0] == TourCategory.PARTIAL_END.value


def test_spatial_gap_threshold_is_configurable():
    """A jump below the configured threshold is not flagged."""
    tours, linked_trips = _frames(TourCategory.COMPLETE, **TOUR_WITH_A_HOLE)

    result = validate_and_correct_tours(
        tours, linked_trips, spatial_gap_threshold_meters=1_000_000.0
    )

    assert result["tour_data_quality"][0] == TourDataQuality.VALID.value


@pytest.mark.parametrize(
    ("second_day_origin", "expected"),
    [
        # The journey continues from where it stopped, so it was merely cut.
        pytest.param((37.5, -122.5), Q.PARTIAL_DAY_SPLIT, id="resuming_next_day_is_a_day_split"),
        # A leg is missing when the next trip starts somewhere else entirely.
        pytest.param((38.9, -123.9), Q.SPATIAL_GAP, id="reappearing_elsewhere_is_a_spatial_gap"),
    ],
)
def test_a_tour_ending_away_overnight(second_day_origin, expected):
    """Both halves of a chain cut at midnight get the same code."""
    tours, linked_trips = _two_day_frames(second_day_origin)

    result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

    assert result["tour_data_quality"].to_list() == [expected.value] * 2


def test_without_coordinates_open_ends_read_as_diary_edges():
    """With no coordinates there is no travel visible either side and no gap to measure."""
    tours = pl.DataFrame(
        {
            "tour_id": ["tour_1", "tour_2", "tour_3", "tour_4"],
            "person_id": [1, 1, 2, 2],
            "person_day_id": [1, 1, 1, 1],
            "trip_count": [2, 1, 3, 2],
            "subtour_num": [0, 0, 0, 0],
            "tour_num": [1, 2, 1, 2],
            "tour_category": [C.value, C.value, PB.value, PS.value],
            "tour_purpose": [
                PurposeCategory.WORK.value,
                None,
                PurposeCategory.SOCIALREC.value,
                PurposeCategory.SHOP.value,
            ],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "tour_id": ["tour_1"] * 2 + ["tour_2"] + ["tour_3"] * 3 + ["tour_4"] * 2,
            "person_id": [1, 1, 1, 2, 2, 2, 2, 2],
            "hh_id": [1, 1, 1, 2, 2, 2, 2, 2],
            "person_day_id": [1] * 8,
            "tour_num": [1, 1, 2, 1, 1, 1, 2, 2],
        }
    )

    result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

    assert result["tour_data_quality"].to_list() == [
        Q.VALID.value,
        Q.NO_DESTINATION.value,
        Q.PARTIAL_DIARY_EDGE.value,
        Q.PARTIAL_DIARY_EDGE.value,
    ]
