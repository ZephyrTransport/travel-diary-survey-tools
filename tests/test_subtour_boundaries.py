"""A subtour's return leg belongs to the subtour, even when the tour ends there.

A subtour is the run of trips that leaves an anchor and comes back to that same
anchor; the legs bounding the anchor period are the commute and belong to the
parent tour. When the tour ends at the anchor (Home > Work > Lunch > Work), the
last trip touching the anchor is the subtour's return leg, not the commute.
``assert_subtour_invariants`` checks the properties that hold for any itinerary.
"""

import polars as pl
import pytest

from data_canon.codebook.generic import LocationType
from processing.tours.detection_helpers import (
    detect_anchor_based_subtours,
    expand_anchor_periods,
)

# A trip as (origin anchor, destination anchor), each the anchor's identifier
# or 0 for none -- the only input subtour detection reads, so stating it directly
# keeps each itinerary legible. Anchor 1 is the usual workplace; 2 is another.
TO_ANCHOR = (0, 1)
LEAVE_ANCHOR = (1, 0)
RETURN_TO_ANCHOR = (0, 1)
ANCHOR_TO_HOME = (1, 0)
AWAY = (0, 0)
ANCHOR_TO_ANCHOR = (1, 2)
TO_OTHER_ANCHOR = (0, 2)
LEAVE_OTHER_ANCHOR = (2, 0)


def _tour(legs: list[tuple[int, int]], anchor: LocationType = LocationType.WORK) -> pl.DataFrame:
    """One tour's trips, with the anchor columns set for *anchor*."""
    n = len(legs)
    at = "work" if anchor is LocationType.WORK else "school"
    other = "school" if anchor is LocationType.WORK else "work"
    ends = {"o": [o for o, _ in legs], "d": [d for _, d in legs]}
    columns = {}
    for end, ids in ends.items():
        columns[f"_{end}_at_{at}"] = pl.Series([bool(i) for i in ids], dtype=pl.Boolean)
        columns[f"_{end}_{at}_anchor_id"] = pl.Series([i or None for i in ids], dtype=pl.Int64)
        columns[f"_{end}_at_{other}"] = pl.Series([False] * n, dtype=pl.Boolean)
        columns[f"_{end}_{other}_anchor_id"] = pl.Series([None] * n, dtype=pl.Int64)
    return pl.DataFrame(
        {
            "person_id": [1] * n,
            "person_day_id": [11] * n,
            "tour_num": [1] * n,
            "linked_trip_id": list(range(1, n + 1)),
            **columns,
        }
    )


def _detect(legs: list[tuple[int, int]], anchor: LocationType = LocationType.WORK) -> pl.DataFrame:
    """Run detection and return the trips in tour order."""
    return detect_anchor_based_subtours(expand_anchor_periods(_tour(legs, anchor))).sort(
        "_trip_num_in_tour"
    )


def assert_subtour_invariants(legs: list[tuple[int, int]], result: pl.DataFrame) -> None:
    """Properties that hold for every itinerary, whatever the numbers come out as.

    1. A subtour is a round trip: its first trip leaves an anchor and its last
       arrives back at that same anchor. A run that does only one of those, or
       comes back to a different anchor, is not a subtour.
    2. Subtours are numbered ``1..k`` within their tour, in the order they occur,
       with no gaps and no reuse.
    3. Every trip of a subtour is contiguous with the rest of that subtour.
    """
    nums = result["subtour_num"].to_list()

    seen: list[int] = []
    for num in nums:
        if num and (not seen or seen[-1] != num):
            assert num not in seen, f"subtour {num} is not contiguous in {nums}"
            seen.append(num)
    assert seen == list(range(1, len(seen) + 1)), f"subtour numbers are not 1..k: {nums}"

    for num in seen:
        members = [i for i, value in enumerate(nums) if value == num]
        first, last = legs[members[0]], legs[members[-1]]
        assert first[0], f"subtour {num} does not start at an anchor: {legs}"
        assert not first[1], f"subtour {num} does not leave its anchor: {legs}"
        assert not last[0], f"subtour {num} does not end by arriving: {legs}"
        assert last[1] == first[0], f"subtour {num} does not return where it left: {legs}"


@pytest.mark.parametrize(
    ("legs", "expected", "anchor"),
    [
        # Home > Work > Lunch > Work. Trips 2 and 3 are one round trip away
        # from the workplace and back, so both are subtour 1; trip 1 is the
        # commute in, and nothing leaves the anchor after the subtour returns.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [0, 1, 1],
            LocationType.WORK,
            id="return_leg_joins_the_subtour",
        ),
        # Home > Work > E1 > E2 > Work. The round trip spans three legs --
        # out, between stops, back -- and all three are the same subtour.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, AWAY, RETURN_TO_ANCHOR],
            [0, 1, 1, 1],
            LocationType.WORK,
            id="multi_stop_subtour_keeps_its_return_leg",
        ),
        # Home > School > Errand > School: the anchor type does not change the rule.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [0, 1, 1],
            LocationType.SCHOOL,
            id="school_anchors_the_same_way",
        ),
        # Home > Work > Lunch > Work > Home: trip 4 is the commute home, and
        # the legs bounding the anchor period belong to the parent tour.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, ANCHOR_TO_HOME],
            [0, 1, 1, 0],
            LocationType.WORK,
            id="commute_out_is_not_a_subtour",
        ),
        # Home > Work > Errand > Home. Leaving the workplace for an errand
        # that goes home instead of back is the commute out with a stop in it,
        # not a round trip from the anchor.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, AWAY],
            [0, 0, 0],
            LocationType.WORK,
            id="errand_on_the_way_home_is_not_a_subtour",
        ),
        # Home > Work > Home: nothing happens at the anchor.
        pytest.param(
            [TO_ANCHOR, ANCHOR_TO_HOME],
            [0, 0],
            LocationType.WORK,
            id="plain_commute_has_no_subtour",
        ),
        # Home > Shop > Home: no anchor period exists to look inside.
        pytest.param(
            [AWAY, AWAY],
            [0, 0],
            LocationType.WORK,
            id="tour_that_never_reaches_an_anchor_has_no_subtour",
        ),
        # Home > Work > Lunch > Work > Errand > Work > Home: subtours are
        # numbered in the order they occur within their parent tour.
        pytest.param(
            [
                TO_ANCHOR,
                LEAVE_ANCHOR,
                RETURN_TO_ANCHOR,
                LEAVE_ANCHOR,
                RETURN_TO_ANCHOR,
                ANCHOR_TO_HOME,
            ],
            [0, 1, 1, 2, 2, 0],
            LocationType.WORK,
            id="two_subtours_are_numbered_separately",
        ),
        # Home > Work > Lunch > Work > Errand > Work. The first case again,
        # reached after an earlier subtour has opened and closed.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [0, 1, 1, 2, 2],
            LocationType.WORK,
            id="second_subtour_survives_the_tour_ending_at_the_anchor",
        ),
        # A trip between two worksites is neither a departure nor a return, so
        # the chain reaches the end of the anchor period still open. Those
        # trips belong to the parent tour, not to a half-built subtour.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, AWAY, ANCHOR_TO_ANCHOR],
            [0, 0, 0, 0],
            LocationType.WORK,
            id="open_chain_falls_back_to_the_parent_tour",
        ),
        # Home > Work 1 > Coffee > Work 2 > Home: no round trip, so no subtour.
        pytest.param(
            [TO_ANCHOR, LEAVE_ANCHOR, TO_OTHER_ANCHOR, LEAVE_OTHER_ANCHOR],
            [0, 0, 0, 0],
            LocationType.WORK,
            id="chain_between_two_workplaces_is_not_a_subtour",
        ),
        # Home > Work 1 > Coffee > Work 2 > Lunch > Work 2 > Home. The coffee
        # run arrived elsewhere and stays on the parent tour; the lunch leaves
        # work 2 and comes back to it, so it is subtour 1.
        pytest.param(
            [
                TO_ANCHOR,
                LEAVE_ANCHOR,
                TO_OTHER_ANCHOR,
                LEAVE_OTHER_ANCHOR,
                TO_OTHER_ANCHOR,
                LEAVE_OTHER_ANCHOR,
            ],
            [0, 0, 0, 1, 1, 0],
            LocationType.WORK,
            id="second_workplace_starts_its_own_subtours",
        ),
        # Home > Work 1 > Lunch > Work 1 > Coffee > Work 2 > Errand > Work 2 > Home.
        pytest.param(
            [
                TO_ANCHOR,
                LEAVE_ANCHOR,
                RETURN_TO_ANCHOR,
                LEAVE_ANCHOR,
                TO_OTHER_ANCHOR,
                LEAVE_OTHER_ANCHOR,
                TO_OTHER_ANCHOR,
                LEAVE_OTHER_ANCHOR,
            ],
            [0, 1, 1, 0, 0, 2, 2, 0],
            LocationType.WORK,
            id="numbering_stays_gapless_after_a_dropped_chain",
        ),
    ],
)
def test_subtour_numbering(legs, expected, anchor):
    """Assert the expected numbering, then the invariants that outlive it."""
    result = _detect(legs, anchor)

    assert result["subtour_num"].to_list() == expected
    assert_subtour_invariants(legs, result)


@pytest.mark.parametrize(
    "legs",
    [
        [TO_ANCHOR, ANCHOR_TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
        [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, LEAVE_ANCHOR, AWAY],
        [TO_ANCHOR, LEAVE_ANCHOR, AWAY, TO_OTHER_ANCHOR, LEAVE_OTHER_ANCHOR, RETURN_TO_ANCHOR],
    ],
)
def test_every_subtour_is_a_closed_round_trip(legs):
    """Itineraries beyond the worked examples above still yield only closed round trips."""
    assert_subtour_invariants(legs, _detect(legs))
