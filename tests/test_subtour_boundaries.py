"""A subtour's return leg belongs to the subtour, even when the tour ends there.

``expand_anchor_periods`` marks the first and last trip *touching* the anchor,
and subtour detection skips both: they are normally the commute in and the
commute out, which belong to the parent tour. That reasoning holds only while
something after the subtour leaves the anchor again.

When the tour ends at the anchor -- ``Home -> Work -> Lunch -> Work``, a diary
day that stops at the workplace -- the last trip touching the anchor *is* the
subtour's return leg. Skipping it scored the outbound leg as a subtour and the
trip back to work as no subtour at all, leaving a one-legged subtour that never
closed.

# What these tests assert

Each case states the itinerary and the subtour number expected per trip, read
off the definition rather than off the implementation: a subtour is the run of
trips that leaves the anchor and comes back to it, and the legs that bound the
anchor period are the commute and belong to the parent tour.

``assert_subtour_invariants`` then checks the properties that must hold for
*any* itinerary, so a future change cannot satisfy the worked examples while
breaking the rule they illustrate.
"""

import polars as pl
import pytest

from data_canon.codebook.generic import LocationType
from processing.tours.detection_helpers import (
    detect_anchor_based_subtours,
    expand_anchor_periods,
)

# A trip as (origin_at_anchor, destination_at_anchor) -- the only input subtour
# detection reads, so stating it directly keeps each itinerary legible.
TO_ANCHOR = (0, 1)
LEAVE_ANCHOR = (1, 0)
RETURN_TO_ANCHOR = (0, 1)
ANCHOR_TO_HOME = (1, 0)
AWAY = (0, 0)
ANCHOR_TO_ANCHOR = (1, 1)


def _tour(legs: list[tuple[int, int]], anchor: LocationType = LocationType.WORK) -> pl.DataFrame:
    """One tour's trips, with the anchor flags set for *anchor*."""
    n = len(legs)
    at = "work" if anchor is LocationType.WORK else "school"
    other = "school" if anchor is LocationType.WORK else "work"
    return pl.DataFrame(
        {
            "person_id": [1] * n,
            "day_id": [11] * n,
            "tour_num": [1] * n,
            "linked_trip_id": list(range(1, n + 1)),
            f"_o_at_{at}": pl.Series([bool(o) for o, _ in legs], dtype=pl.Boolean),
            f"_d_at_{at}": pl.Series([bool(d) for _, d in legs], dtype=pl.Boolean),
            f"_o_at_{other}": pl.Series([False] * n, dtype=pl.Boolean),
            f"_d_at_{other}": pl.Series([False] * n, dtype=pl.Boolean),
        }
    )


def _detect(legs: list[tuple[int, int]], anchor: LocationType = LocationType.WORK) -> pl.DataFrame:
    """Run detection and return the trips in tour order."""
    return detect_anchor_based_subtours(expand_anchor_periods(_tour(legs, anchor))).sort(
        "_trip_num_in_tour"
    )


def assert_subtour_invariants(legs: list[tuple[int, int]], result: pl.DataFrame) -> None:
    """Properties that hold for every itinerary, whatever the numbers come out as.

    1. A subtour is a round trip: its first trip leaves the anchor and its last
       arrives back at it. A run that does only one of those is not a subtour.
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
        assert first == LEAVE_ANCHOR, f"subtour {num} does not start by leaving the anchor: {legs}"
        assert last == RETURN_TO_ANCHOR, f"subtour {num} does not end back at the anchor: {legs}"


def check(legs: list[tuple[int, int]], expected: list[int], anchor=LocationType.WORK) -> None:
    """Assert the expected numbering, then the invariants that outlive it."""
    result = _detect(legs, anchor)
    assert result["subtour_num"].to_list() == expected
    assert_subtour_invariants(legs, result)


class TestTourEndsAtTheAnchor:
    """The regression: nothing leaves the anchor after the subtour returns."""

    def test_return_leg_joins_the_subtour(self):
        """Home > Work > Lunch > Work.

        Trips 2 and 3 are one round trip away from the workplace and back, so
        both are subtour 1. Trip 1 is the commute in.
        """
        check([TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR], [0, 1, 1])

    def test_multi_stop_subtour_keeps_its_return_leg(self):
        """Home > Work > E1 > E2 > Work.

        The round trip spans three legs -- out, between stops, back -- and all
        three belong to the same subtour.
        """
        check([TO_ANCHOR, LEAVE_ANCHOR, AWAY, RETURN_TO_ANCHOR], [0, 1, 1, 1])

    def test_school_anchors_the_same_way(self):
        """Home > School > Errand > School: the anchor type does not change the rule."""
        check(
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [0, 1, 1],
            anchor=LocationType.SCHOOL,
        )


class TestCommuteLegsStayOut:
    """The legs bounding the anchor period belong to the parent tour."""

    def test_commute_out_is_not_a_subtour(self):
        """Home > Work > Lunch > Work > Home: trip 4 is the commute home."""
        check([TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, ANCHOR_TO_HOME], [0, 1, 1, 0])

    def test_errand_on_the_way_home_is_not_a_subtour(self):
        """Home > Work > Errand > Home.

        Leaving the workplace for an errand that goes home instead of back is
        the commute out with a stop in it, not a round trip from the anchor.
        """
        check([TO_ANCHOR, LEAVE_ANCHOR, AWAY], [0, 0, 0])

    def test_plain_commute_has_no_subtour(self):
        """Home > Work > Home: nothing happens at the anchor."""
        check([TO_ANCHOR, ANCHOR_TO_HOME], [0, 0])

    def test_tour_that_never_reaches_an_anchor_has_no_subtour(self):
        """Home > Shop > Home: no anchor period exists to look inside."""
        check([AWAY, AWAY], [0, 0])


class TestNumbering:
    """Subtours are numbered in the order they occur within their parent tour."""

    def test_two_subtours_are_numbered_separately(self):
        """Home > Work > Lunch > Work > Errand > Work > Home."""
        check(
            [
                TO_ANCHOR,
                LEAVE_ANCHOR,
                RETURN_TO_ANCHOR,
                LEAVE_ANCHOR,
                RETURN_TO_ANCHOR,
                ANCHOR_TO_HOME,
            ],
            [0, 1, 1, 2, 2, 0],
        )

    def test_second_subtour_survives_the_tour_ending_at_the_anchor(self):
        """Home > Work > Lunch > Work > Errand > Work.

        The same regression as the first case, reached after an earlier subtour
        has already opened and closed.
        """
        check(
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [0, 1, 1, 2, 2],
        )


class TestUnclosedChainsAreDiscarded:
    """An assignment must never claim a round trip the trips do not show."""

    def test_open_chain_falls_back_to_the_parent_tour(self):
        """A trip both leaving and arriving at an anchor closes nothing.

        Two habitual worksites make ``(1, 1)`` neither a departure nor a
        return, so the chain reaches the end of the anchor period still open.
        Those trips belong to the parent tour, not to a half-built subtour.
        """
        check([TO_ANCHOR, LEAVE_ANCHOR, AWAY, ANCHOR_TO_ANCHOR], [0, 0, 0, 0])


class TestInvariantsHoldAcrossItineraries:
    """The rules above are properties, not just facts about the worked examples."""

    @pytest.mark.parametrize(
        "legs",
        [
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [TO_ANCHOR, LEAVE_ANCHOR, AWAY, RETURN_TO_ANCHOR],
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, ANCHOR_TO_HOME],
            [TO_ANCHOR, LEAVE_ANCHOR, AWAY],
            [TO_ANCHOR, ANCHOR_TO_HOME],
            [TO_ANCHOR, LEAVE_ANCHOR, AWAY, ANCHOR_TO_ANCHOR],
            [TO_ANCHOR, ANCHOR_TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR],
            [TO_ANCHOR, LEAVE_ANCHOR, RETURN_TO_ANCHOR, LEAVE_ANCHOR, AWAY],
            [AWAY, AWAY],
        ],
    )
    def test_every_subtour_is_a_closed_round_trip(self, legs):
        """No itinerary may produce a subtour that does not leave and return."""
        assert_subtour_invariants(legs, _detect(legs))
