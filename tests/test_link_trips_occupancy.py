"""Tests for breaking a linked trip where the travelling party changes.

Two segments joined at a mode-change stop are treated as one journey. If the
number of travellers differs across that stop, the more likely reading is that
somebody was picked up or dropped off, which makes the stop an activity rather
than a transfer. ``split_on_occupancy`` refuses the link in that case.
"""

import inspect
import logging
from datetime import datetime

import polars as pl
import pytest

from processing.link_trips.link import (
    _warn_on_occupancy_change,
    link_trip_ids,
    link_trips,
)

CHANGE_MODE = 10


def _two_segments(num_travelers: list[int | None]) -> pl.DataFrame:
    """Two segments meeting at a change-mode stop, close in time and space.

    Everything except the party size satisfies the linking rules, so any break
    in these tests is attributable to occupancy alone.
    """
    return pl.DataFrame(
        {
            "day_id": [10001, 10001],
            "person_id": [100, 100],
            "depart_time": [datetime(2024, 1, 1, 8, 0), datetime(2024, 1, 1, 8, 15)],
            "arrive_time": [datetime(2024, 1, 1, 8, 10), datetime(2024, 1, 1, 8, 45)],
            "d_purpose_category": [CHANGE_MODE, 1],
            "o_lat": [37.7, 37.71],
            "o_lon": [-122.4, -122.41],
            "d_lat": [37.71, 37.75],
            "d_lon": [-122.41, -122.45],
            "num_travelers": num_travelers,
        },
        schema_overrides={"num_travelers": pl.Int64},
    )


def _link(trips: pl.DataFrame, *, split: bool) -> pl.DataFrame:
    return link_trip_ids(
        trips,
        change_mode_enum=CHANGE_MODE,
        max_dwell_time=120,
        dwell_buffer_distance=100,
        split_on_occupancy=split,
    )


def test_party_change_breaks_the_link_when_enabled():
    """One traveller becoming three is a pick-up, not a transfer."""
    result = _link(_two_segments([1, 3]), split=True)

    assert result["linked_trip_id"].n_unique() == 2


def test_party_change_is_linked_when_disabled():
    """Default behaviour is unchanged, so existing projects are unaffected."""
    result = _link(_two_segments([1, 3]), split=False)

    assert result["linked_trip_id"].n_unique() == 1


def test_steady_party_still_links():
    """Splitting on occupancy must not break an ordinary transfer."""
    result = _link(_two_segments([2, 2]), split=True)

    assert result["linked_trip_id"].n_unique() == 1


@pytest.mark.parametrize("sizes", [[None, 3], [1, None], [None, None]])
def test_unreported_party_size_does_not_break_the_link(sizes):
    """A missing party size is missing data, not evidence of a change."""
    result = _link(_two_segments(sizes), split=True)

    assert result["linked_trip_id"].n_unique() == 1


def test_missing_column_is_refused_rather_than_ignored():
    """Asking to split with nothing to split on is an error, not a silent no-op."""
    trips = _two_segments([1, 3]).drop("num_travelers")

    with pytest.raises(ValueError, match="num_travelers"):
        _link(trips, split=True)


def test_aggregation_warns_when_a_party_changes_inside_a_link(caplog):
    """The max() roll-up stays, but it announces what it is papering over."""
    linked = _link(_two_segments([1, 3]), split=False)

    with caplog.at_level(logging.WARNING, logger="processing.link_trips.link"):
        _warn_on_occupancy_change(linked)

    assert "change party size between segments" in caplog.text
    assert "split_on_occupancy=True" in caplog.text


def test_no_warning_when_the_party_holds(caplog):
    """A steady party must not produce noise."""
    linked = _link(_two_segments([2, 2]), split=False)

    with caplog.at_level(logging.WARNING, logger="processing.link_trips.link"):
        _warn_on_occupancy_change(linked)

    assert "change party size" not in caplog.text


def test_no_warning_when_the_party_was_never_reported(caplog):
    """Nulls alone are not a change, so they must not warn either."""
    linked = _link(_two_segments([None, None]), split=False)

    with caplog.at_level(logging.WARNING, logger="processing.link_trips.link"):
        _warn_on_occupancy_change(linked)

    assert "change party size" not in caplog.text


def test_splitting_removes_the_warning(caplog):
    """Splitting is the remedy the warning points at, so it must silence it."""
    linked = _link(_two_segments([1, 3]), split=True)

    with caplog.at_level(logging.WARNING, logger="processing.link_trips.link"):
        _warn_on_occupancy_change(linked)

    assert "change party size" not in caplog.text


def test_the_choice_has_no_default():
    """No default, so a run cannot leave this decision unmade.

    Either answer changes what a linked trip *is*, and a default would quietly
    pick one on the configuration's behalf.
    """
    for func in (link_trips, link_trip_ids):
        parameter = inspect.signature(func).parameters["split_on_occupancy"]
        assert parameter.default is inspect.Parameter.empty, (
            f"{func.__name__} gives split_on_occupancy a default, which decides "
            f"trip linking semantics for every config that stays silent"
        )
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
