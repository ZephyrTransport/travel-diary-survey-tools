"""Joint trip detection: config validation, buffer and Mahalanobis matching, edge cases."""

import logging
import re
from datetime import datetime, timedelta

import polars as pl
import pytest
from pydantic import ValidationError

from data_canon.codebook.days import TravelDow
from data_canon.codebook.trips import Driver, ModeType, PurposeCategory
from processing.joint_trips import (
    JointTripConfig,
    clique_detection,
    detect_joint_trips,
)
from processing.joint_trips.aggregation import build_joint_trips_table
from processing.joint_trips.similarity import (
    apply_buffer_filter,
    apply_mahalanobis_filter,
    compute_pairwise_distances,
)
from tests.fixtures.tour_pipeline import days_not_asked

FULL_COVARIANCE = [
    [10000, 0, 0, 0],
    [0, 10000, 0, 0],
    [0, 0, 100, 0],
    [0, 0, 0, 100],
]


# Every field a buffer config must state, so each case below varies only what it tests.
_REQUIRED = {"method": "buffer", "time_threshold_minutes": 15.0, "space_threshold_meters": 100.0}
_MAHALANOBIS = {"method": "mahalanobis", "confidence_level": 0.90}


def _buffer(**overrides) -> JointTripConfig:
    """Buffer matching, as a config would state it."""
    return JointTripConfig(**{**_REQUIRED, **overrides})


class TestJointTripConfig:
    """Test configuration validation."""

    @pytest.mark.parametrize(
        ("kwargs", "expected_threshold"),
        [
            pytest.param(_REQUIRED, 0.0, id="buffer_has_no_statistical_threshold"),
            pytest.param(
                {**_MAHALANOBIS, "covariance": [7000, 7000, 20, 20]},
                1.0636232167792241,
                id="diagonal_covariance",
            ),
            pytest.param(
                {**_MAHALANOBIS, "covariance": FULL_COVARIANCE},
                1.0636232167792241,
                id="full_covariance_matrix",
            ),
        ],
    )
    def test_an_accepted_config_yields_its_distance_threshold(self, kwargs, expected_threshold):
        """The chi-squared threshold is what the rest of detection actually reads.

        Confidence 0.90 on four dimensions is the bottom 10% of a
        chi-squared with four degrees of freedom; the buffer method does not
        use a statistical distance at all.
        """
        config = JointTripConfig(**kwargs)

        assert config.covariance == kwargs.get("covariance")
        assert config.get_distance_threshold() == pytest.approx(expected_threshold)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            pytest.param(
                {**_MAHALANOBIS, "covariance": [10000, 10000, 100]},
                "must have 4 values",
                id="diagonal_of_three",
            ),
            pytest.param(
                {**_MAHALANOBIS, "covariance": [[10000, 0], [0, 10000]]},
                "4x4",
                id="two_by_two_matrix",
            ),
            pytest.param(
                {**_REQUIRED, "time_threshold_minutes": -5.0},
                "greater than or equal",
                id="negative_time_threshold",
            ),
            pytest.param(
                {"method": "buffer", "time_threshold_minutes": 15.0},
                "buffer requires space_threshold_meters",
                id="buffer_missing_a_threshold",
            ),
            pytest.param(
                {"method": "mahalanobis", "covariance": [7000, 7000, 20, 20]},
                "mahalanobis requires confidence_level",
                id="mahalanobis_missing_its_confidence_level",
            ),
            pytest.param(
                {**_REQUIRED, "covariance": None},
                "buffer does not use covariance",
                id="buffer_given_a_mahalanobis_setting_even_null",
            ),
            pytest.param(
                {**_MAHALANOBIS, "covariance": [7000, 7000, 20, 20], "space_threshold_meters": 50},
                "mahalanobis does not use space_threshold_meters",
                id="mahalanobis_given_a_buffer_threshold",
            ),
            pytest.param(
                {
                    **_MAHALANOBIS,
                    "covariance": [
                        [10000, 100, 0, 0],
                        [0, 10000, 0, 0],
                        [0, 0, 100, 0],
                        [0, 0, 0, 100],
                    ],
                },
                "symmetric",
                id="asymmetric_off_diagonal",
            ),
            pytest.param(
                {**_MAHALANOBIS, "covariance": [10000, -10000, 100, 100]},
                "positive",
                id="negative_variance",
            ),
        ],
    )
    def test_invalid_config_is_rejected(self, kwargs, match):
        """Bad values, and settings missing from or foreign to the method, stop at configuration."""
        with pytest.raises(ValidationError, match=match):
            JointTripConfig(**kwargs)


class TestSimilarityCalculations:
    """Test similarity computation and filtering functions.

    The three pairs are deliberately spread: identical, ~140 m and 5 minutes
    apart, and ~700 m and 20 minutes apart, so a filter that keeps everything
    and a filter that keeps nothing both fail.
    """

    @pytest.fixture
    def sample_trip_pairs(self):
        """Create sample trip pairs for testing."""
        return pl.DataFrame(
            {
                "linked_trip_id": [1, 2, 3],
                "linked_trip_id_b": [2, 3, 4],
                "o_lat": [37.8, 37.8, 37.8],
                "o_lon": [-122.4, -122.4, -122.4],
                "o_lat_b": [37.8, 37.801, 37.805],
                "o_lon_b": [-122.4, -122.401, -122.405],
                "d_lat": [37.85, 37.85, 37.85],
                "d_lon": [-122.45, -122.45, -122.45],
                "d_lat_b": [37.85, 37.851, 37.860],
                "d_lon_b": [-122.45, -122.451, -122.460],
                "depart_time": [
                    datetime(2024, 1, 15, 9, 0),
                    datetime(2024, 1, 15, 9, 0),
                    datetime(2024, 1, 15, 9, 0),
                ],
                "depart_time_b": [
                    datetime(2024, 1, 15, 9, 1),
                    datetime(2024, 1, 15, 9, 5),
                    datetime(2024, 1, 15, 9, 20),
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 9, 30),
                    datetime(2024, 1, 15, 9, 30),
                    datetime(2024, 1, 15, 9, 30),
                ],
                "arrive_time_b": [
                    datetime(2024, 1, 15, 9, 31),
                    datetime(2024, 1, 15, 9, 35),
                    datetime(2024, 1, 15, 9, 50),
                ],
            }
        )

    def test_compute_pairwise_distances(self, sample_trip_pairs):
        """Origin, destination and both time gaps, per pair."""
        result = compute_pairwise_distances(sample_trip_pairs)

        assert result["origin_dist_m"].to_list() == pytest.approx([0.0, 141.72, 708.58], abs=0.01)
        assert result["dest_dist_m"].to_list() == pytest.approx([0.0, 141.68, 1416.77], abs=0.01)
        assert result["depart_diff_min"].to_list() == [1, 5, 20]
        assert result["arrive_diff_min"].to_list() == [1, 5, 20]

    def test_buffer_filter_strict_and(self, sample_trip_pairs):
        """All four dimensions must pass, and the third pair fails on all four.

        The second pair is ~142 m and 5 minutes off, inside both thresholds, so
        strict AND is not the same as "only exact matches survive".
        """
        pairs_with_dist = compute_pairwise_distances(sample_trip_pairs)

        filtered = apply_buffer_filter(
            pairs_with_dist,
            space_threshold_meters=200,
            time_threshold_minutes=10,
        )

        assert filtered.select("linked_trip_id", "linked_trip_id_b").rows() == [(1, 2), (2, 3)]

    def test_mahalanobis_filter_diagonal(self, sample_trip_pairs):
        """The statistical distance is stricter here than the 200 m buffer.

        Scaled by the given variances the second pair sits at 2.87, past the
        2.5 threshold, so only the identical pair survives.
        """
        pairs_with_dist = compute_pairwise_distances(sample_trip_pairs)

        filtered = apply_mahalanobis_filter(
            pairs_with_dist,
            covariance=[7000, 7000, 20, 20],
            distance_threshold=2.5,
        )

        assert filtered.select("linked_trip_id", "linked_trip_id_b").rows() == [(1, 2)]


def _household(depart_times: list[datetime]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """One household whose members each make the same home -> work trip.

    Every field except the departure time is held constant across members, so
    a pair that is not detected as joint failed on timing alone. Each trip
    takes thirty minutes.
    """
    n = len(depart_times)
    arrive_times = [depart + timedelta(minutes=30) for depart in depart_times]

    households = pl.DataFrame({"hh_id": [1], "home_lat": [37.8], "home_lon": [-122.4]})
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": list(range(1, n + 1)),
            "hh_id": [1] * n,
            "person_day_id": [1] * n,
            "person_id": list(range(1, n + 1)),
            "travel_dow": [TravelDow.WEDNESDAY.value] * n,
            "o_lat": [37.8] * n,
            "o_lon": [-122.4] * n,
            "d_lat": [37.85] * n,
            "d_lon": [-122.45] * n,
            "depart_time": depart_times,
            "arrive_time": arrive_times,
            "o_purpose_category": [PurposeCategory.HOME.value] * n,
            "d_purpose_category": [PurposeCategory.WORK.value] * n,
            "mode_type": [ModeType.CAR.value] * n,
            "driver": [Driver.DRIVER.value] * n,
            "num_travelers": [n] * n,
            "access_mode": [None] * n,
            "egress_mode": [None] * n,
            "duration_minutes": [30.0] * n,
            "distance_meters": [5000.0] * n,
            "depart_date": [datetime(2024, 1, 15)] * n,
            "arrive_date": [datetime(2024, 1, 15)] * n,
            "depart_hour": [depart.hour for depart in depart_times],
            "depart_minute": [depart.minute for depart in depart_times],
            "depart_seconds": [0] * n,
            "arrive_hour": [arrive.hour for arrive in arrive_times],
            "arrive_minute": [arrive.minute for arrive in arrive_times],
            "arrive_seconds": [0] * n,
            "tour_direction": [1] * n,
        },
        schema_overrides={"access_mode": pl.Int64, "egress_mode": pl.Int64},
    )
    return households, linked_trips


def test_detect_matching_trips_buffer():
    """Two members leaving a minute apart from the same door travel together."""
    households, linked_trips = _household(
        [datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 1)]
    )

    result = detect_joint_trips(
        linked_trips=linked_trips,
        person_days=days_not_asked(linked_trips),
        households=households,
        matching=_buffer(time_threshold_minutes=15, space_threshold_meters=100),
    )

    updated_trips = result["linked_trips"]
    joint_trips = result["joint_trips"]

    # Both trips carry the same grouping id
    assert updated_trips["joint_trip_id"].null_count() == 0
    assert updated_trips["joint_trip_id"].n_unique() == 1

    assert len(joint_trips) == 1
    assert joint_trips["num_joint_travelers"][0] == 2


def test_detect_non_matching_trips():
    """Five hours apart is the same route at different times, not joint travel."""
    households, linked_trips = _household(
        [datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 5), datetime(2024, 1, 15, 14, 0)]
    )

    result = detect_joint_trips(
        linked_trips=linked_trips,
        person_days=days_not_asked(linked_trips),
        households=households,
        matching=_buffer(),
    )

    joint_ids = result["linked_trips"].sort("person_id")["joint_trip_id"].to_list()
    assert joint_ids[0] == joint_ids[1] is not None
    assert joint_ids[2] is None


def test_a_chain_of_departures_is_resolved_into_one_group():
    """Three members leaving ten minutes apart in turn cannot all be one party.

    Each neighbouring pair is inside the fifteen-minute threshold but the first
    and last are not, so the pairs overlap without forming a group of three.
    The middle traveller can only belong to one of them, and the tie is settled
    on the quality of the two candidate groupings rather than on row order.
    """
    households, linked_trips = _household(
        [
            datetime(2024, 1, 15, 9, 0),
            datetime(2024, 1, 15, 9, 10),
            datetime(2024, 1, 15, 9, 20),
        ]
    )

    result = detect_joint_trips(
        linked_trips=linked_trips,
        person_days=days_not_asked(linked_trips),
        households=households,
        matching=_buffer(time_threshold_minutes=15, space_threshold_meters=100),
    )

    # The first two travel together; the last is left on their own
    assert result["linked_trips"].select("person_id", "joint_trip_id").rows() == [
        (1, 1001),
        (2, 1001),
        (3, None),
    ]
    assert result["joint_trips"]["num_joint_travelers"].to_list() == [2]


def _far_apart(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """The second member's trip ends 50 km from the first's."""
    return linked_trips.with_columns(
        pl.when(pl.col("person_id") == 2).then(37.4).otherwise(pl.col("d_lat")).alias("d_lat")
    )


@pytest.mark.parametrize(
    ("depart_times", "edit", "counts"),
    [
        pytest.param(
            [datetime(2024, 1, 15, 9, 0)], None, "0 trips in multi-person", id="one_person"
        ),
        pytest.param(
            [datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 14, 0)],
            None,
            "0 pairs overlapping",
            id="no_overlap",
        ),
        pytest.param(
            [datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 0)],
            _far_apart,
            "1 pairs overlapping in time, 0 pairs matched",
            id="no_match",
        ),
    ],
)
def test_no_joint_trips_warns(caplog, depart_times, edit, counts):
    """With no joint trip at all, the step warns how far detection got and returns no groups."""
    households, linked_trips = _household(depart_times)
    if edit is not None:
        linked_trips = edit(linked_trips)

    with caplog.at_level(logging.WARNING):
        result = detect_joint_trips(
            linked_trips=linked_trips,
            person_days=days_not_asked(linked_trips),
            households=households,
            matching=_buffer(),
        )

    assert re.search(f"No joint trips found: .*{counts}", caplog.text)
    assert result["linked_trips"]["joint_trip_id"].null_count() == len(linked_trips)
    assert result["joint_trips"].is_empty()
    assert {"joint_trip_id", "hh_id", "hh_day_id", "travel_date"} <= set(
        result["joint_trips"].columns
    )


class TestGroupingMembership:
    """A joint trip is its member trips, so it needs two people behind it.

    The weighting sums over those members and CT-RAMP multiplies the record by
    their count, so a grouping standing for one person misstates both.
    """

    @staticmethod
    def _members(person_ids: list[int]) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Linked trips and assignments putting every trip in one joint group."""
        n = len(person_ids)
        linked_trips = pl.DataFrame(
            {
                "linked_trip_id": list(range(1, n + 1)),
                "person_id": person_ids,
                "hh_id": [1] * n,
                "person_day_id": [1] * n,
                "o_lat": [37.7] * n,
                "o_lon": [-122.4] * n,
                "d_lat": [37.8] * n,
                "d_lon": [-122.5] * n,
                "depart_time": [datetime(2023, 5, 1, 8, 0)] * n,
                "arrive_time": [datetime(2023, 5, 1, 8, 30)] * n,
            }
        )
        assignments = pl.DataFrame(
            {"linked_trip_id": list(range(1, n + 1)), "joint_trip_id": [500] * n}
        )
        return linked_trips, assignments

    def test_two_participants_are_accepted(self):
        """The control: a genuine pair aggregates without complaint."""
        linked_trips, assignments = self._members([101, 102])

        result = build_joint_trips_table(
            linked_trips.join(assignments, on="linked_trip_id"), days_not_asked(linked_trips)
        )

        assert len(result) == 1
        assert result["num_joint_travelers"][0] == 2

    @pytest.mark.parametrize(
        "person_ids",
        [
            pytest.param([101, 101], id="one_person_travelling_twice"),
            pytest.param([101], id="a_lone_member"),
        ],
    )
    def test_a_group_needs_two_distinct_people(self, person_ids):
        """Counting rows would call these joint; counting people does not."""
        linked_trips, assignments = self._members(person_ids)

        with pytest.raises(ValueError, match="fewer than 2 distinct participants"):
            build_joint_trips_table(
                linked_trips.join(assignments, on="linked_trip_id"), days_not_asked(linked_trips)
            )


@pytest.mark.parametrize(
    "found",
    [
        pytest.param([[1, 2], [3, 4], [2, 3, 5]], id="pairs_found_first"),
        pytest.param([[2, 3, 5], [1, 2], [3, 4]], id="group_found_first"),
    ],
)
def test_a_group_overlapping_two_pairs_replaces_both(monkeypatch, found):
    """Trips 2, 3 and 5 overlap pairs {1, 2} and {3, 4}: the group wins and both pairs lose."""
    monkeypatch.setattr(clique_detection.nx, "find_cliques", lambda _graph: iter(found))
    pairs = pl.DataFrame(
        [(1, 2), (3, 4), (2, 3), (2, 5), (3, 5)],
        schema=["linked_trip_id", "linked_trip_id_b"],
        orient="row",
    )

    assigned, _ = clique_detection.detect_disjoint_cliques(pairs, pl.Series([1, 2, 3, 4, 5]))

    clique = dict(assigned.iter_rows())
    assert clique[2] == clique[3] == clique[5] is not None
    assert clique[1] is None
    assert clique[4] is None
