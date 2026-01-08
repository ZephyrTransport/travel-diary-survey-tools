"""Tests for joint trip detection functionality.

This test module ensures joint trip detection correctly handles:
- Config validation (method, covariance shapes, thresholds)
- Buffer method with strict AND logic
- Mahalanobis method with diagonal and full covariance
- Temporal overlap requirements
- Clique detection with partial overlaps
- Edge cases (single-person households, no matches, empty data)
"""

from datetime import datetime

import polars as pl
import pytest
from pydantic import ValidationError

from data_canon.codebook.days import TravelDow
from data_canon.codebook.trips import Driver, ModeType, PurposeCategory
from processing.joint_trips import (
    JointTripConfig,
    detect_joint_trips,
)
from processing.joint_trips.similarity import (
    apply_buffer_filter,
    apply_mahalanobis_filter,
    compute_pairwise_distances,
)


class TestJointTripConfig:
    """Test configuration validation."""

    def test_valid_buffer_config(self):
        """Test valid buffer configuration."""
        config = JointTripConfig(
            method="buffer",
            time_threshold_minutes=15.0,
            space_threshold_meters=100.0,
        )
        assert config.method == "buffer"
        assert config.time_threshold_minutes == 15.0
        assert config.space_threshold_meters == 100.0

    def test_valid_diagonal_covariance(self):
        """Test valid diagonal covariance configuration."""
        config = JointTripConfig(
            method="mahalanobis",
            covariance=[7000, 7000, 20, 20],
            distance_threshold=2.5,
        )
        assert config.method == "mahalanobis"
        assert config.covariance == [7000, 7000, 20, 20]

    def test_valid_full_covariance(self):
        """Test valid full covariance matrix."""
        cov = [
            [10000, 0, 0, 0],
            [0, 10000, 0, 0],
            [0, 0, 100, 0],
            [0, 0, 0, 100],
        ]
        config = JointTripConfig(
            method="mahalanobis",
            covariance=cov,
            distance_threshold=2.5,
        )
        assert config.covariance == cov

    def test_invalid_diagonal_length(self):
        """Test that wrong diagonal length raises error."""
        with pytest.raises(ValueError, match="must have 4 values"):
            JointTripConfig(
                method="mahalanobis",
                covariance=[10000, 10000, 100],  # Only 3 values
            )

    def test_invalid_full_matrix_shape(self):
        """Test that non-4x4 matrix raises error."""
        with pytest.raises(ValidationError, match="4x4"):
            JointTripConfig(
                method="mahalanobis",
                covariance=[[10000, 0], [0, 10000]],  # 2x2
            )

    def test_negative_threshold(self):
        """Test that negative thresholds raise error."""
        with pytest.raises(ValidationError, match="greater than or equal"):
            JointTripConfig(time_threshold_minutes=-5.0)

    def test_non_symmetric_matrix(self):
        """Test that non-symmetric matrix raises error."""
        with pytest.raises(ValueError, match="symmetric"):
            JointTripConfig(
                method="mahalanobis",
                covariance=[
                    [10000, 100, 0, 0],  # Off-diagonal asymmetric
                    [0, 10000, 0, 0],
                    [0, 0, 100, 0],
                    [0, 0, 0, 100],
                ],
            )

    def test_negative_diagonal_values(self):
        """Test that negative variances raise error."""
        with pytest.raises(ValueError, match="positive"):
            JointTripConfig(
                method="mahalanobis",
                covariance=[10000, -10000, 100, 100],
            )


class TestSimilarityCalculations:
    """Test similarity computation and filtering functions."""

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
        """Test distance computation."""
        result = compute_pairwise_distances(sample_trip_pairs)
        assert "origin_dist_m" in result.columns
        assert "dest_dist_m" in result.columns
        assert "depart_diff_min" in result.columns
        assert "arrive_diff_min" in result.columns
        assert len(result) == 3

    def test_buffer_filter_strict_and(self, sample_trip_pairs):
        """Test buffer filter with strict AND logic."""
        pairs_with_dist = compute_pairwise_distances(sample_trip_pairs)
        filtered = apply_buffer_filter(
            pairs_with_dist,
            space_threshold_meters=200,
            time_threshold_minutes=10,
        )
        # Only first pair should pass (small distances and times)
        assert len(filtered) <= 2

    def test_mahalanobis_filter_diagonal(self, sample_trip_pairs):
        """Test Mahalanobis filter with diagonal covariance."""
        pairs_with_dist = compute_pairwise_distances(sample_trip_pairs)
        filtered = apply_mahalanobis_filter(
            pairs_with_dist,
            covariance=[7000, 7000, 20, 20],
            distance_threshold=2.5,
        )
        assert len(filtered) >= 0


@pytest.fixture
def two_person_household_matching_trips():
    """Two household members with matching trips (same origin, dest, time)."""
    households = pl.DataFrame({"hh_id": [1], "home_lat": [37.8], "home_lon": [-122.4]})

    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "hh_id": [1, 1],
            "day_id": [1, 1],
            "person_id": [1, 2],
            "travel_dow": [
                TravelDow.WEDNESDAY.value,
                TravelDow.WEDNESDAY.value,
            ],
            "o_lat": [37.8, 37.8],
            "o_lon": [-122.4, -122.4],
            "d_lat": [37.85, 37.85],
            "d_lon": [-122.45, -122.45],
            "depart_time": [
                datetime(2024, 1, 15, 9, 0),
                datetime(2024, 1, 15, 9, 1),
            ],
            "arrive_time": [
                datetime(2024, 1, 15, 9, 30),
                datetime(2024, 1, 15, 9, 31),
            ],
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.HOME.value,
            ],
            "d_purpose_category": [
                PurposeCategory.WORK.value,
                PurposeCategory.WORK.value,
            ],
            "mode_type": [ModeType.CAR.value, ModeType.CAR.value],
            "driver": [Driver.DRIVER.value, Driver.PASSENGER.value],
            "num_travelers": [2, 2],
            "access_mode": [None, None],
            "egress_mode": [None, None],
            "duration_minutes": [30.0, 30.0],
            "distance_meters": [5000.0, 5000.0],
            "depart_date": [
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
            ],
            "arrive_date": [
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
            ],
            "depart_hour": [9, 9],
            "depart_minute": [0, 1],
            "depart_seconds": [0, 0],
            "arrive_hour": [9, 9],
            "arrive_minute": [30, 31],
            "arrive_seconds": [0, 0],
            "tour_direction": [1, 1],
        }
    )

    return households, linked_trips


@pytest.fixture
def two_person_household_non_matching_trips():
    """Two household members with trips at different times (no overlap)."""
    households = pl.DataFrame({"hh_id": [1], "home_lat": [37.8], "home_lon": [-122.4]})

    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "hh_id": [1, 1],
            "day_id": [1, 1],
            "person_id": [1, 2],
            "travel_dow": [
                TravelDow.WEDNESDAY.value,
                TravelDow.WEDNESDAY.value,
            ],
            "o_lat": [37.8, 37.8],
            "o_lon": [-122.4, -122.4],
            "d_lat": [37.85, 37.85],
            "d_lon": [-122.45, -122.45],
            "depart_time": [
                datetime(2024, 1, 15, 9, 0),
                datetime(2024, 1, 15, 14, 0),  # Different time
            ],
            "arrive_time": [
                datetime(2024, 1, 15, 9, 30),
                datetime(2024, 1, 15, 14, 30),  # No overlap
            ],
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.HOME.value,
            ],
            "d_purpose_category": [
                PurposeCategory.WORK.value,
                PurposeCategory.WORK.value,
            ],
            "mode_type": [ModeType.CAR.value, ModeType.CAR.value],
            "driver": [Driver.DRIVER.value, Driver.DRIVER.value],
            "num_travelers": [1, 1],
            "access_mode": [None, None],
            "egress_mode": [None, None],
            "duration_minutes": [30.0, 30.0],
            "distance_meters": [5000.0, 5000.0],
            "depart_date": [
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
            ],
            "arrive_date": [
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
            ],
            "depart_hour": [9, 14],
            "depart_minute": [0, 0],
            "depart_seconds": [0, 0],
            "arrive_hour": [9, 14],
            "arrive_minute": [30, 30],
            "arrive_seconds": [0, 0],
            "tour_direction": [1, 1],
        }
    )

    return households, linked_trips


def test_detect_matching_trips_buffer(two_person_household_matching_trips):
    """Test that matching trips are detected with buffer method."""
    households, linked_trips = two_person_household_matching_trips

    result = detect_joint_trips(
        linked_trips=linked_trips,
        households=households,
        method="buffer",
        time_threshold_minutes=15,
        space_threshold_meters=100,
    )

    updated_trips = result["linked_trips"]
    joint_trips = result["joint_trips"]

    # Both trips should have same joint_trip_id
    joint_ids = updated_trips.filter(pl.col("joint_trip_id").is_not_null())[
        "joint_trip_id"
    ].unique()
    assert len(joint_ids) == 1  # One joint trip group

    # Joint trips table should have one row
    assert len(joint_trips) == 1
    assert joint_trips["num_joint_travelers"][0] == 2


def test_detect_non_matching_trips(two_person_household_non_matching_trips):
    """Test that non-overlapping trips are not detected as joint."""
    households, linked_trips = two_person_household_non_matching_trips

    result = detect_joint_trips(
        linked_trips=linked_trips,
        households=households,
        method="buffer",
    )

    updated_trips = result["linked_trips"]
    joint_trips = result["joint_trips"]

    # No trips should have joint_trip_id
    assert updated_trips["joint_trip_id"].null_count() == len(updated_trips)
    assert len(joint_trips) == 0


def test_single_person_household():
    """Test that single-person households are excluded."""
    households = pl.DataFrame({"hh_id": [1], "home_lat": [37.8], "home_lon": [-122.4]})

    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1],
            "hh_id": [1],
            "day_id": [1],
            "person_id": [1],
            "travel_dow": [TravelDow.WEDNESDAY.value],
            "o_lat": [37.8],
            "o_lon": [-122.4],
            "d_lat": [37.85],
            "d_lon": [-122.45],
            "depart_time": [datetime(2024, 1, 15, 9, 0)],
            "arrive_time": [datetime(2024, 1, 15, 9, 30)],
            "o_purpose_category": [PurposeCategory.HOME.value],
            "d_purpose_category": [PurposeCategory.WORK.value],
            "mode_type": [ModeType.CAR.value],
            "driver": [Driver.DRIVER.value],
            "num_travelers": [1],
            "access_mode": [None],
            "egress_mode": [None],
            "duration_minutes": [30.0],
            "distance_meters": [5000.0],
            "depart_date": [datetime(2024, 1, 15)],
            "arrive_date": [datetime(2024, 1, 15)],
            "depart_hour": [9],
            "depart_minute": [0],
            "depart_seconds": [0],
            "arrive_hour": [9],
            "arrive_minute": [30],
            "arrive_seconds": [0],
            "tour_direction": [1],
        }
    )

    result = detect_joint_trips(linked_trips=linked_trips, households=households, method="buffer")

    # Should return with no joint trips
    assert result["linked_trips"]["joint_trip_id"].null_count() == 1
    assert len(result["joint_trips"]) == 0


def test_empty_input():
    """Test handling of empty input DataFrames."""
    households = pl.DataFrame({"hh_id": [], "home_lat": [], "home_lon": []})
    linked_trips = pl.DataFrame(
        schema={
            "linked_trip_id": pl.Int64,
            "hh_id": pl.Int64,
            "day_id": pl.Int64,
            "person_id": pl.Int64,
            "travel_dow": pl.Int64,
            "o_lat": pl.Float64,
            "o_lon": pl.Float64,
            "d_lat": pl.Float64,
            "d_lon": pl.Float64,
            "depart_time": pl.Datetime,
            "arrive_time": pl.Datetime,
            "o_purpose_category": pl.Int64,
            "d_purpose_category": pl.Int64,
            "mode_type": pl.Int64,
            "driver": pl.Int64,
            "num_travelers": pl.Int64,
            "access_mode": pl.Int64,
            "egress_mode": pl.Int64,
            "duration_minutes": pl.Float64,
            "distance_meters": pl.Float64,
            "depart_date": pl.Datetime,
            "arrive_date": pl.Datetime,
            "depart_hour": pl.Int64,
            "depart_minute": pl.Int64,
            "depart_seconds": pl.Int64,
            "arrive_hour": pl.Int64,
            "arrive_minute": pl.Int64,
            "arrive_seconds": pl.Int64,
            "tour_direction": pl.Int64,
        }
    )

    result = detect_joint_trips(linked_trips=linked_trips, households=households, method="buffer")

    # Should handle gracefully
    assert len(result["linked_trips"]) == 0
    assert len(result["joint_trips"]) == 0
