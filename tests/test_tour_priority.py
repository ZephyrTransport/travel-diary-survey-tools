"""Tests for tour priority calculation utilities."""

import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import PersonCategory
from data_canon.codebook.trips import ModeType, PurposeCategory
from processing.tours.priority_utils import (
    add_activity_duration_column,
    add_mode_priority_column,
    add_purpose_priority_column,
)
from processing.tours.tour_configs import TourConfig


@pytest.fixture
def default_config():
    """Get default tour configuration."""
    return TourConfig()


class TestPersonCategory:
    """Test PersonCategory constants."""

    def test_worker_constant(self):
        """Test that WORKER constant has expected value."""
        assert PersonCategory.WORKER == "worker"

    def test_student_constant(self):
        """Test that STUDENT constant has expected value."""
        assert PersonCategory.STUDENT == "student"

    def test_other_constant(self):
        """Test that OTHER constant has expected value."""
        assert PersonCategory.OTHER == "other"


class TestAddPurposePriorityColumn:
    """Test add_purpose_priority_column function."""

    def test_adds_priority_column(self, default_config):
        """Test that priority column is added correctly."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER] * 3,
                "d_purpose_category": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                ],
            }
        )

        result = add_purpose_priority_column(df, default_config)

        assert "purpose_priority" in result.columns
        # HOME should have priority 999
        assert (
            result.filter(pl.col("d_purpose_category") == PurposeCategory.HOME.value)[
                "purpose_priority"
            ][0]
            == 999
        )

    def test_custom_alias(self, default_config):
        """Test that custom column alias works."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER],
                "d_purpose_category": [PurposeCategory.WORK.value],
            }
        )

        result = add_purpose_priority_column(df, default_config, alias="custom_priority")

        assert "custom_priority" in result.columns
        assert "purpose_priority" not in result.columns


class TestAddModePriorityColumn:
    """Test add_mode_priority_column function."""

    def test_adds_mode_priority_column(self):
        """Test that mode priority column is added."""
        mode_hierarchy = [ModeType.WALK, ModeType.BIKE, ModeType.CAR, ModeType.TRANSIT]

        df = pl.DataFrame(
            {
                "mode_type": [
                    ModeType.TRANSIT.value,
                    ModeType.CAR.value,
                    ModeType.WALK.value,
                ],
            }
        )

        result = add_mode_priority_column(df, mode_hierarchy)

        assert "mode_priority" in result.columns
        assert len(result) == 3
        # Transit should have highest priority (last in list)
        transit_priority = result.filter(pl.col("mode_type") == ModeType.TRANSIT.value)[
            "mode_priority"
        ][0]
        walk_priority = result.filter(pl.col("mode_type") == ModeType.WALK.value)["mode_priority"][
            0
        ]
        assert transit_priority > walk_priority

    def test_custom_mode_alias(self):
        """Test custom alias for mode priority."""
        mode_hierarchy = [ModeType.WALK, ModeType.CAR]

        df = pl.DataFrame(
            {
                "mode_type": [ModeType.CAR.value],
            }
        )

        result = add_mode_priority_column(df, mode_hierarchy, alias="custom_mode")

        assert "custom_mode" in result.columns
        assert "mode_priority" not in result.columns


class TestAddActivityDurationColumn:
    """Test add_activity_duration_column function."""

    def test_adds_duration_column(self):
        """Test that activity duration column is calculated correctly."""
        df = pl.DataFrame(
            {
                "person_id": [1, 1, 1],
                "day_id": [1, 1, 1],
                "arrive_time": [
                    datetime.datetime(2023, 1, 1, 8, 0),
                    datetime.datetime(2023, 1, 1, 12, 0),
                    datetime.datetime(2023, 1, 1, 17, 0),
                ],
                "depart_time": [
                    datetime.datetime(2023, 1, 1, 8, 30),
                    datetime.datetime(2023, 1, 1, 13, 0),
                    datetime.datetime(2023, 1, 1, 18, 0),
                ],
            }
        )

        result = add_activity_duration_column(df)

        assert "activity_duration" in result.columns
        # Activity duration = next_trip.depart_time - current_trip.arrive_time
        # Trip 0: arrive 08:00, next departs 13:00 → 13:00 - 08:00 = 300 min
        # Trip 1: arrive 12:00, next departs 18:00 → 18:00 - 12:00 = 360 min
        # Trip 2: last trip, uses default = 240 min
        assert result["activity_duration"][0] == 300.0
        assert result["activity_duration"][1] == 360.0
        assert result["activity_duration"][2] == 240.0

    def test_custom_default_duration(self):
        """Test custom default duration for last trip."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "day_id": [1],
                "arrive_time": [datetime.datetime(2023, 1, 1, 8, 0)],
                "depart_time": [datetime.datetime(2023, 1, 1, 9, 0)],
            }
        )

        result = add_activity_duration_column(df, default_minutes=120.0)

        # Should use custom default
        assert result["activity_duration"][0] == 120.0

    def test_custom_alias(self):
        """Test custom column alias."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "day_id": [1],
                "arrive_time": [datetime.datetime(2023, 1, 1, 8, 0)],
                "depart_time": [datetime.datetime(2023, 1, 1, 9, 0)],
            }
        )

        result = add_activity_duration_column(df, alias="custom_duration")

        assert "custom_duration" in result.columns
        assert "activity_duration" not in result.columns
