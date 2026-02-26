"""Tests for trip linking functionality."""

from datetime import datetime

import polars as pl

from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory
from processing.link_trips.link import (
    aggregate_linked_trips,
    link_trips,
)
from utils.create_ids import (
    create_concatenated_id,
    create_linked_trip_id,
    create_tour_ids,
)


class TestLinkTripsIntegration:
    """Integration tests for the complete link_trips function."""

    def test_end_to_end_linking(self):
        """Should link and aggregate trips end-to-end."""
        trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "day_id": [10001, 10001, 10001],
                "person_id": [100, 100, 100],
                "hh_id": [10, 10, 10],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 17, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                    datetime(2024, 1, 1, 17, 30),
                ],
                "travel_dow": [1, 1, 1],
                "depart_date": [datetime(2024, 1, 1)] * 3,
                "arrive_date": [datetime(2024, 1, 1)] * 3,
                "depart_hour": [8, 8, 17],
                "depart_minute": [0, 15, 0],
                "depart_seconds": [0, 0, 0],
                "arrive_hour": [8, 8, 17],
                "arrive_minute": [10, 45, 30],
                "arrive_seconds": [0, 0, 0],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.CHANGE_MODE.value,
                    PurposeCategory.WORK.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.CHANGE_MODE.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.MODE_CHANGE.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                ],
                "d_purpose": [
                    Purpose.MODE_CHANGE.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                ],
                "o_lat": [37.7, 37.71, 37.75],
                "o_lon": [-122.4, -122.41, -122.45],
                "d_lat": [37.71, 37.75, 37.7],
                "d_lon": [-122.41, -122.45, -122.4],
                "mode_type": [
                    ModeType.WALK.value,
                    ModeType.TRANSIT.value,
                    ModeType.TRANSIT.value,
                ],
                "distance_meters": [804.67, 8046.7, 8046.7],
                "num_travelers": [1, 1, 1],
                "driver": [0, 0, 0],
                "duration_minutes": [10.0, 30.0, 30.0],
                "unlinked_trip_weight": [1.0, 1.0, 1.0],
            }
        )

        result = link_trips(
            trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[ModeType.TRANSIT.value],
            max_dwell_time=120,
            dwell_buffer_distance=100,
        )

        # Should return dict with two DataFrames
        assert "unlinked_trips" in result
        assert "linked_trips" in result

        # Original trips with linked_trip_id
        trips_with_ids = result["unlinked_trips"]
        assert len(trips_with_ids) == 3
        assert "linked_trip_id" in trips_with_ids.columns

        # Aggregated linked trips
        linked_trips = result["linked_trips"]
        assert len(linked_trips) == 2  # Two linked trips

    def test_preserves_all_required_columns(self):
        """Should preserve all required columns in output."""
        trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1],
                "day_id": [10001],
                "person_id": [100],
                "hh_id": [10],
                "depart_time": [datetime(2024, 1, 1, 8, 0)],
                "arrive_time": [datetime(2024, 1, 1, 8, 30)],
                "depart_date": [datetime(2024, 1, 1)],
                "arrive_date": [datetime(2024, 1, 1)],
                "travel_dow": [1],
                "depart_hour": [8],
                "depart_minute": [0],
                "depart_seconds": [0],
                "arrive_hour": [8],
                "arrive_minute": [30],
                "arrive_seconds": [0],
                "o_purpose_category": [PurposeCategory.HOME.value],
                "d_purpose_category": [PurposeCategory.WORK.value],
                "o_purpose": [Purpose.HOME.value],
                "d_purpose": [Purpose.GROCERY.value],
                "o_lat": [37.7],
                "o_lon": [-122.4],
                "d_lat": [37.75],
                "d_lon": [-122.45],
                "mode_type": [ModeType.WALK.value],
                "distance_meters": [804.67],
                "num_travelers": [1],
                "driver": [0],
                "duration_minutes": [30.0],
                "unlinked_trip_weight": [1.0],
            }
        )

        result = link_trips(
            trips,
            change_mode_enum=10,
            transit_mode_enums=[ModeType.TRANSIT.value],
            max_dwell_time=120,
        )

        linked_trips = result["linked_trips"]

        # Check all expected columns are present
        expected_columns = [
            "linked_trip_id",
            "person_id",
            "hh_id",
            "day_id",
            "depart_time",
            "arrive_time",
            "o_purpose_category",
            "d_purpose_category",
            "mode_type",
            "distance_meters",
            "duration_minutes",
            "num_segments",
        ]

        for col in expected_columns:
            assert col in linked_trips.columns


class TestIDCreation:
    """Tests for ID creation functions."""

    def test_multiple_trips_same_day(self):
        """Should handle multiple trips with same day_id correctly."""
        # Multiple trips sharing the same day_id is normal in trip table
        trips = pl.DataFrame(
            {
                "day_id": [10001, 10001, 10001],  # Same day_id for all
                "linked_trip_num": [1, 2, 3],
            }
        )

        result = create_linked_trip_id(trips)

        # Should create unique IDs by combining day_id + trip_num
        assert "linked_trip_id" in result.columns
        assert len(result) == 3
        assert result["linked_trip_id"].n_unique() == 3
        assert result["linked_trip_id"].to_list() == [
            1000101,
            1000102,
            1000103,
        ]

    def test_concatenated_id_basic(self):
        """Should create IDs by concatenating parent + padded sequence."""
        trips = pl.DataFrame(
            {
                "day_id": [10001, 10002, 10003],
                "linked_trip_num": [1, 1, 1],
            }
        )

        result = create_concatenated_id(
            trips,
            output_col="linked_trip_id",
            parent_id_col="day_id",
            sequence_col="linked_trip_num",
        )

        # Should create IDs successfully
        assert "linked_trip_id" in result.columns
        assert len(result) == 3
        assert result["linked_trip_id"].to_list() == [
            1000101,
            1000201,
            1000301,
        ]

    def test_tour_id_with_duplicate_day_id(self):
        """Should handle multiple tours on same day."""
        # Tour with subtours on same day is valid
        trips = pl.DataFrame(
            {
                "day_id": [10001, 10001, 10001],  # Same day
                "linked_trip_id": [1, 2, 3],
                "tour_num": [1, 1, 1],
                "subtour_num": [0, 1, 0],
            }
        )

        result = create_tour_ids(trips)

        # Should create unique tour_ids
        assert "tour_id" in result.columns
        assert len(result) == 3
        assert result["tour_id"].n_unique() == 2

    def test_linked_trip_id_allows_duplicates(self):
        """linked_trip_id can be duplicated in unlinked trips table."""
        # In unlinked trips table, multiple rows can share same linked_trip_id
        # because they're segments of the same linked trip
        trips = pl.DataFrame(
            {
                "day_id": [10001, 10001, 10002],
                "linked_trip_num": [1, 1, 1],
            }
        )

        result = create_linked_trip_id(trips)

        # Should succeed - validation happens at table level, not ID creation
        assert "linked_trip_id" in result.columns
        assert len(result) == 3
        # Two trips have same ID (10001 + 1), one has different (10002 + 1)
        assert result["linked_trip_id"].n_unique() == 2


class TestTableLevelUniqueness:
    """Tests that validate ID uniqueness at the table level."""

    def test_linked_trips_table_has_unique_ids(self):
        """Linked_trips table must have unique linked_trip_id."""
        # Create unlinked trips with some linked segments
        unlinked_trips = pl.DataFrame(
            {
                "linked_trip_id": ["1000101", "1000101", "1000102", "1000201"],
                "linked_trip_num": [1, 1, 2, 1],
                "person_id": [100, 100, 100, 200],
                "hh_id": [10, 10, 10, 20],
                "day_id": [10001, 10001, 10001, 20001],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 17, 0),
                    datetime(2024, 1, 1, 8, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                    datetime(2024, 1, 1, 17, 30),
                    datetime(2024, 1, 1, 8, 30),
                ],
                "travel_dow": [1, 1, 1, 1],
                "depart_date": [datetime(2024, 1, 1)] * 4,
                "arrive_date": [datetime(2024, 1, 1)] * 4,
                "depart_hour": [8, 8, 17, 8],
                "depart_minute": [0, 15, 0, 0],
                "depart_seconds": [0, 0, 0, 0],
                "arrive_hour": [8, 8, 17, 8],
                "arrive_minute": [10, 45, 30, 30],
                "arrive_seconds": [0, 0, 0, 0],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.CHANGE_MODE.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.CHANGE_MODE.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.WORK.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.MODE_CHANGE.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                ],
                "d_purpose": [
                    Purpose.MODE_CHANGE.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                ],
                "o_lat": [37.7, 37.71, 37.75, 37.7],
                "o_lon": [-122.4, -122.41, -122.45, -122.4],
                "d_lat": [37.71, 37.75, 37.7, 37.75],
                "d_lon": [-122.41, -122.45, -122.4, -122.45],
                "mode_type": [
                    ModeType.WALK.value,
                    ModeType.TRANSIT.value,
                    ModeType.TRANSIT.value,
                    ModeType.WALK.value,
                ],
                "distance_meters": [804.67, 8046.7, 8046.7, 804.67],
                "num_travelers": [1, 1, 1, 1],
                "driver": [0, 0, 0, 0],
                "duration_minutes": [10.0, 30.0, 30.0, 30.0],
                "unlinked_trip_weight": [1.0, 1.0, 1.0, 1.0],
            }
        )

        # Aggregate into linked trips table
        linked_trips = aggregate_linked_trips(
            unlinked_trips, transit_mode_enums=[ModeType.TRANSIT.value]
        )

        # CRITICAL: linked_trips table MUST have unique linked_trip_ids
        assert linked_trips["linked_trip_id"].n_unique() == len(linked_trips), (
            "linked_trips table must have unique linked_trip_id values"
        )

        # Should have 3 unique linked trips
        assert len(linked_trips) == 3
        assert set(linked_trips["linked_trip_id"].to_list()) == {
            "1000101",
            "1000102",
            "1000201",
        }

    def test_aggregation_enforces_uniqueness(self):
        """Aggregation by linked_trip_id naturally enforces uniqueness."""
        # Multiple segments with same linked_trip_id
        unlinked_trips = pl.DataFrame(
            {
                "linked_trip_id": ["ABC", "ABC", "ABC", "DEF"],
                "linked_trip_num": [1, 1, 1, 2],
                "person_id": [100, 100, 100, 100],
                "hh_id": [10, 10, 10, 10],
                "day_id": [10001, 10001, 10001, 10001],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 20),
                    datetime(2024, 1, 1, 17, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 5),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 8, 45),
                    datetime(2024, 1, 1, 17, 30),
                ],
                "travel_dow": [1, 1, 1, 1],
                "depart_date": [datetime(2024, 1, 1)] * 4,
                "arrive_date": [datetime(2024, 1, 1)] * 4,
                "depart_hour": [8, 8, 8, 17],
                "depart_minute": [0, 10, 20, 0],
                "depart_seconds": [0, 0, 0, 0],
                "arrive_hour": [8, 8, 8, 17],
                "arrive_minute": [5, 15, 45, 30],
                "arrive_seconds": [0, 0, 0, 0],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.ERRAND.value,
                    PurposeCategory.ERRAND.value,
                    PurposeCategory.WORK.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.ERRAND.value,
                    PurposeCategory.ERRAND.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.ERRAND_NO_APPT.value,
                    Purpose.ERRAND_NO_APPT.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                ],
                "d_purpose": [
                    Purpose.ERRAND_NO_APPT.value,
                    Purpose.ERRAND_NO_APPT.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                ],
                "o_lat": [37.7, 37.71, 37.72, 37.75],
                "o_lon": [-122.4, -122.41, -122.42, -122.45],
                "d_lat": [37.71, 37.72, 37.75, 37.7],
                "d_lon": [-122.41, -122.42, -122.45, -122.4],
                "mode_type": [
                    ModeType.WALK.value,
                    ModeType.TRANSIT.value,
                    ModeType.WALK.value,
                    ModeType.TRANSIT.value,
                ],
                "distance_meters": [804.67, 1000.0, 8046.7, 8046.7],
                "num_travelers": [1, 1, 1, 1],
                "driver": [0, 0, 0, 0],
                "duration_minutes": [5.0, 5.0, 25.0, 30.0],
                "unlinked_trip_weight": [1.0, 1.0, 1.0, 1.0],
            }
        )

        linked_trips = aggregate_linked_trips(
            unlinked_trips, transit_mode_enums=[ModeType.TRANSIT.value]
        )

        # 4 unlinked segments become 2 linked trips
        assert len(unlinked_trips) == 4
        assert len(linked_trips) == 2

        # Each linked_trip_id appears exactly once in linked_trips table
        assert linked_trips["linked_trip_id"].n_unique() == 2
        assert linked_trips["linked_trip_id"].n_unique() == len(linked_trips)

    def test_end_to_end_uniqueness_validation(self):
        """Full pipeline maintains proper uniqueness constraints."""
        trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3, 4, 5],
                "day_id": [10001, 10001, 10001, 10002, 10002],
                "person_id": [100, 100, 100, 100, 100],
                "hh_id": [10, 10, 10, 10, 10],
                "depart_time": [
                    datetime(2024, 1, 1, 8, 0),
                    datetime(2024, 1, 1, 8, 15),
                    datetime(2024, 1, 1, 17, 0),
                    datetime(2024, 1, 2, 8, 0),
                    datetime(2024, 1, 2, 17, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 1, 8, 10),
                    datetime(2024, 1, 1, 8, 45),
                    datetime(2024, 1, 1, 17, 30),
                    datetime(2024, 1, 2, 8, 30),
                    datetime(2024, 1, 2, 17, 30),
                ],
                "travel_dow": [1, 1, 1, 2, 2],
                "depart_date": [
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 2),
                    datetime(2024, 1, 2),
                ],
                "arrive_date": [
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 2),
                    datetime(2024, 1, 2),
                ],
                "depart_hour": [8, 8, 17, 8, 17],
                "depart_minute": [0, 15, 0, 0, 0],
                "depart_seconds": [0, 0, 0, 0, 0],
                "arrive_hour": [8, 8, 17, 8, 17],
                "arrive_minute": [10, 45, 30, 30, 30],
                "arrive_seconds": [0, 0, 0, 0, 0],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.CHANGE_MODE.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.WORK.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.CHANGE_MODE.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.MODE_CHANGE.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                ],
                "d_purpose": [
                    Purpose.MODE_CHANGE.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                ],
                "o_lat": [37.7, 37.71, 37.75, 37.7, 37.75],
                "o_lon": [-122.4, -122.41, -122.45, -122.4, -122.45],
                "d_lat": [37.71, 37.75, 37.7, 37.75, 37.7],
                "d_lon": [-122.41, -122.45, -122.4, -122.45, -122.4],
                "mode_type": [
                    ModeType.WALK.value,
                    ModeType.TRANSIT.value,
                    ModeType.TRANSIT.value,
                    ModeType.WALK.value,
                    ModeType.TRANSIT.value,
                ],
                "distance_meters": [804.67, 8046.7, 8046.7, 804.67, 8046.7],
                "num_travelers": [1, 1, 1, 1, 1],
                "driver": [0, 0, 0, 0, 0],
                "duration_minutes": [10.0, 30.0, 30.0, 30.0, 30.0],
                "unlinked_trip_weight": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )

        result = link_trips(
            trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[ModeType.TRANSIT.value],
            max_dwell_time=120,
        )

        unlinked_trips = result["unlinked_trips"]
        linked_trips = result["linked_trips"]

        # Unlinked trips: linked_trip_id can be duplicated
        assert len(unlinked_trips) == 5
        # Trips 1&2 linked, trip 3 standalone, trips 4&5 separate
        assert unlinked_trips["linked_trip_id"].n_unique() < len(unlinked_trips)

        # Linked trips: linked_trip_id MUST be unique
        assert linked_trips["linked_trip_id"].n_unique() == len(linked_trips)

        # Each linked_trip_id should appear exactly once
        for unlinked_trip_id in linked_trips["linked_trip_id"]:
            count = (linked_trips["linked_trip_id"] == unlinked_trip_id).sum()
            assert count == 1, (
                f"linked_trip_id {unlinked_trip_id} appears {count} times, should be 1"
            )
