"""Tests for step-aware validation of Pydantic models.

This module tests the selective skip behavior of the pipeline, ensuring that
fields are only required in their designated pipeline steps.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError as PydanticValidationError

from data_canon.models.survey import UnlinkedTripModel
from data_canon.validation.row import (
    get_required_fields_for_step,
    validate_row_for_step,
)


class TestSelectiveFieldRequirements:
    """Test that fields are only required in specific steps."""

    def test_step_specific_fields_required_only_in_that_step(self):
        """Fields should only be required in their designated step."""
        # linked_trip_id is not required in link_trips, but is in extract_tours

        required_linking = get_required_fields_for_step(UnlinkedTripModel, "link_trips")
        assert "linked_trip_id" not in required_linking

        required_tours = get_required_fields_for_step(UnlinkedTripModel, "extract_tours")
        assert "linked_trip_id" in required_tours

    def test_fields_added_during_pipeline(self):
        """Fields added during pipeline only required after creation."""
        # depart_time/arrive_time are added in preprocessing step
        # Not required in load_data (before preprocessing)
        required_load = get_required_fields_for_step(UnlinkedTripModel, "load_data")
        assert "depart_time" not in required_load
        assert "arrive_time" not in required_load

        # Required in link_trips (after preprocessing)
        required_link = get_required_fields_for_step(UnlinkedTripModel, "link_trips")
        assert "depart_time" in required_link
        assert "arrive_time" in required_link

        # Also required in extract_tours (after link_trip)
        required_tours = get_required_fields_for_step(UnlinkedTripModel, "extract_tours")
        assert "depart_time" in required_tours
        assert "arrive_time" in required_tours


class TestStepValidationBehavior:
    """Test the actual validation behavior across steps."""

    def test_validation_passes_without_step_specific_fields_in_wrong_step(self):
        """Should allow missing step-specific fields in other steps."""
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 10101,
            # No linked_trip_id or tour_id - OK for preprocessing
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }

        # Should pass - we're not in extract_tours step
        validate_row_for_step(row, UnlinkedTripModel, "preprocessing")

    def test_validation_fails_without_step_specific_fields_in_right_step(self):
        """Should require step-specific fields in their designated step."""
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 10101,
            # Missing linked_trip_id and tour_id
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }

        # Should fail - we're in extract_tours step and need these fields
        with pytest.raises(ValueError, match="linked_trip_id"):
            validate_row_for_step(row, UnlinkedTripModel, "extract_tours")

    def test_validation_passes_with_all_required_fields_for_step(self):
        """Should pass when all step-required fields are present."""
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 10101,
            "linked_trip_id": 1,
            "tour_id": 1,
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
            "depart_time": datetime(2024, 1, 15, 10, 0, 0),
            "arrive_time": datetime(2024, 1, 15, 11, 30, 0),
        }

        # Should pass - all extract_tours fields present
        validate_row_for_step(row, UnlinkedTripModel, "extract_tours")

    def test_datetime_validation_selective_behavior(self):
        """Datetime fields should follow same selective pattern."""
        # Without datetime - OK for preprocessing
        row_no_dt = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 10101,
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_meters": 8000,
        }
        validate_row_for_step(row_no_dt, UnlinkedTripModel, "preprocessing")

        # Without datetime - Fails for link_trip
        with pytest.raises(ValueError, match=r"depart_time|arrive_time"):
            validate_row_for_step(row_no_dt, UnlinkedTripModel, "link_trips")

        # With datetime and location fields - OK for link_trip
        row_with_dt = row_no_dt.copy()
        row_with_dt["depart_time"] = datetime(2024, 1, 15, 10, 0, 0)
        row_with_dt["arrive_time"] = datetime(2024, 1, 15, 11, 30, 0)
        row_with_dt["o_lat"] = 37.7749
        row_with_dt["o_lon"] = -122.4194
        row_with_dt["d_lat"] = 37.7849
        row_with_dt["d_lon"] = -122.4094
        validate_row_for_step(row_with_dt, UnlinkedTripModel, "link_trips")

    def test_validates_present_fields_even_if_not_required_in_step(self):
        """Should validate type/constraints of present fields in any step."""
        # Include linked_trip_id in preprocessing step (not required there)
        # But with invalid value - should still fail validation
        row = {
            "trip_id": 1,
            "person_id": 101,
            "hh_id": 1,
            "day_id": 10101,
            "linked_trip_id": -5,  # Invalid: must be >= 1
            "depart_date": "2024-01-15",
            "depart_hour": 10,
            "depart_minute": 0,
            "depart_seconds": 0,
            "arrive_date": "2024-01-15",
            "arrive_hour": 11,
            "arrive_minute": 30,
            "arrive_seconds": 0,
            "o_purpose_category": 1,
            "d_purpose_category": 2,
            "mode_type": 1,
            "duration_minutes": 90.0,
            "distance_miles": 10.5,
        }

        # Should fail - linked_trip_id is present but invalid
        with pytest.raises(PydanticValidationError, match="greater than or equal"):
            validate_row_for_step(row, UnlinkedTripModel, "preprocessing")
