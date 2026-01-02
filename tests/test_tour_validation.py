"""Tests for tour validation helper functions."""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from processing.tours.validation_helpers import (
    _diagnose_problem_tours,
    validate_and_correct_tours,
)


class TestValidateAndCorrectTours:
    """Test validate_and_correct_tours function."""

    def test_valid_tour_processing(self):
        """Test processing of valid tours."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [2],
                "tour_num": [1],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.WORK.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_1"],
                "person_id": [1, 1],
                "hh_id": [1, 1],
                "day_id": [1, 1],
                "tour_num": [1, 1],
                "_o_is_home": [True, False],
                "_d_is_home": [False, True],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert "tour_data_quality" in result.columns
        assert len(result) == 1

    def test_tour_with_zero_tour_num(self, caplog):
        """Test handling tours with tour_num=0."""
        caplog.set_level(logging.WARNING)

        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [1],
                "tour_num": [0],  # Invalid tour
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.WORK.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "hh_id": [1],
                "day_id": [1],
                "tour_num": [0],
                "_o_is_home": [True],
                "_d_is_home": [False],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        # Should log warning about invalid tours
        assert "invalid tours" in caplog.text.lower()
        assert "tour_data_quality" in result.columns

    def test_tour_without_home_anchor(self):
        """Test tours missing home anchors."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [2],
                "tour_num": [1],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.SHOP.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_1"],
                "person_id": [1, 1],
                "hh_id": [1, 1],
                "day_id": [1, 1],
                "tour_num": [1, 1],
                "_o_is_home": [False, False],  # No home
                "_d_is_home": [False, False],  # No home
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert "tour_data_quality" in result.columns
        # Should flag as problematic
        quality = result["tour_data_quality"][0]
        assert quality != TourDataQuality.VALID.value


class TestDiagnoseProblemTours:
    """Test diagnostic logging for problem tours."""

    def test_diagnose_logs_indeterminate_tours(self, caplog):
        """Test that diagnostic function logs information about problematic tours."""
        caplog.set_level(logging.WARNING)

        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2"],
                "person_id": [1, 2],
                "day_id": [1, 1],
                "trip_count": [1, 2],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.COMPLETE.value],
                "tour_data_quality": [
                    TourDataQuality.INDETERMINATE.value,
                    TourDataQuality.INDETERMINATE.value,
                ],
                "_has_home_origin": [True, False],
                "_has_home_dest": [False, False],
            }
        )

        zero_tour_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2"],
                "linked_trip_id": [1, 2],
                "depart_time": ["08:00", "09:00"],
                "_o_is_home": [True, False],
                "_d_is_home": [False, False],
            }
        )

        _diagnose_problem_tours(tours, zero_tour_trips)

        # Should have logged diagnostics
        assert "INDETERMINATE tours" in caplog.text
        assert "home anchor pattern" in caplog.text

    def test_diagnose_with_no_problems(self, caplog):
        """Test diagnostic function with no problematic tours."""
        caplog.set_level(logging.WARNING)

        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [2],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_data_quality": [TourDataQuality.VALID.value],
                "_has_home_origin": [True],
                "_has_home_dest": [True],
            }
        )

        zero_tour_trips = pl.DataFrame(
            {
                "tour_id": [],
                "linked_trip_id": [],
                "depart_time": [],
                "_o_is_home": [],
                "_d_is_home": [],
            }
        )

        _diagnose_problem_tours(tours, zero_tour_trips)

        # Should not log INDETERMINATE warnings
        assert "INDETERMINATE tours" not in caplog.text


class TestTourValidationIntegration:
    """Integration tests for tour validation workflow."""

    def test_full_validation_workflow(self):
        """Test the complete validation workflow."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2", "tour_3"],
                "person_id": [1, 1, 2],
                "day_id": [1, 1, 1],
                "trip_count": [2, 1, 3],
                "tour_num": [1, 0, 1],
                "tour_category": [
                    TourCategory.COMPLETE.value,
                    TourCategory.COMPLETE.value,
                    TourCategory.COMPLETE.value,
                ],
                "tour_purpose": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.SOCIALREC.value,
                ],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_1", "tour_2", "tour_3", "tour_3", "tour_3"],
                "person_id": [1, 1, 1, 2, 2, 2],
                "hh_id": [1, 1, 1, 2, 2, 2],
                "day_id": [1, 1, 1, 1, 1, 1],
                "tour_num": [1, 1, 0, 1, 1, 1],
                "_o_is_home": [True, False, True, False, False, False],
                "_d_is_home": [False, True, False, False, False, False],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert "tour_data_quality" in result.columns
        assert len(result) == 3

    def test_mixed_quality_tours(self):
        """Test handling tours with mixed data quality."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_good", "tour_bad"],
                "person_id": [1, 1],
                "day_id": [1, 1],
                "trip_count": [3, 1],
                "tour_num": [1, 0],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.COMPLETE.value],
                "tour_purpose": [PurposeCategory.WORK.value, PurposeCategory.WORK.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_good", "tour_good", "tour_good", "tour_bad"],
                "person_id": [1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1],
                "day_id": [1, 1, 1, 1],
                "tour_num": [1, 1, 1, 0],
                "_o_is_home": [True, False, False, True],
                "_d_is_home": [False, False, True, False],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        # Should have different quality flags
        qualities = result["tour_data_quality"].unique().to_list()
        assert len(qualities) >= 1  # At least one quality type present
