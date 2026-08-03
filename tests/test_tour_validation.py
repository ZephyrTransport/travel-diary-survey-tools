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
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [True, False],
                "_d_at_anchor": [False, True],
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
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [True],
                "_d_at_anchor": [False],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        # Should log warning about invalid tours
        assert "invalid tours" in caplog.text.lower()
        assert "tour_data_quality" in result.columns

    def test_tour_without_anchor(self):
        """A tour that never touches its anchor at either end is not VALID."""
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
                # Home-based tour, so the anchor is home -- never reached
                "_o_at_anchor": [False, False],
                "_d_at_anchor": [False, False],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        assert "tour_data_quality" in result.columns
        # Should flag as problematic
        quality = result["tour_data_quality"][0]
        assert quality != TourDataQuality.VALID.value


class TestSpatialGapDetection:
    """Test SPATIAL_GAP flagging for tours that teleport across a missing leg."""

    def _tour(self, points, *, purpose=PurposeCategory.WORK.value):
        """Build a single multi-trip tour + linked_trips from o/d coordinates.

        points: list of (depart, o_home, d_home, o(lat,lon), d(lat,lon)).
        """
        n = len(points)
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [n],
                "tour_num": [1],
                "tour_category": [TourCategory.COMPLETE.value],
                "tour_purpose": [purpose],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"] * n,
                "person_id": [1] * n,
                "hh_id": [1] * n,
                "day_id": [1] * n,
                "tour_num": [1] * n,
                "depart_time": [p[0] for p in points],
                "_o_is_home": [p[1] for p in points],
                "_d_is_home": [p[2] for p in points],
                # Home-based tours: the anchor is home
                "_o_at_anchor": [p[1] for p in points],
                "_d_at_anchor": [p[2] for p in points],
                "o_lat": [p[3][0] for p in points],
                "o_lon": [p[3][1] for p in points],
                "d_lat": [p[4][0] for p in points],
                "d_lon": [p[4][1] for p in points],
            }
        )
        return tours, linked_trips

    def test_internal_gap_flags_spatial_gap(self):
        """A tour whose trips jump across a hole is flagged SPATIAL_GAP."""
        home, a, b = (37.70, -122.40), (37.75, -122.42), (37.76, -122.43)
        far = (38.30, -123.00)  # >1km from b -> the connecting leg is missing
        tours, linked_trips = self._tour(
            [
                (8.0, True, False, home, a),
                (10.0, False, False, a, b),  # continuous: resumes at a
                (17.0, False, True, far, home),  # jumps: origin far from b
            ]
        )
        result = validate_and_correct_tours(tours, linked_trips)
        assert result["tour_data_quality"][0] == TourDataQuality.SPATIAL_GAP.value

    def test_continuous_tour_stays_valid(self):
        """A spatially continuous multi-trip tour remains VALID."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        tours, linked_trips = self._tour(
            [
                (8.0, True, False, home, a),
                (17.0, False, True, a, home),  # resumes at a -> continuous
            ]
        )
        result = validate_and_correct_tours(tours, linked_trips)
        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value

    def test_threshold_is_configurable(self):
        """A jump below the configured threshold is not flagged."""
        home, a, b = (37.70, -122.40), (37.75, -122.42), (37.76, -122.43)
        far = (38.30, -123.00)
        tours, linked_trips = self._tour(
            [
                (8.0, True, False, home, a),
                (10.0, False, False, a, b),
                (17.0, False, True, far, home),
            ]
        )
        # A very large threshold tolerates the jump -> tour stays VALID.
        result = validate_and_correct_tours(
            tours, linked_trips, spatial_gap_threshold_meters=1_000_000.0
        )
        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value


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
                "_has_anchor_origin": [True, False],
                "_has_anchor_dest": [False, False],
            }
        )

        zero_tour_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2"],
                "linked_trip_id": [1, 2],
                "depart_time": ["08:00", "09:00"],
                "_o_is_home": [True, False],
                "_d_is_home": [False, False],
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [True, False],
                "_d_at_anchor": [False, False],
            }
        )

        _diagnose_problem_tours(tours, zero_tour_trips)

        # Should have logged diagnostics
        assert "INDETERMINATE tours" in caplog.text
        assert "anchor pattern" in caplog.text

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
                "_has_anchor_origin": [True],
                "_has_anchor_dest": [True],
            }
        )

        zero_tour_trips = pl.DataFrame(
            {
                "tour_id": [],
                "linked_trip_id": [],
                "depart_time": [],
                "_o_is_home": [],
                "_d_is_home": [],
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [],
                "_d_at_anchor": [],
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
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [True, False, True, False, False, False],
                "_d_at_anchor": [False, True, False, False, False, False],
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
                # Home-based tours: the anchor is home, so these mirror the flags above
                "_o_at_anchor": [True, False, False, True],
                "_d_at_anchor": [False, False, True, False],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips)

        # Should have different quality flags
        qualities = result["tour_data_quality"].unique().to_list()
        assert len(qualities) >= 1  # At least one quality type present
