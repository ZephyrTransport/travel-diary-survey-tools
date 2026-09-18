"""Tests for tour validation helper functions."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from processing.tours.validation_helpers import validate_and_correct_tours


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

    def test_unassigned_trip_raises(self):
        """A trip that boundary detection never placed in a tour is fatal.

        Every first trip of a person-day starts a tour, so tour_num < 1 means
        detection itself broke and the whole table is suspect.
        """
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [1],
                "tour_num": [0],
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
            }
        )

        with pytest.raises(ValueError, match="never assigned to a tour"):
            validate_and_correct_tours(tours, linked_trips)

    def _frames(self, category, *, purpose=PurposeCategory.SHOP.value, trips=2):
        """One tour with *trips* legs and no other travel around it."""
        pts = [(37.0 + 0.1 * (i % 2), -122.0 - 0.1 * (i % 2)) for i in range(trips + 1)]
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1"],
                "person_id": [1],
                "day_id": [1],
                "trip_count": [trips],
                "tour_num": [1],
                "tour_category": [category.value],
                "tour_purpose": [purpose],
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"] * trips,
                "person_id": [1] * trips,
                "hh_id": [1] * trips,
                "day_id": [1] * trips,
                "tour_num": [1] * trips,
                # A continuous chain: each trip starts where the last one ended,
                # so the internal spatial-gap check stays quiet.
                "o_lat": [pts[i][0] for i in range(trips)],
                "o_lon": [pts[i][1] for i in range(trips)],
                "d_lat": [pts[i + 1][0] for i in range(trips)],
                "d_lon": [pts[i + 1][1] for i in range(trips)],
                "depart_time": [datetime(2023, 5, 1, 8 + i) for i in range(trips)],
                "arrive_time": [datetime(2023, 5, 1, 8 + i, 30) for i in range(trips)],
            }
        )
        return tours, linked_trips

    @staticmethod
    def _two_day_frames(second_day_origin):
        """A tour ending away on day 1, and day 2 starting from *second_day_origin*."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2"],
                "person_id": [1, 1],
                "day_id": [1, 2],
                "trip_count": [1, 1],
                "tour_num": [1, 1],
                "tour_category": [
                    TourCategory.PARTIAL_END.value,
                    TourCategory.PARTIAL_START.value,
                ],
                "tour_purpose": [PurposeCategory.SHOP.value] * 2,
            }
        )
        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2"],
                "person_id": [1, 1],
                "hh_id": [1, 1],
                "day_id": [1, 2],
                "tour_num": [1, 1],
                "o_lat": [37.0, second_day_origin[0]],
                "o_lon": [-122.0, second_day_origin[1]],
                "d_lat": [37.5, 37.0],
                "d_lon": [-122.5, -122.0],
                "depart_time": [datetime(2023, 5, 1, 20), datetime(2023, 5, 2, 8)],
                "arrive_time": [datetime(2023, 5, 1, 21), datetime(2023, 5, 2, 9)],
            }
        )
        return tours, linked_trips

    @pytest.mark.parametrize(
        "category",
        [TourCategory.PARTIAL_BOTH, TourCategory.PARTIAL_START, TourCategory.PARTIAL_END],
    )
    def test_lone_partial_tour_is_a_diary_edge(self, category):
        """With no trip either side of it, an open end is where the diary stops."""
        tours, linked_trips = self._frames(category)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.PARTIAL_DIARY_EDGE.value

    def test_complete_tour_is_valid(self):
        """Both ends anchored and a real purpose: nothing to report."""
        tours, linked_trips = self._frames(TourCategory.COMPLETE)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.VALID.value

    def test_category_is_not_rewritten(self):
        """Quality is a verdict on the category; it never edits it."""
        tours, linked_trips = self._frames(TourCategory.PARTIAL_END, trips=1)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_category"][0] == TourCategory.PARTIAL_END.value

    def test_tour_without_a_purpose_has_no_destination(self):
        """A null purpose means nothing was found to anchor the tour on.

        Aggregation leaves it null when every candidate was the return leg or a
        mode change -- an anchor-to-anchor loop being the common case.
        """
        tours, linked_trips = self._frames(TourCategory.COMPLETE, purpose=None, trips=1)

        result = validate_and_correct_tours(tours, linked_trips)

        assert result["tour_data_quality"][0] == TourDataQuality.NO_DESTINATION.value

    def test_open_end_at_a_second_home_is_named_as_such(self):
        """An end sitting on a known other home is not a truncation."""
        tours, linked_trips = self._frames(TourCategory.PARTIAL_END, trips=1)
        habitual = pl.DataFrame(
            {
                "person_id": [1],
                "location_type": [LocationType.HOME.value],
                "is_primary": [False],
                # The tour's last destination, to the metre.
                "lat": [37.1],
                "lon": [-122.1],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips, habitual_locations=habitual)

        assert result["tour_data_quality"][0] == TourDataQuality.PARTIAL_OTHER_HOME.value

    def test_chain_resuming_next_day_is_a_day_split(self):
        """The journey continues from where it stopped, so it was merely cut."""
        tours, linked_trips = self._two_day_frames((37.5, -122.5))

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.PARTIAL_DAY_SPLIT.value,
            TourDataQuality.PARTIAL_DAY_SPLIT.value,
        ]

    def test_reappearing_elsewhere_is_a_spatial_gap(self):
        """A leg is missing when the next trip starts somewhere else entirely."""
        tours, linked_trips = self._two_day_frames((38.9, -123.9))

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.SPATIAL_GAP.value,
            TourDataQuality.SPATIAL_GAP.value,
        ]


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


class TestTourValidationIntegration:
    """Integration tests for tour validation workflow."""

    def test_full_validation_workflow(self):
        """Each structural shape lands on its own quality code."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_1", "tour_2", "tour_3", "tour_4"],
                "person_id": [1, 1, 2, 2],
                "day_id": [1, 1, 1, 1],
                "trip_count": [2, 1, 3, 2],
                "tour_num": [1, 2, 1, 2],
                "tour_category": [
                    TourCategory.COMPLETE.value,
                    TourCategory.COMPLETE.value,
                    TourCategory.PARTIAL_BOTH.value,
                    TourCategory.PARTIAL_START.value,
                ],
                "tour_purpose": [
                    PurposeCategory.WORK.value,
                    # Nothing but the return leg, so aggregation found no purpose.
                    None,
                    PurposeCategory.SOCIALREC.value,
                    PurposeCategory.SHOP.value,
                ],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_1"] * 2 + ["tour_2"] + ["tour_3"] * 3 + ["tour_4"] * 2,
                "person_id": [1, 1, 1, 2, 2, 2, 2, 2],
                "hh_id": [1, 1, 1, 2, 2, 2, 2, 2],
                "day_id": [1] * 8,
                "tour_num": [1, 1, 2, 1, 1, 1, 2, 2],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.VALID.value,  # anchor to anchor, with a purpose
            TourDataQuality.NO_DESTINATION.value,  # nothing to anchor on
            # Without coordinates there is no travel visible either side, which
            # is the diary-edge reading of an open end.
            TourDataQuality.PARTIAL_DIARY_EDGE.value,
            TourDataQuality.PARTIAL_DIARY_EDGE.value,
        ]

    def test_mixed_quality_tours(self):
        """A tour keeps its own verdict; a neighbour defect does not spread."""
        tours = pl.DataFrame(
            {
                "tour_id": ["tour_good", "tour_bad"],
                "person_id": [1, 1],
                "day_id": [1, 1],
                "trip_count": [3, 1],
                "tour_num": [1, 2],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.PARTIAL_BOTH.value],
                "tour_purpose": [PurposeCategory.WORK.value, PurposeCategory.WORK.value],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "tour_id": ["tour_good", "tour_good", "tour_good", "tour_bad"],
                "person_id": [1, 1, 1, 1],
                "hh_id": [1, 1, 1, 1],
                "day_id": [1, 1, 1, 1],
                "tour_num": [1, 1, 1, 2],
            }
        )

        result = validate_and_correct_tours(tours, linked_trips).sort("tour_id")

        assert result["tour_data_quality"].to_list() == [
            TourDataQuality.PARTIAL_DIARY_EDGE.value,
            TourDataQuality.VALID.value,
        ]
