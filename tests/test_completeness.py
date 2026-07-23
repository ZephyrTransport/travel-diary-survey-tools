"""Tests for the canonical completeness / model-usability gates."""

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import (
    cascade_completeness,
    compute_model_usable,
    flag_model_usable,
)


class TestCascadeCompleteness:
    """Tests for cascade_completeness (household-down reporting completeness)."""

    def _tables(self) -> dict:
        return {
            "households": pl.DataFrame({"hh_id": [1, 2], "complete": [True, False]}),
            "persons": pl.DataFrame(
                {"person_id": [1, 2], "hh_id": [1, 2], "complete": [True, True]}
            ),
            "days": pl.DataFrame(
                {"day_id": [1, 2, 3], "person_id": [1, 1, 2], "complete": [True, False, True]}
            ),
            "tours": pl.DataFrame(
                {"tour_id": [1, 2, 3], "day_id": [1, 2, 3], "complete": [True, True, True]}
            ),
            "linked_trips": pl.DataFrame(
                {"linked_trip_id": [1, 2, 3], "day_id": [1, 2, 3], "complete": [True, True, True]}
            ),
        }

    def test_incomplete_household_cascades_to_person(self):
        """Person 2 is in incomplete household 2 -> becomes incomplete."""
        tables = self._tables()
        cascade_completeness(tables)
        assert tables["persons"].sort("person_id")["complete"].to_list() == [True, False]

    def test_incomplete_day_and_ancestors_cascade_to_tours(self):
        """Tours inherit incompleteness from their day (and its ancestors)."""
        tables = self._tables()
        cascade_completeness(tables)
        # day 1: complete; day 2: own incomplete; day 3: complete but person 2's HH incomplete
        assert tables["days"].sort("day_id")["complete"].to_list() == [True, False, False]
        assert tables["tours"].sort("tour_id")["complete"].to_list() == [True, False, False]
        assert tables["linked_trips"].sort("linked_trip_id")["complete"].to_list() == [
            True,
            False,
            False,
        ]

    def test_own_incomplete_respected_under_complete_ancestors(self):
        """A record flagged incomplete stays incomplete even if all ancestors are complete."""
        tables = self._tables()
        tables["tours"] = pl.DataFrame({"tour_id": [1], "day_id": [1], "complete": [False]})
        cascade_completeness(tables)
        assert tables["tours"]["complete"].to_list() == [False]

    def test_idempotent(self):
        """Re-running the cascade never changes an already-cascaded result."""
        tables = self._tables()
        cascade_completeness(tables)
        once = tables["tours"].sort("tour_id")["complete"].to_list()
        cascade_completeness(tables)
        assert tables["tours"].sort("tour_id")["complete"].to_list() == once

    def test_missing_tables_are_skipped(self):
        """Cascade tolerates missing child tables (e.g. only households present)."""
        tables = {"households": pl.DataFrame({"hh_id": [1], "complete": [False]})}
        cascade_completeness(tables)  # must not raise
        assert tables["households"]["complete"].to_list() == [False]


def _gate_tables(
    *,
    tour_quality=None,
    tour_category=None,
    tour_complete=None,
) -> dict:
    """One household/person/day with three tours and one trip per tour.

    Defaults: tour 1 admissible, tour 2 structurally invalid (single-trip),
    tour 3 a partial (valid quality, not home-to-home).
    """
    tour_quality = tour_quality or [
        TourDataQuality.VALID.value,
        TourDataQuality.SINGLE_TRIP.value,
        TourDataQuality.VALID.value,
    ]
    tour_category = tour_category or [
        TourCategory.COMPLETE.value,
        TourCategory.PARTIAL_BOTH.value,
        TourCategory.PARTIAL_END.value,
    ]
    tour_complete = tour_complete or [True, True, True]
    return {
        "households": pl.DataFrame({"hh_id": [1], "complete": [True]}),
        "persons": pl.DataFrame({"person_id": [1], "hh_id": [1], "complete": [True]}),
        "days": pl.DataFrame({"day_id": [1], "person_id": [1], "complete": [True]}),
        "tours": pl.DataFrame(
            {
                "tour_id": [1, 2, 3],
                "day_id": [1, 1, 1],
                "complete": tour_complete,
                "tour_data_quality": tour_quality,
                "tour_category": tour_category,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": [1, 2, 3],
                "day_id": [1, 1, 1],
                "tour_id": [1, 2, 3],
                "complete": [True, True, True],
            }
        ),
    }


class TestComputeModelUsable:
    """Tests for compute_model_usable (the modelling gate)."""

    def test_only_admissible_tours_are_usable(self):
        """VALID + COMPLETE-category tours pass; invalid and partial ones do not."""
        tables = _gate_tables()
        compute_model_usable(tables)
        assert tables["tours"].sort("tour_id")["model_usable"].to_list() == [True, False, False]

    def test_complete_is_never_overwritten(self):
        """Partials/overnights remain valid survey data: `complete` is untouched."""
        tables = _gate_tables()
        compute_model_usable(tables)
        # all three tours were reported completely, even though two are inadmissible
        assert tables["tours"].sort("tour_id")["complete"].to_list() == [True, True, True]

    def test_member_trips_follow_their_tour(self):
        """A trip is usable only if its tour is."""
        tables = _gate_tables()
        compute_model_usable(tables)
        assert tables["linked_trips"].sort("linked_trip_id")["model_usable"].to_list() == [
            True,
            False,
            False,
        ]

    def test_day_with_one_usable_tour_stays_usable(self):
        """A day keeps its gate as long as at least one tour is admissible."""
        tables = _gate_tables()
        compute_model_usable(tables)
        assert tables["days"]["model_usable"].to_list() == [True]

    def test_day_with_tours_but_none_usable_is_gated_out(self):
        """A day whose travel yields no admissible tour is not model-usable."""
        tables = _gate_tables(
            tour_quality=[TourDataQuality.SINGLE_TRIP.value] * 3,
            tour_category=[TourCategory.PARTIAL_BOTH.value] * 3,
        )
        compute_model_usable(tables)
        assert tables["days"]["model_usable"].to_list() == [False]
        # ...but it is still valid survey data
        assert tables["days"]["complete"].to_list() == [True]

    def test_no_travel_day_stays_usable(self):
        """A day with no tours at all is a legitimate home-all-day."""
        tables = _gate_tables()
        tables["tours"] = tables["tours"].clear()
        tables["linked_trips"] = tables["linked_trips"].clear()
        compute_model_usable(tables)
        assert tables["days"]["model_usable"].to_list() == [True]

    def test_incomplete_ancestor_gates_everything_out(self):
        """Reporting incompleteness cascades into the gate."""
        tables = _gate_tables()
        tables["households"] = pl.DataFrame({"hh_id": [1], "complete": [False]})
        compute_model_usable(tables)
        assert tables["persons"]["model_usable"].to_list() == [False]
        assert tables["days"]["model_usable"].to_list() == [False]
        assert tables["tours"]["model_usable"].to_list() == [False, False, False]

    def test_require_valid_tours_false_reduces_to_completeness(self):
        """With structure checks off, the gate is just reporting completeness."""
        tables = _gate_tables()
        compute_model_usable(tables, require_valid_tours=False)
        assert tables["tours"].sort("tour_id")["model_usable"].to_list() == [True, True, True]

    def test_missing_descriptors_fall_back_to_completeness(self):
        """Tours without quality/category columns are gated on completeness alone."""
        tables = _gate_tables()
        tables["tours"] = tables["tours"].drop("tour_data_quality", "tour_category")
        compute_model_usable(tables)
        assert tables["tours"].sort("tour_id")["model_usable"].to_list() == [True, True, True]


class TestFlagModelUsableStep:
    """Tests for the flag_model_usable pipeline step.

    The step is a thin wrapper over :func:`compute_model_usable` (covered above);
    the full pipeline wiring is exercised by the synthetic end-to-end run. Here we
    only assert it is registered and returns just the tables it was given.
    """

    def test_omits_tables_not_supplied(self):
        """Tables passed as None are not present in the result."""
        tables = _gate_tables()
        result = flag_model_usable(households=tables["households"])
        assert set(result) == {"households"}
        assert "model_usable" in result["households"].columns
