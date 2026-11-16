"""Tests for the canonical completeness / model-usability gates."""

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.usability import cascade_usability
from tests.fixtures.cascade import STRICT, config_axes, household, stamp
from tests.fixtures.schema_utils import add_missing_tables


class TestRollupCompleteness:
    """Completeness rolls UP from days; day-records inherit their day's complete."""

    def _tables(self) -> dict:
        # person 1 (hh 1): day 1 complete, day 2 not. person 2 (hh 2): day 3 not.
        return {
            "households": pl.DataFrame({"hh_id": [1, 2], "survey_complete": [True, True]}),
            "persons": pl.DataFrame(
                {"person_id": [1, 2], "hh_id": [1, 2], "survey_complete": [True, True]}
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [1, 2, 3],
                    "person_id": [1, 1, 2],
                    "hh_id": [1, 1, 2],
                    "travel_date": ["d1", "d2", "d1"],
                    "survey_complete": [True, False, False],
                }
            ),
            "tours": pl.DataFrame(
                {
                    "tour_id": [1, 2, 3],
                    "person_day_id": [1, 2, 3],
                    "survey_complete": [True, True, True],
                }
            ),
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2, 3],
                    "person_day_id": [1, 2, 3],
                    "survey_complete": [True, True, True],
                }
            ),
        }

    def test_person_complete_is_any_complete_day(self):
        """Person 1 has a complete day -> complete; person 2 has none -> incomplete."""
        out = stamp(self._tables())
        assert out["persons"].sort("person_id")["survey_complete"].to_list() == [True, False]

    def test_day_records_inherit_their_day(self):
        """A tour/trip is complete only if its day is (own AND day)."""
        out = stamp(self._tables())
        assert out["tours"].sort("tour_id")["survey_complete"].to_list() == [True, False, False]
        assert out["linked_trips"].sort("linked_trip_id")["survey_complete"].to_list() == [
            True,
            False,
            False,
        ]

    def test_own_incomplete_survives_a_complete_day(self):
        """A record flagged incomplete stays incomplete even on a complete day."""
        tables = self._tables()
        tables["tours"] = pl.DataFrame(
            {"tour_id": [1], "person_day_id": [1], "survey_complete": [False]}
        )
        assert stamp(tables)["tours"]["survey_complete"].to_list() == [False]

    @pytest.mark.parametrize(("person_day_id", "expected"), [(1, True), (2, False)])
    def test_no_own_verdict_takes_the_days(self, person_day_id, expected):
        """A trip with no verdict of its own (the vendor's NA) is as complete as its day."""
        tables = self._tables()
        tables["unlinked_trips"] = pl.DataFrame(
            {"unlinked_trip_id": [1], "person_day_id": [person_day_id], "survey_complete": [None]},
            schema_overrides={"survey_complete": pl.Boolean},
        )
        assert stamp(tables)["unlinked_trips"]["survey_complete"].to_list() == [expected]

    def test_household_needs_a_complete_household_day(self):
        """Hh 1 has a complete household-day (d1); hh 2 has none."""
        out = stamp(self._tables())
        assert out["households"].sort("hh_id")["survey_complete"].to_list() == [True, False]

    def test_idempotent(self):
        """Re-running the rollup never changes an already-rolled result."""
        once = stamp(self._tables())
        twice = stamp(once)
        assert (
            twice["tours"].sort("tour_id")["survey_complete"].to_list()
            == once["tours"].sort("tour_id")["survey_complete"].to_list()
        )

    def test_missing_tables_are_skipped(self):
        """The cascade tolerates a bare tables dict (no days)."""
        out = stamp({"households": pl.DataFrame({"hh_id": [1], "survey_complete": [False]})})
        assert out["households"]["survey_complete"].to_list() == [False]


def _gate_tables(*, tour_quality=None, tour_category=None) -> dict:
    """One household/person/day with three tours and one trip per tour.

    Defaults: tour 1 admissible, tour 2 structurally invalid (single-trip),
    tour 3 a partial (valid quality, not home-to-home).
    """
    tour_quality = tour_quality or [
        TourDataQuality.VALID.value,
        TourDataQuality.NO_DESTINATION.value,
        TourDataQuality.VALID.value,
    ]
    tour_category = tour_category or [
        TourCategory.COMPLETE.value,
        TourCategory.PARTIAL_BOTH.value,
        TourCategory.PARTIAL_END.value,
    ]
    return {
        "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
        "persons": pl.DataFrame({"person_id": [1], "hh_id": [1], "survey_complete": [True]}),
        "person_days": pl.DataFrame(
            {
                "person_day_id": [1],
                "person_id": [1],
                "hh_id": [1],
                "travel_date": ["d1"],
                "travel_dow": [2],
                "survey_complete": [True],
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": [1, 2, 3],
                "person_day_id": [1, 1, 1],
                "parent_tour_id": [1, 2, 3],
                "survey_complete": [True, True, True],
                "tour_data_quality": tour_quality,
                "tour_category": tour_category,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": [1, 2, 3],
                "person_day_id": [1, 1, 1],
                "tour_id": [1, 2, 3],
                "survey_complete": [True, True, True],
            }
        ),
    }


class TestComputeModelUsable:
    """Tests for one usability pass (the modelling gate)."""

    def test_only_admissible_tours_are_usable(self):
        """VALID + COMPLETE-category tours pass; the day keeps its gate on that one tour."""
        out = stamp(_gate_tables(), STRICT)
        assert out["tours"].sort("tour_id")["usable_test"].to_list() == [True, False, False]
        assert out["person_days"]["usable_test"].to_list() == [True]

    def test_day_with_tours_but_none_usable_is_gated_out(self):
        """A day whose travel yields no admissible tour is not model-usable, but is complete."""
        tables = _gate_tables(
            tour_quality=[TourDataQuality.NO_DESTINATION.value] * 3,
            tour_category=[TourCategory.PARTIAL_BOTH.value] * 3,
        )
        out = stamp(tables, STRICT)
        assert out["person_days"]["usable_test"].to_list() == [False]
        assert out["person_days"]["survey_complete"].to_list() == [True]

    def test_no_travel_day_stays_usable(self):
        """A day with no tours at all is a legitimate home-all-day."""
        tables = _gate_tables()
        tables["tours"] = tables["tours"].clear()
        tables["linked_trips"] = tables["linked_trips"].clear()
        assert stamp(tables, STRICT)["person_days"]["usable_test"].to_list() == [True]

    def test_incomplete_day_rolls_up_and_down(self):
        """An incomplete day makes its person incomplete and its tours unusable."""
        tables = _gate_tables()
        tables["person_days"] = pl.DataFrame(
            {
                "person_day_id": [1],
                "person_id": [1],
                "hh_id": [1],
                "travel_date": ["d1"],
                "survey_complete": [False],
            }
        )
        out = stamp(tables, STRICT)
        assert out["persons"]["usable_test"].to_list() == [False]
        assert out["person_days"]["usable_test"].to_list() == [False]
        assert out["tours"]["usable_test"].to_list() == [False, False, False]


def test_a_joint_trip_follows_its_member_trips():
    """A joint trip whose tours were all dropped cannot stay usable, though it stays complete."""
    tables = household(
        3, tour_data_quality=[TourDataQuality.NO_DESTINATION.value] * 3, joint_tour_id=[10] * 3
    )
    tables["linked_trips"] = pl.DataFrame(
        {
            "linked_trip_id": [1, 2, 3],
            "person_day_id": [1, 2, 3],
            "tour_id": [10, 20, 30],
            "joint_trip_id": [20] * 3,
            "survey_complete": [True] * 3,
        }
    )
    tables["joint_trips"] = pl.DataFrame(
        {"joint_trip_id": [20], "hh_id": [1], "person_day_id": [1], "survey_complete": [True]}
    )
    tables["joint_tours"] = pl.DataFrame(
        {"joint_tour_id": [10], "hh_id": [1], "person_day_id": [1], "survey_complete": [True]}
    )
    out = stamp(tables, STRICT)
    assert out["joint_trips"]["survey_complete"].to_list() == [True]
    assert out["joint_trips"]["usable_test"].to_list() == [False]


def _hh_day_tables(*, day_complete, dates, persons, surveyable=None):
    """Household 1's days, each with one VALID home-to-home tour.

    The day gate then turns purely on household-day coherence. Pass *surveyable*
    as ``person_id -> 0/1`` to mark members; omitted persons default to surveyable.
    """
    n = len(day_complete)
    day_ids = list(range(1, n + 1))
    people = sorted(set(persons))
    person_frame = pl.DataFrame(
        {"person_id": people, "hh_id": [1] * len(people), "survey_complete": [True] * len(people)}
    )
    if surveyable is not None:
        person_frame = person_frame.with_columns(
            pl.col("person_id").replace_strict(surveyable, default=1).alias("surveyable")
        )
    return {
        "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
        "persons": person_frame,
        "person_days": pl.DataFrame(
            {
                "person_day_id": day_ids,
                "person_id": persons,
                "hh_id": [1] * n,
                "travel_date": dates,
                "survey_complete": day_complete,
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": day_ids,
                "person_day_id": day_ids,
                "parent_tour_id": day_ids,
                "survey_complete": day_complete,
                "tour_data_quality": [TourDataQuality.VALID.value] * n,
                "tour_category": [TourCategory.COMPLETE.value] * n,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": day_ids,
                "person_day_id": day_ids,
                "tour_id": day_ids,
                "survey_complete": day_complete,
            }
        ),
    }


class TestHouseholdDayCoherence:
    """household_days.survey_complete: a date is coherent only if ALL members reported it."""

    def test_all_members_complete_makes_the_household_day_complete(self):
        """One date, two members, both complete -> the household-day is coherent."""
        t = _hh_day_tables(
            day_complete=[True, True], dates=["2023-05-01", "2023-05-01"], persons=[1, 2]
        )
        out = stamp(t, STRICT)
        assert out["household_days"]["survey_complete"].to_list() == [True]
        assert out["person_days"]["usable_test"].to_list() == [True, True]

    @pytest.mark.parametrize(
        "surveyable",
        [
            pytest.param(None, id="no-surveyable-column"),
            # The exclusion is only for unsurveyable members, even when the
            # persons table carries the column.
            pytest.param({2: 1}, id="member-marked-surveyable"),
        ],
    )
    def test_one_incomplete_member_breaks_the_whole_household_day(self, surveyable):
        """If any member's day is incomplete, no member's day is usable, nor the household."""
        t = _hh_day_tables(
            day_complete=[True, False],
            dates=["2023-05-01", "2023-05-01"],
            persons=[1, 2],
            surveyable=surveyable,
        )
        out = stamp(t, STRICT)
        assert out["household_days"]["survey_complete"].to_list() == [False]
        assert out["person_days"]["usable_test"].to_list() == [False, False]
        assert out["households"]["usable_test"].to_list() == [False]

    def test_each_date_is_independent(self):
        """A household is complete on the dates where all members reported.

        One coherent date is then enough to admit the household.
        """
        # person 1 complete both dates; person 2 complete only on date A
        t = _hh_day_tables(
            day_complete=[True, True, True, False],
            dates=["2023-05-01", "2023-05-02", "2023-05-01", "2023-05-02"],
            persons=[1, 1, 2, 2],
        )
        out = stamp(t, STRICT)
        assert out["household_days"].sort("travel_date")["survey_complete"].to_list() == [
            True,
            False,
        ]
        assert out["households"]["usable_test"].to_list() == [True]


def test_the_step_returns_every_table_stamped():
    """``cascade_usability`` returns each table it takes, with the profile's column."""
    tables = add_missing_tables(_gate_tables())
    result = cascade_usability(
        validate_input=False, **tables, usability_profiles={"test": config_axes()}
    )
    assert set(result) == set(tables)
    assert all("usable_test" in df.columns for df in result.values())
