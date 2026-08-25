"""Tests for the canonical completeness / model-usability gates."""

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import (
    _flag_households,
    _flag_joint_groupings,
    cascade_completeness,
    compute_model_usable,
    flag_household_day_complete,
    rollup_completeness,
    rollup_household_complete,
)


class TestRollupCompleteness:
    """Completeness rolls UP from days; day-records inherit their day's complete."""

    def _tables(self) -> dict:
        # person 1 (hh 1): day 1 complete, day 2 not. person 2 (hh 2): day 3 not.
        return {
            "households": pl.DataFrame({"hh_id": [1, 2], "complete": [True, True]}),
            "persons": pl.DataFrame(
                {"person_id": [1, 2], "hh_id": [1, 2], "complete": [True, True]}
            ),
            "days": pl.DataFrame(
                {
                    "day_id": [1, 2, 3],
                    "person_id": [1, 1, 2],
                    "hh_id": [1, 1, 2],
                    "travel_date": ["d1", "d2", "d1"],
                    "complete": [True, False, False],
                }
            ),
            "tours": pl.DataFrame(
                {"tour_id": [1, 2, 3], "day_id": [1, 2, 3], "complete": [True, True, True]}
            ),
            "linked_trips": pl.DataFrame(
                {"linked_trip_id": [1, 2, 3], "day_id": [1, 2, 3], "complete": [True, True, True]}
            ),
        }

    def test_person_complete_is_any_complete_day(self):
        """Person 1 has a complete day -> complete; person 2 has none -> incomplete."""
        tables = self._tables()
        rollup_completeness(tables)
        assert tables["persons"].sort("person_id")["complete"].to_list() == [True, False]

    def test_day_records_inherit_their_day(self):
        """A tour/trip is complete only if its day is (own AND day)."""
        tables = self._tables()
        rollup_completeness(tables)
        assert tables["tours"].sort("tour_id")["complete"].to_list() == [True, False, False]
        assert tables["linked_trips"].sort("linked_trip_id")["complete"].to_list() == [
            True,
            False,
            False,
        ]

    def test_own_incomplete_survives_a_complete_day(self):
        """A record flagged incomplete stays incomplete even on a complete day."""
        tables = self._tables()
        tables["tours"] = pl.DataFrame({"tour_id": [1], "day_id": [1], "complete": [False]})
        rollup_completeness(tables)
        assert tables["tours"]["complete"].to_list() == [False]

    def test_household_needs_a_complete_household_day(self):
        """Hh 1 has a complete household-day (d1); hh 2 has none."""
        tables = self._tables()
        rollup_completeness(tables)
        flag_household_day_complete(tables)
        rollup_household_complete(tables)
        assert tables["households"].sort("hh_id")["complete"].to_list() == [True, False]

    def test_idempotent(self):
        """Re-running the rollup never changes an already-rolled result."""
        tables = self._tables()
        rollup_completeness(tables)
        once = tables["tours"].sort("tour_id")["complete"].to_list()
        rollup_completeness(tables)
        assert tables["tours"].sort("tour_id")["complete"].to_list() == once

    def test_missing_tables_are_skipped(self):
        """Rollup tolerates a bare tables dict (no days)."""
        tables = {"households": pl.DataFrame({"hh_id": [1], "complete": [False]})}
        rollup_completeness(tables)  # must not raise
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
        TourDataQuality.NO_DESTINATION.value,
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
            tour_quality=[TourDataQuality.NO_DESTINATION.value] * 3,
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

    def test_incomplete_day_rolls_up_and_down(self):
        """An incomplete day makes its records unusable and its person incomplete.

        Completeness rolls *up* now: an incomplete day is not forced by an
        ancestor, it makes the person incomplete (no complete day) and its own
        tours/trips inherit the incompleteness downward.
        """
        tables = _gate_tables()
        tables["days"] = pl.DataFrame({"day_id": [1], "person_id": [1], "complete": [False]})
        compute_model_usable(tables)
        assert tables["persons"]["model_usable"].to_list() == [False]
        assert tables["days"]["model_usable"].to_list() == [False]
        assert tables["tours"]["model_usable"].to_list() == [False, False, False]

    def test_missing_descriptors_fall_back_to_completeness(self):
        """Tours without quality/category columns are gated on completeness alone."""
        tables = _gate_tables()
        tables["tours"] = tables["tours"].drop("tour_data_quality", "tour_category")
        compute_model_usable(tables)
        assert tables["tours"].sort("tour_id")["model_usable"].to_list() == [True, True, True]


def _joint_tables(*, tour_usable: list[bool]):
    """A household on one day whose three tours form a single joint group."""
    n = len(tour_usable)
    return {
        "households": pl.DataFrame({"hh_id": [1], "complete": [True]}),
        "persons": pl.DataFrame(
            {"person_id": list(range(1, n + 1)), "hh_id": [1] * n, "complete": [True] * n}
        ),
        "days": pl.DataFrame({"day_id": [1], "person_id": [1], "complete": [True]}),
        "tours": pl.DataFrame(
            {
                "tour_id": list(range(1, n + 1)),
                "person_id": list(range(1, n + 1)),
                "day_id": [1] * n,
                "joint_tour_id": [10] * n,
                "complete": [True] * n,
                # Quality/category decide usability; drive them from tour_usable.
                "tour_data_quality": [
                    TourDataQuality.VALID.value if u else TourDataQuality.NO_DESTINATION.value
                    for u in tour_usable
                ],
                "tour_category": [TourCategory.COMPLETE.value] * n,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": list(range(1, n + 1)),
                "day_id": [1] * n,
                "tour_id": list(range(1, n + 1)),
                "joint_trip_id": [20] * n,
                "complete": [True] * n,
            }
        ),
        "joint_trips": pl.DataFrame({"joint_trip_id": [20], "day_id": [1], "complete": [True]}),
        "joint_tours": pl.DataFrame({"joint_tour_id": [10], "day_id": [1], "complete": [True]}),
    }


class TestJointGroupingsNeedTwoMembers:
    """A joint entity is only usable while it is still joint."""

    def test_joint_group_with_two_usable_members_survives(self):
        """Two surviving participants still make a joint group."""
        tables = _joint_tables(tour_usable=[True, True, False])
        compute_model_usable(tables)
        assert tables["joint_tours"]["model_usable"].to_list() == [True]
        assert tables["joint_trips"]["model_usable"].to_list() == [True]

    def test_joint_group_down_to_one_member_is_not_joint(self):
        """One surviving participant is not a joint tour, so it is not usable."""
        tables = _joint_tables(tour_usable=[True, False, False])
        compute_model_usable(tables)
        assert tables["joint_tours"]["model_usable"].to_list() == [False]
        assert tables["joint_trips"]["model_usable"].to_list() == [False]

    def test_joint_trip_follows_its_member_trips(self):
        """A joint trip whose tours were all dropped cannot stay usable.

        Before this rule joint_trips gated on `complete` alone, so a joint trip
        survived its own members being dropped.
        """
        tables = _joint_tables(tour_usable=[False, False, False])
        compute_model_usable(tables)
        assert tables["joint_trips"]["complete"].to_list() == [True]
        assert tables["joint_trips"]["model_usable"].to_list() == [False]


def _hh_day_tables(*, day_complete, dates, hh_ids, persons=None, surveyable=None):
    """Days for a household-day coherence scenario, with matching tours per day.

    Each day gets one VALID home-to-home tour so the day gate turns on purely on
    household-day coherence, not tour structure. Pass *surveyable* as a dict of
    ``person_id -> 0/1`` to mark unsurveyable members; omitted persons default
    to surveyable.
    """
    n = len(day_complete)
    day_ids = list(range(1, n + 1))
    people = persons or [1] * n
    person_frame = pl.DataFrame(
        {
            "person_id": sorted(set(people)),
            "hh_id": [hh_ids[people.index(p)] for p in sorted(set(people))],
            "complete": [True] * len(set(people)),
        }
    )
    if surveyable is not None:
        person_frame = person_frame.with_columns(
            pl.col("person_id").replace_strict(surveyable, default=1).alias("surveyable")
        )
    return {
        "households": pl.DataFrame(
            {"hh_id": sorted(set(hh_ids)), "complete": [True] * len(set(hh_ids))}
        ),
        "persons": person_frame,
        "days": pl.DataFrame(
            {
                "day_id": day_ids,
                "person_id": people,
                "hh_id": hh_ids,
                "travel_date": dates,
                "complete": day_complete,
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": day_ids,
                "day_id": day_ids,
                "complete": day_complete,
                "tour_data_quality": [TourDataQuality.VALID.value] * n,
                "tour_category": [TourCategory.COMPLETE.value] * n,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": day_ids,
                "day_id": day_ids,
                "tour_id": day_ids,
                "complete": day_complete,
            }
        ),
    }


class TestHouseholdDayCoherence:
    """hh_day_complete: a date is coherent only if ALL members reported it."""

    def test_all_members_complete_makes_the_household_day_complete(self):
        """One date, two members, both complete -> the household-day is coherent."""
        t = _hh_day_tables(
            day_complete=[True, True],
            dates=["2023-05-01", "2023-05-01"],
            hh_ids=[1, 1],
            persons=[1, 2],
        )
        compute_model_usable(t)
        assert t["days"]["hh_day_complete"].to_list() == [True, True]
        assert t["days"]["model_usable"].to_list() == [True, True]

    def test_one_incomplete_member_breaks_the_whole_household_day(self):
        """Strict: if any member's day is incomplete, no member's day is usable."""
        t = _hh_day_tables(
            day_complete=[True, False],
            dates=["2023-05-01", "2023-05-01"],
            hh_ids=[1, 1],
            persons=[1, 2],
        )
        compute_model_usable(t)
        # member 1 reported a complete day, but member 2 did not, so the whole
        # household-date is incoherent and neither day is usable
        assert t["days"]["hh_day_complete"].to_list() == [False, False]
        assert t["days"]["model_usable"].to_list() == [False, False]

    def test_each_date_is_independent(self):
        """A household is complete on the dates where all members reported."""
        # person 1 complete both dates; person 2 complete only on date A
        t = _hh_day_tables(
            day_complete=[True, True, True, False],
            dates=["2023-05-01", "2023-05-02", "2023-05-01", "2023-05-02"],
            hh_ids=[1, 1, 1, 1],
            persons=[1, 1, 2, 2],
        )
        compute_model_usable(t)
        d = t["days"].sort("day_id")
        # date A (days 1,3) coherent; date B (days 2,4) not
        assert d["hh_day_complete"].to_list() == [True, False, True, False]

    def test_household_needs_one_complete_household_day(self):
        """One coherent date is enough to admit the household."""
        t = _hh_day_tables(
            day_complete=[True, True, True, False],
            dates=["2023-05-01", "2023-05-02", "2023-05-01", "2023-05-02"],
            hh_ids=[1, 1, 1, 1],
            persons=[1, 1, 2, 2],
        )
        compute_model_usable(t)
        assert t["households"]["model_usable"].to_list() == [True]

    def test_household_with_no_complete_day_is_dropped(self):
        """No date where all members reported -> household is not admissible."""
        t = _hh_day_tables(
            day_complete=[True, False],
            dates=["2023-05-01", "2023-05-01"],
            hh_ids=[1, 1],
            persons=[1, 2],
        )
        compute_model_usable(t)
        assert t["households"]["model_usable"].to_list() == [False]

    def test_unsurveyable_member_does_not_veto_the_household_day(self):
        """An unsurveyable member's day neither vetoes the date nor becomes usable.

        The vendor gives unsurveyable persons (unrelated members, e.g. roommates)
        no day rows at all; where a source carries any, the ALL reduction runs
        over surveyable members only. Member 2's incomplete day must not break
        member 1's date -- but member 2's own day stays unusable (their travel
        was never collected).
        """
        t = _hh_day_tables(
            day_complete=[True, False],
            dates=["2023-05-01", "2023-05-01"],
            hh_ids=[1, 1],
            persons=[1, 2],
            surveyable={2: 0},
        )
        compute_model_usable(t)
        d = t["days"].sort("day_id")
        assert d["hh_day_complete"].to_list() == [True, False]
        assert d["model_usable"].to_list() == [True, False]
        # The household is admissible through its surveyable member's date.
        assert t["households"]["model_usable"].to_list() == [True]

    def test_surveyable_member_still_vetoes_the_household_day(self):
        """A surveyable member's incomplete day still breaks the date.

        The exclusion is only for unsurveyable members, even when the persons
        table carries the surveyable column.
        """
        t = _hh_day_tables(
            day_complete=[True, False],
            dates=["2023-05-01", "2023-05-01"],
            hh_ids=[1, 1],
            persons=[1, 2],
            surveyable={2: 1},
        )
        compute_model_usable(t)
        assert t["days"].sort("day_id")["hh_day_complete"].to_list() == [False, False]
        assert t["households"]["model_usable"].to_list() == [False]


class TestUnflaggedMemberTablesRaise:
    """An unflagged member table must fail loudly, never silently pass.

    Each of these rules reads a child's ``model_usable``. If that column has not
    been stamped yet the old guards fell back to bare ``complete``, which passes
    every record -- the exact behaviour the rules exist to replace, with no error
    to notice. They now raise instead.
    """

    def test_joint_grouping_with_unflagged_members_raises(self):
        """Counting usable members before they are flagged must not pass silently."""
        tables = _joint_tables(tour_usable=[True, False, False])
        # linked_trips present but never flagged: calling the rule directly is
        # the ordering mistake this guards against.
        with pytest.raises(ValueError, match="no model_usable column yet"):
            _flag_joint_groupings(tables)

    def test_households_with_uncohered_days_raise(self):
        """The household rule reads days.hh_day_usable; unflagged days must raise."""
        tables = _joint_tables(tour_usable=[True, True])
        # days present but flag_household_day_usable not run yet
        assert "hh_day_usable" not in tables["days"].columns
        with pytest.raises(ValueError, match="no hh_day_usable column yet"):
            _flag_households(tables)

    def test_partial_call_without_the_member_table_is_still_allowed(self):
        """Omitting the member table entirely is a legitimate partial call."""
        tables = _joint_tables(tour_usable=[True, True])
        # No linked_trips at all -> joint_trips falls back to its own `complete`.
        del tables["linked_trips"]
        del tables["joint_tours"]
        _flag_joint_groupings(tables)
        assert tables["joint_trips"]["model_usable"].to_list() == [True]


class TestCascadeCompletenessStep:
    """Tests for the cascade_completeness pipeline step.

    The step is a thin wrapper over :func:`compute_model_usable` (covered above);
    the full pipeline wiring is exercised by the synthetic end-to-end run. Here we
    only assert it is registered and returns just the tables it was given.
    """

    def test_omits_tables_not_supplied(self):
        """Tables passed as None are not present in the result."""
        tables = _gate_tables()
        result = cascade_completeness(households=tables["households"])
        assert set(result) == {"households"}
        assert "model_usable" in result["households"].columns
