"""The usable cascade writes wherever it is told, and reads only what it wrote.

``stamp_usable`` derives one usability verdict per record. Which columns it
writes is a parameter, so a run stamps several verdicts side by side -- one per
profile -- without any of them overwriting another.

That only holds if the parameterisation is *total*. A single column name left
hardcoded anywhere in the cascade would make a second pass read the first
pass's answer, and the bug would be invisible: the numbers would still look
plausible because they would be somebody's real verdict, just not this pass's.

So the test here is an equivalence rather than a fixed expectation. The same
input stamped under two different names must produce identical values, column
for column, on every table. If a literal survives, the two disagree.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import (
    ALL_MEMBERS,
    PRIMARY_HOME,
    UsabilityProfile,
    cascade_complete,
    stamp_usable,
)

# Same standard, two names. Any hardcoded column would make them disagree.
FIRST = UsabilityProfile("ctramp_usable", PRIMARY_HOME, ALL_MEMBERS)
ALIAS = UsabilityProfile("alt_usable", PRIMARY_HOME, ALL_MEMBERS)


def _tables() -> dict[str, pl.DataFrame]:
    """A household with two people, spanning the paths the cascade takes.

    Person 1 has a clean tour; person 2 has a structurally invalid one, so the
    household-day reductions have something to fail on and the joint grouping
    loses a member.
    """
    return {
        "households": pl.DataFrame({"hh_id": [1], "complete": [True]}),
        "persons": pl.DataFrame(
            {
                "person_id": [1, 2],
                "hh_id": [1, 1],
                "complete": [True, True],
                "surveyable": [True, True],
            }
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1, 2],
                "person_id": [1, 2],
                "hh_id": [1, 1],
                "travel_date": [datetime(2023, 5, 1)] * 2,
                "complete": [True, True],
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": [10, 20],
                "day_id": [1, 2],
                "person_id": [1, 2],
                "complete": [True, True],
                "parent_tour_id": [10, 20],
                "joint_tour_id": [100, 100],
                "tour_data_quality": [
                    TourDataQuality.VALID.value,
                    TourDataQuality.SPATIAL_GAP.value,
                ],
                "tour_category": [
                    TourCategory.COMPLETE.value,
                    TourCategory.PARTIAL_END.value,
                ],
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": [1000, 2000],
                "tour_id": [10, 20],
                "day_id": [1, 2],
                "joint_trip_id": [500, 500],
                "complete": [True, True],
            }
        ),
        "unlinked_trips": pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2],
                "tour_id": [10, 20],
                "day_id": [1, 2],
                "complete": [True, True],
            }
        ),
        "joint_tours": pl.DataFrame({"joint_tour_id": [100], "day_id": [1], "complete": [True]}),
        "joint_trips": pl.DataFrame({"joint_trip_id": [500], "day_id": [1], "complete": [True]}),
    }


@pytest.fixture
def stamped() -> dict[str, pl.DataFrame]:
    """Tables carrying two verdicts derived from one set of complete flags."""
    tables: dict[str, pl.DataFrame | None] = dict(_tables())
    cascade_complete(tables)
    stamp_usable(tables, FIRST)
    stamp_usable(tables, ALIAS)
    return {name: df for name, df in tables.items() if df is not None}


class TestParameterisationIsTotal:
    """A second pass must not read the first pass's columns."""

    def test_every_table_agrees_under_either_name(self, stamped):
        """The verdict is the same whatever it is called."""
        disagreed = {
            name: (df[FIRST.flag].to_list(), df[ALIAS.flag].to_list())
            for name, df in stamped.items()
            if FIRST.flag in df.columns and df[FIRST.flag].to_list() != df[ALIAS.flag].to_list()
        }
        assert not disagreed, (
            "A hardcoded column name survives the parameterisation, so the second "
            f"pass read the first pass's verdict: {disagreed}"
        )

    def test_the_household_day_reduction_agrees_too(self, stamped):
        """The days table carries a second derived column, not just the flag."""
        days = stamped["days"]
        assert days[FIRST.household_day].to_list() == days[ALIAS.household_day].to_list()

    def test_both_passes_actually_ran(self, stamped):
        """Guard the guard: identical columns prove nothing if neither was written."""
        for name, df in stamped.items():
            assert ALIAS.flag in df.columns, f"{name} never received the second verdict"
        assert ALIAS.household_day in stamped["days"].columns


class TestTheCascadeStillDiscriminates:
    """An equivalence test passes trivially if everything is True.

    These pin that the fixture actually exercises the rules, so the agreement
    above is between two real verdicts rather than two constants.
    """

    def test_the_invalid_tour_is_rejected(self, stamped):
        """Person 2's spatially gapped tour is not usable."""
        assert stamped["tours"][FIRST.flag].to_list() == [True, False]

    def test_rejection_reaches_the_member_trips(self, stamped):
        """Its linked and unlinked trips follow it out."""
        assert stamped["linked_trips"][FIRST.flag].to_list() == [True, False]
        assert stamped["unlinked_trips"][FIRST.flag].to_list() == [True, False]

    def test_the_joint_grouping_loses_its_quorum(self, stamped):
        """One usable member is below MIN_JOINT_PARTICIPANTS, so the group falls."""
        assert stamped["joint_tours"][FIRST.flag].to_list() == [False]
        assert stamped["joint_trips"][FIRST.flag].to_list() == [False]

    def test_the_household_day_fails_for_both_members(self, stamped):
        """One member's day being unusable takes the whole date down."""
        assert stamped["days"][FIRST.household_day].to_list() == [False, False]


class TestCompleteIsUntouched:
    """The reporting flags belong to ``cascade_complete`` and never move."""

    def test_a_second_usable_pass_does_not_rewrite_complete(self):
        """Only ``cascade_complete`` writes ``complete``."""
        tables: dict[str, pl.DataFrame | None] = dict(_tables())
        cascade_complete(tables)
        before = {name: df["complete"].to_list() for name, df in tables.items() if df is not None}

        stamp_usable(tables, FIRST)
        stamp_usable(tables, ALIAS)

        after = {name: df["complete"].to_list() for name, df in tables.items() if df is not None}
        assert after == before
