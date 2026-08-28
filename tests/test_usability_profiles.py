"""A profile means exactly what its two axes say, and says both of them.

``usability_profiles`` stamps one verdict per profile so consumers can hold
different standards in the same run -- a joint-tour model needs whole
households, a trip-level estimation does not. Each profile states its standard
on two axes: which home has to close a tour, and what the household-date has to
show.

Three things have to hold for that to be readable and safe. A profile must
admit exactly what its axes name, so the config alone tells you what a column
means. Loosening an axis must only ever *add* records, or "looser" would be a
lie and nobody could reason about which column is the widest. And no setting of
either axis may admit a tour with missing data -- an open end is a place the
tour stopped, a missing leg is a hole in it, and no amount of tolerance for the
first should let through the second.

The axis needing the most care is ``household_day_needs``, because relaxing it
changes three separate places in the cascade. Miss the third and the profile
looks like it works -- tours and days flip -- while households stay strict and
the weighting re-zeroes everything downstream.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import (
    ALL_MEMBERS,
    ANY_HOME,
    ANYWHERE,
    NOTHING,
    PRIMARY_HOME,
    UsabilityProfile,
    cascade_complete,
    parse_usability_profiles,
    stamp_usable,
)

STRICT = UsabilityProfile("strict", PRIMARY_HOME, ALL_MEMBERS)
SECOND_HOMES = UsabilityProfile("second_homes", ANY_HOME, ALL_MEMBERS)
OPEN_ENDS = UsabilityProfile("open_ends", ANYWHERE, ALL_MEMBERS)
LOOSE_DAYS = UsabilityProfile("loose_days", PRIMARY_HOME, NOTHING)
WIDEST = UsabilityProfile("widest", ANYWHERE, NOTHING)

# Codes marking a hole in the tour rather than an unexpected end. No setting of
# either axis admits these.
MISSING_DATA = [TourDataQuality.NO_DESTINATION, TourDataQuality.SPATIAL_GAP]


def _axes(closes_at: str, household: str) -> dict[str, str]:
    return {"tour_closes_at": closes_at, "household_day_needs": household}


def _one_household(quality: TourDataQuality, category: TourCategory) -> dict:
    """Two people sharing a date. Person 1's tour is clean, person 2's is not.

    One member failing is what makes the household-day reduction bite, so this
    shape exercises both axes from a single fixture.
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
                "tour_data_quality": [TourDataQuality.VALID.value, quality.value],
                "tour_category": [TourCategory.COMPLETE.value, category.value],
            }
        ),
    }


def _stamp(profiles: list[UsabilityProfile], tables: dict) -> dict[str, pl.DataFrame]:
    """Run the reporting cascade once, then every profile over it."""
    working: dict[str, pl.DataFrame | None] = dict(tables)
    cascade_complete(working)
    for profile in profiles:
        stamp_usable(working, profile)
    return {name: df for name, df in working.items() if df is not None}


class TestEveryProfileAnswersEveryAxis:
    """A half-specified profile would leave a column's meaning implicit."""

    def test_a_missing_axis_is_rejected(self):
        """The whole point is that the config says what a column means."""
        with pytest.raises(ValueError, match="does not say 'household_day_needs'"):
            parse_usability_profiles({"p": {"tour_closes_at": PRIMARY_HOME}})

    def test_the_error_lists_the_legal_values(self):
        """A rejection is only useful if it says what to write instead."""
        with pytest.raises(ValueError, match=ANYWHERE):
            parse_usability_profiles({"p": {"household_day_needs": ALL_MEMBERS}})

    def test_an_unknown_value_is_rejected(self):
        """A typo must stop the run, not quietly pick something."""
        with pytest.raises(ValueError, match="not one of"):
            parse_usability_profiles({"p": _axes("primry_home", ALL_MEMBERS)})

    def test_an_unknown_axis_is_rejected(self):
        """A misspelled axis would otherwise be silently ignored."""
        spec = {"p": {**_axes(PRIMARY_HOME, ALL_MEMBERS), "tour_closes": ANY_HOME}}
        with pytest.raises(ValueError, match="unknown axis"):
            parse_usability_profiles(spec)

    def test_an_empty_block_is_rejected(self):
        """No profiles means no usability column for anyone to read."""
        with pytest.raises(ValueError, match="empty"):
            parse_usability_profiles({})

    @pytest.mark.parametrize("reserved", ["complete", "hh_day_complete"])
    def test_a_profile_cannot_take_a_reporting_column_name(self, reserved):
        """Reporting flags are survey facts; a profile must not overwrite one."""
        with pytest.raises(ValueError, match="collides"):
            parse_usability_profiles({reserved: _axes(PRIMARY_HOME, ALL_MEMBERS)})

    def test_declaration_order_is_preserved(self):
        """Profiles are stamped in the order the config names them."""
        spec = {name: _axes(PRIMARY_HOME, ALL_MEMBERS) for name in ("a", "b", "c")}
        assert [p.name for p in parse_usability_profiles(spec)] == ["a", "b", "c"]


class TestTourClosesAt:
    """Each setting admits one more kind of open end than the last."""

    @pytest.mark.parametrize(
        ("profile", "expected"),
        [
            (STRICT, [True, False]),
            (SECOND_HOMES, [True, True]),
            (OPEN_ENDS, [True, True]),
        ],
        ids=lambda v: getattr(v, "name", str(v)),
    )
    def test_a_second_home_needs_any_home_or_wider(self, profile, expected):
        """The tour reached a home of this person's, just not the usual one."""
        tables = _one_household(TourDataQuality.PARTIAL_OTHER_HOME, TourCategory.PARTIAL_END)
        assert _stamp([profile], tables)["tours"][profile.flag].to_list() == expected

    @pytest.mark.parametrize(
        "quality",
        [TourDataQuality.PARTIAL_DAY_SPLIT, TourDataQuality.PARTIAL_DIARY_EDGE],
        ids=lambda q: q.name,
    )
    def test_other_open_ends_need_anywhere(self, quality):
        """A tour cut by the diary edge closes at no home at all."""
        tables = _one_household(quality, TourCategory.PARTIAL_END)
        out = _stamp([STRICT, SECOND_HOMES, OPEN_ENDS], tables)
        assert out["tours"][STRICT.flag].to_list() == [True, False]
        assert out["tours"][SECOND_HOMES.flag].to_list() == [True, False]
        assert out["tours"][OPEN_ENDS.flag].to_list() == [True, True]


class TestMissingDataIsNeverAdmitted:
    """The axis walks down open ends. A hole in the tour is not one."""

    @pytest.mark.parametrize("quality", MISSING_DATA, ids=lambda q: q.name)
    def test_no_setting_admits_it(self, quality):
        """Not even the widest profile: a missing leg stays missing."""
        tables = _one_household(quality, TourCategory.PARTIAL_END)
        out = _stamp([STRICT, SECOND_HOMES, OPEN_ENDS, WIDEST], tables)
        for profile in (STRICT, SECOND_HOMES, OPEN_ENDS, WIDEST):
            assert out["tours"][profile.flag].to_list() == [True, False], (
                f"{profile.name} admitted {quality.name}, which is missing data "
                "rather than an open end"
            )


class TestHouseholdDayNeeds:
    """Relaxing this has to reach households, not just tours and days."""

    def test_nothing_admits_the_household(self):
        """The third place. Relaxing tours and days alone leaves this False.

        Person 2's day is unusable, so no date has all members usable. Strict
        drops the household; the relaxed profile counts usable *days* instead.
        """
        tables = _one_household(TourDataQuality.SPATIAL_GAP, TourCategory.PARTIAL_END)
        out = _stamp([STRICT, LOOSE_DAYS], tables)
        assert out["households"][STRICT.flag].to_list() == [False]
        assert out["households"][LOOSE_DAYS.flag].to_list() == [True]

    def test_it_says_nothing_about_tour_structure(self):
        """The two axes are independent: this one cannot admit a broken tour."""
        tables = _one_household(TourDataQuality.SPATIAL_GAP, TourCategory.PARTIAL_END)
        out = _stamp([LOOSE_DAYS], tables)
        assert out["tours"][LOOSE_DAYS.flag].to_list() == [True, False]

    def test_the_household_day_column_is_recorded_either_way(self):
        """Computed for every profile, so the column means one thing everywhere.

        This profile does not gate on it, but it is the only record of which
        dates were fully usable -- which is how a dropped household is traced
        back to a cause.
        """
        tables = _one_household(TourDataQuality.SPATIAL_GAP, TourCategory.PARTIAL_END)
        out = _stamp([LOOSE_DAYS], tables)
        assert out["days"][LOOSE_DAYS.household_day].to_list() == [False, False]


ALL_PROFILES = (STRICT, SECOND_HOMES, OPEN_ENDS, LOOSE_DAYS, WIDEST)


class TestProfilesAreIndependent:
    """Stamping one must not disturb another."""

    def test_declaration_order_changes_no_verdict(self):
        """Order in the config is presentation, never meaning."""
        tables = _one_household(TourDataQuality.PARTIAL_OTHER_HOME, TourCategory.PARTIAL_END)
        forwards = _stamp(list(ALL_PROFILES), tables)
        backwards = _stamp(list(reversed(ALL_PROFILES)), tables)
        for name, df in forwards.items():
            for profile in ALL_PROFILES:
                assert df[profile.flag].to_list() == backwards[name][profile.flag].to_list()

    def test_loosening_an_axis_never_removes_a_record(self):
        """A wider profile is a superset of a narrower one, at every level.

        Checked on every table rather than just tours: the cascade could widen a
        tour and still lose its day or household to an axis applied
        inconsistently.
        """
        wider_than = {
            STRICT: (SECOND_HOMES, OPEN_ENDS, LOOSE_DAYS, WIDEST),
            SECOND_HOMES: (OPEN_ENDS, WIDEST),
            OPEN_ENDS: (WIDEST,),
            LOOSE_DAYS: (WIDEST,),
        }
        for quality in TourDataQuality:
            tables = _one_household(quality, TourCategory.PARTIAL_END)
            out = _stamp(list(ALL_PROFILES), tables)
            for narrow, wides in wider_than.items():
                for table, df in out.items():
                    narrow_verdict = df[narrow.flag].to_list()
                    for wide in wides:
                        lost = [
                            i
                            for i, (was, now) in enumerate(
                                zip(narrow_verdict, df[wide.flag], strict=True)
                            )
                            if was and not now
                        ]
                        assert not lost, (
                            f"{wide.name} dropped {table} row(s) {lost} that "
                            f"{narrow.name} kept, with tour quality {quality.name}"
                        )
