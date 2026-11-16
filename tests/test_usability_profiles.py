"""A profile means exactly what its two axes say, and says both of them.

A profile admits exactly what its axes name; loosening an axis only ever adds
records; and no setting admits a tour with missing data. ``household_day_needs``
needs the most care: relaxing it must reach households, not just tours and days.
"""

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.usability import (
    HouseholdDayNeeds,
    TourClosesAt,
    UsabilityProfile,
    parse_usability_profiles,
    stamp_usable,
)
from tests.fixtures.cascade import WIDEST, config_axes, household, make_profile, stamp

STRICT = make_profile("strict")
SECOND_HOMES = make_profile("second_homes", TourClosesAt.ANY_HOME)
OPEN_ENDS = make_profile("open_ends", TourClosesAt.ANYWHERE)
LOOSE_DAYS = make_profile("loose_days", household=HouseholdDayNeeds.NOTHING)
ALL_PROFILES = (STRICT, SECOND_HOMES, OPEN_ENDS, LOOSE_DAYS, WIDEST)

# Codes marking a hole in the tour rather than an unexpected end. No setting of
# either axis admits these.
MISSING_DATA = [TourDataQuality.NO_DESTINATION, TourDataQuality.SPATIAL_GAP]

# Same standard, two names. Any hardcoded column would make them disagree.
FIRST = make_profile("first_name")
ALIAS = make_profile("second_name")


def _tours_when_person_2_is_rejected(profile: UsabilityProfile) -> list[bool]:
    """What the two tours read once person 2's tour is rejected under *profile*.

    A whole-household-day profile has no date on which both members pass, so the
    household falls and takes person 1's clean tour with it.
    """
    return [False, False] if profile.needs_whole_household_day else [True, False]


def _one_household(quality: TourDataQuality) -> dict:
    """Two people sharing a date: person 1's tour is clean, person 2's is partial at *quality*."""
    return household(
        2,
        tour_data_quality=[TourDataQuality.VALID.value, quality.value],
        tour_category=[TourCategory.COMPLETE.value, TourCategory.PARTIAL_END.value],
    )


class TestEveryProfileAnswersEveryAxis:
    """A half-specified profile would leave a column's meaning implicit."""

    @pytest.mark.parametrize(
        ("spec", "missing", "legal_value"),
        [
            pytest.param(
                {"tour_closes_at": TourClosesAt.PRIMARY_HOME},
                "household_day_needs",
                HouseholdDayNeeds.NOTHING,
                id="no-household",
            ),
            pytest.param(
                {"household_day_needs": HouseholdDayNeeds.ALL_MEMBERS},
                "tour_closes_at",
                TourClosesAt.ANYWHERE,
                id="no-tour",
            ),
        ],
    )
    def test_a_missing_axis_is_rejected(self, spec: dict, missing: str, legal_value: str):
        """The config has to say what a column means, and be told what to write."""
        with pytest.raises(ValueError, match=f"does not say '{missing}'") as excinfo:
            parse_usability_profiles({"p": spec})

        # A rejection is only useful if it names the values that would be accepted.
        assert legal_value in str(excinfo.value)

    def test_an_unknown_value_is_rejected(self):
        """A typo must stop the run, not quietly pick something."""
        with pytest.raises(ValueError, match="not one of"):
            parse_usability_profiles({"p": config_axes("primry_home")})

    def test_an_unknown_axis_is_rejected(self):
        """A misspelled axis would otherwise be silently ignored."""
        spec = {
            "p": {
                **config_axes(),
                "tour_closes": TourClosesAt.ANY_HOME,
            }
        }
        with pytest.raises(ValueError, match="unknown axis"):
            parse_usability_profiles(spec)

    def test_an_empty_block_is_rejected(self):
        """No profiles means no usability column for anyone to read."""
        with pytest.raises(ValueError, match="empty"):
            parse_usability_profiles({})

    @pytest.mark.parametrize("reserved", ["survey_complete"])
    def test_a_profile_cannot_take_a_reporting_column_name(self, reserved):
        """Reporting flags are survey facts; a profile must not overwrite one."""
        with pytest.raises(ValueError, match="collides"):
            parse_usability_profiles({reserved: config_axes()})

    def test_declaration_order_is_preserved(self):
        """Profiles are stamped in the order the config names them."""
        spec = {name: config_axes() for name in ("a", "b", "c")}
        assert [p.name for p in parse_usability_profiles(spec)] == ["a", "b", "c"]


class TestTravelDays:
    """The days a profile takes are listed out, and a bad list stops the run."""

    def _days(self, days) -> dict:
        return {
            "p": {
                **config_axes(),
                "travel_days": days,
            }
        }

    def test_a_missing_list_is_rejected(self):
        """Saying nothing must not quietly take every day."""
        spec = self._days(None)
        del spec["p"]["travel_days"]
        with pytest.raises(ValueError, match="does not say 'travel_days'") as excinfo:
            parse_usability_profiles(spec)
        assert "tuesday" in str(excinfo.value)

    @pytest.mark.parametrize(
        ("days", "match"),
        [
            pytest.param([], "at least one day", id="empty"),
            pytest.param("tuesday", "at least one day", id="not-a-list"),
            pytest.param(["tuesday", "tusday"], "not days", id="misspelled"),
            pytest.param(["Tuesday"], "not days", id="capitalised"),
            pytest.param(["tuesday", "tuesday"], "twice", id="repeated"),
        ],
    )
    def test_a_bad_list_is_rejected(self, days, match: str):
        """A typo or an empty list must stop the run, not quietly take some days."""
        with pytest.raises(ValueError, match=match):
            parse_usability_profiles(self._days(days))

    def test_the_days_are_kept_in_week_order(self):
        """Whatever order config lists them in, the profile reads Tue, Wed, Thu."""
        (profile,) = parse_usability_profiles(self._days(["thursday", "tuesday", "wednesday"]))
        assert profile.travel_days == (TravelDow.TUESDAY, TravelDow.WEDNESDAY, TravelDow.THURSDAY)
        assert profile.limits_travel_days

    def test_all_seven_days_limit_nothing(self):
        """Listing every day is the same as asking nothing of the day of the week."""
        (profile,) = parse_usability_profiles(self._days([d.name.lower() for d in TravelDow]))
        assert not profile.limits_travel_days


class TestTourClosesAt:
    """Each setting admits one more kind of open end than the last."""

    @pytest.mark.parametrize(
        ("profile", "expected"),
        [
            (STRICT, _tours_when_person_2_is_rejected(STRICT)),
            (SECOND_HOMES, [True, True]),
            (OPEN_ENDS, [True, True]),
        ],
        ids=lambda v: getattr(v, "name", str(v)),
    )
    def test_a_second_home_needs_any_home_or_wider(self, profile, expected):
        """The tour reached a home of this person's, just not the usual one."""
        tables = _one_household(TourDataQuality.OTHER_HOME)
        assert stamp(tables, profile)["tours"][profile.flag].to_list() == expected

    @pytest.mark.parametrize(
        "quality",
        [TourDataQuality.PARTIAL_DAY_SPLIT, TourDataQuality.PARTIAL_DIARY_EDGE],
        ids=lambda q: q.name,
    )
    def test_other_open_ends_need_anywhere(self, quality):
        """A tour cut by the diary edge closes at no home at all."""
        tables = _one_household(quality)
        out = stamp(tables, STRICT, SECOND_HOMES, OPEN_ENDS)
        for profile in (STRICT, SECOND_HOMES):
            assert out["tours"][profile.flag].to_list() == _tours_when_person_2_is_rejected(profile)
        assert out["tours"][OPEN_ENDS.flag].to_list() == [True, True]


class TestMissingDataIsNeverAdmitted:
    """The axis walks down open ends. A hole in the tour is not one."""

    @pytest.mark.parametrize("quality", MISSING_DATA, ids=lambda q: q.name)
    def test_no_setting_admits_it(self, quality):
        """Not even the widest profile: a missing leg stays missing."""
        tables = _one_household(quality)
        out = stamp(tables, STRICT, SECOND_HOMES, OPEN_ENDS, LOOSE_DAYS, WIDEST)
        # LOOSE_DAYS is in the list because the axes are independent: relaxing what
        # the household-date has to show cannot admit a broken tour.
        for profile in (STRICT, SECOND_HOMES, OPEN_ENDS, LOOSE_DAYS, WIDEST):
            assert out["tours"][profile.flag].to_list() == _tours_when_person_2_is_rejected(
                profile
            ), (
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
        tables = _one_household(TourDataQuality.SPATIAL_GAP)
        out = stamp(tables, STRICT, LOOSE_DAYS)
        assert out["households"][STRICT.flag].to_list() == [False]
        assert out["households"][LOOSE_DAYS.flag].to_list() == [True]

    def test_the_household_day_column_is_recorded_either_way(self):
        """Computed for every profile, so the column means one thing everywhere.

        This profile does not gate on it, but it is the only record of which
        dates were fully usable -- which is how a dropped household is traced
        back to a cause.
        """
        tables = _one_household(TourDataQuality.SPATIAL_GAP)
        out = stamp(tables, LOOSE_DAYS)
        assert out["household_days"][LOOSE_DAYS.flag].to_list() == [False]


class TestProfilesAreIndependent:
    """Stamping one must not disturb another."""

    def test_declaration_order_changes_no_verdict(self):
        """Order in the config is presentation, never meaning."""
        tables = _one_household(TourDataQuality.OTHER_HOME)
        forwards = stamp(tables, *ALL_PROFILES)
        backwards = stamp(tables, *reversed(ALL_PROFILES))
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
            tables = _one_household(quality)
            out = stamp(tables, *ALL_PROFILES)
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


def _household_with_joint_records() -> dict[str, pl.DataFrame]:
    """A household with two people, spanning the paths the cascade takes.

    Person 1 has a clean tour; person 2 has a structurally invalid one, so the
    household-day reductions have something to fail on and the joint grouping
    loses a member.
    """
    tables = _one_household(TourDataQuality.SPATIAL_GAP)
    tables["tours"] = tables["tours"].with_columns(joint_tour_id=pl.Series([100, 100]))
    tables["linked_trips"] = pl.DataFrame(
        {
            "linked_trip_id": [1000, 2000],
            "tour_id": [10, 20],
            "person_day_id": [1, 2],
            "joint_trip_id": [500, 500],
            "survey_complete": [True, True],
        }
    )
    tables["unlinked_trips"] = pl.DataFrame(
        {
            "unlinked_trip_id": [1, 2],
            "tour_id": [10, 20],
            "person_day_id": [1, 2],
            "survey_complete": [True, True],
        }
    )
    tables["joint_tours"] = pl.DataFrame(
        {"joint_tour_id": [100], "hh_id": [1], "person_day_id": [1], "survey_complete": [True]}
    )
    tables["joint_trips"] = pl.DataFrame(
        {"joint_trip_id": [500], "hh_id": [1], "person_day_id": [1], "survey_complete": [True]}
    )
    return tables


@pytest.fixture
def stamped() -> dict[str, pl.DataFrame]:
    """Tables carrying two verdicts derived from one set of complete flags."""
    return stamp(_household_with_joint_records(), FIRST, ALIAS)


class TestParameterisationIsTotal:
    """A second pass must not read the first pass's columns.

    The same input stamped under two names must match column for column on
    every table; a column name left hardcoded in the cascade would make them differ.
    """

    def test_every_table_agrees_under_either_name(self, stamped):
        """The verdict is the same whatever it is called."""
        # Guard the guard: identical columns prove nothing if neither was written.
        for name, df in stamped.items():
            assert ALIAS.flag in df.columns, f"{name} never received the second verdict"

        disagreed = {
            name: (df[FIRST.flag].to_list(), df[ALIAS.flag].to_list())
            for name, df in stamped.items()
            if df[FIRST.flag].to_list() != df[ALIAS.flag].to_list()
        }
        assert not disagreed, (
            "A hardcoded column name survives the parameterisation, so the second "
            f"pass read the first pass's verdict: {disagreed}"
        )


class TestTheCascadeStillDiscriminates:
    """An equivalence test passes trivially if everything is True.

    This pins that the fixture actually exercises the rules, so the agreement
    above is between two real verdicts rather than two constants.
    """

    def test_the_invalid_tour_is_rejected_all_the_way_down(self, stamped):
        """Person 2's spatially gapped tour, and everything hanging off it, falls.

        Person 1's clean tour falls too: with no date on which both members pass,
        the household is unusable, and nothing inside it stays usable.
        """
        assert stamped["tours"][FIRST.flag].to_list() == [False, False]

        # Linked and unlinked trips follow their tours out.
        assert stamped["linked_trips"][FIRST.flag].to_list() == [False, False]
        assert stamped["unlinked_trips"][FIRST.flag].to_list() == [False, False]

        # One usable member is below the joint quorum, so the group falls.
        assert stamped["joint_tours"][FIRST.flag].to_list() == [False]
        assert stamped["joint_trips"][FIRST.flag].to_list() == [False]

        # One member's day being unusable takes the whole date down.
        assert stamped["household_days"][FIRST.flag].to_list() == [False]


class TestCompleteIsUntouched:
    """The reporting flags belong to ``cascade_complete`` and never move."""

    def test_a_second_usable_pass_does_not_rewrite_complete(self):
        """Only ``cascade_complete`` writes ``survey_complete``."""
        tables = stamp(_household_with_joint_records())
        before = {name: df["survey_complete"].to_list() for name, df in tables.items()}

        stamp_usable(tables, FIRST)
        stamp_usable(tables, ALIAS)

        after = {name: df["survey_complete"].to_list() for name, df in tables.items()}
        assert after == before
