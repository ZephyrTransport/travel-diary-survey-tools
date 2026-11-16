"""Degenerate and adversarial inputs to the usability cascade.

A verdict column is a boolean, always: every consumer reads it as one. So an
input the cascade cannot judge (a missing or null descriptor, no rows, no
surveyable members, a missing parent) comes out False, never null.
"""

from datetime import datetime
from typing import ClassVar

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.formatting.kept_records import suggest_usability_columns
from processing.usability import parse_usability_profiles, stamp_usable
from tests.fixtures.cascade import (
    DATE,
    LOOSE,
    STRICT,
    WIDEST,
    config_axes,
    household,
    make_profile,
    stamp,
)

EXCLUDED = "survey_tech excluded: browser"


def _one_day_two_tours(quality_11: TourDataQuality = TourDataQuality.VALID) -> dict:
    """One person's day with tour 10 (VALID) and tour 11."""
    return household(
        1,
        tour_id=[10, 11],
        person_day_id=[1, 1],
        person_id=[1, 1],
        survey_complete=[True, True],
        parent_tour_id=[10, 11],
        tour_data_quality=[TourDataQuality.VALID.value, quality_11.value],
        tour_category=[TourCategory.COMPLETE.value] * 2,
    )


class TestAVerdictIsAlwaysABoolean:
    """No table may leave a null in the column consumers gate on."""

    @pytest.mark.parametrize(
        "override",
        [
            # ``is_in`` returns null for a null input, so this term needs filling.
            pytest.param({"tour_data_quality": [None]}, id="quality"),
            # Same for the equality term, which is only live at ``primary_home``.
            pytest.param({"tour_category": [None]}, id="category"),
        ],
    )
    def test_a_null_descriptor_is_rejected_not_propagated(self, override: dict):
        """An input the cascade cannot judge comes out False, never null."""
        tables = household(1, **override)
        assert tables["tours"][next(iter(override))].to_list() == [None]

        stamped = stamp(tables, STRICT)

        assert stamped["tours"]["usable_test"].to_list() == [False]

    def test_no_table_carries_a_null_verdict(self):
        """The property itself, over every table the cascade touches."""
        tables = household(1, tour_data_quality=[None], tour_category=[None], joint_tour_id=[1])
        tables["linked_trips"] = pl.DataFrame(
            {
                "linked_trip_id": [100],
                "tour_id": [10],
                "person_day_id": [1],
                "survey_complete": [None],
            }
        )
        tables["joint_tours"] = pl.DataFrame(
            {"joint_tour_id": [1], "hh_id": [1], "person_day_id": [1], "survey_complete": [None]}
        )

        stamped = stamp(tables, STRICT)

        nulls = {
            name: df["usable_test"].null_count()
            for name, df in stamped.items()
            if "usable_test" in df.columns
        }
        assert nulls == dict.fromkeys(nulls, 0)


class TestTheCategoryTermOnlyAppliesAtPrimaryHome:
    """Past ``primary_home`` the category term would re-impose what the axis relaxed.

    ``tour_category`` and ``tour_data_quality`` state the same fact about where a
    tour ends. That the term is live at ``primary_home`` is pinned by
    ``test_usability::test_only_admissible_tours_are_usable``.
    """

    @pytest.mark.parametrize("category", list(TourCategory))
    def test_anywhere_admits_a_partial_tour_whatever_its_category(self, category):
        """Every category, so the term is gone rather than merely usually agreeing."""
        stamped = stamp(
            household(
                1,
                tour_data_quality=[TourDataQuality.PARTIAL_DIARY_EDGE.value],
                tour_category=[category.value],
            ),
            WIDEST,
        )

        assert stamped["tours"]["usable_widest"].to_list() == [True]


class TestEmptyTables:
    """A run can legitimately have no rows; it must still have the verdict columns."""

    EMPTY: ClassVar[dict[str, dict[str, pl.DataType]]] = {
        "households": {"hh_id": pl.Int64, "survey_complete": pl.Boolean},
        "persons": {
            "person_id": pl.Int64,
            "hh_id": pl.Int64,
            "survey_complete": pl.Boolean,
            "surveyable": pl.Boolean,
        },
        "person_days": {
            "person_day_id": pl.Int64,
            "person_id": pl.Int64,
            "hh_id": pl.Int64,
            "travel_date": pl.Datetime,
            "survey_complete": pl.Boolean,
        },
        "tours": {
            "tour_id": pl.Int64,
            "person_day_id": pl.Int64,
            "survey_complete": pl.Boolean,
            "parent_tour_id": pl.Int64,
            "tour_data_quality": pl.Int64,
            "tour_category": pl.Int64,
        },
    }

    @pytest.fixture
    def stamped(self) -> dict[str, pl.DataFrame]:
        """Every table zero-row but fully typed, as an empty run delivers them."""
        return stamp({name: pl.DataFrame(schema=s) for name, s in self.EMPTY.items()}, STRICT)

    @pytest.mark.parametrize("table", list(EMPTY))
    def test_every_table_still_gains_the_column(self, stamped, table):
        """A vanished column would change the output schema on an empty run."""
        assert stamped[table].height == 0
        # Boolean, not Null-dtype, or the writer's schema check fails downstream.
        assert stamped[table].schema["usable_test"] == pl.Boolean

    def test_household_days_gain_both_columns_too(self, stamped):
        """The household-day's completeness and the profile's verdict, typed."""
        assert stamped["household_days"].schema["survey_complete"] == pl.Boolean
        assert stamped["household_days"].schema["usable_test"] == pl.Boolean


class TestSurveyableMembers:
    """The ALL reduction runs over surveyable member-days only."""

    def _two_members(self, surveyable: list[bool], survey_complete: list[bool]) -> dict:
        tables = household(2)
        tables["persons"] = tables["persons"].with_columns(pl.Series("surveyable", surveyable))
        tables["person_days"] = tables["person_days"].with_columns(
            pl.Series("survey_complete", survey_complete)
        )
        return tables

    def test_an_unsurveyable_member_cannot_veto_the_date(self):
        """A roommate who files nothing must not cost the household its day."""
        stamped = stamp(self._two_members([True, False], [True, False]), STRICT)

        assert stamped["household_days"]["survey_complete"].to_list() == [True]
        assert stamped["household_days"]["usable_test"].to_list() == [True]
        assert stamped["households"]["usable_test"].to_list() == [True]
        # Their own row keeps its own verdict rather than borrowing the good one.
        assert stamped["person_days"]["usable_test"].to_list() == [True, False]

    def test_a_date_with_no_surveyable_member_carries_no_usable_day(self):
        """The household-day exists, but nobody it was meant to observe was observed."""
        stamped = stamp(self._two_members([False, False], [False, False]), STRICT)

        assert stamped["household_days"]["survey_complete"].to_list() == [False]
        assert stamped["person_days"]["usable_test"].to_list() == [False, False]
        assert stamped["households"]["usable_test"].to_list() == [False]

    def test_a_contradictory_unsurveyable_day_is_believed_not_overruled(self):
        """An unsurveyable person marked complete keeps that verdict.

        The cascade does not correct the upstream defect, and it cannot rescue the
        household: the one surveyable member's day is incomplete.
        """
        stamped = stamp(self._two_members([True, False], [False, True]), STRICT)

        assert stamped["person_days"]["survey_complete"].to_list() == [False, True]
        assert stamped["household_days"]["usable_test"].to_list() == [False]
        assert stamped["households"]["usable_test"].to_list() == [False]


def test_an_orphan_subtour_keeps_its_own_verdict():
    """A parent missing from the frame is a different defect, not hidden behind a False."""
    stamped = stamp(household(1, parent_tour_id=[99]), STRICT)

    assert stamped["tours"]["usable_test"].to_list() == [True]


class TestTravelDays:
    """A day on a day of the week the profile does not take is unusable, with its travel."""

    TUE_THU = make_profile(travel_days=(TravelDow.TUESDAY, TravelDow.WEDNESDAY, TravelDow.THURSDAY))

    def _days_on(self, dows: list[int | None]) -> dict:
        """One person with one day per given weekday, each day carrying one tour and trip."""
        n = len(dows)
        ids = list(range(1, n + 1))
        tables = household(
            1,
            tour_id=[10 * i for i in ids],
            person_day_id=ids,
            person_id=[1] * n,
            survey_complete=[True] * n,
            parent_tour_id=[10 * i for i in ids],
            tour_data_quality=[TourDataQuality.VALID.value] * n,
            tour_category=[TourCategory.COMPLETE.value] * n,
        )
        tables["person_days"] = pl.DataFrame(
            {
                "person_day_id": ids,
                "person_id": [1] * n,
                "hh_id": [1] * n,
                "day_num": ids,
                "travel_date": [datetime(2023, 5, i) for i in ids],
                "travel_dow": dows,
                "survey_complete": [True] * n,
            },
            schema_overrides={"travel_dow": pl.Int64},
        )
        tables["linked_trips"] = pl.DataFrame(
            {
                "linked_trip_id": [100 * i for i in ids],
                "tour_id": [10 * i for i in ids],
                "person_day_id": ids,
                "survey_complete": [True] * n,
            }
        )
        return tables

    def test_a_day_the_profile_does_not_take_is_unusable_with_its_travel(self):
        """Saturday falls with its tour and trip; Tuesday keeps the household and person."""
        stamped = stamp(
            self._days_on([TravelDow.TUESDAY.value, TravelDow.SATURDAY.value]), self.TUE_THU
        )

        for table, key in (
            ("person_days", "person_day_id"),
            ("tours", "tour_id"),
            ("linked_trips", "linked_trip_id"),
            ("household_days", "hh_day_id"),
        ):
            assert stamped[table].sort(key)["usable_test"].to_list() == [True, False], table
        assert stamped["households"]["usable_test"].to_list() == [True]
        assert stamped["persons"]["usable_test"].to_list() == [True]

    def test_a_household_with_only_days_it_does_not_take_is_unusable(self):
        """Monday and Saturday are both outside Tue-Thu, so nothing is left to keep."""
        stamped = stamp(
            self._days_on([TravelDow.MONDAY.value, TravelDow.SATURDAY.value]), self.TUE_THU
        )

        assert stamped["households"]["usable_test"].to_list() == [False]
        assert stamped["persons"]["usable_test"].to_list() == [False]

    def test_a_day_whose_weekday_is_unknown_is_unusable(self):
        """A day that cannot be placed in the week cannot be shown to be one it takes."""
        stamped = stamp(self._days_on([TravelDow.TUESDAY.value, None]), self.TUE_THU)

        assert stamped["person_days"].sort("person_day_id")["usable_test"].to_list() == [
            True,
            False,
        ]

    def test_every_day_taken_reads_no_weekday(self):
        """A profile taking all seven days asks nothing of travel_dow, so it need not be there."""
        tables = self._days_on([TravelDow.SATURDAY.value])
        tables["person_days"] = tables["person_days"].drop("travel_dow")

        stamped = stamp(tables, STRICT)

        assert stamped["person_days"]["usable_test"].to_list() == [True]


class TestExcludedPersons:
    """A person the cleaner excluded has no usable day, under any profile."""

    def _two_members(self, reasons: list[str | None]) -> dict:
        """Two members on one date, each with one complete tour and trip."""
        tables = household(2)
        tables["persons"] = tables["persons"].with_columns(
            pl.Series("exclusion_reason", reasons, dtype=pl.String)
        )
        tables["linked_trips"] = pl.DataFrame(
            {
                "linked_trip_id": [100, 200],
                "tour_id": [10, 20],
                "person_day_id": [1, 2],
                "survey_complete": [True, True],
            }
        )
        return tables

    def test_the_excluded_persons_day_tour_and_trip_are_unusable(self):
        """Everything of the excluded member goes; the other member and household stay.

        ``survey_complete`` stays the vendor's verdict; only usability narrows.
        """
        stamped = stamp(self._two_members([None, EXCLUDED]), LOOSE)

        for table, key in (
            ("persons", "person_id"),
            ("person_days", "person_day_id"),
            ("tours", "tour_id"),
            ("linked_trips", "linked_trip_id"),
        ):
            assert stamped[table].sort(key)["usable_test"].to_list() == [True, False], table
        assert stamped["households"]["usable_test"].to_list() == [True]
        assert stamped["person_days"]["survey_complete"].to_list() == [True, True]

    def test_a_household_needing_every_member_loses_the_date(self):
        """An excluded member is an unusable member, so the whole date fails."""
        stamped = stamp(self._two_members([None, EXCLUDED]), STRICT)

        assert stamped["household_days"]["usable_test"].to_list() == [False]
        assert stamped["households"]["usable_test"].to_list() == [False]
        assert stamped["persons"]["usable_test"].to_list() == [False, False]

    def test_a_household_whose_members_are_all_excluded_is_unusable(self):
        """No household reason is needed: with no usable day, the household falls."""
        stamped = stamp(self._two_members([EXCLUDED, EXCLUDED]), LOOSE)

        assert stamped["households"]["usable_test"].to_list() == [False]

    def test_without_the_column_no_one_is_excluded(self):
        """The field defaults to null, so a table without it excludes no one."""
        tables = self._two_members([None, None])
        tables["persons"] = tables["persons"].drop("exclusion_reason")

        stamped = stamp(tables, STRICT)

        assert stamped["households"]["usable_test"].to_list() == [True]


class TestExcludedHouseholdsAndDays:
    """A household or day the cleaner excluded is unusable, under any profile."""

    REASON = "excluded"

    def _two_members_two_dates(
        self, *, household: str | None = None, days: tuple = (None, None, None, None)
    ) -> dict:
        """Two members, each with a complete no-travel day on two dates.

        Days 1 and 2 are on the first date (members 1 and 2), days 3 and 4 on the second.
        """
        second = datetime(2023, 5, 2)
        return {
            "households": pl.DataFrame(
                {"hh_id": [1], "survey_complete": [True], "exclusion_reason": [household]},
                schema_overrides={"exclusion_reason": pl.String},
            ),
            "persons": pl.DataFrame(
                {
                    "person_id": [1, 2],
                    "hh_id": [1, 1],
                    "survey_complete": [True, True],
                    "surveyable": [True, True],
                }
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [1, 2, 3, 4],
                    "person_id": [1, 2, 1, 2],
                    "hh_id": [1] * 4,
                    "travel_date": [DATE, DATE, second, second],
                    "survey_complete": [True] * 4,
                    "exclusion_reason": list(days),
                },
                schema_overrides={"exclusion_reason": pl.String},
            ),
        }

    def _usable_days(self, stamped: dict) -> list[bool]:
        return stamped["person_days"].sort("person_day_id")["usable_test"].to_list()

    @pytest.mark.parametrize("profile", [STRICT, LOOSE], ids=["all_members", "nothing"])
    def test_an_excluded_household_is_unusable_with_every_day(self, profile):
        """The household falls on its own reason, and every day in it with it."""
        stamped = stamp(self._two_members_two_dates(household=self.REASON), profile)

        assert stamped["households"]["usable_test"].to_list() == [False]
        assert self._usable_days(stamped) == [False] * 4

    def test_an_excluded_day_alone_is_unusable_where_no_whole_date_is_needed(self):
        """Without a whole-date rule, the other member's day on that date stands."""
        tables = self._two_members_two_dates(days=(None, self.REASON, None, None))

        stamped = stamp(tables, LOOSE)

        assert self._usable_days(stamped) == [True, False, True, True]
        assert stamped["households"]["usable_test"].to_list() == [True]

    def test_an_excluded_day_makes_its_date_incomplete_where_every_member_is_needed(self):
        """The other member's day on that date falls; the other date is untouched."""
        tables = self._two_members_two_dates(days=(None, self.REASON, None, None))

        stamped = stamp(tables, STRICT)

        assert self._usable_days(stamped) == [False, False, True, True]
        assert stamped["household_days"].sort("travel_date")["usable_test"].to_list() == [
            False,
            True,
        ]
        assert stamped["households"]["usable_test"].to_list() == [True]

    def test_survey_complete_is_untouched(self):
        """The vendor's verdict stays the vendor's; only usability narrows."""
        tables = self._two_members_two_dates(
            household=self.REASON, days=(None, self.REASON, None, None)
        )

        stamped = stamp(tables, STRICT)

        assert stamped["households"]["survey_complete"].to_list() == [True]
        assert stamped["person_days"]["survey_complete"].to_list() == [True] * 4

    def test_without_the_columns_nothing_is_excluded(self):
        """The field defaults to null, so tables without it exclude nothing."""
        tables = self._two_members_two_dates()
        tables["households"] = tables["households"].drop("exclusion_reason")
        tables["person_days"] = tables["person_days"].drop("exclusion_reason")

        stamped = stamp(tables, STRICT)

        assert self._usable_days(stamped) == [True] * 4


class TestATourNeedsEveryTrip:
    """One unusable trip fails its tour, which then takes the rest of its trips down."""

    def _two_tours(self, table: str, on_tour_10: list[bool]) -> dict:
        """Tour 10 carries the given trips; tour 11 carries one complete trip."""
        tables = _one_day_two_tours()
        tables[table] = pl.DataFrame(
            {
                "trip_id": list(range(len(on_tour_10) + 1)),
                "tour_id": [10] * len(on_tour_10) + [11],
                "person_day_id": [1] * (len(on_tour_10) + 1),
                "survey_complete": [*on_tour_10, True],
            }
        )
        return tables

    @pytest.mark.parametrize("table", ["linked_trips", "unlinked_trips"])
    def test_one_unusable_trip_fails_its_tour_and_its_other_trips(self, table: str):
        """Tour 10 falls with its usable trip; tour 11 and the day survive."""
        stamped = stamp(self._two_tours(table, on_tour_10=[True, False]), STRICT)

        assert stamped["tours"].sort("tour_id")["usable_test"].to_list() == [False, True]
        assert stamped[table].sort("trip_id")["usable_test"].to_list() == [False, False, True]
        assert stamped["person_days"]["usable_test"].to_list() == [True]

    def test_every_trip_usable_leaves_the_tour_alone(self):
        """The rule changes nothing when every trip is usable."""
        stamped = stamp(self._two_tours("linked_trips", on_tour_10=[True, True]), STRICT)

        assert stamped["tours"].sort("tour_id")["usable_test"].to_list() == [True, True]


class TestJointQuorum:
    """A grouping is joint only while enough members survive."""

    @pytest.mark.parametrize(
        ("n_valid", "joint"),
        [
            pytest.param(2, True, id="exactly_the_minimum_survives"),
            # A one-participant joint tour would violate CT-RAMP's num_participants.
            pytest.param(1, False, id="one_below_the_minimum_is_no_longer_joint"),
        ],
    )
    def test_a_joint_tour_needs_two_usable_members(self, n_valid, joint):
        """Two usable members keep the joint tour; one does not."""
        valid, gap = TourDataQuality.VALID.value, TourDataQuality.SPATIAL_GAP.value
        qualities = [valid] * n_valid + [gap] * (3 - n_valid)
        tables = household(3, tour_data_quality=qualities, joint_tour_id=[1] * 3)
        tables["joint_tours"] = pl.DataFrame(
            {"joint_tour_id": [1], "hh_id": [1], "person_day_id": [1], "survey_complete": [True]}
        )

        stamped = stamp(tables, WIDEST)

        assert stamped["tours"]["usable_widest"].to_list()[:n_valid] == [True] * n_valid
        assert stamped["joint_tours"]["usable_widest"].to_list() == [joint]


def test_a_repeated_pass_is_idempotent():
    """Re-running the step on resume must not compound its own output."""
    tables = stamp(_one_day_two_tours(TourDataQuality.SPATIAL_GAP), STRICT)
    first = {name: df["usable_test"].to_list() for name, df in tables.items()}
    columns_before = {name: df.columns for name, df in tables.items()}

    working: dict[str, pl.DataFrame | None] = dict(tables)
    stamp_usable(working, STRICT)

    assert {name: working[name]["usable_test"].to_list() for name in first} == first
    # The second pass writes the same two names, never a suffixed copy.
    assert {name: working[name].columns for name in columns_before} == columns_before


def test_no_two_profile_names_share_a_verdict_column():
    """Every name writes its own ``usable_``-prefixed column, all distinct.

    The names include ones that would collide without the prefix, and the prefix itself.
    """
    adversarial = ("a", "hh_day_a", "usable_a", "usable", "hh_day")
    adversarial += ("hh_day_usable_a", "usable_hh_day_a")
    profiles = parse_usability_profiles({n: config_axes() for n in adversarial})

    written = [p.flag for p in profiles]
    assert len(set(written)) == len(written) == len(adversarial)
    assert all(p.flag.startswith("usable_") for p in profiles)
    by_name = {p.name: p for p in profiles}
    assert (by_name["a"].flag, by_name["hh_day_a"].flag) == ("usable_a", "usable_hh_day_a")


class TestTheDidYouMeanLine:
    """The candidates a consumer is offered when its column is not there.

    A profile's name comes from config, so the line offers every boolean column
    rather than filtering to a shape it cannot know.
    """

    def test_every_boolean_is_offered_in_order(self):
        """Freely named profiles included, non-booleans excluded, sorted by name."""
        frame = pl.DataFrame(
            {
                "zeta": [True],
                "tour_id": [1],
                "keep_for_ctramp": [True],
                "tour_purpose": ["work"],
                "alpha": [True],
            }
        )

        line = suggest_usability_columns(frame)

        assert line.endswith("alpha, keep_for_ctramp, zeta.")
        assert "tour_purpose" not in line
        assert "tour_id" not in line

    def test_a_frame_with_no_booleans_says_so(self):
        """Better than an empty list, which reads as a truncated message."""
        frame = pl.DataFrame({"tour_id": [1], "tour_purpose": ["work"]})

        assert suggest_usability_columns(frame) == "It carries no boolean columns at all."
