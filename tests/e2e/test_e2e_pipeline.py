"""End-to-end tests of the survey-processing pipeline on the toy survey.

``TestStepToggling`` runs on every profile in ``conftest.PROFILES``; the rest read the
full run and assert what only a whole run shows. The committed baseline says that a
number moved; these say which rule broke.
"""

from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPTourCategory
from data_canon.codebook.daysim import DaysimPurpose
from data_canon.codebook.households import IncomeBroad
from tests.e2e.assertions import assert_referential_integrity, assert_tables_non_empty
from tests.e2e.conftest import FORMATTED_TABLES

# Canonical tables produced whichever steps a profile drops.
CORE_TABLES = ["households", "persons", "person_days", "unlinked_trips", "linked_trips", "tours"]

_INCOME_MISSING = IncomeBroad.MISSING.value

# The week diaries run Monday (1) to Sunday (7) and the strict profiles take
# Tuesday to Thursday, so a test about one household's travel reads its Tuesday.
TUESDAY = 2


class TestStepToggling:
    def test_core_tables_populated(self, profile_run):
        _steps, result, _out = profile_run
        assert_tables_non_empty(result, CORE_TABLES)

    def test_referential_integrity(self, profile_run):
        _steps, result, _out = profile_run
        assert_referential_integrity(result)

    def test_enabled_formatters_produced(self, profile_run):
        steps, result, _out = profile_run
        expected = [t for step, tables in FORMATTED_TABLES.items() if step in steps for t in tables]
        assert_tables_non_empty(result, expected)

    def test_core_output_files_written(self, profile_run):
        _steps, _result, output_dir = profile_run
        for table in CORE_TABLES:
            path = Path(output_dir) / "survey" / f"{table}.csv"
            assert path.exists(), f"Missing output file: {path}"
            assert path.stat().st_size > 0, f"Empty output file: {path}"


class TestDaysimTourLinkage:
    """DaySim trips and tours reference each other; schema validation cannot see a wrong tour."""

    _KEY: ClassVar[list[str]] = ["hhno", "pno", "day", "tour"]

    def test_no_trip_references_a_missing_tour(self, full_result):
        tour_keys = full_result.tours_daysim.select(self._KEY).unique()
        trip_keys = full_result.linked_trips_daysim.select(self._KEY).unique()
        orphans = trip_keys.join(tour_keys, on=self._KEY, how="anti")
        assert orphans.height == 0, f"trips reference non-existent tours:\n{orphans}"

    def test_no_tour_is_left_without_trips(self, full_result):
        tour_keys = full_result.tours_daysim.select(self._KEY).unique()
        trip_keys = full_result.linked_trips_daysim.select(self._KEY).unique()
        childless = tour_keys.join(trip_keys, on=self._KEY, how="anti")
        assert childless.height == 0, f"tour records with no trips:\n{childless}"


class TestImputation:
    def test_missing_income_bins_were_imputed(self, full_result):
        hh = full_result.households
        stashed_missing = hh.filter(pl.col("income_bin_preimputed") == _INCOME_MISSING)
        assert stashed_missing.height > 0, "expected some stashed MISSING income bins"
        assert (stashed_missing["income_bin"] != _INCOME_MISSING).all(), (
            "MISSING income bins should have been imputed to valid values"
        )

    def test_unmissing_values_unchanged(self, full_result):
        hh = full_result.households
        kept = hh.filter(pl.col("income_bin_preimputed") != _INCOME_MISSING)
        assert (kept["income_bin"] == kept["income_bin_preimputed"]).all()


class TestEdgeCaseCoverage:
    """The toy population still spans the categories the formatters branch on."""

    @pytest.mark.parametrize(
        ("table", "column", "expected"),
        [
            (
                "persons_ctramp",
                "type",
                {
                    "Full-time worker",
                    "Part-time worker",
                    "University student",
                    "Non-worker",
                    "Retired",
                    "Student of driving age",
                    "Student of non-driving age",
                    "Child too young for school",
                },
            ),
            (
                "mandatory_locations_ctramp",
                "StudentCategory",
                {"College or higher", "Grade or high school", "Not student"},
            ),
            ("persons_ctramp", "activity_pattern", {"M", "N", "H"}),
            # VALID(0), PARTIAL_DIARY_EDGE(3), NO_DESTINATION(4), SPATIAL_GAP(5).
            # Absent by construction: a person with a second home (1) and a chain
            # resuming across diary days (2); both are covered by unit tests.
            ("tours", "tour_data_quality", {0, 3, 4, 5}),
            # COMPLETE(1), PARTIAL_END(2), PARTIAL_START(3), PARTIAL_BOTH(4).
            ("tours", "tour_category", {1, 2, 3, 4}),
            # ADULTS_ONLY(1), CHILDREN_ONLY(2), ADULTS_AND_CHILDREN(3).
            ("joint_tours_ctramp", "tour_composition", {1, 2, 3}),
        ],
        ids=lambda v: v if isinstance(v, str) else "",
    )
    def test_every_category_the_formatters_branch_on_is_exercised(
        self, full_result, table, column, expected
    ):
        present = set(getattr(full_result, table)[column].to_list())
        assert expected <= present, f"{table}.{column} is missing {expected - present}"

    def test_no_destination_means_a_closed_tour_without_a_purpose(self, full_result):
        """A tour is NO_DESTINATION exactly when it is COMPLETE and has no purpose."""
        tours = full_result.tours
        nothing_to_anchor_on = pl.col("tour_purpose").is_null() & (pl.col("tour_category") == 1)
        assert tours.filter((pl.col("tour_data_quality") == 4) & ~nothing_to_anchor_on).is_empty()
        assert tours.filter((pl.col("tour_data_quality") != 4) & nothing_to_anchor_on).is_empty()


class TestWorkRelatedTours:
    """HH 31 and 32: work-related stops, and workplaces the survey never asked for.

    3101 is employed with no reported workplace and a full day at one place;
    3102 is retired and volunteers; 3201 is employed with no reported workplace
    and only a short meeting. CT-RAMP person ids are the survey person_day_id.
    """

    OBSERVED_WORKPLACE, RETIREE, NO_WORKPLACE = (pid * 100 + TUESDAY for pid in (3101, 3102, 3201))

    @staticmethod
    def _ctramp(full_result, person_day):
        tour = full_result.individual_tours_ctramp.filter(pl.col("person_id") == person_day)
        location = full_result.mandatory_locations_ctramp.filter(pl.col("PersonID") == person_day)
        return tour.row(0, named=True), location.row(0, named=True)

    def test_an_employed_persons_work_related_tour_is_work(self, full_result):
        """And their observed workplace stands in as the usual one (the toy zone is 1)."""
        tour, location = self._ctramp(full_result, self.OBSERVED_WORKPLACE)

        assert tour["tour_purpose"].startswith("work_")
        assert tour["tour_category"] == CTRAMPTourCategory.MANDATORY.value
        assert location["WorkLocation"] == 1

    def test_a_retirees_work_related_tour_is_not_work(self, full_result):
        """Volunteering by someone without a job stays non-mandatory."""
        tour, _ = self._ctramp(full_result, self.RETIREE)

        assert tour["tour_purpose"] == "othdiscr"

    def test_a_work_tour_with_no_usual_workplace_stays_work(self, full_result):
        """Too short a stop to be a workplace: still a work tour, with location 0."""
        tour, location = self._ctramp(full_result, self.NO_WORKPLACE)

        assert tour["tour_purpose"].startswith("work_")
        assert location["WorkLocation"] == 0

    def test_daysim_agrees(self, full_result):
        """The same three tours, and the same usual workplace, in DaySim."""
        tours = full_result.tours_daysim
        persons = full_result.persons_daysim

        def pdpurp(hhno, pno):
            return tours.filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno))["pdpurp"][0]

        assert pdpurp(31, 1) == DaysimPurpose.WORK.value
        assert pdpurp(31, 2) == DaysimPurpose.PERSONAL_BUSINESS.value
        assert pdpurp(32, 1) == DaysimPurpose.WORK.value
        assert persons.filter((pl.col("hhno") == 31) & (pl.col("pno") == 1))["pwtaz"][0] == 1


class TestTravelDays:
    def test_ctramp_writes_only_the_days_its_profile_takes(self, full_result):
        """Tuesday to Thursday; CT-RAMP person ids are the survey person_day_id."""
        dow = full_result.person_days.select("person_day_id", "travel_dow")
        written = full_result.persons_ctramp.join(
            dow, left_on="person_id", right_on="person_day_id", how="left"
        )
        assert written["travel_dow"].null_count() == 0
        assert set(written["travel_dow"].to_list()) == {2, 3, 4}


class TestExcludedPersons:
    """HH 33 reported by form, and the cleaner excluded its only member for it."""

    HH = 33

    def test_no_output_carries_it(self, full_result):
        """CT-RAMP ids are the survey hh_id times 100 plus the day number."""
        ctramp = full_result.households_ctramp.filter(pl.col("hh_id") // 100 == self.HH)
        daysim = full_result.households_daysim.filter(pl.col("hhno") == self.HH)
        assert ctramp.is_empty()
        assert daysim.is_empty()

    def test_the_reason_reaches_the_delivered_persons(self, full_result):
        person = full_result.persons.filter(pl.col("hh_id") == self.HH)
        assert person["exclusion_reason"].to_list() == ["survey_tech excluded: browser"]
