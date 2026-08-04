"""End-to-end integration tests for the survey-processing pipeline.

Two kinds of tests:

* ``TestStepToggling`` is parametrized over the toggle *profiles* (see
  ``conftest.PROFILES``) — a leave-one-out matrix over the optional steps. It
  verifies that turning an optional step on/off does not break the downstream
  steps (the pipeline still completes and produces valid, referentially-consistent
  output). The enabled formatters/joint tables must appear; the disabled ones must
  not be required.

* The remaining classes use the ``full_result`` fixture (all optional steps on)
  to assert specific behaviours: trip linking, joint detection, tour extraction,
  imputation stashing, and the systematic edge-case coverage matrix (see
  tests/e2e/COVERAGE.md).
"""

from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPTourCategory
from data_canon.codebook.tours import TourCategory, TourDataQuality, TourDirection, TourType
from tests.e2e.assertions import assert_referential_integrity, assert_tables_non_empty

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# Core canonical tables produced regardless of the optional-step toggles.
CORE_TABLES = ["households", "persons", "days", "unlinked_trips", "linked_trips", "tours"]

_DAYSIM_TABLES = [
    "households_daysim",
    "persons_daysim",
    "days_daysim",
    "linked_trips_daysim",
    "tours_daysim",
]
_CTRAMP_TABLES = [
    "households_ctramp",
    "persons_ctramp",
    "individual_tours_ctramp",
    "individual_trips_ctramp",
]

_INCOME_MISSING = 995  # IncomeBroad.MISSING


# ── Step toggling (parametrized over profiles) ────────────────────────
# Runs the whole matrix: for each profile, prove toggling a step off/on does
# not break the downstream steps.


class TestStepToggling:
    def test_core_tables_populated(self, profile_run):
        _name, _enabled, result, _out = profile_run
        assert_tables_non_empty(result, CORE_TABLES)

    def test_referential_integrity(self, profile_run):
        _name, _enabled, result, _out = profile_run
        assert_referential_integrity(result)

    def test_tours_extracted_regardless_of_upstream(self, profile_run):
        # extract_tours is downstream of both imputation and detect_joint_trips;
        # it must still produce tours whether or not those upstream steps ran.
        _name, _enabled, result, _out = profile_run
        assert result.tours is not None
        assert result.tours.height > 0

    def test_enabled_formatters_produced(self, profile_run):
        _name, enabled, result, _out = profile_run
        if "format_daysim" in enabled:
            for name in _DAYSIM_TABLES:
                df = getattr(result, name, None)
                assert df is not None and df.height > 0, f"DaySim table '{name}' missing/empty"
        if "format_ctramp" in enabled:
            for name in _CTRAMP_TABLES:
                df = getattr(result, name, None)
                assert df is not None and df.height > 0, f"CT-RAMP table '{name}' missing/empty"

    def test_joint_trips_gated_on_step(self, profile_run):
        # When detect_joint_trips is on, the joint_trips table exists; when off,
        # the run must still have completed (asserted above) -> downstream steps
        # tolerate its absence.
        _name, enabled, result, _out = profile_run
        if "detect_joint_trips" in enabled:
            assert result.joint_trips is not None
            assert isinstance(result.joint_trips, pl.DataFrame)

    def test_core_output_files_written(self, profile_run):
        _name, _enabled, _result, output_dir = profile_run
        survey = Path(output_dir) / "survey"
        for fname in ["households", "persons", "days", "unlinked_trips", "linked_trips", "tours"]:
            path = survey / f"{fname}.csv"
            assert path.exists(), f"Missing output file: {path}"
            assert path.stat().st_size > 0, f"Empty output file: {path}"


# ── Trip linking (full profile) ───────────────────────────────────────


class TestTripLinking:
    def test_transit_trips_linked(self, full_result):
        """HH2's four transit segments should merge into 2 linked trips."""
        hh2 = full_result.linked_trips.filter(pl.col("hh_id") == 2)
        assert hh2.height == 2, f"Expected 2 linked trips for HH2, got {hh2.height}"

    def test_linked_trip_count_reasonable(self, full_result):
        assert full_result.linked_trips.height <= full_result.unlinked_trips.height


# ── Joint trips (full profile) ────────────────────────────────────────


class TestJointTrips:
    def test_joint_trips_detected(self, full_result):
        jt = full_result.joint_trips
        assert jt is not None
        if jt.height > 0:
            assert jt.filter(pl.col("hh_id") == 3).height > 0, "Expected joint trips for HH3"


# ── Tour extraction (full profile) ────────────────────────────────────


class TestTourExtraction:
    def test_simple_commute_tour(self, full_result):
        assert full_result.tours.filter(pl.col("hh_id") == 1).height >= 1

    def test_multi_stop_tour(self, full_result):
        assert full_result.tours.filter(pl.col("hh_id") == 4).height >= 1

    def test_single_trip_tour_flagged(self, full_result):
        hh7 = full_result.tours.filter(pl.col("hh_id") == 7)
        assert hh7.height >= 1
        if "single_trip_tour" in hh7.columns:
            assert hh7["single_trip_tour"].to_list()[0] is True

    def test_at_work_subtour_extracted(self, full_result):
        # HH 6 is home -> work -> lunch -> work -> home. The lunch leg is an
        # at-work subtour: anchored at the workplace, so it never touches home
        # and is only COMPLETE against its own anchor (issue #85).
        hh6 = full_result.tours.filter(pl.col("hh_id") == 6)
        subtours = hh6.filter(pl.col("tour_type") == TourType.WORK_BASED.value)
        assert subtours.height == 1, f"expected one at-work subtour for HH 6, got {hh6.height}"
        assert subtours["tour_category"].to_list() == [TourCategory.COMPLETE.value]
        assert subtours["tour_data_quality"].to_list() == [TourDataQuality.VALID.value]

    def test_at_work_subtour_reaches_ctramp(self, full_result):
        # The whole point of #85: the subtour must survive the model_usable gate
        # and the CT-RAMP drop, and be emitted as an AT_WORK tour whose parent
        # reports it in atWork_freq.
        tours = full_result.individual_tours_ctramp
        hh6 = tours.filter(pl.col("hh_id") == 6)
        at_work = hh6.filter(pl.col("tour_category") == CTRAMPTourCategory.AT_WORK.value)
        assert at_work.height == 1, "HH 6's at-work subtour is missing from CT-RAMP output"
        assert hh6["tour_id"].n_unique() == hh6.height, (
            "a work tour and its at-work subtour must not share a CT-RAMP tour_id"
        )
        assert hh6["atWork_freq"].max() == 1, "the parent work tour should report one subtour"

    def test_subtour_trips_have_a_real_direction(self, full_result):
        # A subtour is split into halves against its own anchor (the workplace),
        # like any other tour. Stamping both legs with a "subtour" sentinel threw
        # the direction away, which DaySim (half) and CT-RAMP (inbound) require.
        subtour_trips = full_result.linked_trips.filter(pl.col("subtour_num") > 0).sort(
            "depart_time"
        )
        assert subtour_trips.height == 2
        assert subtour_trips["tour_direction"].to_list() == [
            TourDirection.OUTBOUND.value,
            TourDirection.INBOUND.value,
        ]


class TestDaysimTourLinkage:
    """DaySim trips must reference the tour records they belong to.

    Schema validation alone does not cover this: a subtour trip filed under its
    parent's tour number is individually well-formed, it just points at the
    wrong tour. These assertions are what catch that.
    """

    _KEY: ClassVar[list[str]] = ["hhno", "pno", "day", "tour"]

    def test_no_trip_references_a_missing_tour(self, full_result):
        tour_keys = full_result.tours_daysim.select(self._KEY).unique()
        trip_keys = full_result.linked_trips_daysim.select(self._KEY).unique()
        orphans = trip_keys.join(tour_keys, on=self._KEY, how="anti")
        assert orphans.height == 0, f"trips reference non-existent tours:\n{orphans}"

    def test_no_tour_is_left_without_trips(self, full_result):
        # The subtour's own tour record went trip-less when its trips were
        # numbered by tour_num (shared with the parent) instead of tour_id.
        tour_keys = full_result.tours_daysim.select(self._KEY).unique()
        trip_keys = full_result.linked_trips_daysim.select(self._KEY).unique()
        childless = tour_keys.join(trip_keys, on=self._KEY, how="anti")
        assert childless.height == 0, f"tour records with no trips:\n{childless}"

    def test_half_is_only_ever_outbound_or_inbound(self, full_result):
        halves = set(full_result.linked_trips_daysim["half"].unique().to_list())
        assert halves <= {1, 2}, f"DaySim half must be 1 or 2, got {sorted(halves)}"


# ── Imputation (full profile) ─────────────────────────────────────────
# The full profile runs imputation (RF on households.income_bin); these exercise
# the pre-imputation stash + RF feature importance.


class TestImputation:
    def test_preimputed_stash_created(self, full_result):
        assert "income_bin_preimputed" in full_result.households.columns

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

    def test_rf_feature_importance_produced(self, full_result):
        fi = getattr(full_result, "_feature_importance", None)
        assert fi is not None and fi.height > 0, "expected RF feature importance output"
        assert "income_bin" in fi["column"].unique().to_list()


# ── Edge-case coverage (full profile; enforces the classification matrix) ──
# Assert that the synthetic data exercises each distinct classification bucket the
# formatters branch on. If a scenario household is removed, the relevant assertion
# fails. See tests/e2e/COVERAGE.md for the scenario -> edge-case map.


class TestEdgeCaseCoverage:
    def test_all_person_types_present(self, full_result):
        types = set(full_result.persons_ctramp["type"].to_list())
        expected = {
            "Full-time worker",
            "Part-time worker",
            "University student",
            "Nonworker",
            "Retired",
            "Child of driving age",
            "Child of non-driving age",
            "Child too young for school",
        }
        assert expected <= types, f"missing person types: {expected - types}"

    def test_all_student_categories_present(self, full_result):
        cats = set(full_result.mandatory_locations_ctramp["StudentCategory"].to_list())
        assert {"College or higher", "Grade or high school", "Not a student"} <= cats

    def test_all_activity_patterns_present(self, full_result):
        patterns = set(full_result.persons_ctramp["activity_pattern"].to_list())
        assert {"M", "N", "H"} <= patterns, f"missing activity patterns: {patterns}"

    def test_tour_data_quality_buckets_present(self, full_result):
        # VALID(0), SINGLE_TRIP(1), LOOP_TRIP(2), MISSING_ANCHOR(3),
        # CHANGE_MODE(5). INDETERMINATE(4) is a contradictory diagnostic bucket
        # not representable with clean input, so it is intentionally excluded.
        q = set(full_result.tours["tour_data_quality"].to_list())
        assert {0, 1, 2, 3, 5} <= q, f"missing tour_data_quality buckets: {q}"

    def test_all_tour_categories_present(self, full_result):
        # COMPLETE(1), PARTIAL_END(2), PARTIAL_START(3), PARTIAL_BOTH(4).
        cats = set(full_result.tours["tour_category"].to_list())
        assert {1, 2, 3, 4} <= cats, f"missing tour_category buckets: {cats}"

    def test_all_joint_compositions_present(self, full_result):
        # ADULTS_ONLY(1), CHILDREN_ONLY(2), ADULTS_AND_CHILDREN(3).
        comps = set(full_result.joint_tours_ctramp["tour_composition"].to_list())
        assert {1, 2, 3} <= comps, f"missing joint tour compositions: {comps}"
