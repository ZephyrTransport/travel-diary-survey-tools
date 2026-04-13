"""Tests for incidence-sum checksums."""

import logging

import polars as pl
import pytest

from processing.weighting.controls.base import ControlLevel
from processing.weighting.validation.checksums import check_incidence_sums, check_recode_nulls


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def good_seed() -> pl.DataFrame:
    """Seed table where all controls are properly accounted for.

    3 households:
      HH1: 1 person (male, employed_full)
      HH2: 3 persons (1 male + 2 female, 1 employed_full + 1 employed_part + 1 not_employed)
      HH3: 2 persons (1 male + 1 female, 2 not_employed)
    """
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "p_total": [1, 3, 2],
            "h_income__low": [1, 0, 0],
            "h_income__mid": [0, 1, 0],
            "h_income__high": [0, 0, 1],
            "p_gender__male": [1, 1, 1],
            "p_gender__female": [0, 2, 1],
            "p_employment__employed_full": [1, 1, 0],
            "p_employment__employed_part": [0, 1, 0],
            "p_employment__not_employed": [0, 1, 2],
        },
    )


@pytest.fixture
def overcount_seed() -> pl.DataFrame:
    """Seed where p_gender incidence sums > p_total for HH3 (double-classified)."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "p_total": [1, 3, 2],
            "h_income__low": [1, 0, 0],
            "h_income__mid": [0, 1, 0],
            "h_income__high": [0, 0, 1],
            # HH3: 3 gender incidence but only 2 persons (double-classified)
            "p_gender__male": [1, 1, 2],
            "p_gender__female": [0, 2, 1],
            "p_employment__employed_full": [1, 1, 0],
            "p_employment__employed_part": [0, 1, 0],
            "p_employment__not_employed": [0, 1, 2],
        },
    )


# ---------------------------------------------------------------------------
# check_incidence_sums
# ---------------------------------------------------------------------------
class TestCheckIncidenceSums:
    """Tests for incidence-sum validation on the seed table."""

    def test_passes_on_correct_seed(self, good_seed, caplog):
        """No error when all incidence sums match structural totals."""
        with caplog.at_level(logging.INFO):
            check_incidence_sums(
                good_seed,
                ["h_income", "p_gender", "p_employment"],
                source_label="test",
            )
        assert "all controls pass" in caplog.text

    def test_undercount_raises(self, good_seed):
        """Undercount (< p_total) now raises — nulls should be filled."""
        seed = good_seed.with_columns(
            # HH2 employment sums to 2 instead of 3 (undercount)
            pl.when(pl.col("hh_id") == 2)
            .then(0)
            .otherwise(pl.col("p_employment__employed_part"))
            .alias("p_employment__employed_part")
        )
        with pytest.raises(ValueError, match="p_employment"):
            check_incidence_sums(seed, ["p_employment"], source_label="test")

    def test_fails_on_person_overcount(self, overcount_seed):
        """Overcount in person control should raise ValueError."""
        with pytest.raises(ValueError, match="p_gender"):
            check_incidence_sums(overcount_seed, ["p_gender"], source_label="test")

    def test_fails_on_hh_control_mismatch(self):
        """HH-level control with wrong sum raises ValueError (no h_total needed)."""
        seed = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "h_income__low": [1, 0],
                "h_income__mid": [0, 0],  # HH2 sums to 0 instead of 1
                "h_income__high": [0, 0],
            },
        )
        with pytest.raises(ValueError, match="h_income"):
            check_incidence_sums(seed, ["h_income"], source_label="test")

    def test_tolerance_allows_fractional_drift(self, good_seed):
        """Small floating-point deviation passes with tolerance."""
        seed = good_seed.with_columns(
            (pl.col("p_gender__male").cast(pl.Float64) + 0.005).alias("p_gender__male")
        )
        # Fails with zero tolerance
        with pytest.raises(ValueError, match="p_gender"):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        # Passes with tolerance
        check_incidence_sums(seed, ["p_gender"], source_label="test", tolerance=0.01)

    def test_error_shows_hh_id_and_counts(self, overcount_seed):
        """Error message should contain hh_id, actual, and expected."""
        with pytest.raises(ValueError, match="p_gender") as exc_info:
            check_incidence_sums(overcount_seed, ["p_gender"], source_label="survey")
        msg = str(exc_info.value)
        assert "3" in msg  # hh_id 3
        assert "survey" in msg

    def test_unknown_target_skipped(self, good_seed, caplog):
        """Unknown target names are silently skipped."""
        with caplog.at_level(logging.INFO):
            check_incidence_sums(good_seed, ["bogus_control"], source_label="test")
        assert "all controls pass" in caplog.text

    def test_missing_incidence_cols_skipped(self, caplog):
        """Person control with no __ columns is silently skipped."""
        seed = pl.DataFrame({"hh_id": [1], "p_total": [1]})
        with caplog.at_level(logging.INFO):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        assert "all controls pass" in caplog.text

    def test_warns_when_p_total_missing(self, caplog):
        """Person-control check warns and skips when p_total is absent."""
        seed = pl.DataFrame(
            {
                "hh_id": [1],
                "p_gender__male": [1],
                "p_gender__female": [0],
            },
        )
        with caplog.at_level(logging.WARNING):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        assert "p_total column missing" in caplog.text


# ---------------------------------------------------------------------------
# check_recode_nulls
# ---------------------------------------------------------------------------
class TestCheckRecodeNulls:
    """Tests for null-detection on recoded DataFrames (pre-aggregation)."""

    def test_passes_when_all_classified(self, caplog):
        """No warning when every record is non-null."""
        df = pl.DataFrame({"person_id": [1, 2], "p_gender": [1, 2]})
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                ["p_gender"],
                level=ControlLevel.PERSON,
                id_col="person_id",
                source_label="test",
            )
        assert "all records classified" in caplog.text

    def test_warns_on_null_person_control(self, caplog):
        """Null in person control should warn, not raise."""
        df = pl.DataFrame({"person_id": [1, 2, 3], "p_gender": [1, None, 2]})
        with caplog.at_level(logging.WARNING):
            check_recode_nulls(
                df,
                ["p_gender"],
                level=ControlLevel.PERSON,
                id_col="person_id",
                source_label="PUMS",
            )
        assert "Null recode" in caplog.text
        assert "2" in caplog.text  # person_id 2
        assert "p_gender" in caplog.text

    def test_warns_on_null_hh_control(self, caplog):
        """Null in HH control should warn, not raise."""
        df = pl.DataFrame({"hh_id": [10, 20], "h_workers": [1, None]})
        with caplog.at_level(logging.WARNING):
            check_recode_nulls(
                df,
                ["h_workers"],
                level=ControlLevel.HOUSEHOLD,
                id_col="hh_id",
                source_label="survey",
            )
        assert "Null recode" in caplog.text
        assert "20" in caplog.text  # hh_id 20

    def test_does_not_raise(self):
        """Null recode check must never raise ValueError."""
        df = pl.DataFrame({"person_id": [1], "p_gender": [None]})
        # Should complete without exception
        check_recode_nulls(
            df,
            ["p_gender"],
            level=ControlLevel.PERSON,
            id_col="person_id",
            source_label="test",
        )

    def test_filters_by_level(self, caplog):
        """Only controls matching the requested level are checked."""
        df = pl.DataFrame({"hh_id": [1], "h_workers": [None], "p_gender": [None]})
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                ["h_workers", "p_gender"],
                level=ControlLevel.HOUSEHOLD,
                id_col="hh_id",
                source_label="test",
            )
        # h_workers is HH → should be flagged
        assert "h_workers" in caplog.text
        # p_gender is person → should NOT appear (wrong level)
        assert "p_gender" not in caplog.text

    def test_unknown_target_skipped(self, caplog):
        """Unknown targets are silently skipped."""
        df = pl.DataFrame({"person_id": [1], "p_gender": [1]})
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                ["bogus_control"],
                level=ControlLevel.PERSON,
                id_col="person_id",
                source_label="test",
            )
        assert "all records classified" in caplog.text

    def test_missing_column_skipped(self, caplog):
        """Control column not present in DataFrame is skipped."""
        df = pl.DataFrame({"person_id": [1]})
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                ["p_gender"],
                level=ControlLevel.PERSON,
                id_col="person_id",
                source_label="test",
            )
        assert "all records classified" in caplog.text
