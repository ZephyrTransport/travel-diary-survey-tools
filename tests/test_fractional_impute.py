"""Tests for fractional imputation of null-deficient incidence rows.

Uses the real control registry (auto-registered at import) with synthetic
survey/PUMS data.  No custom controls are registered, so there is no
registry pollution across test files.
"""

import numpy as np
import polars as pl

from processing.weighting.data_prep.control_data import (
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.fractional_impute import (
    _train_and_predict,
    fill_null_incidence,
)
from processing.weighting.data_prep.incidence import (
    build_incidence_table,
    pivot_person_controls,
)
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
)

# We use a small subset of real controls that are always registered.
TARGETS = ["h_total", "h_size", "h_income", "p_total", "p_gender", "p_age"]


# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------
def _null_mask(df: pl.DataFrame, cols: list[str]) -> pl.Series:
    """Boolean mask: True where all *cols* are zero (null-encoded row)."""
    return df.select(pl.sum_horizontal(cols).eq(0)).to_series()


def _make_pums_hh(n: int = 50) -> pl.DataFrame:
    """Generate synthetic PUMS households (complete, no nulls)."""
    rng = np.random.default_rng(42)
    return pl.DataFrame(
        {
            "SERIALNO": list(range(1, n + 1)),
            "NP": rng.choice([1, 2, 3, 4], n).tolist(),
            "HINCP": rng.choice([10000, 30000, 60000, 100000, 200000], n).tolist(),
            "VEH": rng.choice([0, 1, 2, 3], n).tolist(),
            "WGTP": [10] * n,
            "PUMA": ["00100"] * n,
        }
    )


def _make_pums_per(hh_df: pl.DataFrame) -> pl.DataFrame:
    """Generate synthetic PUMS persons matching household sizes."""
    rng = np.random.default_rng(43)
    rows = []
    for row in hh_df.iter_rows(named=True):
        serialno = row["SERIALNO"]
        n_persons = row["NP"]
        rows.extend(
            {
                "SERIALNO": serialno,
                "SPORDER": sp,
                "SEX": rng.choice([1, 2]),
                "AGEP": int(rng.choice([5, 20, 35, 55, 75])),
                "ESR": int(rng.choice([1, 2, 3, 6])),
                "SCHG": int(rng.choice([0, 1, 15])),
                "SCHL": int(rng.choice([16, 20, 21, 24])),
                "RAC1P": int(rng.choice([1, 2, 6])),
                "HISP": int(rng.choice([1, 2])),
                "JWTRNS": int(rng.choice([1, 2, 10])),
                "PUMA": "00100",
            }
            for sp in range(1, n_persons + 1)
        )
    return pl.DataFrame(rows)


def _make_survey_hh(n: int = 30, null_income_rate: float = 0.0) -> pl.DataFrame:
    """Generate synthetic survey households with optional null income."""
    rng = np.random.default_rng(44)
    income_values = [1, 2, 3, 4, 5, 6]  # IncomeBroad enum values
    incomes = rng.choice(income_values, n).tolist()
    if null_income_rate > 0:
        n_null = max(1, int(n * null_income_rate))
        for i in rng.choice(n, n_null, replace=False):
            incomes[i] = None
    return pl.DataFrame(
        {
            "hh_id": list(range(1, n + 1)),
            "income_bin": incomes,
            "num_vehicles": rng.choice([0, 1, 2, 3], n).tolist(),
            "complete": [True] * n,
        }
    )


def _make_survey_per(hh_df: pl.DataFrame, null_gender_rate: float = 0.0) -> pl.DataFrame:
    """Generate synthetic survey persons with optional null gender."""
    rng = np.random.default_rng(45)
    rows = []
    person_id = 100
    persons_per_hh = rng.choice([1, 2, 3], len(hh_df)).tolist()
    for i, hh_row in enumerate(hh_df.iter_rows(named=True)):
        n_per = persons_per_hh[i]
        for _ in range(n_per):
            person_id += 1
            rows.append(
                {
                    "hh_id": hh_row["hh_id"],
                    "person_id": person_id,
                    "gender": int(rng.choice([1, 2])),
                    "age": int(rng.choice([1, 3, 5, 7, 9])),
                    "employment": int(rng.choice([1, 2, 3, 5])),
                    "student": int(rng.choice([1, 2])),
                    "work_mode": int(rng.choice([1, 3, 5])),
                }
            )

    df = pl.DataFrame(rows)
    if null_gender_rate > 0:
        n_null = max(1, int(len(df) * null_gender_rate))
        null_ids = rng.choice(len(df), n_null, replace=False).tolist()
        mask = pl.Series([i in null_ids for i in range(len(df))])
        df = df.with_columns(pl.when(mask).then(None).otherwise(pl.col("gender")).alias("gender"))
    return df


# ---------------------------------------------------------------------------
# Recode helpers (call the real recode functions)
# ---------------------------------------------------------------------------
def _recode_pums(targets=TARGETS):
    """Build complete PUMS incidence bundle + recoded persons."""
    hh = _make_pums_hh()
    per = _make_pums_per(hh)
    hh_r = recode_pums_households(hh, per, targets)
    per_r = recode_pums_persons(per, targets)
    bundle = build_incidence_table(
        hh_r, per_r, targets, hh_id_col="SERIALNO", extra_cols=["WGTP", "PUMA"]
    )
    return bundle, per_r


def _recode_survey(null_gender_rate=0.0, null_income_rate=0.0, targets=TARGETS):
    """Build survey incidence bundle + recoded persons with optional nulls."""
    hh = _make_survey_hh(null_income_rate=null_income_rate)
    per = _make_survey_per(hh, null_gender_rate=null_gender_rate)
    hh_r = recode_survey_households(hh, per, targets)
    per_r = recode_survey_persons(per, targets)
    bundle = build_incidence_table(hh_r, per_r, targets)
    return bundle, per_r


# ===========================================================================
# Test classes
# ===========================================================================


class TestPivotHelpers:
    """Tests for the refactored pivot helper functions."""

    def test_person_pivot_shape(self):
        """Person pivot should have one row per person with 0/1 indicators."""
        _, pums_per = _recode_pums()
        pp, pa = pivot_person_controls(pums_per, TARGETS, "SERIALNO")
        assert len(pp) == len(pums_per)
        assert len(pa) < len(pp)  # aggregated has fewer rows

    def test_person_pivot_binary_values(self):
        """Non-structural person pivot columns should be 0 or 1."""
        _, pums_per = _recode_pums()
        pp, _ = pivot_person_controls(pums_per, TARGETS, "SERIALNO")
        dunder_cols = [c for c in pp.columns if "__" in c]
        for col in dunder_cols:
            vals = set(pp[col].to_list())
            assert vals <= {0, 1}, f"Column {col} has non-binary values: {vals}"

    def test_person_agg_matches_incidence(self):
        """Person aggregation should match what build_incidence_table produces."""
        hh = _make_pums_hh()
        per = _make_pums_per(hh)
        hh_r = recode_pums_households(hh, per, TARGETS)
        per_r = recode_pums_persons(per, TARGETS)

        bundle = build_incidence_table(hh_r, per_r, TARGETS, hh_id_col="SERIALNO")
        _, person_agg = pivot_person_controls(per_r, TARGETS, "SERIALNO")

        # Person-level columns in incidence should match person_agg
        agg_cols = [c for c in person_agg.columns if c != "SERIALNO"]
        for col in agg_cols:
            inc_vals = bundle.incidence.sort("SERIALNO")[col].to_list()
            agg_vals = person_agg.sort("SERIALNO")[col].to_list()
            assert inc_vals == agg_vals, f"Mismatch in {col}"

    def test_null_person_yields_zero_indicators(self):
        """A person with null gender should get all-zero gender indicators."""
        _, per_r = _recode_survey(null_gender_rate=1.0)
        pp, _ = pivot_person_controls(per_r, TARGETS, "hh_id")
        gender_cols = [c for c in pp.columns if c.startswith("p_gender__")]
        row_sums = pp.select(pl.sum_horizontal(gender_cols))
        # All should be 0 since all genders are null
        assert row_sums.to_series().to_list() == [0] * len(pp)


class TestDetectNullRows:
    """Tests for the null-row detection function."""

    def test_no_nulls_gives_empty_mask(self):
        """With no nulls, mask should be all False."""
        pums_bundle, _ = _recode_pums()
        gender_cols = [c for c in pums_bundle.incidence.columns if c.startswith("p_gender__")]
        mask = _null_mask(pums_bundle.incidence, gender_cols)
        assert mask.sum() == 0

    def test_null_gender_detected(self):
        """Persons with null gender should be flagged."""
        survey_bundle, _ = _recode_survey(null_gender_rate=0.5)
        gender_cols = [c for c in survey_bundle.person_pivot.columns if c.startswith("p_gender__")]
        mask = _null_mask(survey_bundle.person_pivot, gender_cols)
        assert mask.sum() > 0

    def test_null_hh_income_detected(self):
        """HHs with null income should be flagged in the incidence table."""
        survey_bundle, _ = _recode_survey(null_income_rate=0.5)
        income_cols = [c for c in survey_bundle.incidence.columns if c.startswith("h_income__")]
        mask = _null_mask(survey_bundle.incidence, income_cols)
        assert mask.sum() > 0


class TestTrainAndPredict:
    """Tests for the RF training and prediction function."""

    def test_probabilities_sum_to_one(self):
        """Predicted probabilities for each row should sum to 1.0."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_gender_rate=0.5)

        gender_cols = [c for c in survey_bundle.person_pivot.columns if c.startswith("p_gender__")]
        null_mask = _null_mask(survey_bundle.person_pivot, gender_cols)

        proba_df, _cv_ll, _cv_f1 = _train_and_predict(
            pums_bundle.person_pivot,
            survey_bundle.person_pivot,
            null_mask,
            gender_cols,
            id_col="person_id",
        )
        assert not proba_df.is_empty()

        # Check each row sums to ~1.0
        row_sums = proba_df.select(gender_cols).sum_horizontal()
        for s in row_sums.to_list():
            assert abs(s - 1.0) < 1e-6, f"Row probability sum {s} != 1.0"


class TestFillNullIncidence:
    """Integration tests for the full fill_null_incidence orchestrator."""

    def test_no_nulls_passthrough(self):
        """With no nulls, incidence should be unchanged."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_gender_rate=0.0, null_income_rate=0.0)

        result, summaries = fill_null_incidence(survey_bundle, pums_bundle, TARGETS)

        # Should be identical (integer columns unchanged)
        for col in survey_bundle.incidence.columns:
            assert result[col].to_list() == survey_bundle.incidence[col].to_list(), (
                f"Column {col} changed"
            )
        # All summaries should show 0 nulls
        assert all(s.n_null == 0 for s in summaries)

    def test_person_marginal_preserved(self):
        """After filling, sum(p_gender__*) should equal p_total for each HH."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_gender_rate=0.5)

        result, _summaries = fill_null_incidence(survey_bundle, pums_bundle, TARGETS)

        gender_cols = [c for c in result.columns if c.startswith("p_gender__")]
        row_sums = result.select(pl.sum_horizontal(gender_cols)).to_series()
        p_total = result["p_total"].cast(pl.Float64)
        diff = (row_sums - p_total).abs()
        # Allow small floating point tolerance
        assert diff.max() < 0.01, f"Max marginal deviation: {diff.max()}"

    def test_hh_marginal_preserved(self):
        """After filling, sum(h_income__*) should be ~1.0 per HH."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_income_rate=0.5)

        result, _summaries = fill_null_incidence(survey_bundle, pums_bundle, TARGETS)

        income_cols = [c for c in result.columns if c.startswith("h_income__")]
        row_sums = result.select(pl.sum_horizontal(income_cols)).to_series()
        for s in row_sums.to_list():
            assert abs(s - 1.0) < 0.01, f"HH income marginal sum {s} != 1.0"

    def test_values_become_fractional(self):
        """Null rows should get fractional (non-integer) values."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_gender_rate=0.5)

        result, _summaries = fill_null_incidence(survey_bundle, pums_bundle, TARGETS)

        gender_cols = [c for c in result.columns if c.startswith("p_gender__")]
        # At least some values should be fractional (not integers)
        all_vals = []
        for col in gender_cols:
            all_vals.extend(result[col].to_list())
        fractional = [v for v in all_vals if v != int(v)]
        assert len(fractional) > 0, "Expected some fractional values after imputation"

    def test_all_null_gender(self):
        """With 100% null gender, every person gets fractional prediction."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_gender_rate=1.0)

        result, _summaries = fill_null_incidence(survey_bundle, pums_bundle, TARGETS)

        gender_cols = [c for c in result.columns if c.startswith("p_gender__")]
        row_sums = result.select(pl.sum_horizontal(gender_cols)).to_series()
        p_total = result["p_total"].cast(pl.Float64)
        diff = (row_sums - p_total).abs()
        assert diff.max() < 0.01, f"Max marginal deviation with all-null: {diff.max()}"

    def test_summaries_report_nulls(self):
        """Summaries should report non-zero nulls for imputed controls."""
        pums_bundle, _ = _recode_pums()
        survey_bundle, _ = _recode_survey(null_gender_rate=0.5, null_income_rate=0.3)

        _result, summaries = fill_null_incidence(survey_bundle, pums_bundle, TARGETS)

        by_ctrl = {s.control: s for s in summaries}
        # Gender was 50% null
        assert by_ctrl["p_gender"].n_null > 0
        assert by_ctrl["p_gender"].level == "person"
        assert by_ctrl["p_gender"].log_loss is not None
        assert by_ctrl["p_gender"].f1_macro is not None
        # Income was 30% null
        assert by_ctrl["h_income"].n_null > 0
        assert by_ctrl["h_income"].level == "household"
        # h_size should have 0 nulls (never null in synthetic data)
        assert by_ctrl["h_size"].n_null == 0
        assert by_ctrl["h_size"].log_loss is None
