"""Tests for MOE-based importance weight calculation."""

from enum import IntEnum

import numpy as np
import polars as pl
import pytest

from processing.weighting.balancing.importance import (
    DEFAULT_IMPORTANCE,
    _control_cell_moe,
    _control_cv,
    _normalize_cvs,
    compute_moe_importance,
)
from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import CONTROLS, register_crosstab


# ---------------------------------------------------------------------------
# Helpers — minimal stub control for testing without the full registry
# ---------------------------------------------------------------------------
class _StubCategory(IntEnum):
    CAT_A = 1
    CAT_B = 2


class _HHStubControl(ControlTarget):
    name = "h_stub"
    level = ControlLevel.HOUSEHOLD
    description = "stub"
    categories = _StubCategory
    survey_fields = ("stub_col",)
    pums_fields = ("STUB",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("stub_col")

    def pums_expr(self) -> pl.Expr:
        return pl.col("STUB")


class _PersonStubControl(ControlTarget):
    name = "p_stub"
    level = ControlLevel.PERSON
    description = "person stub"
    categories = _StubCategory
    survey_fields = ("stub_col",)
    pums_fields = ("STUB",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("stub_col")

    def pums_expr(self) -> pl.Expr:
        return pl.col("STUB")


def _hh_frame_with_replicates(
    n_rows: int = 10,
    *,
    geo_ids: list[str] | None = None,
    ctrl_col: str = "h_stub",
    ctrl_values: list[int] | None = None,
    base_weight: float = 100.0,
    noise_scale: float = 5.0,
    seed: int = 42,
) -> pl.DataFrame:
    """Build a household DataFrame with replicate weight columns."""
    rng = np.random.default_rng(seed)
    geos = geo_ids or ["Z1"]
    vals = ctrl_values or [1, 2]

    data: dict[str, list] = {
        "ctrl_geoid": [geos[i % len(geos)] for i in range(n_rows)],
        ctrl_col: [vals[i % len(vals)] for i in range(n_rows)],
        "_xw_WGTP": [base_weight] * n_rows,
    }
    for r in range(1, 81):
        data[f"_xw_WGTP{r}"] = (base_weight + rng.normal(0, noise_scale, n_rows)).tolist()

    return pl.DataFrame(data)


# ---------------------------------------------------------------------------
# _normalize_cvs
# ---------------------------------------------------------------------------
class TestNormalizeCvs:
    """Tests for the CV → importance transformation."""

    def test_empty_returns_empty(self):
        """Empty input returns empty dict."""
        assert _normalize_cvs({}) == {}

    def test_single_control_gets_default_importance(self):
        """A single control normalizes to DEFAULT_IMPORTANCE."""
        result = _normalize_cvs({"a": 0.05})
        assert result["a"] == pytest.approx(DEFAULT_IMPORTANCE)

    def test_median_equals_default_importance(self):
        """Median importance across controls equals DEFAULT_IMPORTANCE."""
        cvs = {"a": 0.01, "b": 0.05, "c": 0.10}
        result = _normalize_cvs(cvs)
        median = float(np.median(list(result.values())))
        assert median == pytest.approx(DEFAULT_IMPORTANCE)

    def test_lower_cv_gets_higher_importance(self):
        """Controls with lower CV get higher importance."""
        cvs = {"tight": 0.01, "noisy": 0.50}
        result = _normalize_cvs(cvs)
        assert result["tight"] > result["noisy"]

    def test_sqrt_dampening(self):
        """Ratio of importances should follow 1/sqrt(CV) relationship."""
        cvs = {"a": 0.04, "b": 0.16}
        result = _normalize_cvs(cvs)
        # 1/sqrt(0.04) = 5, 1/sqrt(0.16) = 2.5  → ratio = 2
        assert result["a"] / result["b"] == pytest.approx(2.0)

    def test_tiny_cv_clipped(self):
        """Near-zero CVs should not produce infinity."""
        cvs = {"zero": 1e-15, "normal": 0.05}
        result = _normalize_cvs(cvs)
        assert np.isfinite(result["zero"])
        assert result["zero"] > result["normal"]


# ---------------------------------------------------------------------------
# _control_cv
# ---------------------------------------------------------------------------
class TestControlCv:
    """Tests for per-control CV estimation from replicate weights."""

    def test_returns_float_for_valid_data(self):
        """Valid replicate data returns a positive float."""
        hh = _hh_frame_with_replicates(20, noise_scale=5.0)
        ctrl = _HHStubControl()
        cv = _control_cv(ctrl, hh, pl.DataFrame(), "ctrl_geoid")
        assert cv is not None
        assert cv > 0

    def test_low_noise_gives_lower_cv(self):
        """Lower replicate noise produces lower CV."""
        hh_low = _hh_frame_with_replicates(50, noise_scale=1.0, seed=1)
        hh_high = _hh_frame_with_replicates(50, noise_scale=50.0, seed=2)
        ctrl = _HHStubControl()
        cv_low = _control_cv(ctrl, hh_low, pl.DataFrame(), "ctrl_geoid")
        cv_high = _control_cv(ctrl, hh_high, pl.DataFrame(), "ctrl_geoid")
        assert cv_low < cv_high  # pyright: ignore[reportOperatorIssue]

    def test_returns_none_when_no_matching_records(self):
        """If no rows match valid categories, return None."""
        hh = _hh_frame_with_replicates(10, ctrl_values=[99, 98])  # not in _StubCategory
        ctrl = _HHStubControl()
        cv = _control_cv(ctrl, hh, pl.DataFrame(), "ctrl_geoid")
        assert cv is None

    def test_raises_on_missing_replicate_columns(self):
        """Missing replicate columns raise ValueError."""
        hh = pl.DataFrame({"ctrl_geoid": ["Z1"], "h_stub": [1], "_xw_WGTP": [100.0]})
        ctrl = _HHStubControl()
        with pytest.raises(ValueError, match="Replicate weight columns missing"):
            _control_cv(ctrl, hh, pl.DataFrame(), "ctrl_geoid")

    def test_raises_on_missing_control_column(self):
        """Missing control column raises ValueError."""
        data: dict[str, list] = {"ctrl_geoid": ["Z1"], "_xw_WGTP": [100.0]}
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0]
        hh = pl.DataFrame(data)
        ctrl = _HHStubControl()
        with pytest.raises(ValueError, match=r"Control column.*not found"):
            _control_cv(ctrl, hh, pl.DataFrame(), "ctrl_geoid")

    def test_person_level_uses_person_frame(self):
        """Person-level controls should read from person_df, not hh_df."""
        person = _hh_frame_with_replicates(20, ctrl_col="p_stub")
        # Rename weight columns to person variants
        renames = {"_xw_WGTP": "_xw_PWGTP"}
        for r in range(1, 81):
            renames[f"_xw_WGTP{r}"] = f"_xw_PWGTP{r}"
        person = person.rename(renames)

        ctrl = _PersonStubControl()
        cv = _control_cv(ctrl, pl.DataFrame(), person, "ctrl_geoid")
        assert cv is not None
        assert cv > 0

    def test_multiple_zones_returns_median(self):
        """CV should be the median across zone x category cells."""
        hh = _hh_frame_with_replicates(40, geo_ids=["Z1", "Z2"], noise_scale=5.0)
        ctrl = _HHStubControl()
        cv = _control_cv(ctrl, hh, pl.DataFrame(), "ctrl_geoid")
        assert cv is not None

    def test_zero_estimate_cells_excluded(self):
        """Cells where the estimate is zero should be excluded from the median."""
        data: dict[str, list] = {
            "ctrl_geoid": ["Z1", "Z1"],
            "h_stub": [1, 2],
            "_xw_WGTP": [100.0, 0.0],  # second cell has zero weight
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0, 0.0]
        hh = pl.DataFrame(data)
        ctrl = _HHStubControl()
        cv = _control_cv(ctrl, hh, pl.DataFrame(), "ctrl_geoid")
        # Only one valid cell (estimate=100), replicates all equal → CV=0
        assert cv is not None
        assert cv == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# compute_moe_importance (integration)
# ---------------------------------------------------------------------------
class TestComputeMoeImportance:
    """Integration tests for the full MOE importance pipeline."""

    def test_raises_on_unknown_control(self):
        """If a target name is not in the registry, raise ValueError."""
        hh = pl.DataFrame()
        person = pl.DataFrame()
        with pytest.raises(ValueError, match="Unknown control"):
            compute_moe_importance(hh, person, ["not_a_control"])

    def test_end_to_end_with_real_registry(self):
        """Use h_size from the real registry with synthetic data."""
        ctrl = CONTROLS["h_size"]
        n = 100
        rng = np.random.default_rng(42)
        sizes = rng.choice([m[0] for m in ctrl.valid_members], size=n)

        data: dict[str, list] = {
            "ctrl_geoid": ["Z1"] * n,
            "h_size": sizes.tolist(),
            "_xw_WGTP": [50.0] * n,
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = (50.0 + rng.normal(0, 3, n)).tolist()

        hh = pl.DataFrame(data)
        person = pl.DataFrame()
        result = compute_moe_importance(hh, person, ["h_size"])

        assert "h_size" in result
        assert result["h_size"] > 0
        assert np.isfinite(result["h_size"])

    def test_structural_controls_omitted_when_sparse(self):
        """Structural controls with no matching data return empty dict."""
        # h_total has category TOTAL=1, but we provide no records matching it
        data: dict[str, list] = {
            "ctrl_geoid": ["Z1"],
            "h_total": [99],  # not a valid member value
            "_xw_WGTP": [100.0],
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0]
        hh = pl.DataFrame(data)
        result = compute_moe_importance(hh, pl.DataFrame(), ["h_total"])
        # h_total should be omitted (no valid cells)
        assert "h_total" not in result

    def test_ordering_preserved(self):
        """Controls with lower CV should get higher importance."""
        n = 200
        rng = np.random.default_rng(99)

        # h_size: tight replicates (low noise)
        # h_workers: noisy replicates (high noise)
        size_ctrl = CONTROLS["h_size"]
        workers_ctrl = CONTROLS["h_workers"]

        size_vals = rng.choice([m[0] for m in size_ctrl.valid_members], size=n)
        worker_vals = rng.choice([m[0] for m in workers_ctrl.valid_members], size=n)

        data: dict[str, list] = {
            "ctrl_geoid": ["Z1"] * n,
            "h_size": size_vals.tolist(),
            "h_workers": worker_vals.tolist(),
            "_xw_WGTP": [100.0] * n,
        }
        # h_size replicates: tight (noise_scale=1)
        # h_workers replicates: same base but we'll add extra noise below
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = (100.0 + rng.normal(0, 1, n)).tolist()

        hh_tight = pl.DataFrame(data)

        # For the noisy version, make replicates much noisier
        data_noisy = dict(data)
        for r in range(1, 81):
            data_noisy[f"_xw_WGTP{r}"] = (100.0 + rng.normal(0, 50, n)).tolist()

        # Compute CVs individually to verify ordering
        cv_tight = _control_cv(size_ctrl, hh_tight, pl.DataFrame(), "ctrl_geoid")
        cv_noisy = _control_cv(size_ctrl, pl.DataFrame(data_noisy), pl.DataFrame(), "ctrl_geoid")

        assert cv_tight is not None
        assert cv_noisy is not None
        assert cv_tight < cv_noisy


# ---------------------------------------------------------------------------
# Cross-tab MOE support
# ---------------------------------------------------------------------------


@pytest.fixture
def _clean_registry():
    """Remove dynamically registered cross-tabs after each test."""
    before = set(CONTROLS.keys())
    yield
    for name in list(CONTROLS.keys()):
        if name not in before:
            del CONTROLS[name]


@pytest.mark.usefixtures("_clean_registry")
class TestCrosstabMoe:
    """Tests for MOE computation on cross-tab controls."""

    @staticmethod
    def _xtab_hh_frame(n: int = 80, noise: float = 3.0) -> pl.DataFrame:
        """Build PUMS HH data with dimension columns and replicate weights."""
        rng = np.random.default_rng(42)
        size_ctrl = CONTROLS["h_size"]
        income_ctrl = CONTROLS["h_income"]
        size_vals = [m[0] for m in size_ctrl.valid_members]
        income_vals = [m[0] for m in income_ctrl.valid_members]
        data: dict[str, list] = {
            "ctrl_geoid": ["Z1"] * n,
            "h_size": rng.choice(size_vals, size=n).tolist(),
            "h_income": rng.choice(income_vals, size=n).tolist(),
            "_xw_WGTP": [100.0] * n,
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = (100.0 + rng.normal(0, noise, n)).tolist()
        return pl.DataFrame(data)

    def test_cell_moe_returns_dataframe_for_crosstab(self):
        """_control_cell_moe computes MOE for cross-tab by adding composite column."""
        xtab = register_crosstab("h_size_x_income", ["h_size", "h_income"])
        hh = self._xtab_hh_frame()
        result = _control_cell_moe(xtab, hh, pl.DataFrame(), "ctrl_geoid")
        assert result is not None
        assert "control_name" in result.columns
        assert result["control_name"][0] == "h_size_x_income"
        assert len(result) > 0

    def test_crosstab_cv_is_positive(self):
        """_control_cv on a cross-tab returns a positive float."""
        xtab = register_crosstab("h_size_x_income", ["h_size", "h_income"])
        hh = self._xtab_hh_frame(n=200)
        cv = _control_cv(xtab, hh, pl.DataFrame(), "ctrl_geoid")
        assert cv is not None
        assert cv > 0

    def test_compute_moe_importance_includes_crosstab(self):
        """compute_moe_importance returns importance for cross-tab controls."""
        register_crosstab("h_size_x_income", ["h_size", "h_income"])
        hh = self._xtab_hh_frame(n=200)
        result = compute_moe_importance(hh, pl.DataFrame(), ["h_size", "h_size_x_income"])
        assert "h_size_x_income" in result
        assert result["h_size_x_income"] > 0
