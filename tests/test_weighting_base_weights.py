"""Tests for base_weights module — initial expansion weight computation."""

import polars as pl
import pytest

from processing.weighting.balancing.balancer import balance_weights
from processing.weighting.balancing.base_weights import (
    SamplePlan,
    compute_base_weights,
    load_sample_plan,
)
from processing.weighting.core.specs import ControlTotals


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_control_totals(
    zone_data: dict[str, list[tuple[str, int, float]]],
) -> ControlTotals:
    """Build a minimal ControlTotals from {geo_id: [(ctrl_name, cat, total)]}.

    ``zone_data`` maps geo_id strings to lists of (control_name, category,
    target_total) tuples, which become rows of the tidy totals frame.
    """
    rows: list[dict] = []
    for geo_id, entries in zone_data.items():
        for ctrl_name, cat, total in entries:
            rows.append(
                {
                    "geo_id": geo_id,
                    "control_name": ctrl_name,
                    "category": cat,
                    "target_total": total,
                }
            )
    totals = pl.DataFrame(rows).cast(
        {"geo_id": pl.Utf8, "category": pl.Int64, "target_total": pl.Float64}
    )
    return ControlTotals(
        totals=totals,
        pums_hh_count=0,
        pums_person_count=0,
        geo_ids=sorted(zone_data),
    )


def _make_seed(geo_col: str, zone_hh: dict[str, int]) -> pl.DataFrame:
    """Build a minimal seed table with hh_id and geo_col."""
    rows: list[dict] = []
    hh_id = 1
    for geo_id, n in zone_hh.items():
        for _ in range(n):
            rows.append({"hh_id": hh_id, geo_col: geo_id})
            hh_id += 1
    return pl.DataFrame(rows).cast({"hh_id": pl.Int64, geo_col: pl.Utf8})


def _make_seed_with_bg(geo_col: str, bg_hh: dict[str, tuple[str, int]]) -> pl.DataFrame:
    """Build a seed table with hh_id, geo_col, and bg_geo_id.

    *bg_hh* maps ``bg_geo_id → (ctrl_geoid, n_households)``.
    """
    rows: list[dict] = []
    hh_id = 1
    for bg_id, (zone_id, n) in bg_hh.items():
        for _ in range(n):
            rows.append({"hh_id": hh_id, geo_col: zone_id, "bg_geo_id": bg_id})
            hh_id += 1
    return pl.DataFrame(rows).cast({"hh_id": pl.Int64, geo_col: pl.Utf8, "bg_geo_id": pl.Utf8})


# ---------------------------------------------------------------------------
# Response inversion tests
# ---------------------------------------------------------------------------
class TestResponseInversion:
    """Default strategy: target_hh_pop / n_responses per zone."""

    def test_single_zone(self):
        """100 responses in a zone with 100 000 HH → base_weight = 1000."""
        ct = _make_control_totals({"Z1": [("h_size", 1, 30_000.0), ("h_size", 2, 70_000.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 100})
        result = compute_base_weights(seed, ct, ["h_size"], geo_col="ctrl_geoid")

        assert "base_weight" in result.columns
        assert result.height == 100
        weights = result["base_weight"].to_list()
        assert all(w == pytest.approx(1_000.0) for w in weights)

    def test_multiple_zones(self):
        """Each zone gets its own ratio."""
        ct = _make_control_totals(
            {
                "A": [("h_size", 1, 5_000.0), ("h_size", 2, 5_000.0)],
                "B": [("h_size", 1, 80_000.0), ("h_size", 2, 20_000.0)],
            }
        )
        seed = _make_seed("ctrl_geoid", {"A": 50, "B": 200})
        result = compute_base_weights(seed, ct, ["h_size"], geo_col="ctrl_geoid")

        zone_a = result.filter(pl.col("ctrl_geoid") == "A")
        zone_b = result.filter(pl.col("ctrl_geoid") == "B")

        # A: 10_000/50 = 200, B: 100_000/200 = 500
        assert zone_a["base_weight"][0] == pytest.approx(200.0)
        assert zone_b["base_weight"][0] == pytest.approx(500.0)

    def test_uses_first_hh_control(self):
        """When multiple controls present, uses first HH-level for total."""
        ct = _make_control_totals(
            {
                "Z1": [
                    ("h_size", 1, 60_000.0),
                    ("h_size", 2, 40_000.0),
                    ("h_income", 1, 50_000.0),
                    ("h_income", 2, 50_000.0),
                ],
            }
        )
        seed = _make_seed("ctrl_geoid", {"Z1": 100})
        # h_size is first HH control → total 100_000, base_weight = 1000
        result = compute_base_weights(seed, ct, ["h_size", "h_income"], geo_col="ctrl_geoid")
        assert result["base_weight"][0] == pytest.approx(1_000.0)

    def test_preserves_existing_columns(self):
        """base_weight is added without losing other columns."""
        ct = _make_control_totals({"Z1": [("h_size", 1, 1_000.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 10}).with_columns(pl.lit(42).alias("extra_col"))
        result = compute_base_weights(seed, ct, ["h_size"], geo_col="ctrl_geoid")
        assert "extra_col" in result.columns
        assert "base_weight" in result.columns
        assert result["extra_col"].to_list() == [42] * 10

    def test_no_hh_controls_raises(self):
        """Fail if no household-level control in targets."""
        ct = _make_control_totals({"Z1": [("p_age", 1, 1_000.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 10})
        with pytest.raises(ValueError, match="no household-level control"):
            compute_base_weights(seed, ct, ["p_age"], geo_col="ctrl_geoid")

    def test_column_dtype(self):
        """base_weight should be Float64."""
        ct = _make_control_totals({"Z1": [("h_size", 1, 500.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 5})
        result = compute_base_weights(seed, ct, ["h_size"], geo_col="ctrl_geoid")
        assert result["base_weight"].dtype == pl.Float64

    def test_single_response_per_zone(self):
        """One response → base_weight equals the full zone target."""
        ct = _make_control_totals({"Z1": [("h_size", 1, 50_000.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 1})
        result = compute_base_weights(seed, ct, ["h_size"], geo_col="ctrl_geoid")
        assert result["base_weight"][0] == pytest.approx(50_000.0)

    def test_hh_id_preserved(self):
        """hh_id values survive the join."""
        ct = _make_control_totals({"Z1": [("h_size", 1, 1_000.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 3})
        result = compute_base_weights(seed, ct, ["h_size"], geo_col="ctrl_geoid")
        assert sorted(result["hh_id"].to_list()) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Sample plan tests (block-group level)
# ---------------------------------------------------------------------------
class TestSamplePlan:
    """Explicit sample plan with BG-level segment-based stratification."""

    def test_single_segment_all_bgs(self):
        """All BGs in one segment → weight = total_bg_pop / total_responses."""
        plan = SamplePlan(
            strata=pl.DataFrame(
                {
                    "bg_geo_id": ["060010001001", "060010001002"],
                    "sample_segment": ["seg_a", "seg_a"],
                }
            )
        )
        bg_pops = pl.DataFrame(
            {"bg_geo_id": ["060010001001", "060010001002"], "bg_population": [60_000, 40_000]}
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)]})
        # 10 + 20 = 30 responses in one segment, pop = 100k → weight = 100k/30
        seed = _make_seed_with_bg(
            "ctrl_geoid",
            {"060010001001": ("Z1", 10), "060010001002": ("Z1", 20)},
        )
        result = compute_base_weights(
            seed,
            ct,
            ["h_size"],
            geo_col="ctrl_geoid",
            sample_plan=plan,
            bg_populations=bg_pops,
        )
        expected = 100_000.0 / 30
        assert result["base_weight"][0] == pytest.approx(expected)

    def test_multiple_segments(self):
        """Different segments get different weights."""
        plan = SamplePlan(
            strata=pl.DataFrame(
                {
                    "bg_geo_id": ["060010001001", "060130001001", "060130001002"],
                    "sample_segment": ["urban", "rural", "rural"],
                }
            )
        )
        bg_pops = pl.DataFrame(
            {
                "bg_geo_id": ["060010001001", "060130001001", "060130001002"],
                "bg_population": [100_000, 50_000, 50_000],
            }
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)], "Z2": [("h_size", 1, 999.0)]})
        # urban: 100 HHs, pop=100k → 1000
        # rural: 50+50 = 100 HHs, pop=100k → 1000
        seed = _make_seed_with_bg(
            "ctrl_geoid",
            {
                "060010001001": ("Z1", 100),
                "060130001001": ("Z2", 50),
                "060130001002": ("Z2", 50),
            },
        )
        result = compute_base_weights(
            seed,
            ct,
            ["h_size"],
            geo_col="ctrl_geoid",
            sample_plan=plan,
            bg_populations=bg_pops,
        )
        z1 = result.filter(pl.col("ctrl_geoid") == "Z1")
        z2 = result.filter(pl.col("ctrl_geoid") == "Z2")
        assert z1["base_weight"][0] == pytest.approx(1_000.0)
        assert z2["base_weight"][0] == pytest.approx(1_000.0)

    def test_unequal_segments(self):
        """Segments with different pop/response ratios."""
        plan = SamplePlan(
            strata=pl.DataFrame(
                {
                    "bg_geo_id": ["060010001001", "060750001001"],
                    "sample_segment": ["big", "small"],
                }
            )
        )
        bg_pops = pl.DataFrame(
            {"bg_geo_id": ["060010001001", "060750001001"], "bg_population": [200_000, 50_000]}
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)], "Z2": [("h_size", 1, 999.0)]})
        # big: 200k/100 = 2000, small: 50k/50 = 1000
        seed = _make_seed_with_bg(
            "ctrl_geoid",
            {"060010001001": ("Z1", 100), "060750001001": ("Z2", 50)},
        )
        result = compute_base_weights(
            seed,
            ct,
            ["h_size"],
            geo_col="ctrl_geoid",
            sample_plan=plan,
            bg_populations=bg_pops,
        )
        z1 = result.filter(pl.col("ctrl_geoid") == "Z1")
        z2 = result.filter(pl.col("ctrl_geoid") == "Z2")
        assert z1["base_weight"][0] == pytest.approx(2_000.0)
        assert z2["base_weight"][0] == pytest.approx(1_000.0)

    def test_segment_with_zero_responses_raises(self):
        """If a segment has no survey responses, fail loud."""
        plan = SamplePlan(
            strata=pl.DataFrame(
                {
                    "bg_geo_id": ["060010001001", "060750001001"],
                    "sample_segment": ["has_data", "empty"],
                }
            )
        )
        bg_pops = pl.DataFrame(
            {"bg_geo_id": ["060010001001", "060750001001"], "bg_population": [100_000, 50_000]}
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)]})
        # Only BG1 has responses; BG2 (segment "empty") has none
        seed = _make_seed_with_bg("ctrl_geoid", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="zero survey responses"):
            compute_base_weights(
                seed,
                ct,
                ["h_size"],
                geo_col="ctrl_geoid",
                sample_plan=plan,
                bg_populations=bg_pops,
            )

    def test_sample_plan_missing_column_raises(self):
        """SamplePlan validates required columns on construction."""
        with pytest.raises(ValueError, match="missing required columns"):
            SamplePlan(strata=pl.DataFrame({"geo_id": ["Z1"], "target_population": [100]}))

    def test_missing_bg_populations_raises(self):
        """sample_plan without bg_populations should raise."""
        plan = SamplePlan(
            strata=pl.DataFrame({"bg_geo_id": ["060010001001"], "sample_segment": ["seg_a"]})
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)]})
        seed = _make_seed_with_bg("ctrl_geoid", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="bg_populations is required"):
            compute_base_weights(
                seed,
                ct,
                ["h_size"],
                geo_col="ctrl_geoid",
                sample_plan=plan,
            )

    def test_missing_bg_geo_id_raises(self):
        """Seed without bg_geo_id should raise when sample plan is used."""
        plan = SamplePlan(
            strata=pl.DataFrame({"bg_geo_id": ["060010001001"], "sample_segment": ["seg_a"]})
        )
        bg_pops = pl.DataFrame({"bg_geo_id": ["060010001001"], "bg_population": [100_000]})
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)]})
        seed = _make_seed("ctrl_geoid", {"Z1": 10})  # no bg_geo_id
        with pytest.raises(ValueError, match="missing 'bg_geo_id'"):
            compute_base_weights(
                seed,
                ct,
                ["h_size"],
                geo_col="ctrl_geoid",
                sample_plan=plan,
                bg_populations=bg_pops,
            )

    def test_sample_plan_extra_columns_ok(self):
        """Extra columns are allowed."""
        plan = SamplePlan(
            strata=pl.DataFrame(
                {
                    "bg_geo_id": ["060010001001"],
                    "sample_segment": ["seg1"],
                    "county": ["Alameda"],
                }
            )
        )
        assert "county" in plan.strata.columns


# ---------------------------------------------------------------------------
# Balancer integration: base_weight required
# ---------------------------------------------------------------------------
class TestBalancerRequiresBaseWeight:
    """_prepare_zone must fail if base_weight is missing."""

    def test_balance_weights_missing_base_weight_raises(self):
        """balance_weights → _prepare_zone should raise on missing col."""
        ct = _make_control_totals(
            {"Z1": [("h_total", 1, 3.0), ("h_size", 1, 500.0), ("h_size", 2, 500.0)]}
        )
        seed = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "ctrl_geoid": ["Z1", "Z1", "Z1"],
                "h_total": [1, 1, 1],
                "h_size": [1, 2, 1],
            }
        )
        with pytest.raises(ValueError, match="missing 'base_weight'"):
            balance_weights(
                seed,
                ct,
                ["h_total", "h_size"],
            )


# ---------------------------------------------------------------------------
# CSV loading tests
# ---------------------------------------------------------------------------
class TestLoadSamplePlan:
    """load_sample_plan reads a CSV into a SamplePlan."""

    def test_load_valid_csv(self, tmp_path):
        """A well-formed CSV should load without error."""
        csv = tmp_path / "plan.csv"
        csv.write_text("bg_geo_id,sample_segment\n060010001001,seg_a\n060010001002,seg_a\n")
        plan = load_sample_plan(csv)
        assert isinstance(plan, SamplePlan)
        assert plan.strata.height == 2
        assert plan.strata["bg_geo_id"].to_list() == ["060010001001", "060010001002"]

    def test_load_file_not_found(self, tmp_path):
        """Loading a nonexistent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            load_sample_plan(tmp_path / "nonexistent.csv")

    def test_load_missing_columns(self, tmp_path):
        """Loading a CSV with missing required columns should raise ValueError."""
        csv = tmp_path / "bad.csv"
        csv.write_text("bg_geo_id,target_population\n060010001001,100000\n")
        with pytest.raises(ValueError, match="missing required columns"):
            load_sample_plan(csv)

    def test_load_with_extra_columns(self, tmp_path):
        """Loading a CSV with extra columns should succeed (extra columns ignored)."""
        csv = tmp_path / "extra.csv"
        csv.write_text("bg_geo_id,sample_segment,county\n060010001001,seg_a,Alameda\n")
        plan = load_sample_plan(csv)
        assert "county" in plan.strata.columns

    def test_end_to_end_csv_to_base_weights(self, tmp_path):
        """Load CSV → SamplePlan → compute_base_weights."""
        csv = tmp_path / "plan.csv"
        csv.write_text("bg_geo_id,sample_segment\n060010001001,urban\n060750001001,rural\n")
        plan = load_sample_plan(csv)
        bg_pops = pl.DataFrame(
            {"bg_geo_id": ["060010001001", "060750001001"], "bg_population": [100_000, 50_000]}
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)], "Z2": [("h_size", 1, 999.0)]})
        seed = _make_seed_with_bg(
            "ctrl_geoid",
            {"060010001001": ("Z1", 200), "060750001001": ("Z2", 100)},
        )
        result = compute_base_weights(
            seed,
            ct,
            ["h_size"],
            geo_col="ctrl_geoid",
            sample_plan=plan,
            bg_populations=bg_pops,
        )
        z1 = result.filter(pl.col("ctrl_geoid") == "Z1")
        z2 = result.filter(pl.col("ctrl_geoid") == "Z2")
        # urban: 100k/200 = 500, rural: 50k/100 = 500
        assert z1["base_weight"][0] == pytest.approx(500.0)
        assert z2["base_weight"][0] == pytest.approx(500.0)
