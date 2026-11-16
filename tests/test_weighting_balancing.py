"""What the balancer is given, and how hard it is told to pull.

Three inputs, read together because each shapes the same fit. Base weights are
the starting expansion factors, from a zone's response rate or from an explicit
sample plan's strata. Importance says how hard to chase each control, derived
from the MOE its PUMS replicate weights imply. The expansion-factor grid search
then trades fit error against weight spread, and reports what each choice costs.
"""

from enum import IntEnum

import numpy as np
import plotly.graph_objects as go
import polars as pl
import pytest

from processing.weighting.balancing.balancer import (
    _prepare_zone,
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import (
    SamplePlan,
    compute_base_weights,
    grow_block_households,
    load_sample_plan,
)
from processing.weighting.balancing.importance import (
    DEFAULT_IMPORTANCE,
    _control_cell_moe,
    _control_cv,
    _normalize_cvs,
    compute_control_moe,
    compute_moe_importance,
)
from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import CONTROLS, register_crosstab
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlTotals,
    GridPoint,
)
from processing.weighting.diagnostics.charts import ef_tradeoff_figure

_BALANCING = BalancingConfig(
    max_expansion_factor=10.0,
    min_expansion_factor=0.1,
    max_base_weight_gap=0.005,
    min_weight=None,
    max_iterations=10_000,
    n_workers=1,
    moe_based_importance=False,
    default_importance=100.0,
    min_survey_share_ratio=0.25,
)


def _cv(ctrl: ControlTarget, hh: pl.DataFrame, person: pl.DataFrame) -> float | None:
    """One control's CV, from its cells as ``compute_control_moe`` builds them."""
    cells = _control_cell_moe(ctrl, hh, person)
    return None if cells is None else _control_cv(cells)


def _moe_importance(hh: pl.DataFrame, person: pl.DataFrame, names: list[str]) -> dict[str, float]:
    """Importance per control, through the per-cell MOE as the pipeline runs it."""
    return compute_moe_importance(compute_control_moe(hh, person, names), names)


def _replicates(values, weight: str = "WGTP") -> dict[str, list[float]]:
    """The 80 replicate weight columns: *values* shared by all, or an 80-row array."""
    rows = values.tolist() if isinstance(values, np.ndarray) else [values] * 80
    return {f"_xw_{weight}{r}": row for r, row in enumerate(rows, start=1)}


def _make_control_totals(
    zone_data: dict[str, list[tuple[str, int, float]]],
) -> ControlTotals:
    """Build a minimal ControlTotals from {zone_group_id: [(ctrl_name, cat, total)]}."""
    rows: list[dict] = []
    for zone_group_id, entries in zone_data.items():
        for ctrl_name, cat, total in entries:
            rows.append(
                {
                    "zone_group_id": zone_group_id,
                    "control_name": ctrl_name,
                    "category": cat,
                    "target_total": total,
                }
            )
    totals = pl.DataFrame(rows).cast(
        {"zone_group_id": pl.Utf8, "category": pl.Utf8, "target_total": pl.Float64}
    )
    return ControlTotals(
        totals=totals,
        pums_hh_count=0,
        pums_person_count=0,
        zone_group_ids=sorted(zone_data),
    )


def _make_seed(geo_col: str, zone_hh: dict[str, int]) -> pl.DataFrame:
    """Build a minimal seed table with hh_id and geo_col."""
    rows: list[dict] = []
    hh_id = 1
    for zone_group_id, n in zone_hh.items():
        for _ in range(n):
            rows.append({"hh_id": hh_id, geo_col: zone_group_id})
            hh_id += 1
    return pl.DataFrame(rows).cast({"hh_id": pl.Int64, geo_col: pl.Utf8})


def _make_seed_with_bg(geo_col: str, bg_hh: dict[str, tuple[str, int]]) -> pl.DataFrame:
    """Build a seed table with hh_id, geo_col, and bg_geo_id.

    *bg_hh* maps ``bg_geo_id → (zone_group_id, n_households)``.
    """
    rows: list[dict] = []
    hh_id = 1
    for bg_id, (zone_id, n) in bg_hh.items():
        for _ in range(n):
            rows.append({"hh_id": hh_id, geo_col: zone_id, "bg_geo_id": bg_id})
            hh_id += 1
    return pl.DataFrame(rows).cast({"hh_id": pl.Int64, geo_col: pl.Utf8, "bg_geo_id": pl.Utf8})


class TestResponseInversion:
    """Default strategy: target_hh_pop / n_responses per zone."""

    def test_multiple_zones(self):
        """Each zone gets its own ratio, and the seed's own columns survive."""
        ct = _make_control_totals(
            {
                "A": [("h_total", 1, 10_000.0)],
                "B": [("h_total", 1, 100_000.0)],
            }
        )
        seed = _make_seed("zone_group_id", {"A": 50, "B": 200}).with_columns(
            pl.lit(42).alias("extra_col")
        )
        result = compute_base_weights(seed, ct)

        zone_a = result.filter(pl.col("zone_group_id") == "A")
        zone_b = result.filter(pl.col("zone_group_id") == "B")

        # A: 10_000/50 = 200, B: 100_000/200 = 500
        assert zone_a["base_weight"].to_list() == [pytest.approx(200.0)] * 50
        assert zone_b["base_weight"].to_list() == [pytest.approx(500.0)] * 200
        assert result["base_weight"].dtype == pl.Float64
        assert result["extra_col"].to_list() == [42] * 250
        assert sorted(result["hh_id"].to_list()) == list(range(1, 251))

    def test_reads_h_total_not_another_household_control(self):
        """A household control that leaves households out doesn't set the base weight."""
        ct = _make_control_totals({"Z1": [("h_children", 1, 30_000.0), ("h_total", 1, 100_000.0)]})
        seed = _make_seed("zone_group_id", {"Z1": 100})
        result = compute_base_weights(seed, ct)
        assert result["base_weight"][0] == pytest.approx(1_000.0)

    def test_no_h_total_raises(self):
        """Fail if the control totals have no h_total target."""
        ct = _make_control_totals({"Z1": [("h_size", 1, 1_000.0)]})
        seed = _make_seed("zone_group_id", {"Z1": 10})
        with pytest.raises(ValueError, match="no h_total target"):
            compute_base_weights(seed, ct)


class TestSamplePlan:
    """Explicit sample plan: segments of block groups, split by zone group."""

    @staticmethod
    def _plan(segments: dict[str, str]) -> SamplePlan:
        """A plan from ``{bg_geo_id: sample_segment}``."""
        return SamplePlan(
            strata=pl.DataFrame(
                {"bg_geo_id": list(segments), "sample_segment": list(segments.values())}
            )
        )

    @staticmethod
    def _blocks(blocks: dict[str, tuple[str, float]]) -> pl.DataFrame:
        """Block households from ``{block_id: (zone_group_id, households)}``."""
        return pl.DataFrame(
            {
                "block_id": list(blocks),
                "zone_group_id": [z for z, _ in blocks.values()],
                "households": [float(h) for _, h in blocks.values()],
            }
        )

    @staticmethod
    def _weights(seed, plan, blocks, zones=("Z1",)) -> pl.DataFrame:
        ct = _make_control_totals({z: [("h_size", 1, 999.0)] for z in zones})
        return compute_base_weights(seed, ct, sample_plan=plan, block_households=blocks)

    def test_single_segment_all_bgs(self):
        """All BGs in one segment and zone → weight = households / responses."""
        plan = self._plan({"060010001001": "seg_a", "060010001002": "seg_a"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 60_000), "060010001002000": ("Z1", 40_000)}
        )
        # 10 + 20 = 30 responses in one cell, households = 100k → weight = 100k/30
        seed = _make_seed_with_bg(
            "zone_group_id",
            {"060010001001": ("Z1", 10), "060010001002": ("Z1", 20)},
        )
        result = self._weights(seed, plan, blocks)
        assert result["base_weight"].to_list() == [pytest.approx(100_000.0 / 30)] * 30

    def test_blocks_add_up_to_their_block_group(self):
        """Every block in a planned block group counts toward its cell."""
        plan = self._plan({"060010001001": "seg_a"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 30_000), "060010001001001": ("Z1", 70_000)}
        )
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 100)})
        result = self._weights(seed, plan, blocks)
        assert result["base_weight"][0] == pytest.approx(1_000.0)

    def test_multiple_segments(self):
        """A segment spanning two block groups pools them, and only itself.

        The two segments are deliberately unequal, so pooling every response
        together (180 000 / 140 = 1285.7) fails both assertions.
        """
        plan = self._plan(
            {"060010001001": "urban", "060130001001": "rural", "060130001002": "rural"}
        )
        blocks = self._blocks(
            {
                "060010001001000": ("Z1", 100_000),
                "060130001001000": ("Z2", 50_000),
                "060130001002000": ("Z2", 30_000),
            }
        )
        # urban: 100 HHs, households = 100k → 1000
        # rural: 25 + 15 = 40 HHs, households = 50k + 30k = 80k → 2000
        seed = _make_seed_with_bg(
            "zone_group_id",
            {
                "060010001001": ("Z1", 100),
                "060130001001": ("Z2", 25),
                "060130001002": ("Z2", 15),
            },
        )
        result = self._weights(seed, plan, blocks, zones=("Z1", "Z2"))
        z1 = result.filter(pl.col("zone_group_id") == "Z1")
        z2 = result.filter(pl.col("zone_group_id") == "Z2")
        assert z1["base_weight"].to_list() == [pytest.approx(1_000.0)] * 100
        # both rural block groups share the segment's single ratio
        assert z2["base_weight"].to_list() == [pytest.approx(2_000.0)] * 40

    def test_a_segment_crossing_zones_is_split_by_zone(self):
        """Each zone's base weights add up to its own households, not its share of responses.

        Pooled, the segment's 100k over 100 responses gives every home 1,000, so the
        zone with 95 responses would carry 95k against its 80k households.
        """
        plan = self._plan({"060010001001": "seg", "060130001001": "seg"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 80_000), "060130001001000": ("Z2", 20_000)}
        )
        seed = _make_seed_with_bg(
            "zone_group_id", {"060010001001": ("Z1", 95), "060130001001": ("Z2", 5)}
        )
        result = self._weights(seed, plan, blocks, zones=("Z1", "Z2"))
        sums = dict(result.group_by("zone_group_id").agg(pl.col("base_weight").sum()).iter_rows())
        assert sums == {"Z1": pytest.approx(80_000.0), "Z2": pytest.approx(20_000.0)}

    def test_cell_with_zero_responses_raises(self):
        """If a segment has households in a zone but no responses there, fail loud."""
        plan = self._plan({"060010001001": "has_data", "060750001001": "empty"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 100_000), "060750001001000": ("Z1", 50_000)}
        )
        # Only BG1 has responses; BG2 (segment "empty") has none
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="zero survey responses"):
            self._weights(seed, plan, blocks)

    def test_responses_without_households_raise(self):
        """Responses in a cell the Census gives no households cannot be weighted."""
        plan = self._plan({"060010001001": "seg"})
        blocks = self._blocks({"060010001001000": ("Z1", 0)})
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="responses but no Census households"):
            self._weights(seed, plan, blocks)

    def test_sample_plan_missing_column_raises(self):
        """SamplePlan validates required columns on construction."""
        with pytest.raises(ValueError, match="missing required columns"):
            SamplePlan(strata=pl.DataFrame({"zone_group_id": ["Z1"], "target_population": [100]}))

    def test_missing_block_households_raises(self):
        """sample_plan without block_households should raise."""
        plan = self._plan({"060010001001": "seg_a"})
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)]})
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="block_households is required"):
            compute_base_weights(
                seed,
                ct,
                sample_plan=plan,
            )

    def test_missing_bg_geo_id_raises(self):
        """Seed without bg_geo_id should raise when sample plan is used."""
        plan = self._plan({"060010001001": "seg_a"})
        blocks = self._blocks({"060010001001000": ("Z1", 100_000)})
        seed = _make_seed("zone_group_id", {"Z1": 10})  # no bg_geo_id
        with pytest.raises(ValueError, match="missing 'bg_geo_id'"):
            self._weights(seed, plan, blocks)


class TestGrowBlockHouseholds:
    """Census block households are grown to the PUMS year by their PUMA."""

    BLOCKS = pl.DataFrame(
        {
            "block_id": ["060010001001000", "060010001001001", "060130001001000"],
            "puma_id": ["00101", "00101", "01301"],
            # the third block's PUMA sits outside the zones; it gets no growth
            "zone_group_id": ["Z1", None, None],
            "households": [60, 40, 500],
        }
    )
    PUMS = pl.DataFrame({"PUMA": ["00101", "00101", "01301"], "WGTP": [70.0, 40.0, 600.0]})

    def test_each_puma_scales_its_blocks(self):
        """Growth is the PUMA's PUMS households over all its Census households."""
        grown, growth = grow_block_households(self.BLOCKS, self.PUMS)
        # PUMA 00101: 110 PUMS households over 100 Census households, outside block included
        assert growth.select("puma_id", "growth").rows() == [("00101", pytest.approx(1.1))]
        assert grown.rows() == [("060010001001000", "Z1", pytest.approx(66.0))]

    def test_a_puma_without_pums_households_stops(self):
        """No PUMS count means no growth, and the run should not go on."""
        with pytest.raises(ValueError, match="no PUMS households"):
            grow_block_households(self.BLOCKS, self.PUMS.filter(pl.col("PUMA") != "00101"))


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
                "zone_group_id": ["Z1", "Z1", "Z1"],
                "h_total": [1, 1, 1],
                "h_size": [1, 2, 1],
            }
        )
        with pytest.raises(ValueError, match="missing 'base_weight'"):
            balance_weights(
                seed,
                ct,
                ["h_total", "h_size"],
                balancing=_BALANCING,
                importance={"h_total": 100.0, "h_size": 100.0},
            )


class TestBaseWeightsMatchTheTarget:
    """The expansion limits are multiples of the base weight, so nothing rescales them."""

    CT = _make_control_totals({"Z1": [("h_total", 1, 300.0)]})

    @staticmethod
    def _seed(base: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "hh_id": list(range(1, len(base) + 1)),
                "zone_group_id": ["Z1"] * len(base),
                "h_total": [1] * len(base),
                "base_weight": base,
            }
        )

    def test_base_weights_off_the_target_stop(self):
        """Base weights adding to half the target mean the base weights are wrong."""
        with pytest.raises(ValueError, match="max_base_weight_gap"):
            balance_weights(
                self._seed([50.0, 50.0, 50.0]),
                self.CT,
                ["h_total"],
                balancing=_BALANCING,
                importance={"h_total": 100.0},
            )

    def test_limits_are_the_base_weight_times_the_factors(self):
        """A home's cap is its base weight x max_expansion_factor, unscaled."""
        # 301 against 300 is within 0.5%, so the zone balances
        zone = _prepare_zone(
            self._seed([100.0, 100.0, 101.0]),
            self.CT.totals.filter(pl.col("zone_group_id") == "Z1"),
            ["h_total"],
            "Z1",
            importance={"h_total": 100.0},
            min_expansion_factor=0.1,
            max_expansion_factor=10.0,
            max_base_weight_gap=0.005,
            min_weight=None,
            max_weight=None,
            max_iterations=10,
            verbose=False,
        )
        assert zone.ub.tolist() == [1_000.0, 1_000.0, 1_010.0]
        assert zone.lb.tolist() == pytest.approx([10.0, 10.0, 10.1])


class TestLoadSamplePlan:
    """load_sample_plan reads a CSV into a SamplePlan."""

    def test_load_valid_csv(self, tmp_path):
        """A well-formed CSV loads, and columns beyond the required two survive."""
        csv = tmp_path / "plan.csv"
        csv.write_text(
            "bg_geo_id,sample_segment,county\n"
            "060010001001,seg_a,Alameda\n"
            "060010001002,seg_a,Alameda\n"
        )
        plan = load_sample_plan(csv)
        assert isinstance(plan, SamplePlan)
        assert plan.strata.height == 2
        assert plan.strata["bg_geo_id"].to_list() == ["060010001001", "060010001002"]
        assert plan.strata["county"].to_list() == ["Alameda", "Alameda"]

    def test_load_file_not_found(self, tmp_path):
        """Loading a nonexistent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            load_sample_plan(tmp_path / "nonexistent.csv")


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
    ctrl_col: str = "h_stub",
    ctrl_values: list[int] | None = None,
    base_weight: float = 100.0,
    weight: str = "WGTP",
) -> pl.DataFrame:
    """A household frame with replicate weights scattered around *base_weight*."""
    rng = np.random.default_rng(42)
    vals = ctrl_values or [1, 2]
    return pl.DataFrame(
        {
            "zone_group_id": ["Z1"] * n_rows,
            ctrl_col: [vals[i % len(vals)] for i in range(n_rows)],
            f"_xw_{weight}": [base_weight] * n_rows,
            **_replicates(base_weight + rng.normal(0, 5.0, (80, n_rows)), weight),
        }
    )


class TestNormalizeCvs:
    """Tests for the CV → importance transformation."""

    def test_empty_returns_empty(self):
        """Empty input returns empty dict."""
        assert _normalize_cvs({}) == {}

    def test_tiny_cv_clipped(self):
        """Near-zero CVs should not produce infinity."""
        cvs = {"zero": 1e-15, "normal": 0.05}
        result = _normalize_cvs(cvs)
        assert np.isfinite(result["zero"])
        assert result["zero"] > result["normal"]


class TestControlCv:
    """Tests for per-control CV estimation from replicate weights."""

    def test_returns_none_when_no_matching_records(self):
        """If no rows match valid categories, return None."""
        hh = _hh_frame_with_replicates(10, ctrl_values=[99, 98])  # not in _StubCategory
        ctrl = _HHStubControl()
        cv = _cv(ctrl, hh, pl.DataFrame())
        assert cv is None

    @pytest.mark.parametrize(
        ("with_replicates", "with_control", "match"),
        [
            pytest.param(False, True, "Replicate weight columns missing", id="no_replicates"),
            pytest.param(True, False, r"Control column.*not found", id="no_control_column"),
        ],
    )
    def test_raises_on_missing_columns(self, with_replicates, with_control, match):
        """Both the replicate block and the control column are required."""
        data: dict[str, list] = {"zone_group_id": ["Z1"], "_xw_WGTP": [100.0]}
        if with_control:
            data["h_stub"] = [1]
        if with_replicates:
            data |= _replicates([100.0])
        hh = pl.DataFrame(data)
        ctrl = _HHStubControl()
        with pytest.raises(ValueError, match=match):
            _cv(ctrl, hh, pl.DataFrame())

    def test_person_level_uses_person_frame(self):
        """Person-level controls should read from person_df, not hh_df."""
        person = _hh_frame_with_replicates(20, ctrl_col="p_stub", weight="PWGTP")
        cv = _cv(_PersonStubControl(), pl.DataFrame(), person)
        assert cv is not None
        assert cv > 0

    def test_multiple_zones_returns_median(self):
        """CV is the median across every zone x category cell, not one zone's.

        Four cells, one row each, all with an estimate of 100.  Every
        replicate of a row sits a constant distance from that estimate, so
        the cell's standard error is ``sqrt((4/80) * 80 * d**2) == 2*d`` and
        its CV is ``2*d / 100``.  Offsets of 1, 2, 3, 4 give CVs of 0.02,
        0.04, 0.06, 0.08, whose median is 0.05 — a number no single zone or
        single cell produces.
        """
        hh = pl.DataFrame(
            {
                "zone_group_id": ["Z1", "Z1", "Z2", "Z2"],
                "h_stub": [1, 2, 1, 2],
                "_xw_WGTP": [100.0] * 4,
                **_replicates([101.0, 102.0, 103.0, 104.0]),
            }
        )

        cv = _cv(_HHStubControl(), hh, pl.DataFrame())
        assert cv == pytest.approx(0.05)

    def test_zero_estimate_cells_excluded(self):
        """Cells where the estimate is zero should be excluded from the median."""
        hh = pl.DataFrame(
            {
                "zone_group_id": ["Z1", "Z1"],
                "h_stub": [1, 2],
                "_xw_WGTP": [100.0, 0.0],
                **_replicates([100.0, 0.0]),
            }
        )
        # one valid cell, its replicates all equal to the estimate
        assert _cv(_HHStubControl(), hh, pl.DataFrame()) == pytest.approx(0.0)


class TestComputeMoeImportance:
    """Integration tests for the full MOE importance pipeline."""

    def test_raises_on_unknown_control(self):
        """If a target name is not in the registry, raise ValueError."""
        hh = pl.DataFrame()
        person = pl.DataFrame()
        with pytest.raises(ValueError, match="Unknown control"):
            compute_control_moe(hh, person, ["not_a_control"])

    def test_structural_controls_omitted_when_sparse(self):
        """Structural controls with no matching data return empty dict."""
        hh = pl.DataFrame(
            {"zone_group_id": ["Z1"], "h_total": [99], "_xw_WGTP": [100.0], **_replicates([100.0])}
        )
        assert "h_total" not in _moe_importance(hh, pl.DataFrame(), ["h_total"])

    def test_ordering_preserved(self):
        """The control whose cells are tighter comes out of the pipeline heavier.

        Four households in one zone, all weighted 100, whose replicates sit a
        constant 1, 1, 1 and 9 above that weight.  ``h_size`` splits them into
        two cells of two — offsets (1, 1) and (1, 9) — for cell CVs of 0.02
        and 0.10 and a median of 0.06.  ``h_workers`` puts every household in
        a cell of its own, for CVs of 0.02, 0.02, 0.02, 0.18 and a median of
        0.02.  So ``h_workers`` is the tighter control and must come back with
        the higher importance, by exactly ``sqrt(0.06 / 0.02)``.
        """
        hh = pl.DataFrame(
            {
                "zone_group_id": ["Z1"] * 4,
                "h_size": [1, 1, 2, 2],
                "h_workers": [0, 1, 2, 3],
                "_xw_WGTP": [100.0] * 4,
                **_replicates([101.0, 101.0, 101.0, 109.0]),
            }
        )

        # the CVs the two groupings produce, asserted before the normalisation
        assert _cv(CONTROLS["h_size"], hh, pl.DataFrame()) == pytest.approx(0.06)
        assert _cv(CONTROLS["h_workers"], hh, pl.DataFrame()) == pytest.approx(0.02)

        result = _moe_importance(hh, pl.DataFrame(), ["h_size", "h_workers"])
        assert result["h_workers"] > result["h_size"]
        assert result["h_workers"] / result["h_size"] == pytest.approx(np.sqrt(3.0))
        # the two importances straddle the default, since their median is it
        assert np.median([result["h_size"], result["h_workers"]]) == pytest.approx(
            DEFAULT_IMPORTANCE
        )


@pytest.mark.usefixtures("_clean_registry")
class TestCrosstabMoe:
    """Tests for MOE computation on cross-tab controls."""

    @staticmethod
    def _xtab_hh_frame(n: int) -> pl.DataFrame:
        """Build PUMS HH data with dimension columns and replicate weights."""
        rng = np.random.default_rng(42)
        size_ctrl = CONTROLS["h_size"]
        income_ctrl = CONTROLS["h_income"]
        size_vals = [m[0] for m in size_ctrl.valid_members]
        income_vals = [m[0] for m in income_ctrl.valid_members]
        data: dict[str, list] = {
            "zone_group_id": ["Z1"] * n,
            "h_size": rng.choice(size_vals, size=n).tolist(),
            "h_income": rng.choice(income_vals, size=n).tolist(),
            "_xw_WGTP": [100.0] * n,
        }
        return pl.DataFrame(data | _replicates(100.0 + rng.normal(0, 3.0, (80, n))))

    def test_compute_moe_importance_includes_crosstab(self):
        """compute_moe_importance returns importance for cross-tab controls."""
        register_crosstab("h_size_x_income", ["h_size", "h_income"])
        hh = self._xtab_hh_frame(n=200)
        result = _moe_importance(hh, pl.DataFrame(), ["h_size", "h_size_x_income"])
        assert "h_size_x_income" in result
        assert result["h_size_x_income"] > 0


def _grid_search(ef_grid: list[float]) -> list[GridPoint]:
    """Search *ef_grid*, with 5 selected, for 20 homes at 5 against 100 households."""
    targets = ["h_total", "h_size"]
    ct = _make_control_totals(
        {"Z1": [("h_total", 1, 100.0), ("h_size", 1, 50.0), ("h_size", 2, 50.0)]}
    )
    seed = pl.DataFrame(
        {
            "hh_id": list(range(1, 21)),
            "zone_group_id": ["Z1"] * 20,
            "base_weight": [5.0] * 20,
            "h_total": [1] * 20,
            "h_size": [1, 2] * 10,
        }
    )
    return grid_search_expansion_factor(
        seed,
        ct,
        targets,
        ef_grid=ef_grid,
        selected_ef=5.0,
        balancing=_BALANCING,
        importance=dict.fromkeys(targets, 100.0),
    )


class TestGridSearch:
    """The expansion-factor grid search on a tiny single-zone setup."""

    @pytest.mark.parametrize(
        ("ef_grid", "expected"),
        [
            pytest.param([2.0, 5.0, 10.0], [2.0, 5.0, 10.0], id="one_point_per_ef"),
            pytest.param([2.0, 10.0], [2.0, 5.0, 10.0], id="inserted_in_order"),
            pytest.param([], [5.0], id="empty_grid_is_just_the_selection"),
        ],
    )
    def test_selected_ef_injected_into_grid(self, ef_grid, expected):
        """One point per factor; selected_ef is searched whether or not the grid names it."""
        assert [r.max_expansion_factor for r in _grid_search(ef_grid)] == expected

    def test_metrics_are_finite(self):
        """Metrics are within their ranges."""
        gp = _grid_search([5.0])[0]
        assert gp.mape >= 0
        assert gp.p90 >= 0
        assert gp.cv >= 0
        assert 0 <= gp.ess_pct <= 100
        assert gp.converged_zones <= gp.total_zones


class TestEFTradeoffFigure:
    """Tests for the EF tradeoff chart generation."""

    def _sample_grid(self) -> list[GridPoint]:
        return [
            GridPoint(2.0, 10, 10, 5.0, 12.0, 20.0, 0.3, 90.0),
            GridPoint(5.0, 10, 10, 3.0, 8.0, 15.0, 0.5, 75.0),
            GridPoint(10.0, 10, 10, 2.0, 5.0, 10.0, 0.8, 60.0),
        ]

    def test_returns_figure(self):
        """A Figure carrying the three fit-error series plus CV and ESS%."""
        fig = ef_tradeoff_figure(self._sample_grid(), selected_ef=5.0)
        assert isinstance(fig, go.Figure)
        names = {t.name for t in fig.data}  # pyright: ignore[reportAttributeAccessIssue]
        assert names == {"MAPE (%)", "P90 (%)", "Max Error (%)", "CV", "ESS (%)"}
