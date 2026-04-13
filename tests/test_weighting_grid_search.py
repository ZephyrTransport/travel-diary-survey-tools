"""Tests for expansion-factor grid search and the EF tradeoff chart."""

import plotly.graph_objects as go
import polars as pl
import pytest

from processing.weighting.balancing.balancer import grid_search_expansion_factor
from processing.weighting.diagnostics.charts import ef_tradeoff_figure
from processing.weighting.specs import ControlTotals, GridPoint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_control_totals(
    zone_data: dict[str, list[tuple[str, int, float]]],
) -> ControlTotals:
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
        {"geo_id": pl.Utf8, "category": pl.Utf8, "target_total": pl.Float64}
    )
    return ControlTotals(
        totals=totals,
        pums_hh_count=0,
        pums_person_count=0,
        geo_ids=sorted(zone_data),
    )


def _make_seed(zone_hh: dict[str, int]) -> pl.DataFrame:
    """Build a seed table with base_weight and h_total/h_size category columns."""
    rows: list[dict] = []
    hh_id = 1
    for geo_id, n in zone_hh.items():
        for i in range(n):
            rows.append(
                {
                    "hh_id": hh_id,
                    "ctrl_geoid": geo_id,
                    "base_weight": 1.0,
                    "h_total": 1,
                    "h_size": 1 if i % 2 == 0 else 2,
                }
            )
            hh_id += 1
    return pl.DataFrame(rows).cast(
        {
            "hh_id": pl.Int64,
            "ctrl_geoid": pl.Utf8,
            "base_weight": pl.Float64,
            "h_total": pl.Int64,
            "h_size": pl.Int64,
        }
    )


# ---------------------------------------------------------------------------
# GridPoint dataclass
# ---------------------------------------------------------------------------


class TestGridPoint:
    """Tests for the GridPoint dataclass used to store results of each grid search point."""

    def test_fields(self):
        """Fields should be set correctly and accessible."""
        gp = GridPoint(
            max_expansion_factor=5.0,
            converged_zones=10,
            total_zones=12,
            mape=3.5,
            p90=8.2,
            max_error=15.0,
            cv=0.45,
            ess_pct=78.0,
        )
        assert gp.max_expansion_factor == 5.0
        assert gp.converged_zones == 10
        assert gp.total_zones == 12
        assert gp.mape == 3.5
        assert gp.p90 == 8.2
        assert gp.max_error == 15.0
        assert gp.cv == 0.45
        assert gp.ess_pct == 78.0


# ---------------------------------------------------------------------------
# grid_search_expansion_factor
# ---------------------------------------------------------------------------


class TestGridSearch:
    """Integration-style test with a tiny single-zone setup."""

    @pytest.fixture
    def tiny_setup(self):
        """Single zone with two controls (h_total and h_size) and 20 seed households."""
        targets = ["h_total", "h_size"]
        ct = _make_control_totals(
            {
                "Z1": [
                    ("h_total", 1, 100.0),
                    ("h_size", 1, 50.0),
                    ("h_size", 2, 50.0),
                ]
            }
        )
        seed = _make_seed({"Z1": 20})
        return seed, ct, targets

    def test_returns_one_point_per_ef(self, tiny_setup):
        """Returns one GridPoint per max_expansion_factor in the grid."""
        seed, ct, targets = tiny_setup
        grid = [2.0, 5.0, 10.0]
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=grid,
            selected_ef=5.0,
        )
        assert len(results) == 3
        assert [r.max_expansion_factor for r in results] == [2.0, 5.0, 10.0]

    def test_selected_ef_injected_into_grid(self, tiny_setup):
        """Even if selected_ef is not in ef_grid, it should be included in results."""
        seed, ct, targets = tiny_setup
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=[2.0, 10.0],
            selected_ef=5.0,
        )
        efs = [r.max_expansion_factor for r in results]
        assert 5.0 in efs
        assert len(results) == 3

    def test_metrics_are_finite(self, tiny_setup):
        """Metrics should be finite and within expected ranges."""
        seed, ct, targets = tiny_setup
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=[5.0],
            selected_ef=5.0,
        )
        gp = results[0]
        assert gp.mape >= 0
        assert gp.p90 >= 0
        assert gp.cv >= 0
        assert 0 <= gp.ess_pct <= 100
        assert gp.converged_zones <= gp.total_zones

    def test_empty_grid_returns_empty(self, tiny_setup):
        """If the grid is empty, only the selected_ef should be returned."""
        seed, ct, targets = tiny_setup
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=[],
            selected_ef=5.0,
        )
        # selected_ef is auto-injected so we get 1 result
        assert len(results) == 1
        assert results[0].max_expansion_factor == 5.0


# ---------------------------------------------------------------------------
# ef_tradeoff_figure
# ---------------------------------------------------------------------------


class TestEFTradeoffFigure:
    """Tests for the EF tradeoff chart generation."""

    def _sample_grid(self) -> list[GridPoint]:
        return [
            GridPoint(2.0, 10, 10, 5.0, 12.0, 20.0, 0.3, 90.0),
            GridPoint(5.0, 10, 10, 3.0, 8.0, 15.0, 0.5, 75.0),
            GridPoint(10.0, 10, 10, 2.0, 5.0, 10.0, 0.8, 60.0),
        ]

    def test_returns_figure(self):
        """Returns a Plotly Figure object."""
        fig = ef_tradeoff_figure(self._sample_grid(), selected_ef=5.0)
        assert isinstance(fig, go.Figure)

    def test_has_five_traces(self):
        """Three fit-error traces plus CV and ESS%."""
        fig = ef_tradeoff_figure(self._sample_grid(), selected_ef=5.0)
        assert len(fig.data) == 5  # pyright: ignore[reportArgumentType] # MAPE, P90, Max Error, CV, ESS%

    def test_trace_names(self):
        """Traces should be named correctly for legend."""
        fig = ef_tradeoff_figure(self._sample_grid(), selected_ef=5.0)
        names = {t.name for t in fig.data}  # pyright: ignore[reportAttributeAccessIssue]
        assert names == {"MAPE (%)", "P90 (%)", "Max Error (%)", "CV", "ESS (%)"}

    def test_layout_dimensions(self):
        """Figure should have specified width and height for consistent rendering."""
        fig = ef_tradeoff_figure(self._sample_grid(), selected_ef=5.0)
        assert fig.layout.width == 960
        assert fig.layout.height == 600
