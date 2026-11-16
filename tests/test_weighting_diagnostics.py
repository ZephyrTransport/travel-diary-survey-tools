"""What a run reports about the weights it produced: the comparer, the cascade, the report."""

import json
import re
from typing import ClassVar

import numpy as np
import polars as pl
import pytest

from processing.weighting.core.hierarchy import LEVELS
from processing.weighting.core.specs import (
    ControlTotals,
    GeographyCoverage,
    ImputationSummary,
    ProfileFit,
    ZoneStatus,
)
from processing.weighting.diagnostics.charts import (
    fit_diverging_figure,
    fit_x_limit,
    weight_distribution_figure,
)
from processing.weighting.diagnostics.comparison import (
    MAX_POINTS,
    MIN_OVERLAP,
    N_DECILES,
    Comparison,
    WeightSet,
    all_pairs,
    compare_pair,
    fitted_weight_sets,
    reference_weight_sets,
)
from processing.weighting.diagnostics.data import (
    exclusions,
    redistribution,
    weight_cascade,
    zone_fit_summary,
)
from processing.weighting.diagnostics.report import generate_report
from processing.weighting.diagnostics.tables import (
    crosswalk_summary_table,
    unweighted_cell_counts,
)

HOUSEHOLDS = LEVELS["households"]
PERSONS = LEVELS["persons"]
PROFILE = "ctramp"
FLAG = "usable_ctramp"
TARGETS = ["h_total", "p_total"]
ZONE = "01"


def _set(name: str, column: str, *, table: str = "households", fitted: bool = True) -> WeightSet:
    """A weight set reaching one table under one column."""
    return WeightSet(name=name, label=name, columns={table: column}, fitted=fitted)


def _frame(**columns: list[float]) -> pl.DataFrame:
    """A table of weight columns."""
    return pl.DataFrame(columns)


def _log_spaced(n: int, low: float = 10, high: float = 1000) -> np.ndarray:
    """*n* weights evenly spaced in log between *low* and *high*."""
    return np.exp(np.linspace(np.log(low), np.log(high), n))


def _compare(a, b):
    """``compare_pair`` of household weights *a* against *b*."""
    return compare_pair(_frame(a=list(a), b=list(b)), HOUSEHOLDS, _set("a", "a"), _set("b", "b"))


def _stats(a, b):
    """``_compare``, for a pair with enough overlap to report."""
    stats = _compare(a, b)
    assert stats is not None
    return stats


class TestFit:
    """The reduced-major-axis line through the scatter."""

    def test_slope_recovers_a_known_relationship(self):
        """An exact relationship of ln A = 0.5 ln B reads as a slope of 0.5."""
        log_b = np.linspace(np.log(10), np.log(10_000), 500)
        assert _stats(np.exp(0.5 * log_b), np.exp(log_b)).slope == pytest.approx(0.5, abs=1e-9)

    def test_weak_correlation_does_not_flatten_the_slope(self):
        """A least-squares slope shrinks toward zero as correlation weakens; this one does not.

        Two sets with the same spread and r of about 0.5 have a least-squares
        slope near 0.5, which would read as one set compressing the other.
        """
        rng = np.random.default_rng(0)
        common = rng.normal(0, 1, 5000)
        log_a = 5 + common + rng.normal(0, 1, 5000)
        log_b = 5 + common + rng.normal(0, 1, 5000)
        stats = _stats(np.exp(log_a), np.exp(log_b))

        least_squares = np.cov(log_a, log_b, bias=True)[0, 1] / log_b.var()
        assert least_squares == pytest.approx(0.5, abs=0.05)
        assert stats.slope == pytest.approx(1.0, abs=0.05)

    def test_swapping_the_sets_gives_the_reciprocal(self):
        """The fit is symmetric, so the reverse needs no second regression."""
        rng = np.random.default_rng(7)
        log_b = rng.uniform(np.log(20), np.log(2000), 300)
        log_a = 0.4 * log_b + rng.normal(0, 0.5, 300)
        forward = _stats(np.exp(log_a), np.exp(log_b))
        reverse = _stats(np.exp(log_b), np.exp(log_a))

        assert forward.slope * reverse.slope == pytest.approx(1.0)

    def test_the_line_passes_through_the_centre(self):
        """The intercept places the line through the mean of both logs."""
        rng = np.random.default_rng(3)
        log_b = rng.uniform(np.log(20), np.log(2000), 400)
        log_a = 1.3 + 0.7 * log_b + rng.normal(0, 0.3, 400)
        stats = _stats(np.exp(log_a), np.exp(log_b))

        assert stats.intercept + stats.slope * log_b.mean() == pytest.approx(log_a.mean())


def _fit_table(rows: list[tuple[str, str, float, float | None]]) -> pl.DataFrame:
    """An h_size fit table from (area, group, error %, SE %) rows, targets all 100."""
    return pl.DataFrame(
        {
            "zone_group_id": [r[0] for r in rows],
            "control_name": ["h_size"] * len(rows),
            "category": [r[1] for r in rows],
            "label": [r[1] for r in rows],
            "target_total": [100.0] * len(rows),
            "weighted_total": [100.0 + r[2] for r in rows],
            "diff": [r[2] for r in rows],
            "diff_pct": [r[2] for r in rows],
            "moe_pct": pl.Series([r[3] for r in rows], dtype=pl.Float64),
        }
    )


class TestFitBars:
    """A bar is red outside its target's ACS 90% margin of error (1.645 SE) and 5%."""

    @staticmethod
    def _bars(rows: list[tuple[str, str, float, float | None]]) -> dict[tuple[str, str], tuple]:
        """(panel, group) -> (color, whisker) from a fit table of (area, group, error %, SE %)."""
        fig = fit_diverging_figure(_fit_table(rows))
        panels = [a.text for a in fig.layout.annotations]
        bars = [t for t in fig.data if t.type == "bar"]
        # a panel with no margin of error anywhere draws no whisker trace
        whiskers = {t.xaxis: t.error_x.array for t in fig.data if t.type == "scatter"}
        out = {}
        for panel, bar in zip(panels, bars, strict=True):
            margins = whiskers.get(bar.xaxis, [None] * len(bar.y))
            for group, color, margin in zip(bar.y, bar.marker.color, margins, strict=True):
                out[panel, group] = (color, margin)
        return out

    @pytest.mark.parametrize(
        ("error", "se", "red"),
        [
            pytest.param(7.0, 4.0, True, id="outside_a_6.6pct_margin"),
            pytest.param(6.0, 4.0, False, id="inside_a_6.6pct_margin"),
            pytest.param(6.0, 1.0, True, id="outside_the_5pct_floor"),
            pytest.param(3.0, 1.0, False, id="outside_a_1.6pct_margin_but_inside_5pct"),
        ],
    )
    def test_red_outside_both_the_margin_of_error_and_5_percent(self, error, se, red):
        """The whisker is the margin of error alone; the floor only decides the color."""
        bars = self._bars([("A", "size_1", error, se)])
        assert bars["A", "size_1"] == ("#c33" if red else "#999", pytest.approx(1.645 * se))

    def test_no_standard_error_is_red_past_15_percent(self):
        """With no margin of error to compare against, the 15% rule decides."""
        bars = self._bars([("A", "size_1", 10.0, None), ("A", "size_2", 20.0, None)])
        assert bars["A", "size_1"][0] == "#999"
        assert bars["A", "size_2"][0] == "#c33"

    def test_one_area_without_a_standard_error_leaves_the_region_without_one(self):
        """Not 0%, which would turn any miss red; the region falls back to the 15% rule.

        Area A alone would give the region a margin of 0.8%, and 8% would be red.
        """
        bars = self._bars([("A", "size_1", 8.0, 1.0), ("B", "size_1", 8.0, None)])
        assert bars["A", "size_1"][0] == "#c33"
        assert bars["Overall", "size_1"][0] == "#999"

    def test_every_profile_shares_one_x_range(self):
        """A 12% miss in one profile sets the range for both, so a 2% miss looks small."""
        tight = _fit_table([("A", "size_1", 2.0, 1.0)])
        loose = _fit_table([("A", "size_1", 12.0, 1.0)])
        assert fit_x_limit([tight]) == 5
        limit = fit_x_limit([tight, loose])
        assert limit == 20
        assert tuple(fit_diverging_figure(tight, limit).layout.xaxis.range) == (-20, 20)


class TestWeightDistribution:
    """Each zone's weights as points spread by density, never wider than the column."""

    WEIGHTED = pl.DataFrame(
        {
            "zone_group_id": ["1"] * 40 + ["2"] * 3,
            "hh_weight": [float(w) for w in range(10, 410, 10)] + [5.0, 50.0, 500.0],
        }
    )

    @staticmethod
    def _points(fig):
        return [t for t in fig.data if t.mode == "markers"]

    def test_one_point_trace_and_one_mean_line_per_zone(self):
        """Every zone gets its points and a line at its mean."""
        fig = weight_distribution_figure(self.WEIGHTED)
        assert [t.name for t in self._points(fig)] == ["1", "2"]
        means = {t.name: t.y[0] for t in fig.data if t.mode == "lines"}
        assert means == {"1 mean": pytest.approx(205.0), "2 mean": pytest.approx(185.0)}

    def test_points_stay_inside_their_column_on_both_scales(self):
        """The densest weights reach the column edge and nothing goes past it."""
        fig = weight_distribution_figure(self.WEIGHTED)
        buttons = {b.label: b.args[0]["x"] for b in fig.layout.updatemenus[0].buttons}
        for xs in buttons.values():
            for i, x in enumerate(xs[0::2]):  # point traces, mean lines between them
                assert all(abs(v - i) <= 0.42 + 1e-9 for v in x)

    def test_buttons_switch_axis_and_spread_together(self):
        """Log and Linear each set the axis and the matching sideways positions."""
        fig = weight_distribution_figure(self.WEIGHTED)
        log, linear = fig.layout.updatemenus[0].buttons
        assert (log.args[1]["yaxis.type"], linear.args[1]["yaxis.type"]) == ("log", "linear")
        assert log.args[0]["x"][0] != linear.args[0]["x"][0]
        assert list(linear.args[0]["x"][0]) == list(self._points(fig)[0].x)

    def test_same_weights_draw_the_same_chart(self):
        """The spread is random but seeded, so a report can be reproduced."""
        a = weight_distribution_figure(self.WEIGHTED).to_json()
        assert a == weight_distribution_figure(self.WEIGHTED).to_json()


class TestZoneTable:
    """Households per zone in their groups, named by Census county."""

    CROSSWALK = pl.DataFrame(
        {
            "puma_id": ["A", "B", "C"],
            "target_zone_id": ["001", "013", "075"],
            "zone_group_id": ["East Bay", "East Bay", "San Francisco"],
        }
    )
    COUNTIES: ClassVar[dict[str, str]] = {
        "001": "Alameda County",
        "013": "Contra Costa County",
        "075": "San Francisco County",
    }

    @staticmethod
    def _rows(html: str) -> list[list[str]]:
        rows = re.findall(r"<tr>(.*?)</tr>", html, re.DOTALL)
        cells = [re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", r) for r in rows]
        return [[re.sub(r"<.*?>", "", c) for c in row] for row in cells]

    def test_groups_counties_and_totals(self):
        """Each group lists its zones and a subtotal; outside homes get their own row."""
        seed = pl.DataFrame({"target_zone_id": ["001", "001", "013", "075", None]})
        rows = self._rows(crosswalk_summary_table(self.CROSSWALK, seed, self.COUNTIES))
        assert rows == [
            ["Zone group", "Zone", "Census county", "Households"],
            ["East Bay", "001", "Alameda County", "2"],
            ["", "013", "Contra Costa County", "1"],
            ["", "", "Group", "3"],
            ["San Francisco", "075", "San Francisco County", "1"],
            ["Outside all zones", "", "", "1"],
            ["Total", "", "", "5"],
        ]

    def test_without_groups_there_is_no_group_column(self):
        """Zones balanced on their own need no group column or subtotals."""
        crosswalk = self.CROSSWALK.with_columns(pl.col("target_zone_id").alias("zone_group_id"))
        seed = pl.DataFrame({"target_zone_id": ["001", "075"]})
        rows = self._rows(crosswalk_summary_table(crosswalk, seed, self.COUNTIES))
        assert rows[0] == ["Zone", "Census county", "Households"]
        assert ["001", "Alameda County", "1"] in rows


class TestSummaries:
    """The five tile values: fold difference at three points, and two correlations."""

    def test_a_set_against_itself(self):
        """The anchor: no difference anywhere, perfect agreement."""
        weights = _log_spaced(200)
        stats = _stats(weights, weights)

        assert stats.median_fold == pytest.approx(1.0)
        assert stats.p90_fold == pytest.approx(1.0)
        assert stats.max_fold == pytest.approx(1.0)
        assert stats.rank_corr == pytest.approx(1.0)
        assert stats.r2 == pytest.approx(1.0)
        assert stats.slope == pytest.approx(1.0)

    def test_fold_difference_is_symmetric(self):
        """A record twice as heavy in A and one twice as heavy in B differ equally."""
        a = np.full(100, 100.0)
        b = a.copy()
        a[:50] *= 2
        b[50:] *= 2
        stats = _stats(a, b)

        assert stats.median_fold == pytest.approx(2.0)
        assert stats.max_fold == pytest.approx(2.0)

    def test_a_single_extreme_record_moves_only_the_max(self):
        """The tail number and the typical numbers answer different questions."""
        weights = _log_spaced(200)
        a = weights.copy()
        a[0] *= 500
        stats = _stats(a, weights)

        assert stats.median_fold == pytest.approx(1.0)
        assert stats.p90_fold == pytest.approx(1.0)
        assert stats.max_fold == pytest.approx(500.0)

    def test_rank_correlation_ignores_how_the_scale_is_stretched(self):
        """Same ordering, any monotone rescaling: the ranks agree exactly."""
        weights = _log_spaced(200)
        rng = np.random.default_rng(5)
        stats = _stats(weights**3 + rng.uniform(0, 1e-6, 200), weights)

        assert stats.rank_corr == pytest.approx(1.0)
        assert stats.median_fold > 1.0

    def test_float_noise_is_no_difference(self):
        """Two fits that agree to 1e-14 read as the same, not as 2% identical."""
        base = _log_spaced(500)
        stats = _stats(base, base * (1 + np.linspace(-2e-14, 2e-14, 500)))

        assert stats.max_fold == pytest.approx(1.0)


class TestOverlap:
    """Which records a comparison is entitled to read."""

    @pytest.mark.parametrize(
        "blank",
        [
            # a zero weight is an exclusion from that fit, not a small number
            pytest.param(0.0, id="zero_weights"),
            # a record one side never weighted says nothing about the difference
            pytest.param(None, id="null_weights"),
        ],
    )
    def test_records_one_side_did_not_weight_are_excluded(self, blank):
        """Only records both sets actually weighted are compared."""
        weights = list(_log_spaced(100))
        assert _stats([blank] * 40 + weights, weights + [blank] * 40).n == 60

    def test_too_little_overlap_is_not_reported(self):
        """Below the floor a slope describes the sample, so report nothing."""
        weights = _log_spaced(MIN_OVERLAP - 1, high=100)
        assert _compare(weights, weights) is None

    def test_missing_column_is_not_an_error(self):
        """A set that does not reach this level is skipped, not a failure."""
        df = _frame(a=[1.0] * 100)

        assert compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b")) is None
        assert compare_pair(df, PERSONS, _set("a", "a"), _set("b", "b")) is None


class TestDeciles:
    """The stability strip: how the ratio behaves across the range of B."""

    def test_heavy_ties_collapse_bins_rather_than_erroring(self):
        """A set where most weights are equal has fewer than ten distinct edges."""
        stats = _stats([100.0] * 90 + [500.0] * 10, [100.0] * 90 + [250.0] * 10)
        assert 0 < len(stats.deciles_ab) < N_DECILES

    def test_a_stable_pair_has_a_flat_band(self):
        """Constant offset between the sets means every decile reads the same."""
        weights = _log_spaced(500, high=10_000)
        stats = _stats(weights * 3, weights)

        # the strip never carries more bands than it has room to label
        assert len(stats.deciles_ab) == N_DECILES
        medians = [d.median for d in stats.deciles_ab]
        assert max(medians) - min(medians) == pytest.approx(0.0, abs=1e-9)
        assert all(d.iqr == pytest.approx(0.0, abs=1e-9) for d in stats.deciles_ab)


class TestSampling:
    """The points drawn for the scatter."""

    @pytest.mark.parametrize(
        ("n", "expected_points"),
        [
            pytest.param(MAX_POINTS * 3, MAX_POINTS, id="capped"),
            pytest.param(50, 50, id="every_point_kept"),
        ],
    )
    def test_sample_is_capped(self, n, expected_points):
        """The scatter carries every overlapping record, up to the cap."""
        weights = _log_spaced(n, high=10_000)
        assert len(_stats(weights, weights).points) == expected_points

    def test_sampling_is_reproducible(self):
        """Rerunning on unchanged data must rewrite an identical report."""
        weights = _log_spaced(MAX_POINTS * 2, high=10_000)
        assert _stats(weights, weights).points == _stats(weights, weights).points


class TestWeightSets:
    """Resolving which sets exist before anything is compared."""

    def test_fitted_sets_take_columns_from_the_hierarchy(self):
        """The profile suffix is spelled in one place, and it is not here."""
        (ctramp,) = fitted_weight_sets(["ctramp"])

        assert ctramp.name == "ctramp"
        assert ctramp.fitted
        assert ctramp.columns["households"] == "hh_weight_ctramp"
        assert ctramp.columns["unlinked_trips"] == "unlinked_trip_weight_ctramp"

    def test_unprofiled_fit_is_named_for_the_survey(self):
        """A run weighting the whole survey has no suffix to carry."""
        (survey,) = fitted_weight_sets([None])

        assert survey.name == "survey"
        assert survey.columns["households"] == "hh_weight"

    def test_reference_sets_are_named_not_discovered(self):
        """A supplied column need not share the level's base name, so the config states it."""
        (vendor,) = reference_weight_sets(
            {
                "vendor": {
                    "label": "vendor",
                    "columns": {"households": "hh_weight", "unlinked_trips": "trip_weight"},
                }
            }
        )

        assert not vendor.fitted
        assert vendor.columns["unlinked_trips"] == "trip_weight"

    @pytest.mark.parametrize(
        "config",
        [
            pytest.param({"vendor": {"label": "vendor"}}, id="no_columns"),
            pytest.param(None, id="nothing_configured"),
        ],
    )
    def test_a_set_with_nothing_to_compare_is_dropped(self, config):
        """Only reference sets that reach at least one level survive."""
        assert reference_weight_sets(config) == []


class TestAllPairs:
    """Every comparable pair, at every level both sets reach."""

    def test_one_entry_per_pair_per_level(self):
        """Pairs are unordered, so three sets make three pairs at each of two levels."""
        weights = list(_log_spaced(100))
        sets = [
            WeightSet(n, n, {"households": n, "persons": n}, fitted=True) for n in ("x", "y", "z")
        ]
        table = _frame(x=weights, y=weights, z=weights)
        pairs = all_pairs({"households": table, "persons": table}, sets)

        assert len(pairs) == 6
        assert {p.level for p in pairs} == {"households", "persons"}

    def test_levels_a_set_does_not_reach_are_skipped(self):
        """A set that stops at households says nothing about persons."""
        weights = list(_log_spaced(100))
        sets = [
            WeightSet("x", "x", {"households": "x", "persons": "x"}, fitted=True),
            WeightSet("y", "y", {"households": "y"}, fitted=True),
        ]
        table = _frame(x=weights, y=weights)
        pairs = all_pairs({"households": table, "persons": table}, sets)

        assert [p.level for p in pairs] == ["households"]

    def test_nothing_to_pair_is_not_an_error(self):
        """A table the run never built, and a lone set, both yield no pairs."""
        weights = list(_log_spaced(100))

        assert all_pairs({"households": None}, [_set("x", "x"), _set("y", "y")]) == []
        assert all_pairs({"households": _frame(x=weights)}, [_set("x", "x")]) == []


def _tables(
    *,
    hh_usable: list[bool],
    hh_weight: list[float],
    per_usable: list[bool],
    per_weight: list[float],
    per_hh: list[int],
) -> dict[str, pl.DataFrame]:
    """Minimal households/persons pair carrying one profile's columns."""
    return {
        "households": pl.DataFrame(
            {
                "hh_id": list(range(1, len(hh_usable) + 1)),
                FLAG: hh_usable,
                f"hh_weight_{PROFILE}": hh_weight,
            }
        ),
        "persons": pl.DataFrame(
            {
                "person_id": list(range(1, len(per_usable) + 1)),
                "hh_id": per_hh,
                FLAG: per_usable,
                f"person_weight_{PROFILE}": per_weight,
            }
        ),
    }


class TestWeightCascade:
    """Records per level, and how many of them the profile admits."""

    def test_counts_rows_and_admitted_records(self):
        """Each level counts its own rows and the ones its own flag admits."""
        tables = _tables(
            hh_usable=[True, False],
            hh_weight=[10.0, 0.0],
            per_usable=[True, True, False],
            per_weight=[10.0, 0.0, 0.0],
            per_hh=[1, 2, 1],
        )
        cascade = weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        rows = {r.table: r for r in cascade}

        assert (rows["households"].rows, rows["households"].usable) == (2, 1)
        assert (rows["persons"].rows, rows["persons"].usable) == (3, 2)

    @pytest.mark.parametrize(
        "drop",
        [
            pytest.param("rename", id="another_profiles_column"),
            pytest.param("delete", id="table_not_supplied"),
        ],
    )
    def test_a_level_this_profile_does_not_reach_is_skipped(self, drop):
        """Whether the column belongs to another profile or the table is absent."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[1.0],
            per_usable=[True],
            per_weight=[1.0],
            per_hh=[1],
        )
        if drop == "rename":
            tables["persons"] = tables["persons"].rename(
                {f"person_weight_{PROFILE}": "person_weight_x"}
            )
        else:
            del tables["persons"]

        reported = [
            r.table for r in weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        ]
        assert reported == ["households"]


class TestExclusions:
    """The records the cleaner excluded, counted per level and reason."""

    def test_counts_each_level_by_its_own_reason(self):
        """A level is read off its own column; a level without one is skipped."""
        tables = {
            "households": pl.DataFrame(
                {"hh_id": [1, 2, 3], "exclusion_reason": ["no subregion", None, "no subregion"]}
            ),
            "persons": pl.DataFrame({"person_id": [1]}),
            "person_days": pl.DataFrame({"person_day_id": [1, 2], "exclusion_reason": ["b", "a"]}),
        }

        rows = [(r.table, r.reason, r.records) for r in exclusions(tables)]

        assert rows == [
            ("households", "no subregion", 2),
            ("person_days", "a", 1),
            ("person_days", "b", 1),
        ]

    def test_nothing_excluded_is_no_rows(self):
        """A null reason excludes nothing, so there is nothing to show."""
        tables = {
            "households": pl.DataFrame({"hh_id": [1]}, schema={"hh_id": pl.Int64}).with_columns(
                pl.lit(None, dtype=pl.String).alias("exclusion_reason")
            )
        }

        assert exclusions(tables) == []


class TestRedistribution:
    """The factor a survivor absorbs for the siblings the gate removed."""

    def test_ratio_is_one_when_every_sibling_is_usable(self):
        """Nothing was removed, so nothing is being carried."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, True],
            per_weight=[10.0, 10.0],
            per_hh=[1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.unchanged == 100.0
        assert row.bands == (0.0, 0.0, 0.0, 0.0)
        assert row.maximum == 1.0

    def test_survivor_carries_the_whole_household(self):
        """One usable member of four stands in for all of them."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, False, False, False],
            per_weight=[40.0, 0.0, 0.0, 0.0],
            per_hh=[1, 1, 1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.maximum == 4.0
        assert row.unchanged == 0.0
        assert row.bands == (0.0, 0.0, 0.0, 100.0)

    def test_an_exact_factor_stays_in_its_range(self):
        """One of three kept is 3x, even when float arithmetic lands a hair above it."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[0.1],
            per_usable=[True, False, False],
            per_weight=[0.1 * 3, 0.0, 0.0],  # 0.30000000000000004
            per_hh=[1, 1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.bands == (0.0, 0.0, 100.0, 0.0)

    def test_each_kept_record_lands_in_one_range(self):
        """Two households, one kept whole and one keeping 2 of 3: shares add up to 100."""
        tables = _tables(
            hh_usable=[True, True],
            hh_weight=[10.0, 10.0],
            per_usable=[True, True, True, False],
            per_weight=[10.0, 15.0, 15.0, 0.0],
            per_hh=[1, 2, 2, 2],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.records == 3
        assert row.unchanged == pytest.approx(100 / 3)
        assert row.bands == pytest.approx((200 / 3, 0.0, 0.0, 0.0))
        assert row.unchanged + sum(row.bands) == pytest.approx(100.0)

    def test_dropped_households_are_excluded(self):
        """A rejected household's members say nothing about redistribution."""
        tables = _tables(
            hh_usable=[True, False],
            hh_weight=[10.0, 0.0],
            per_usable=[True, True],
            per_weight=[10.0, 0.0],
            per_hh=[1, 2],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.maximum == 1.0


def _control_totals() -> ControlTotals:
    """Targets for one zone at both structural controls."""
    return ControlTotals(
        totals=pl.DataFrame(
            {
                "zone_group_id": [ZONE, ZONE],
                "control_name": ["h_total", "p_total"],
                "category": ["total", "total"],
                "target_total": [1000.0, 2500.0],
            }
        ),
        pums_hh_count=500,
        pums_person_count=1200,
        zone_group_ids=[ZONE],
    )


def _fit(profile: str | None, *, n_hh: int = 4) -> ProfileFit:
    """A completed fit carrying the minimum every report section reads."""
    hh_ids = list(range(1, n_hh + 1))
    seed = pl.DataFrame(
        {
            "hh_id": hh_ids,
            "zone_group_id": [ZONE] * n_hh,
            "target_zone_id": [ZONE] * n_hh,
            "h_total": [1.0] * n_hh,
            "p_total": [2.0] * n_hh,
            "base_weight": [200.0] * n_hh,
        }
    )
    return ProfileFit(
        profile=profile,
        usability_flag_col=f"usable_{profile}" if profile else "survey_complete",
        seed_incidence=seed,
        pre_imputation_incidence=seed,
        imputation_summary=[ImputationSummary("h_size", "household", n_hh, 1, 0.4, 0.8)],
        coverage=GeographyCoverage(profile=profile, n_universe=n_hh, n_placed=n_hh),
        weights=pl.DataFrame({"hh_id": hh_ids, "hh_weight": [250.0] * n_hh}),
        statuses=[
            ZoneStatus(
                zone_group_id=ZONE, converged=True, iterations=7, delta=1e-9, max_gamma_diff=1e-9
            )
        ],
    )


def _propagated_tables(profiles: list[str | None]) -> dict[str, pl.DataFrame]:
    """Propagated canonical tables carrying one weight column per profile."""
    households = pl.DataFrame({"hh_id": [1, 2, 3, 4]})
    persons = pl.DataFrame({"person_id": [1, 2, 3, 4], "hh_id": [1, 1, 2, 3]})
    for profile in profiles:
        suffix = f"_{profile}" if profile else ""
        flag = f"usable_{profile}" if profile else "survey_complete"
        households = households.with_columns(
            pl.Series(flag, [True, True, True, False]),
            pl.Series(f"hh_weight{suffix}", [250.0, 250.0, 250.0, 0.0]),
        )
        persons = persons.with_columns(
            pl.Series(flag, [True, True, True, True]),
            pl.Series(f"person_weight{suffix}", [250.0, 250.0, 250.0, 250.0]),
        )
    return {"households": households, "persons": persons}


def _headings(html: str) -> list[tuple[int, str]]:
    """Every numbered section heading in document order, as (number, title)."""
    return [
        (int(number), title.strip())
        for number, title in re.findall(r"<h2>(\d+) &mdash; ([^<]*)", html)
    ]


def _numbered_headings(html: str) -> list[int]:
    """Distinct section numbers, in the order they first appear across the panes."""
    seen: list[int] = []
    for number, _ in _headings(html):
        if number not in seen:
            seen.append(number)
    return seen


def _section(html: str, section_id: str) -> str:
    """One section's markup, from its opening tag to its close."""
    start = html.index(f'<section id="{section_id}"')
    return html[start : html.index("</section>", start)]


def _panes(html: str) -> list[str]:
    """The per-profile panes, as raw HTML fragments."""
    return html.split('class="profile-pane"')[1:]


def _render(tmp_path, profiles=("ctramp",), *, fits=None, tables=None, **kwargs) -> str:
    """The report's HTML for one fit per profile, or for *fits*."""
    fits = fits or {p: _fit(p) for p in profiles}
    out = generate_report(
        fits=fits,
        control_totals=_control_totals(),
        target_names=TARGETS,
        tables=_propagated_tables(list(fits)) if tables is None else tables,
        output_path=tmp_path / "diagnostics.html",
        **kwargs,
    )
    return out.read_text(encoding="utf-8")


class TestCellCountImportance:
    """Each control's importance, beside the counts that explain its fit."""

    def _table(self, **importance) -> str:
        return unweighted_cell_counts(_fit("ctramp").seed_incidence, TARGETS, **importance)

    def test_shows_what_balancing_used_with_the_moe_value_in_parentheses(self):
        """A set value shows with the MOE's beside it; a control with no MOE shows a dash."""
        html = self._table(
            moe_importance={"h_total": 64.4},
            importance_used={"h_total": 200.0, "p_total": 100.0},
        )

        assert "<th>Importance (MOE-computed)</th>" in html
        assert (
            '<td rowspan="1">200 <em style="color:#888;font-weight:normal">(64)</em></td>' in html
        )
        assert (
            '<td rowspan="1">100 <em style="color:#888;font-weight:normal">(&mdash;)</em></td>'
            in html
        )

    def test_no_parentheses_when_importance_was_not_moe_based(self):
        """Without MOE-based importance there is nothing to compare against."""
        html = self._table(importance_used={"h_total": 200.0, "p_total": 100.0})

        assert "<th>Importance</th>" in html
        assert '<th rowspan="1">h_total</th><td rowspan="1">200</td>' in html

    def test_no_importance_columns_when_none_given(self):
        """A caller with no importance gets the counts alone."""
        assert "importance" not in self._table().lower()


class TestMapeLeavesOutRowsWithNoTarget:
    """A zero target has no % error, so it must not count as a perfect fit."""

    def test_zero_target_rows_do_not_lower_mape(self):
        """Two targets missed by 10% and 20%, plus a row with no target: MAPE is 15%."""
        fit = pl.DataFrame(
            {
                "zone_group_id": [ZONE] * 3,
                "control_name": ["h_total", "h_size", "h_size"],
                "category": ["total", "size_1", "size_2"],
                "target_total": [100.0, 50.0, 0.0],
                "weighted_total": [110.0, 60.0, 0.0],
                "diff_pct": [10.0, 20.0, 0.0],
            }
        )
        assert zone_fit_summary(fit)["mape"].item() == 15.0


def test_zone_totals_are_h_total_and_p_total():
    """A control that covers only some households or persons doesn't set the totals."""
    fit = pl.DataFrame(
        {
            "zone_group_id": [ZONE] * 4,
            "control_name": ["h_children", "h_total", "p_education", "p_total"],
            "category": ["children_1_plus", "total", "college", "total"],
            "target_total": [30.0, 100.0, 120.0, 250.0],
            "weighted_total": [30.0, 102.0, 120.0, 245.0],
            "diff_pct": [0.0, 2.0, 0.0, -2.0],
        }
    )
    row = zone_fit_summary(fit).row(0, named=True)
    assert (row["hh_target"], row["per_target"]) == (100.0, 250.0)


class TestCellCountEmptyCategories:
    """A category zone merges replaced in every zone gets no row."""

    SEED = pl.DataFrame(
        {
            "hh_id": [1, 2],
            "zone_group_id": [ZONE, ZONE],
            "h_total": [1.0, 1.0],
            "h_size__size_1": [1.0, 0.0],
            "h_size__size_2": [0.0, 0.0],
            "h_size__size_2_plus": [0.0, 1.0],
        }
    )

    @staticmethod
    def _totals(size_2_target: float) -> ControlTotals:
        return ControlTotals(
            totals=pl.DataFrame(
                {
                    "zone_group_id": [ZONE] * 4,
                    "control_name": ["h_total", "h_size", "h_size", "h_size"],
                    "category": ["total", "size_1", "size_2", "size_2_plus"],
                    "target_total": [100.0, 40.0, size_2_target, 60.0],
                }
            ),
            pums_hh_count=50,
            pums_person_count=0,
            zone_group_ids=[ZONE],
        )

    def test_no_records_and_no_target_is_left_out(self):
        """size_2 was merged into size_2_plus, so it has nothing to show."""
        html = unweighted_cell_counts(self.SEED, ["h_total", "h_size"], self._totals(0.0))
        assert "Size 2 Plus" in html
        assert ">Size 2<" not in html

    def test_a_target_with_no_records_is_kept(self):
        """A group the survey missed but the PUMS has is the row worth seeing."""
        html = unweighted_cell_counts(self.SEED, ["h_total", "h_size"], self._totals(10.0))
        assert ">Size 2<" in html

    def test_records_only_where_it_is_not_a_target_are_left_out(self):
        """The seed keeps a zone-merged category's own column, records and all.

        Where the zone merge replaced it, it is no target, so those records
        are already counted in the merged group's row.
        """
        seed = self.SEED.with_columns(pl.Series("h_size__size_2", [0.0, 1.0]))
        totals = self._totals(0.0)
        totals = ControlTotals(
            totals=totals.totals.filter(pl.col("category") != "size_2"),
            pums_hh_count=50,
            pums_person_count=0,
            zone_group_ids=[ZONE],
        )
        html = unweighted_cell_counts(seed, ["h_total", "h_size"], totals)
        assert ">Size 2<" not in html


class TestGenerateReport:
    """The document a run writes, however many profiles it fitted."""

    def test_single_unprofiled_fit_renders(self, tmp_path):
        """A run weighting the whole survey collapses the toggle to one pane."""
        html = _render(tmp_path, (None,))
        assert html.count('class="profile-pane"') == 1
        assert 'data-profile="survey"' in html

    def test_one_pane_per_fit(self, tmp_path):
        """Each profile gets a pane and a button; run-level sections appear once.

        Every pane carries the same section numbers, so a number is an address
        that does not move under the toggle.
        """
        profiles = ("ctramp", "daysim", "analysis")
        html = _render(tmp_path, profiles, run_meta={"PUMS": "2023 &middot; FIPS 06"})

        assert html.count('class="profile-pane"') == 3
        assert len({tuple(_headings(pane)) for pane in _panes(html)}) == 1
        for profile in profiles:
            assert f'data-profile="{profile}"' in html
        # a report shared detached from its run still identifies the run
        assert "2023 &middot; FIPS 06" in html

        titles = [title for _, title in _headings(html)]
        assert titles.count("Profile Comparison") == 1
        assert titles.count("Weight Cascade") == 1
        cascade = _section(html, "sec-cascade")
        for profile in profiles:
            assert f"<th>{profile}</th>" in cascade
            # weight carried for dropped records follows each profile's toggle
            assert f'class="profile-view" data-profile="{profile}"' in cascade
        # every kept record is weighted or the run stops, so only kept counts show
        assert "Unweighted" not in cascade
        # no geometry was passed, so the crosswalk is left out entirely
        assert "Crosswalk Map" not in html

    @pytest.mark.parametrize("excluded", [True, False], ids=["excluded", "none"])
    def test_the_cascade_shows_what_the_cleaner_excluded(self, tmp_path, excluded):
        """Shown only when something was excluded."""
        tables = _propagated_tables(["ctramp"])
        if excluded:
            tables["households"] = tables["households"].with_columns(
                pl.Series("exclusion_reason", [None, None, None, "no subregion"])
            )
        cascade = _section(_render(tmp_path, tables=tables), "sec-cascade")

        assert ("Excluded by the cleaner" in cascade) is excluded
        assert ("<td>no subregion</td>" in cascade) is excluded

    def test_omitted_imputation_leaves_no_gap(self, tmp_path):
        """A run that imputed nothing skips the section without skipping a number."""
        fit = _fit("ctramp")
        fit.imputation_summary = []
        html = _render(tmp_path, fits={"ctramp": fit})
        assert "Fractional Seed Imputation" not in html
        numbers = _numbered_headings(html)
        assert numbers == list(range(1, len(numbers) + 1))

    def test_no_fits_raises(self, tmp_path):
        """An empty run has nothing to describe; say so rather than write a shell."""
        with pytest.raises(ValueError, match="at least one completed fit"):
            generate_report(
                fits={},
                control_totals=_control_totals(),
                target_names=TARGETS,
                tables={},
                output_path=tmp_path / "diagnostics.html",
            )


class TestSectionNumbering:
    """Heading numbers as addresses: fixed under the toggle."""

    def test_pane_missing_a_section_keeps_the_others_in_place(self, tmp_path):
        """One profile with nothing to impute must not renumber the other's pane.

        The number is assigned once for the document, so the profile that skips
        the section leaves a gap in its own pane rather than shifting every
        heading below it.
        """
        fits = {p: _fit(p) for p in ("ctramp", "analysis")}
        fits["analysis"].imputation_summary = []
        ctramp, analysis = (
            {title: number for number, title in _headings(pane)}
            for pane in _panes(_render(tmp_path, fits=fits))
        )

        imputed = next(t for t in ctramp if t.startswith("Fractional"))
        assert imputed not in analysis
        assert ctramp[imputed] not in analysis.values()

        for title in ctramp.keys() & analysis.keys():
            assert ctramp[title] == analysis[title]


class TestComparerSection:
    """The pairwise weight-set comparer, which is run-level or absent."""

    def _comparison(self, names):
        """A comparison over the household weights the fixture tables carry."""
        sets = [
            WeightSet(name=n, label=n, columns={"households": f"hh_weight_{n}"}, fitted=True)
            for n in names
        ]
        weights = _log_spaced(200)
        table = pl.DataFrame({f"hh_weight_{n}": weights * (i + 1) for i, n in enumerate(names)})
        return Comparison(sets=sets, pairs=all_pairs({"households": table}, sets))

    def test_absent_when_nothing_to_compare(self, tmp_path):
        """One weight set has no pair, so the section is left out entirely."""
        assert "Weight Set Comparer" not in _render(tmp_path, comparison=None)

    def test_pairs_are_embedded_as_data(self, tmp_path):
        """One payload drawn in the browser, not one baked figure per pair."""
        html = _render(tmp_path, comparison=self._comparison(["ctramp", "vendor"]))
        comparer = _section(html, "sec-comparer")
        blob = html.split("var DATA = ")[1].split(";\n")[0]
        data = json.loads(blob)

        # the section is run-level, so it sits above the per-profile panes,
        # appears once, and carries the five agreed summary tiles
        assert html.index('id="sec-comparer"') < html.index('class="profile-pane"')
        assert [t for _, t in _headings(html)].count("Weight Set Comparer") == 1
        assert comparer.count('class="tile"') == 5
        # the caveat is above the plot it applies to
        assert html.index("not a validation") < html.index('id="cmp-scatter"')
        # adding a run-level section must not leave a gap in the numbering
        numbers = _numbered_headings(html)
        assert numbers == list(range(1, len(numbers) + 1))

        assert [s["name"] for s in data["sets"]] == ["ctramp", "vendor"]
        assert data["levels"] == [{"name": "households", "label": "Households"}]
        assert len(data["pairs"]) == 1
        pair = data["pairs"][0]
        assert pair["n"] == 200
        assert {"med", "p90", "max", "rho", "r2", "slope", "icept", "decAB", "decBA"} <= set(pair)


class TestNavigationAndDefinitions:
    """The sidebar outline and the Definitions section every header links into."""

    def test_every_definition_link_has_a_target(self, tmp_path):
        """A header that links to a definition the page does not carry is a dead end."""
        html = _render(tmp_path, ("ctramp", "analysis"))
        links = set(re.findall(r'href="#(g-[\w]+)"', html))
        targets = set(re.findall(r'id="(g-[\w]+)"', html))

        assert links
        assert links <= targets

    def test_definitions_list_only_what_the_page_uses(self, tmp_path):
        """With nothing to compare, the comparer's statistics are not listed."""
        html = _render(tmp_path, ("ctramp", "analysis"))

        assert 'id="g-ess"' in html
        assert 'id="g-rho"' not in html

    def test_sidebar_lists_every_numbered_section(self, tmp_path):
        """One entry per rendered section, carrying the same number as the heading."""
        html = _render(tmp_path, ("ctramp", "analysis"))
        nav = html[html.index('<nav class="side"') : html.index("</nav>")]
        entries = re.findall(r'data-sec="(\w+)"><span class="n">(\d+)</span>', nav)

        assert [int(n) for _, n in entries] == _numbered_headings(html)

    def test_section_ids_are_unique(self, tmp_path):
        """Each pane repeats its sections, so their ids carry the profile."""
        html = _render(tmp_path, ("ctramp", "analysis"))
        ids = re.findall(r'<section id="([^"]+)"', html)

        assert len(ids) == len(set(ids))
        assert "sec-balancer-ctramp" in ids
        assert "sec-balancer-analysis" in ids
