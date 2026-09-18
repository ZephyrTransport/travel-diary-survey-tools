"""Tests for comparing two weight sets.

The cases that matter are the ones where a plausible-looking implementation
still reports the wrong thing: a regression run in the wrong direction, an
overlap that quietly includes records one side never weighted, and a pair with
too little in common to say anything at all.
"""

import numpy as np
import polars as pl
import pytest

from processing.weighting.core.hierarchy import LEVELS
from processing.weighting.diagnostics.comparison import (
    MAX_POINTS,
    MIN_OVERLAP,
    N_DECILES,
    WeightSet,
    all_pairs,
    compare_pair,
    fitted_weight_sets,
    inheritance,
    reference_weight_sets,
)

HOUSEHOLDS = LEVELS["households"]
PERSONS = LEVELS["persons"]


def _set(name: str, column: str, *, table: str = "households", fitted: bool = True) -> WeightSet:
    """A weight set reaching one table under one column."""
    return WeightSet(name=name, label=name, columns={table: column}, fitted=fitted)


def _frame(**columns: list[float]) -> pl.DataFrame:
    """A table of weight columns."""
    return pl.DataFrame(columns)


class TestFit:
    """The reduced-major-axis line through the scatter."""

    def test_slope_recovers_a_known_relationship(self):
        """An exact relationship of ln A = 0.5 ln B reads as a slope of 0.5."""
        log_b = np.linspace(np.log(10), np.log(10_000), 500)
        df = _frame(a=list(np.exp(0.5 * log_b)), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.slope == pytest.approx(0.5, abs=1e-9)

    def test_weak_correlation_does_not_flatten_the_slope(self):
        """Regression: a least-squares slope shrinks toward zero as correlation weakens.

        Two sets with the same spread and r of about 0.5 have a least-squares
        slope near 0.5, which reads as one set compressing the other when neither
        does. Measured on bats_2023: least squares gave 0.42 for ours on the
        vendor's, where the spreads differ by only 0.86.
        """
        rng = np.random.default_rng(0)
        common = rng.normal(0, 1, 5000)
        log_a = 5 + common + rng.normal(0, 1, 5000)
        log_b = 5 + common + rng.normal(0, 1, 5000)
        df = _frame(a=list(np.exp(log_a)), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        least_squares = np.cov(log_a, log_b, bias=True)[0, 1] / log_b.var()
        assert stats is not None
        assert least_squares == pytest.approx(0.5, abs=0.05)
        assert stats.slope == pytest.approx(1.0, abs=0.05)

    def test_swapping_the_sets_gives_the_reciprocal(self):
        """The fit is symmetric, so the reverse needs no second regression."""
        rng = np.random.default_rng(7)
        log_b = rng.uniform(np.log(20), np.log(2000), 300)
        log_a = 0.4 * log_b + rng.normal(0, 0.5, 300)
        forward = compare_pair(
            _frame(a=list(np.exp(log_a)), b=list(np.exp(log_b))),
            HOUSEHOLDS,
            _set("a", "a"),
            _set("b", "b"),
        )
        reverse = compare_pair(
            _frame(a=list(np.exp(log_b)), b=list(np.exp(log_a))),
            HOUSEHOLDS,
            _set("a", "a"),
            _set("b", "b"),
        )

        assert forward is not None
        assert reverse is not None
        assert forward.slope * reverse.slope == pytest.approx(1.0)

    def test_the_line_passes_through_the_centre(self):
        """The intercept places the line through the mean of both logs."""
        rng = np.random.default_rng(3)
        log_b = rng.uniform(np.log(20), np.log(2000), 400)
        log_a = 1.3 + 0.7 * log_b + rng.normal(0, 0.3, 400)
        df = _frame(a=list(np.exp(log_a)), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.intercept + stats.slope * log_b.mean() == pytest.approx(log_a.mean())

    def test_related_weights_give_a_positive_slope(self):
        """Regression: correlating the ratio against B is negative by construction."""
        rng = np.random.default_rng(0)
        base = rng.uniform(np.log(50), np.log(5000), 400)
        df = _frame(
            a=list(np.exp(base + rng.normal(0, 0.2, 400))),
            b=list(np.exp(base + rng.normal(0, 0.2, 400))),
        )
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.slope > 0


class TestSummaries:
    """The five tile values: fold difference at three points, and two correlations."""

    def test_a_set_against_itself(self):
        """The anchor: no difference anywhere, perfect agreement."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 200)))
        stats = compare_pair(
            _frame(a=weights, b=list(weights)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.median_fold == pytest.approx(1.0)
        assert stats.p90_fold == pytest.approx(1.0)
        assert stats.max_fold == pytest.approx(1.0)
        assert stats.rank_corr == pytest.approx(1.0)
        assert stats.r2 == pytest.approx(1.0)
        assert stats.slope == pytest.approx(1.0)

    def test_fold_difference_is_symmetric(self):
        """A record twice as heavy in A and one twice as heavy in B differ equally."""
        base = np.full(100, 100.0)
        a = base.copy()
        b = base.copy()
        a[:50] *= 2
        b[50:] *= 2
        stats = compare_pair(
            _frame(a=list(a), b=list(b)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.median_fold == pytest.approx(2.0)
        assert stats.max_fold == pytest.approx(2.0)

    def test_fold_percentiles_are_ordered(self):
        """Median, p90 and max are three points on one distribution."""
        rng = np.random.default_rng(11)
        log_b = rng.uniform(np.log(10), np.log(1000), 1000)
        df = _frame(a=list(np.exp(log_b + rng.normal(0, 0.6, 1000))), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert 1.0 <= stats.median_fold <= stats.p90_fold <= stats.max_fold

    def test_a_single_extreme_record_moves_only_the_max(self):
        """The tail number and the typical numbers answer different questions."""
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 200))
        a = weights.copy()
        a[0] *= 500
        stats = compare_pair(
            _frame(a=list(a), b=list(weights)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.median_fold == pytest.approx(1.0)
        assert stats.p90_fold == pytest.approx(1.0)
        assert stats.max_fold == pytest.approx(500.0)

    def test_rank_correlation_ignores_how_the_scale_is_stretched(self):
        """Same ordering, any monotone rescaling: the ranks agree exactly."""
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 200))
        rng = np.random.default_rng(5)
        bent = weights**3 + rng.uniform(0, 1e-6, 200)
        stats = compare_pair(
            _frame(a=list(bent), b=list(weights)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.rank_corr == pytest.approx(1.0)
        assert stats.median_fold > 1.0

    def test_float_noise_is_no_difference(self):
        """Two fits that agree to 1e-14 read as the same, not as 2% identical."""
        base = np.exp(np.linspace(np.log(10), np.log(1000), 500))
        nudged = base * (1 + np.linspace(-2e-14, 2e-14, 500))
        stats = compare_pair(
            _frame(a=list(base), b=list(nudged)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.max_fold == pytest.approx(1.0)


class TestOverlap:
    """Which records a comparison is entitled to read."""

    def test_zero_weights_are_excluded(self):
        """A zero weight is an exclusion from that fit, not a small number."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))
        df = _frame(a=[0.0] * 40 + weights, b=weights + [0.0] * 40)
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.n == 60

    def test_nulls_are_excluded(self):
        """A record one side never weighted says nothing about how they differ."""
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 100))
        df = pl.DataFrame({"a": weights, "b": weights}).with_columns(
            pl.when(pl.int_range(pl.len()) < 30).then(None).otherwise(pl.col("b")).alias("b")
        )
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.n == 70

    def test_too_little_overlap_is_not_reported(self):
        """Below the floor a slope describes the sample, so report nothing."""
        n = MIN_OVERLAP - 1
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), n)))
        df = _frame(a=weights, b=list(weights))

        assert compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b")) is None

    def test_missing_column_is_not_an_error(self):
        """A set that does not reach this level is skipped, not a failure."""
        df = _frame(a=[1.0] * 100)

        assert compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b")) is None
        assert compare_pair(df, PERSONS, _set("a", "a"), _set("b", "b")) is None


class TestDeciles:
    """The stability strip: how the ratio behaves across the range of B."""

    def test_at_most_ten_bins(self):
        """The strip never carries more bands than it has room to label."""
        rng = np.random.default_rng(3)
        log_b = rng.uniform(np.log(10), np.log(10_000), 1000)
        df = _frame(a=list(np.exp(log_b + rng.normal(0, 0.3, 1000))), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert 0 < len(stats.deciles_ab) <= N_DECILES

    def test_heavy_ties_collapse_bins_rather_than_erroring(self):
        """A set where most weights are equal has fewer than ten distinct edges."""
        df = _frame(a=[100.0] * 90 + [500.0] * 10, b=[100.0] * 90 + [250.0] * 10)
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert 0 < len(stats.deciles_ab) < N_DECILES

    def test_a_stable_pair_has_a_flat_band(self):
        """Constant offset between the sets means every decile reads the same."""
        weights = np.exp(np.linspace(np.log(10), np.log(10_000), 500))
        df = _frame(a=list(weights * 3), b=list(weights))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        medians = [d.median for d in stats.deciles_ab]
        assert max(medians) - min(medians) == pytest.approx(0.0, abs=1e-9)
        assert all(d.iqr == pytest.approx(0.0, abs=1e-9) for d in stats.deciles_ab)


class TestSampling:
    """The points drawn for the scatter."""

    def test_sample_is_capped(self):
        """A level with a quarter-million records still ships a readable scatter."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(10_000), MAX_POINTS * 3)))
        df = _frame(a=weights, b=list(weights))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert len(stats.points) == MAX_POINTS

    def test_small_overlap_keeps_every_point(self):
        """Under the cap nothing is thrown away."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 50)))
        df = _frame(a=weights, b=list(weights))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert len(stats.points) == 50

    def test_sampling_is_reproducible(self):
        """Rerunning on unchanged data must rewrite an identical report."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(10_000), MAX_POINTS * 2)))
        df = _frame(a=weights, b=list(weights))
        first = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))
        second = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert first is not None
        assert second is not None
        assert first.points == second.points


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
        """A supplied column need not share the level's base name.

        The vendor's trip weights arrive as ``trip_weight`` while the hierarchy
        calls that level ``unlinked_trip_weight``, so no suffix rule can reach
        them and the config states the column outright.
        """
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

    def test_reference_label_defaults_to_the_name(self):
        """Naming a set is enough; labelling it is optional."""
        (vendor,) = reference_weight_sets({"vendor": {"columns": {"households": "hh_weight"}}})

        assert vendor.label == "vendor"

    def test_reference_set_without_columns_is_dropped(self):
        """A set reaching no level cannot be compared to anything."""
        assert reference_weight_sets({"vendor": {"label": "vendor"}}) == []

    def test_no_references_configured(self):
        """With nothing configured the run compares its own fits and stops."""
        assert reference_weight_sets(None) == []


class TestAllPairs:
    """Every comparable pair, at every level both sets reach."""

    def test_one_entry_per_pair_per_level(self):
        """Pairs are unordered, so three sets make three comparisons, not six."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))
        sets = [
            WeightSet("x", "x", {"households": "x", "persons": "x"}, fitted=True),
            WeightSet("y", "y", {"households": "y", "persons": "y"}, fitted=True),
            WeightSet("z", "z", {"households": "z", "persons": "z"}, fitted=True),
        ]
        table = _frame(x=weights, y=list(weights), z=list(weights))
        pairs = all_pairs({"households": table, "persons": table}, sets)

        # Three sets make three pairs, at each of the two levels.
        assert len(pairs) == 6
        assert {p.level for p in pairs} == {"households", "persons"}

    def test_levels_a_set_does_not_reach_are_skipped(self):
        """A set that stops at households says nothing about persons."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))
        sets = [
            WeightSet("x", "x", {"households": "x", "persons": "x"}, fitted=True),
            WeightSet("y", "y", {"households": "y"}, fitted=True),
        ]
        table = _frame(x=weights, y=list(weights))
        pairs = all_pairs({"households": table, "persons": table}, sets)

        assert [p.level for p in pairs] == ["households"]

    def test_absent_table_is_skipped(self):
        """A table the run never built is not an error."""
        sets = [_set("x", "x"), _set("y", "y")]

        assert all_pairs({"households": None}, sets) == []

    def test_one_set_has_nothing_to_compare(self):
        """A comparison needs two sides."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))

        assert all_pairs({"households": _frame(x=weights)}, [_set("x", "x")]) == []


class TestInheritance:
    """How a level's weight descends from its parent, and by what mechanism."""

    def _tables(self, *, trips, days, day_of_trip=None):
        """A days table and a trips table, trips mapped to days by *day_of_trip*."""
        ids = day_of_trip if day_of_trip is not None else list(range(len(trips)))
        return {
            "days": pl.DataFrame({"day_id": list(range(len(days))), "dw": days}),
            "unlinked_trips": pl.DataFrame({"day_id": ids, "tw": trips}),
        }

    def _set(self):
        return WeightSet(
            name="v", label="v", columns={"days": "dw", "unlinked_trips": "tw"}, fitted=False
        )

    def test_a_pure_copy_adds_nothing(self):
        """Every child carries its parent's weight, so the level restates its parent."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), 100)))
        (row,) = inheritance(self._tables(trips=weights, days=list(weights)), [self._set()])

        assert row.level == "unlinked_trips"
        assert row.parent == "days"
        assert row.share == pytest.approx(100.0)
        assert row.mode == "copied"

    def test_one_factor_per_scope_is_redistribution(self):
        """Siblings absorbing a dropped sibling all move by the same factor.

        Two trips share each day; both are scaled together, so the level is still
        a faithful expansion of the level above.
        """
        days = list(np.exp(np.linspace(np.log(10), np.log(100), 60)))
        day_of_trip, trips = [], []
        for i, dw in enumerate(days):
            factor = 1.5 if i % 3 == 0 else 1.0
            day_of_trip += [i, i]
            trips += [dw * factor, dw * factor]
        (row,) = inheritance(
            self._tables(trips=trips, days=days, day_of_trip=day_of_trip), [self._set()]
        )

        assert row.n_scopes_varying == 0
        assert row.max_scope_spread == pytest.approx(1.0)
        assert row.share < 100.0
        assert row.mode == "redistributed"

    def test_siblings_on_different_factors_are_an_adjustment(self):
        """The case worth flagging: records rescaled individually after descending.

        Measured on bats_2023, the vendor's trip weights vary within 1,102 of
        25,312 days by up to exactly 2x, while every fitted profile varies within
        none. A summary that only counts departures from the parent cannot tell
        the two apart.
        """
        days = list(np.exp(np.linspace(np.log(10), np.log(100), 60)))
        day_of_trip, trips = [], []
        for i, dw in enumerate(days):
            day_of_trip += [i, i]
            trips += [dw, dw * 2 if i % 5 == 0 else dw]
        (row,) = inheritance(
            self._tables(trips=trips, days=days, day_of_trip=day_of_trip), [self._set()]
        )

        assert row.n_scopes_varying == 12
        assert row.max_scope_spread == pytest.approx(2.0)
        assert row.mode == "adjusted"

    def test_float_noise_is_not_an_adjustment(self):
        """Division noise must not be read as siblings on different factors."""
        days = list(np.exp(np.linspace(np.log(10), np.log(100), 60)))
        day_of_trip, trips = [], []
        for i, dw in enumerate(days):
            day_of_trip += [i, i]
            trips += [dw, dw * (1 + 1e-15)]
        (row,) = inheritance(
            self._tables(trips=trips, days=days, day_of_trip=day_of_trip), [self._set()]
        )

        assert row.n_scopes_varying == 0
        assert row.mode != "adjusted"

    def test_split_levels_are_excluded(self):
        """Days divide their person's weight, so sibling ratios differ by design."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), 100)))
        tables = {
            "persons": pl.DataFrame({"person_id": list(range(100)), "pw": weights}),
            "days": pl.DataFrame({"person_id": list(range(100)), "dw": list(weights)}),
        }
        weight_set = WeightSet("v", "v", {"persons": "pw", "days": "dw"}, fitted=False)

        assert inheritance(tables, [weight_set]) == []

    def test_a_set_not_reaching_both_levels_is_skipped(self):
        """Inheritance needs a parent weight to inherit from."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), 100)))
        weight_set = WeightSet("v", "v", {"unlinked_trips": "tw"}, fitted=False)

        assert inheritance(self._tables(trips=weights, days=list(weights)), [weight_set]) == []
