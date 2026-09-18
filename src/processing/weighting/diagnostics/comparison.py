"""Pairwise comparison of weight sets, at every level of the hierarchy.

A *weight set* is one complete assignment of weights across the hierarchy: a
profile this run fitted, or a set that arrived with the survey and is being used
as a reference. Any two of them can be compared at any level where both carry a
column.

# What a comparison can and cannot say

Weights from two methods are **not** expected to agree record by record.
Balancing matches *marginal totals*, and many different weight vectors satisfy
the same margins equally well, so two methods can both be correct and still
disagree on nearly every household. Low correlation is not evidence that either
is wrong, and high correlation is not evidence that either is right. Nothing
here is a validation, and the report says so above the plot rather than in a
footnote.

What the comparison *can* show is how far apart two sets are, whether they
agree on which records are heavy, and whether the gap is even across the range
of weights or widens at one end. The report shows those numbers and leaves the
judgement to the analyst.

# The fit line

Least squares is the wrong fit for two weight sets. It treats one set as known
and the other as noisy, so ln(A) on ln(B) and ln(B) on ln(A) give slopes that
are not reciprocals, and a weak correlation drags both toward zero. On bats_2023
the household fit of our weights on the vendor's gave 0.42, the reverse fit
implied 1.74, and the spreads actually differ by a factor of 0.86. The reduced
major axis is symmetric and does not dilute: its slope is the ratio of the two
spreads, signed by the correlation.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from itertools import combinations

import numpy as np
import polars as pl

from processing.weighting.core.hierarchy import HIERARCHY, LEVELS, Flow, Level

logger = logging.getLogger(__name__)

# Below this many records in common, a slope describes the sample rather than
# the methods, so the pair is dropped instead of reported with a caveat.
MIN_OVERLAP = 30

# Deciles of B. Ten is what the eye can read across a strip that narrow.
N_DECILES = 10

# Points drawn per pair. The full overlap is hundreds of thousands of records at
# the day and trip levels, which no scatter can show and no browser should carry;
# every statistic is computed on the whole overlap regardless.
MAX_POINTS = 700

# Fixed so that rerunning on unchanged data rewrites an identical file.
SAMPLE_SEED = 20260915

# A child weight this close to its parent's counts as inherited unchanged.
# Loose enough to swallow floating-point representation, which bitwise equality
# would not, and tight enough that no one could act on the difference.
AGREEMENT_TOL = 0.01


@dataclass(frozen=True)
class WeightSet:
    """One complete assignment of weights across the hierarchy.

    Attributes:
        name: Stable key, used in markup and to address the set in the picker.
        label: How the set is named to a reader.
        columns: Weight column per table. A set need not reach every level.
        fitted: Whether this run produced it, as against it arriving with the
            survey. A comparison between two fitted sets holds the method
            constant and varies the universe; a comparison against a supplied
            set varies everything, and the two read differently.
    """

    name: str
    label: str
    columns: Mapping[str, str]
    fitted: bool


@dataclass(frozen=True)
class Decile:
    """One decile of B, describing the ratio within it.

    Attributes:
        x_mid: Mean of log(B) over the decile, where the marker is drawn.
        median: Median of log(A/B) -- where the band sits.
        iqr: Interquartile range of log(A/B) -- how wide the band is at this
            weight level.
    """

    x_mid: float
    median: float
    iqr: float


@dataclass(frozen=True)
class PairStats:
    """Two weight sets compared at one level.

    Every summary is symmetric in the two sets, so it reads the same whichever is
    called A. Only the decile strip depends on direction, since it bins on one
    set, so it is held both ways.

    Attributes:
        level: Table the comparison was made on.
        a: Name of the first set.
        b: Name of the second set.
        n: Records carrying a non-zero weight in both.
        median_fold: Median over records of max(A/B, B/A).
        p90_fold: 90th percentile of the same.
        max_fold: Its maximum.
        rank_corr: Spearman correlation of the two weights.
        r2: Squared Pearson correlation of their logs.
        slope: Reduced-major-axis slope of ln(A) against ln(B). The reverse fit
            is exactly its reciprocal.
        intercept: Where that line crosses ln(B) = 0.
        deciles_ab: ln(A/B) within each decile of B.
        deciles_ba: ln(B/A) within each decile of A.
        points: Sample of ``(ln A, ln B)`` for the scatter.
    """

    level: str
    a: str
    b: str
    n: int
    median_fold: float
    p90_fold: float
    max_fold: float
    rank_corr: float
    r2: float
    slope: float
    intercept: float
    deciles_ab: list[Decile]
    deciles_ba: list[Decile]
    points: list[tuple[float, float]] = field(default_factory=list)


@dataclass(frozen=True)
class Comparison:
    """Every weight set a run could compare, and every pair that came of it.

    Held together because the pairs name sets by key and are unreadable without
    them: the sets carry the labels, the ordering the picker offers, and which
    side of a pair was fitted here as against supplied.

    Attributes:
        sets: The weight sets considered, in the order to offer them.
        pairs: One entry per (level, pair) that had enough overlap to report.
    """

    sets: list[WeightSet]
    pairs: list[PairStats]

    def __bool__(self) -> bool:
        """Whether there is anything to show."""
        return bool(self.pairs)


def fitted_weight_sets(profiles: Sequence[str | None]) -> list[WeightSet]:
    """One weight set per profile this run fitted.

    Columns come from the hierarchy rather than from a pattern over the table's
    names: the suffix a profile's column carries is spelled in exactly one place
    and this is not it.
    """
    return [
        WeightSet(
            name=profile or "survey",
            label=profile or "the survey",
            columns={level.table: level.weight_col_for(profile) for level in HIERARCHY},
            fitted=True,
        )
        for profile in profiles
    ]


def reference_weight_sets(spec: Mapping[str, Mapping] | None) -> list[WeightSet]:
    """Weight sets that arrived with the survey, as named in the config.

    Named rather than discovered, because neither half of a supplied column's
    name is reliable. A vendor suffix is indistinguishable from a profile suffix
    -- ``hh_weight_rmove_only`` parses as a profile called ``rmove_only`` that
    was never fitted -- and a supplied column may not share the level's base name
    at all, as the vendor's ``trip_weight`` does not match the hierarchy's
    ``unlinked_trip_weight``. An un-suffixed column is worse still: it may be the
    vendor's, or it may be what a previous run of this pipeline left behind,
    which is the ambiguity ``drop_unsuffixed_weights`` exists to remove. Only the
    person configuring the run knows which, so they say.

    Args:
        spec: ``{name: {"label": str, "columns": {table: column}}}``, as read
            from the ``diagnostics.compare_weights`` block. ``label`` is
            optional and defaults to the name.

    Returns:
        One set per entry, in the order given.
    """
    sets: list[WeightSet] = []
    for name, entry in (spec or {}).items():
        columns = dict(entry.get("columns") or {})
        if not columns:
            logger.warning("Reference weight set %r names no columns; ignoring it", name)
            continue
        sets.append(
            WeightSet(
                name=name,
                label=str(entry.get("label") or name),
                columns=columns,
                fitted=False,
            )
        )
    return sets


def _overlap(df: pl.DataFrame, col_a: str, col_b: str) -> tuple[np.ndarray, np.ndarray]:
    """Logs of the two weights over the records carrying both.

    A zero weight is an absence, not a small number: the record was excluded
    from that fit, so it has nothing to say about how the two methods differ.
    Comparing it would also put log(0) on an axis.
    """
    both = df.select(
        pl.col(col_a).cast(pl.Float64).alias("a"), pl.col(col_b).cast(pl.Float64).alias("b")
    ).filter(
        pl.col("a").is_not_null()
        & pl.col("b").is_not_null()
        & (pl.col("a") > 0)
        & (pl.col("b") > 0)
    )
    return np.log(both["a"].to_numpy()), np.log(both["b"].to_numpy())


def _deciles(log_a: np.ndarray, log_b: np.ndarray) -> list[Decile]:
    """Ratio behaviour across the range of B, in ``N_DECILES`` bins.

    Bins are cut on quantiles of B, so a set with many tied weights yields fewer
    than ten distinct edges; the empty bins are dropped rather than reported as
    zero-width bands.
    """
    ratio = log_a - log_b
    edges = np.unique(np.quantile(log_b, np.linspace(0, 1, N_DECILES + 1)))
    if edges.size < 2:  # noqa: PLR2004 - a single distinct value has no deciles
        return []
    # right=False with the final edge nudged so the maximum lands in the last bin.
    index = np.searchsorted(edges[1:-1], log_b, side="right")
    deciles: list[Decile] = []
    for bin_id in range(edges.size - 1):
        mask = index == bin_id
        if not mask.any():
            continue
        in_bin = ratio[mask]
        q1, q3 = np.quantile(in_bin, [0.25, 0.75])
        deciles.append(
            Decile(
                x_mid=float(log_b[mask].mean()),
                median=float(np.median(in_bin)),
                iqr=float(q3 - q1),
            )
        )
    return deciles


def _rma(log_a: np.ndarray, log_b: np.ndarray, r: float) -> tuple[float, float]:
    """Reduced-major-axis slope and intercept of ln(A) against ln(B).

    A set with no spread at all has no meaningful line; its slope is reported as
    zero rather than dividing by it.
    """
    sd_b = float(log_b.std())
    if sd_b == 0 or not np.isfinite(r):
        return 0.0, float(log_a.mean())
    slope = float(np.sign(r) * log_a.std() / sd_b)
    return slope, float(log_a.mean() - slope * log_b.mean())


def _rank_corr(log_a: np.ndarray, log_b: np.ndarray) -> float:
    """Spearman correlation, with tied weights sharing their average rank."""
    rank_a = pl.Series(log_a).rank("average").to_numpy()
    rank_b = pl.Series(log_b).rank("average").to_numpy()
    if rank_a.std() == 0 or rank_b.std() == 0:
        return 0.0
    return float(np.corrcoef(rank_a, rank_b)[0, 1])


def _sample(log_a: np.ndarray, log_b: np.ndarray) -> list[tuple[float, float]]:
    """Up to ``MAX_POINTS`` of the overlap, drawn reproducibly."""
    n = log_a.size
    if n <= MAX_POINTS:
        picked = np.arange(n)
    else:
        picked = np.random.default_rng(SAMPLE_SEED).choice(n, MAX_POINTS, replace=False)
        picked.sort()
    return [
        (round(float(a), 4), round(float(b), 4))
        for a, b in zip(log_a[picked], log_b[picked], strict=True)
    ]


def compare_pair(
    df: pl.DataFrame,
    level: Level,
    set_a: WeightSet,
    set_b: WeightSet,
) -> PairStats | None:
    """Compare two weight sets on one table, or ``None`` if there is too little to say.

    Args:
        df: The table both sets carry a column on.
        level: The hierarchy level that table belongs to.
        set_a: First weight set.
        set_b: Second weight set.

    Returns:
        The pair's statistics, or ``None`` when either column is missing or
        fewer than ``MIN_OVERLAP`` records carry both weights.
    """
    col_a = set_a.columns.get(level.table)
    col_b = set_b.columns.get(level.table)
    if col_a is None or col_b is None:
        return None
    if col_a not in df.columns or col_b not in df.columns:
        return None

    log_a, log_b = _overlap(df, col_a, col_b)
    if log_a.size < MIN_OVERLAP:
        logger.debug(
            "%s vs %s on %s: %d records in common, below the %d needed to compare",
            set_a.name,
            set_b.name,
            level.table,
            log_a.size,
            MIN_OVERLAP,
        )
        return None

    r = float(np.corrcoef(log_a, log_b)[0, 1]) if log_a.std() and log_b.std() else 0.0
    slope, intercept = _rma(log_a, log_b, r)
    fold = np.abs(log_a - log_b)
    return PairStats(
        level=level.table,
        a=set_a.name,
        b=set_b.name,
        n=int(log_a.size),
        median_fold=float(np.exp(np.median(fold))),
        p90_fold=float(np.exp(np.quantile(fold, 0.9))),
        max_fold=float(np.exp(fold.max())),
        rank_corr=_rank_corr(log_a, log_b),
        r2=r**2,
        slope=slope,
        intercept=intercept,
        deciles_ab=_deciles(log_a, log_b),
        deciles_ba=_deciles(log_b, log_a),
        points=_sample(log_a, log_b),
    )


def all_pairs(
    tables: Mapping[str, pl.DataFrame | None],
    sets: Sequence[WeightSet],
) -> list[PairStats]:
    """Every comparable pair of weight sets, at every level both reach.

    Computed exhaustively rather than on demand. A pair is a few numbers over a
    column already in memory, so precomputing all of them costs little and buys
    a picker where choosing a new set on one side leaves the other side alone --
    which a lazily-computed subset cannot offer.

    Args:
        tables: Canonical tables by name, after propagation.
        sets: The weight sets to compare, in the order they should be offered.

    Returns:
        One entry per (level, pair), in hierarchy order then pair order. Empty
        when fewer than two sets reach any level in common.
    """
    pairs: list[PairStats] = []
    for level in HIERARCHY:
        df = tables.get(level.table)
        if df is None:
            continue
        for set_a, set_b in combinations(sets, 2):
            stats = compare_pair(df, level, set_a, set_b)
            if stats is not None:
                pairs.append(stats)
    return pairs


def _decile_payload(deciles: list[Decile]) -> list[list[float]]:
    """Deciles as ``[mean ln, median ratio, IQR]`` triples, rounded for the wire."""
    return [[round(d.x_mid, 4), round(d.median, 4), round(d.iqr, 4)] for d in deciles]


# A ratio spread wider than this within one parent means the children were not
# all scaled by the same factor. Loose enough to ignore float noise, tight enough
# that a real per-record adjustment cannot hide under it.
_SAME_FACTOR_TOL = 1e-9


@dataclass(frozen=True)
class Inheritance:
    """How a level's weight descends from its parent, for one weight set.

    Attributes:
        level: The child table.
        parent: The table its weight descends from.
        set_name: Which weight set this describes.
        n: Records carrying a weight at both levels.
        n_close: Records whose weight matches their parent's to within
            ``AGREEMENT_TOL``.
        n_scopes: Parent records with at least one weighted child.
        n_scopes_varying: Parent records whose children do **not** all stand in
            the same ratio to it. Zero under any rule that scales a whole scope
            at once; non-zero only if something touched children individually.
        max_scope_spread: Largest ratio between the highest and lowest factor
            within a single parent. 1.0 when every scope is internally uniform.
    """

    level: str
    parent: str
    set_name: str
    n: int
    n_close: int
    n_scopes: int
    n_scopes_varying: int
    max_scope_spread: float

    @property
    def share(self) -> float:
        """Percentage of records inheriting the parent weight unchanged."""
        return 100.0 * self.n_close / self.n if self.n else 0.0

    @property
    def mode(self) -> str:
        """Which of the three mechanisms produced this level's weights.

        ``copied`` -- every child carries its parent's weight unchanged, so the level adds
        nothing of its own and comparing two sets here restates the level above.

        ``redistributed`` -- children depart from the parent, but every child in
        a scope departs by the same factor. That is conservation: the admitted
        records absorb the share of any sibling the gate removed, and the level
        stays a faithful expansion of its parent.

        ``adjusted`` -- children in the same scope carry different factors, so
        something rescaled records individually after the weights descended. The
        level no longer nests inside its parent: two children of one parent now
        claim to represent different amounts of it.
        """
        if self.n_scopes_varying:
            return "adjusted"
        return "copied" if self.n_close == self.n else "redistributed"


def inheritance(
    tables: Mapping[str, pl.DataFrame | None],
    sets: Sequence[WeightSet],
) -> list[Inheritance]:
    """Per set and copy-down level, how the weight descends from the parent.

    Distinguishes conservation from post-hoc adjustment, which look alike in any
    summary that only counts departures from the parent weight. Both leave most
    children unequal to their parent; only one leaves the level coherent.

    The discriminator is whether the child-to-parent ratio is constant *within*
    each parent. Conservation rescales a whole scope by one factor -- every
    usable sibling absorbs the same share of what the gate removed -- so the
    ratio cannot vary between siblings. A factor applied to individual records
    can, and does: measured on bats_2023, the vendor's trip weights vary within
    1,102 of 25,312 days, by up to exactly 2x, while every fitted profile and the
    vendor's own rMove weights vary within none.

    Split levels are excluded: days divide their person's weight rather than
    copying it, so sibling ratios differ there by design.

    Args:
        tables: Canonical tables by name, after propagation.
        sets: The weight sets to describe.

    Returns:
        One entry per (level, set) that could be measured, in hierarchy order.
    """
    rows: list[Inheritance] = []
    for level in HIERARCHY:
        if level.flow is not Flow.DOWN or level.split or level.parent is None:
            continue
        child_df, parent_df = tables.get(level.table), tables.get(level.parent)
        if child_df is None or parent_df is None:
            continue
        key = LEVELS[level.parent].id_col
        if key not in child_df.columns or key not in parent_df.columns:
            continue
        for weight_set in sets:
            row = _inheritance_row(child_df, parent_df, key, level, weight_set)
            if row is not None:
                rows.append(row)
    return rows


def _inheritance_row(
    child_df: pl.DataFrame,
    parent_df: pl.DataFrame,
    key: str,
    level: Level,
    weight_set: WeightSet,
) -> Inheritance | None:
    """One set's inheritance at one level, or None if it cannot be measured."""
    child_col = weight_set.columns.get(level.table)
    parent_col = weight_set.columns.get(level.parent)
    if child_col is None or parent_col is None:
        return None
    if child_col not in child_df.columns or parent_col not in parent_df.columns:
        return None

    joined = (
        child_df.select(key, pl.col(child_col).alias("c"))
        .join(parent_df.select(key, pl.col(parent_col).alias("p")), on=key, how="inner")
        .filter(
            pl.col("c").is_not_null()
            & pl.col("p").is_not_null()
            & (pl.col("c") > 0)
            & (pl.col("p") > 0)
        )
        .with_columns((pl.col("c") / pl.col("p")).alias("ratio"))
    )
    if joined.height < MIN_OVERLAP:
        return None

    # Compared as a spread rather than a distinct count, so float noise in the
    # division cannot masquerade as siblings on different factors.
    scopes = joined.group_by(key).agg(
        (pl.col("ratio").max() / pl.col("ratio").min()).alias("spread")
    )
    varying = scopes.filter(pl.col("spread") > 1 + _SAME_FACTOR_TOL)
    return Inheritance(
        level=level.table,
        parent=level.parent,
        set_name=weight_set.name,
        n=joined.height,
        n_close=joined.filter((pl.col("ratio") - 1).abs() <= AGREEMENT_TOL).height,
        n_scopes=scopes.height,
        n_scopes_varying=varying.height,
        max_scope_spread=float(scopes["spread"].max()) if scopes.height else 1.0,
    )


def payload(comparison: Comparison) -> dict:
    """Everything the comparer needs, as one JSON-serialisable object.

    Embedded whole and rendered in the browser rather than baked into one figure
    per pair: forty static plots would multiply the document for a reader who
    looks at two or three of them, and a picker that can only offer what was
    pre-rendered cannot let one side of the comparison move on its own.

    Only the sets that actually appear in a pair are listed, so the picker never
    offers a choice that resolves to nothing.

    Args:
        comparison: The run's weight sets and the pairs computed from them.

    Returns:
        ``{"sets": [...], "levels": [...], "pairs": [...]}``, empty lists when
        nothing was comparable.
    """
    pairs, sets = comparison.pairs, comparison.sets
    used = {pair.a for pair in pairs} | {pair.b for pair in pairs}
    levels = [level.table for level in HIERARCHY if any(p.level == level.table for p in pairs)]
    return {
        "sets": [
            {"name": s.name, "label": s.label, "fitted": s.fitted} for s in sets if s.name in used
        ],
        "levels": [{"name": t, "label": t.replace("_", " ").title()} for t in levels],
        "pairs": [
            {
                "level": p.level,
                "a": p.a,
                "b": p.b,
                "n": p.n,
                "med": round(p.median_fold, 4),
                "p90": round(p.p90_fold, 4),
                "max": round(p.max_fold, 4),
                "rho": round(p.rank_corr, 4),
                "r2": round(p.r2, 4),
                "slope": round(p.slope, 6),
                "icept": round(p.intercept, 6),
                "decAB": _decile_payload(p.deciles_ab),
                "decBA": _decile_payload(p.deciles_ba),
                "pts": [[a, b] for a, b in p.points],
            }
            for p in pairs
        ],
    }
