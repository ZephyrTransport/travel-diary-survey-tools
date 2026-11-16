"""Data transformations for the diagnostics report."""

from dataclasses import dataclass
from itertools import product as itertools_product

import polars as pl

from processing.weighting.controls.base import ControlLevel, CrosstabControlTarget
from processing.weighting.controls.registry import CONTROLS, resolve_targets
from processing.weighting.core.hierarchy import (
    HIERARCHY,
    LEVELS,
    Flow,
    Level,
    levels_with_flow,
)
from processing.weighting.core.propagation import is_usable
from processing.weighting.core.specs import ControlTotals, MergeSpec, ProfileFit
from processing.weighting.data_prep.incidence import control_columns

# How far a kept record's weight can be raised to carry dropped records beside it,
# as (label, lower, upper) multiples of the record above. "Unchanged" comes first.
RAISE_BANDS: tuple[tuple[str, float, float], ...] = (
    ("Up to 1.5&times;", 1.0, 1.5),
    ("1.5&times; to 2&times;", 1.5, 2.0),
    ("2&times; to 3&times;", 2.0, 3.0),
    ("Over 3&times;", 3.0, float("inf")),
)
_UNCHANGED_TOL = 1e-6  # ratios this close to 1, or to a range edge, are that value


def _collect_merge_labels(
    merges: list[MergeSpec] | None,
) -> tuple[set[tuple[str, str]], dict[tuple[str, str], str]]:
    """Parse merge specs into hidden-member set and merged-label dict."""
    hidden: set[tuple[str, str]] = set()
    labels: dict[tuple[str, str], str] = {}
    for spec in merges or []:
        for merged_label, base_members in spec.groups.items():
            # Only hide constituents for global merges (originals dropped).
            if spec.zones is None:
                for m in base_members:
                    hidden.add((spec.control, m.lower()))
            labels[(spec.control, merged_label.lower())] = merged_label.replace("_", " ").title()
    return hidden, labels


def category_label_map(
    target_names: list[str],
    merges: list[MergeSpec] | None = None,
) -> dict[tuple[str, str], str]:
    """Map ``(control_name, category_str)`` to a human-readable label.

    Categories are string member names (e.g. ``"size_1"``).  Merged
    categories (e.g. ``"size_4_plus"``) get a title-cased label from
    their merge spec.
    """
    merged_members, merged_labels = _collect_merge_labels(merges)

    labels: dict[tuple[str, str], str] = {}
    for name in target_names:
        ctrl = CONTROLS.get(name)
        if ctrl is None:
            continue
        if isinstance(ctrl, CrosstabControlTarget):
            # Build labels with "by" separator between dimensions
            for combo in itertools_product(*ctrl.dim_value_groups):
                composite_name = "_".join(grp_name for grp_name, _ in combo)
                key = (name, composite_name.lower())
                if key in merged_members:
                    continue
                dim_labels = [grp_name.replace("_", " ").title() for grp_name, _ in combo]
                labels[key] = " by ".join(dim_labels)
        else:
            for _value, member in ctrl.valid_members:
                key = (name, member.lower())
                if key in merged_members:
                    continue
                lbl = member.replace("_", " ").title()
                if len(ctrl.valid_members) == 1:
                    lbl = ctrl.description
                labels[key] = lbl
    labels.update(merged_labels)
    return labels


def _pad_missing_rows(result: pl.DataFrame) -> pl.DataFrame:
    """Pad missing (control_name, label) pairs for every zone."""
    all_zones = result["zone_group_id"].unique()
    all_cl = result.select("control_name", "category", "label").unique()
    full_grid = all_cl.join(all_zones.to_frame("zone_group_id"), how="cross")
    existing = result.select("zone_group_id", "control_name", "label").unique()
    missing = full_grid.join(existing, on=["zone_group_id", "control_name", "label"], how="anti")

    if missing.is_empty():
        return result

    schema = result.schema
    pad = missing
    for col_name in ["target_total", "weighted_total", "diff", "diff_pct"]:
        pad = pad.with_columns(pl.lit(None).cast(schema[col_name]).alias(col_name))
    return pl.concat([result, pad.select(result.columns)])


def merge_control_moe(
    control_moe: pl.DataFrame,
    merges: list[MergeSpec] | None,
) -> pl.DataFrame:
    """Apply category merges to the MOE table so it matches post-merge fit categories.

    For each merge spec, constituent category rows are combined:
    SE_merged = sqrt(Σ SE_i²), target_merged = Σ target_i, then
    moe_pct_merged = SE_merged / target_merged * 100.

    Global merges (``zones=None``) replace originals for all zones.
    Zone-specific merges replace originals only for the listed zones.
    """
    if not merges:
        return control_moe

    df = control_moe
    for spec in merges:
        for merged_label, base_members in spec.groups.items():
            member_names = [m.lower() for m in base_members]
            is_match = (pl.col("control_name") == spec.control) & pl.col("category").is_in(
                member_names
            )
            if spec.zones is not None:
                is_match = is_match & pl.col("zone_group_id").is_in(spec.zones)

            to_merge = df.filter(is_match)
            if to_merge.is_empty():  # a group of one still merges, as a rename
                continue

            keep = df.filter(~is_match)
            merged_rows = (
                to_merge.group_by("zone_group_id")
                .agg(
                    pl.col("target_total").sum(),
                    (pl.col("se") ** 2).sum().sqrt().alias("se"),
                )
                .with_columns(
                    pl.lit(spec.control).alias("control_name"),
                    pl.lit(merged_label.lower()).alias("category"),
                    (
                        pl.when(pl.col("target_total") > 0)
                        .then(pl.col("se") / pl.col("target_total") * 100)
                        .otherwise(0.0)
                    ).alias("moe_pct"),
                )
                .select(
                    "zone_group_id", "control_name", "category", "target_total", "se", "moe_pct"
                )
            )
            df = pl.concat([keep, merged_rows])

    return df


def apply_fit_merges(
    fit: pl.DataFrame,
    merges: list | None,
    target_names: list[str],
) -> pl.DataFrame:
    """Add human-readable ``label`` column to the fit table.

    With category merges already applied at the data level (both
    incidence tables and control totals), the fit table already
    reflects the correct merged/unmerged categories per zone.
    This function only adds labels and pads missing rows.

    A category with no target and no weight in any zone is dropped first, so
    one that zone merges replaced everywhere, such as each PUMA once PUMAs are
    grouped by county, does not get a row of placeholders.
    """
    # Build label lookup from registry + merge specs
    lmap = category_label_map(target_names, merges)
    label_rows = [
        {"control_name": ctrl, "category": cat, "label": lbl} for (ctrl, cat), lbl in lmap.items()
    ]
    label_df = pl.DataFrame(label_rows)
    result = fit.join(label_df, on=["control_name", "category"], how="left").with_columns(
        pl.col("label").fill_null(pl.col("control_name") + ":" + pl.col("category"))
    )
    empty = (pl.col("target_total").fill_null(0) == 0) & (
        pl.col("weighted_total").fill_null(0) == 0
    )
    result = result.filter(~empty.all().over("control_name", "category"))

    result = _pad_missing_rows(result)
    return result.sort("control_name", "category", "label", "zone_group_id")


def zone_fit_summary(fit: pl.DataFrame) -> pl.DataFrame:
    """Per-zone summary: HH/Person pop target & weighted, %Err, MAPE.

    The household and person totals are the ``h_total`` and ``p_total``
    targets; a run without ``p_total`` shows no person total.

    Returns columns: zone_group_id, hh_target, hh_weighted, hh_pct_err,
    per_target, per_weighted, per_pct_err, mape.
    """

    def _pop(zf: pl.DataFrame, ctrl_name: str) -> tuple[float, float, float]:
        cf = zf.filter(pl.col("control_name") == ctrl_name)
        target = cf["target_total"].sum() or 0.0
        weighted = cf["weighted_total"].sum() or 0.0
        pct_err = (weighted - target) / target * 100 if target else 0.0  # pyright: ignore[reportOperatorIssue]
        return target, weighted, pct_err  # pyright: ignore[reportReturnType]

    zones = sorted(fit["zone_group_id"].unique().to_list())
    rows: list[dict] = []

    for z in zones:
        zf = fit.filter(pl.col("zone_group_id") == z)
        # A row with no target has no % error; counted as 0% it would dilute these.
        abs_errs = zf.filter(pl.col("target_total") > 0)["diff_pct"].abs()
        mape = abs_errs.mean() or 0.0
        p90 = abs_errs.quantile(0.9, interpolation="higher") or 0.0
        max_err = abs_errs.max() or 0.0

        ht, hw, he = _pop(zf, "h_total")
        pt, pw, pe = _pop(zf, "p_total")
        rows.append(
            {
                "zone_group_id": z,
                "hh_target": ht,
                "hh_weighted": hw,
                "hh_pct_err": he,
                "per_target": pt,
                "per_weighted": pw,
                "per_pct_err": pe,
                "mape": mape,
                "p90_err": p90,
                "max_err": max_err,
            }
        )

    return pl.DataFrame(rows)


def compute_weighted_totals(
    seed: pl.DataFrame,
    weights: pl.DataFrame,
    target_names: list[str],
) -> pl.DataFrame:
    """Weighted totals per (zone_group_id, control_name, category).

    Uses uniform column handling for all controls:
    - Structural controls: unpivoted column (e.g., `h_total`)
    - Non-structural controls: pivoted columns (e.g., `h_size__size_1`)
    """
    sw = seed.join(weights.select("hh_id", "hh_weight"), on="hh_id", how="left")
    rows: list[dict] = []
    all_controls = resolve_targets(target_names, ControlLevel.HOUSEHOLD) + resolve_targets(
        target_names, ControlLevel.PERSON
    )
    for ctrl in all_controls:
        for col, member in control_columns(ctrl, sw.columns).items():
            agg = sw.group_by("zone_group_id").agg(
                (pl.col(col) * pl.col("hh_weight")).sum().alias("weighted_total")
            )
            rows.extend(
                {
                    "zone_group_id": r["zone_group_id"],
                    "control_name": ctrl.name,
                    "category": member,
                    "weighted_total": r["weighted_total"],
                }
                for r in agg.iter_rows(named=True)
            )
    return pl.DataFrame(rows)


def fit_table(
    control_totals: ControlTotals,
    weighted_totals: pl.DataFrame,
) -> pl.DataFrame:
    """Join targets to weighted totals; add ``diff`` and ``diff_pct`` columns."""
    return with_diff(
        control_totals.totals.join(
            weighted_totals, on=["zone_group_id", "control_name", "category"], how="left"
        ).with_columns(pl.col("weighted_total").fill_null(0))
    )


def with_diff(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``diff`` (weighted minus target) and ``diff_pct`` (it as % of target, 0 if none)."""
    diff = pl.col("weighted_total") - pl.col("target_total")
    return df.with_columns(
        diff.alias("diff"),
        (diff / pl.col("target_total") * 100).fill_nan(0).fill_null(0).alias("diff_pct"),
    )


# ---------------------------------------------------------------------------
# Profile comparison -- one row per fit in the run
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProfileSummary:
    """One fit, reduced to the figures that compare it against the others."""

    profile: str | None
    seed_households: int
    weight_sums: dict[str, float]
    ess_pct: float
    cv: float
    max_over_median: float
    mape: float
    zones_converged: int
    zones_total: int


def profile_summary(
    fit: ProfileFit,
    tables: dict[str, pl.DataFrame | None],
    fit_table_df: pl.DataFrame,
) -> ProfileSummary:
    """Reduce one completed fit to a comparison row.

    Every figure is read off the fit's own rows -- the weight sums from each
    level's own column, never an ancestor's -- so a level that failed to receive
    a weight shows as a smaller sum rather than inheriting its parent's.
    """
    weight_sums: dict[str, float] = {}
    for level in HIERARCHY:
        df = tables.get(level.table)
        if df is None:
            continue
        col = level.weight_col_for(fit.profile)
        if col in df.columns:
            weight_sums[level.table] = float(df[col].sum() or 0.0)

    households = tables.get("households")
    hh_col = LEVELS["households"].weight_col_for(fit.profile)
    ess_pct = cv = max_over_median = 0.0
    if households is not None and hh_col in households.columns:
        w = households.filter(pl.col(hh_col) > 0)[hh_col]
        if len(w):
            total, sq = w.sum(), (w * w).sum()
            mean, median = w.mean(), w.median()
            ess_pct = 100.0 * (total**2 / sq) / len(w) if sq else 0.0  # pyright: ignore[reportOperatorIssue]
            cv = float(w.std() / mean) if mean else 0.0  # pyright: ignore[reportOperatorIssue]
            max_over_median = float(w.max() / median) if median else 0.0  # pyright: ignore[reportOperatorIssue]

    return ProfileSummary(
        profile=fit.profile,
        seed_households=fit.seed_incidence.height,
        weight_sums=weight_sums,
        ess_pct=float(ess_pct),
        cv=cv,
        max_over_median=max_over_median,
        mape=float(fit_table_df.filter(pl.col("target_total") > 0)["diff_pct"].abs().mean() or 0.0),
        zones_converged=sum(1 for s in fit.statuses if s.converged),
        zones_total=len(fit.statuses),
    )


# ---------------------------------------------------------------------------
# Weight cascade -- what carries weight below the household
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CascadeRow:
    """One level's records under one profile: how many, and how many it admits."""

    table: str
    rows: int
    usable: int


def weight_cascade(
    tables: dict[str, pl.DataFrame | None],
    *,
    profile: str | None,
    usability_flag_col: str,
) -> list[CascadeRow]:
    """Count, per level, the records and how many the profile admitted.

    Walks [`HIERARCHY`][processing.weighting.core.hierarchy.HIERARCHY] rather
    than a list of table names, and reads each level's weight column through
    ``weight_col_for`` -- the same declarations
    [`propagate_weights`][processing.weighting.core.propagation.propagate_weights]
    walks, so this cannot describe a rule the propagation does not follow. No
    level's count is inferred from its parent's; each is read off its own rows.

    A level whose table is absent, or which carries neither the flag nor this
    profile's weight column, is skipped rather than reported as empty.
    """
    rows: list[CascadeRow] = []
    for level in HIERARCHY:
        df = tables.get(level.table)
        if df is None:
            continue
        weight_col = level.weight_col_for(profile)
        if weight_col not in df.columns or usability_flag_col not in df.columns:
            continue
        rows.append(
            CascadeRow(
                table=level.table,
                rows=df.height,
                usable=df.filter(is_usable(usability_flag_col)).height,
            )
        )
    return rows


@dataclass(frozen=True)
class ExclusionRow:
    """Records the project cleaner excluded at one level, for one reason."""

    table: str
    reason: str
    records: int


def exclusions(tables: dict[str, pl.DataFrame | None]) -> list[ExclusionRow]:
    """Count, per level and reason, the records the project cleaner excluded.

    Each level is read off its own ``exclusion_reason``; a record unusable only
    because the level above was excluded is not counted again. Profile-free:
    an exclusion holds under every profile.
    """
    rows: list[ExclusionRow] = []
    for level in HIERARCHY:
        df = tables.get(level.table)
        if df is None or "exclusion_reason" not in df.columns:
            continue
        counts = (
            df.filter(pl.col("exclusion_reason").is_not_null())
            .group_by("exclusion_reason")
            .len()
            .sort("exclusion_reason")
        )
        rows += [ExclusionRow(level.table, reason, n) for reason, n in counts.iter_rows()]
    return rows


@dataclass(frozen=True)
class RedistributionRow:
    """How far one level's weights were scaled to cover unusable siblings.

    The ratio is a child's weight over its parent's. Under the copy-and-conserve
    rule every child in a scope is scaled by the same factor, so the ratio *is*
    that factor: 1.0 where every sibling was usable, and rising as the survivors
    take on the share of those that were not.

    ``unchanged`` and ``bands`` are percentages of ``records``; ``bands`` follows
    [`RAISE_BANDS`][processing.weighting.diagnostics.data.RAISE_BANDS].
    """

    table: str
    parent: str
    records: int
    unchanged: float
    bands: tuple[float, ...]
    maximum: float


def _edge_columns(
    level: Level,
    child: pl.DataFrame,
    parent: pl.DataFrame,
    profile: str | None,
) -> tuple[str, str, str] | None:
    """Resolve (child weight, parent weight, join key) when the edge is readable."""
    if level.parent is None:
        return None
    child_col = level.weight_col_for(profile)
    parent_col = LEVELS[level.parent].weight_col_for(profile)
    key = level.key
    if key is None:
        return None
    present = (
        child_col in child.columns
        and key in child.columns
        and parent_col in parent.columns
        and key in parent.columns
    )
    return (child_col, parent_col, key) if present else None


def redistribution(
    tables: dict[str, pl.DataFrame | None],
    *,
    profile: str | None,
    usability_flag_col: str,
) -> list[RedistributionRow]:
    """Summarise the child/parent weight ratio on every copy-and-conserve edge.

    Split edges (days) are excluded: their children divide the parent's weight
    rather than copying it, so a ratio below 1 is the rule working, not a record
    standing in for its siblings.
    """
    out: list[RedistributionRow] = []
    for level in levels_with_flow(Flow.DOWN):
        if level.split or level.parent is None:
            continue
        child, parent = tables.get(level.table), tables.get(level.parent)
        if child is None or parent is None or usability_flag_col not in parent.columns:
            continue
        cols = _edge_columns(level, child, parent, profile)
        if cols is None:
            continue
        child_col, parent_col, key = cols

        admitted = parent.filter(is_usable(usability_flag_col)).select(key, parent_col)
        joined = (
            child.filter(pl.col(child_col) > 0)
            .select(key, child_col)
            .join(admitted, on=key, how="inner")
            .filter(pl.col(parent_col) > 0)
        )
        if joined.is_empty():
            continue

        ratio = joined[child_col] / joined[parent_col]
        n = len(ratio)

        def share(mask: pl.Series, n: int = n) -> float:
            return 100.0 * float(mask.sum()) / n

        out.append(
            RedistributionRow(
                table=level.table,
                parent=level.parent,
                records=n,
                unchanged=share((ratio - 1).abs() <= _UNCHANGED_TOL),
                # Edges carry the tolerance too: 1 of 3 kept is 3x, give or take float error
                bands=tuple(
                    share((ratio > lo + _UNCHANGED_TOL) & (ratio <= hi + _UNCHANGED_TOL))
                    for _, lo, hi in RAISE_BANDS
                ),
                maximum=float(ratio.max() or 0.0),
            )
        )
    return out
