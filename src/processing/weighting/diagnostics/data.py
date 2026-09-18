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


def _collect_merge_labels(
    merges: list[MergeSpec] | None,
) -> tuple[set[tuple[str, str]], dict[tuple[str, str], str]]:
    """Parse merge specs into hidden-member set and merged-label dict."""
    hidden: set[tuple[str, str]] = set()
    labels: dict[tuple[str, str], str] = {}
    for spec in merges or []:
        for merged_label, base_members in spec.groups.items():
            if isinstance(base_members, dict):
                continue
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
    all_zones = result["geo_id"].unique()
    all_cl = result.select("control_name", "category", "label").unique()
    full_grid = all_cl.join(all_zones.to_frame("geo_id"), how="cross")
    existing = result.select("geo_id", "control_name", "label").unique()
    missing = full_grid.join(existing, on=["geo_id", "control_name", "label"], how="anti")

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
            if isinstance(base_members, dict):
                continue  # skip N-D merges (not applicable to flat MOE table)
            member_names = [m.lower() for m in base_members]
            is_match = (pl.col("control_name") == spec.control) & pl.col("category").is_in(
                member_names
            )
            if spec.zones is not None:
                is_match = is_match & pl.col("geo_id").is_in(spec.zones)

            to_merge = df.filter(is_match)
            if to_merge.is_empty() or len(to_merge) < 2:  # noqa: PLR2004
                continue

            keep = df.filter(~is_match)
            merged_rows = (
                to_merge.group_by("geo_id")
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
                .select("geo_id", "control_name", "category", "target_total", "se", "moe_pct")
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

    result = _pad_missing_rows(result)
    return result.sort("control_name", "category", "label", "geo_id")


def _first_control_name(target_names: list[str], level: ControlLevel) -> str | None:
    """Return the first control name at *level*, or None."""
    ctrls = resolve_targets(target_names, level)
    return ctrls[0].name if ctrls else None


def zone_fit_summary(
    fit: pl.DataFrame,
    target_names: list[str],
) -> pl.DataFrame:
    """Per-zone summary: HH/Person pop target & weighted, %Err, MAPE.

    Population totals are derived by summing categories of one representative
    control at each level (any control's categories partition the population).

    Returns columns: geo_id, hh_target, hh_weighted, hh_pct_err,
    per_target, per_weighted, per_pct_err, mape.
    """
    hh_ctrl = _first_control_name(target_names, ControlLevel.HOUSEHOLD)
    per_ctrl = _first_control_name(target_names, ControlLevel.PERSON)

    def _pop(zf: pl.DataFrame, ctrl_name: str | None) -> tuple[float, float, float]:
        if ctrl_name is None:
            return 0.0, 0.0, 0.0
        cf = zf.filter(pl.col("control_name") == ctrl_name)
        target = cf["target_total"].sum() or 0.0
        weighted = cf["weighted_total"].sum() or 0.0
        pct_err = (weighted - target) / target * 100 if target else 0.0  # pyright: ignore[reportOperatorIssue]
        return target, weighted, pct_err  # pyright: ignore[reportReturnType]

    zones = sorted(fit["geo_id"].unique().to_list())
    rows: list[dict] = []

    for z in zones:
        zf = fit.filter(pl.col("geo_id") == z)
        mape = zf["diff_pct"].abs().mean() or 0.0
        abs_errs = zf["diff_pct"].abs()
        p90 = abs_errs.quantile(0.9, interpolation="higher") or 0.0
        max_err = abs_errs.max() or 0.0

        ht, hw, he = _pop(zf, hh_ctrl)
        pt, pw, pe = _pop(zf, per_ctrl)
        rows.append(
            {
                "geo_id": z,
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
    """Weighted totals per (geo_id, control_name, category).

    Uses uniform column handling for all controls:
    - Structural controls: unpivoted column (e.g., `h_total`)
    - Non-structural controls: pivoted columns (e.g., `h_size__size_1`)
    """
    sw = seed.join(weights.select("hh_id", "hh_weight"), on="hh_id", how="left")
    rows: list[dict] = []

    # Unified loop: HH and person controls use identical logic now
    all_controls = resolve_targets(target_names, ControlLevel.HOUSEHOLD) + resolve_targets(
        target_names, ControlLevel.PERSON
    )

    for ctrl in all_controls:
        if ctrl.structural:
            col = ctrl.name
            member = ctrl.valid_members[0][1].lower()
            if col not in sw.columns:
                continue
            agg = sw.group_by("ctrl_geoid").agg(
                (pl.col(col) * pl.col("hh_weight")).sum().alias("weighted_total")
            )
            rows.extend(
                {
                    "geo_id": r["ctrl_geoid"],
                    "control_name": ctrl.name,
                    "category": member,
                    "weighted_total": r["weighted_total"],
                }
                for r in agg.iter_rows(named=True)
            )
        else:
            # Discover all {ctrl.name}__* columns (includes merged)
            prefix = f"{ctrl.name}__"
            ctrl_cols = [c for c in sw.columns if c.startswith(prefix)]
            for col in ctrl_cols:
                member = col[len(prefix) :]
                agg = sw.group_by("ctrl_geoid").agg(
                    (pl.col(col) * pl.col("hh_weight")).sum().alias("weighted_total")
                )
                rows.extend(
                    {
                        "geo_id": r["ctrl_geoid"],
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
    return (
        control_totals.totals.join(
            weighted_totals, on=["geo_id", "control_name", "category"], how="left"
        )
        .with_columns(pl.col("weighted_total").fill_null(0))
        .with_columns((pl.col("weighted_total") - pl.col("target_total")).alias("diff"))
        .with_columns(
            (pl.col("diff") / pl.col("target_total") * 100)
            .fill_nan(0)
            .fill_null(0)
            .alias("diff_pct")
        )
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
        mape=float(fit_table_df["diff_pct"].abs().mean() or 0.0),
        zones_converged=sum(1 for s in fit.statuses if s.converged),
        zones_total=len(fit.statuses),
    )


# ---------------------------------------------------------------------------
# Weight cascade -- what carries weight below the household
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CascadeRow:
    """One level's weight coverage under one profile.

    ``rows`` splits three ways and the parts are exhaustive: a record is
    rejected by the profile's own flag, or admitted and carrying weight, or
    admitted and carrying none. Only the last needs explaining, and what it
    means depends on the level -- for the anchor it is a household the control
    geography could not place, and below it, a record whose parent was dropped.
    """

    table: str
    rows: int
    usable: int
    weighted: int

    @property
    def gated(self) -> int:
        """Records the profile's own flag rejected."""
        return self.rows - self.usable

    @property
    def unweighted(self) -> int:
        """Records the profile admits that still carry no weight."""
        return self.usable - self.weighted


def weight_cascade(
    tables: dict[str, pl.DataFrame | None],
    *,
    profile: str | None,
    usability_flag_col: str,
) -> list[CascadeRow]:
    """Count, per level, what the profile admitted and what carries weight.

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
                weighted=df.filter(pl.col(weight_col) > 0).height,
            )
        )
    return rows


_REDISTRIBUTION_TAIL = 1.5


@dataclass(frozen=True)
class RedistributionRow:
    """How far one level's weights were scaled to cover unusable siblings.

    The ratio is a child's weight over its parent's. Under the copy-and-conserve
    rule every child in a scope is scaled by the same factor, so the ratio *is*
    that factor: 1.0 where every sibling was usable, and rising as the survivors
    take on the share of those that were not.
    """

    table: str
    parent: str
    p50: float
    p90: float
    p99: float
    maximum: float
    share_above: float


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
    standing in for its siblings. Those are described by
    [`split_identity`][processing.weighting.diagnostics.data.split_identity].
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
        out.append(
            RedistributionRow(
                table=level.table,
                parent=level.parent,
                p50=float(ratio.quantile(0.5) or 0.0),
                p90=float(ratio.quantile(0.9) or 0.0),
                p99=float(ratio.quantile(0.99) or 0.0),
                maximum=float(ratio.max() or 0.0),
                share_above=100.0 * float((ratio > _REDISTRIBUTION_TAIL).sum()) / len(ratio),
            )
        )
    return out


@dataclass(frozen=True)
class SplitIdentityRow:
    """Whether a split level's children still sum to what their parent carries.

    Arithmetic the pipeline controls, so a residual past floating-point noise is
    a bug rather than a finding. ``stranded_parents`` is the exception it cannot
    fix: a parent carrying weight with no child rows at all has nothing to
    represent it below, and that weight is deliberately left unrepresented
    rather than pooled onto other parents.
    """

    table: str
    parent: str
    parents_checked: int
    max_residual: float
    stranded_parents: int
    stranded_weight: float
    min_children: int
    median_children: int
    max_children: int


def split_identity(
    tables: dict[str, pl.DataFrame | None],
    *,
    profile: str | None,
) -> list[SplitIdentityRow]:
    """Verify ``sum(child weight) == parent weight`` on every split edge."""
    out: list[SplitIdentityRow] = []
    for level in levels_with_flow(Flow.DOWN):
        if not level.split or level.parent is None:
            continue
        child, parent = tables.get(level.table), tables.get(level.parent)
        if child is None or parent is None:
            continue
        cols = _edge_columns(level, child, parent, profile)
        if cols is None:
            continue
        child_col, parent_col, key = cols

        per_parent = (
            child.filter(pl.col(child_col) > 0)
            .group_by(key)
            .agg(pl.col(child_col).sum().alias("_sum"), pl.len().alias("_n"))
        )
        carrying = parent.filter(pl.col(parent_col) > 0).select(key, parent_col)
        merged = carrying.join(per_parent, on=key, how="inner")
        stranded = carrying.join(per_parent.select(key), on=key, how="anti")
        if merged.is_empty():
            continue

        out.append(
            SplitIdentityRow(
                table=level.table,
                parent=level.parent,
                parents_checked=merged.height,
                max_residual=float((merged["_sum"] - merged[parent_col]).abs().max() or 0.0),
                stranded_parents=stranded.height,
                stranded_weight=float(stranded[parent_col].sum() or 0.0),
                min_children=int(merged["_n"].min() or 0),
                median_children=int(merged["_n"].median() or 0),
                max_children=int(merged["_n"].max() or 0),
            )
        )
    return out
