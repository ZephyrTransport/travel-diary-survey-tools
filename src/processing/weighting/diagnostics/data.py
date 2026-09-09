"""Data transformations for the diagnostics report."""

from itertools import product as itertools_product

import polars as pl

from processing.weighting.controls.base import ControlLevel, CrosstabControlTarget
from processing.weighting.controls.registry import CONTROLS, resolve_targets
from processing.weighting.core.specs import ControlTotals, MergeSpec


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
