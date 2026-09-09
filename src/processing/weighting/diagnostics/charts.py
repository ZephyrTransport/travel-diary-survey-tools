"""Plotly chart builders for the diagnostics report."""

import math
from collections import defaultdict

import geopandas as gpd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from processing.weighting.controls.registry import CONTROLS, resolve_targets

_WARN_PCT = 15  # fallback red threshold when MOE is unavailable or zero
_MIN_MOE = 5  # floor for MOE-based red trigger (avoids over-sensitivity on precise targets)


def _add_panel_traces(
    fig: go.Figure,
    pdf: pl.DataFrame,
    has_moe: bool,
    row: int,
    col: int,
) -> None:
    """Add bar (and optional MOE whisker) traces for one panel of the fit chart."""
    y, x, colors, hovers, moe_values = [], [], [], [], []
    for r in pdf.iter_rows(named=True):
        lbl = r["label"]
        target = r["target_total"]

        if target is None:
            y.append(lbl)
            x.append(None)
            colors.append("rgba(0,0,0,0)")
            hovers.append("")
            moe_values.append(None)
            continue

        pct = r["diff_pct"]
        moe = r.get("moe_pct") if has_moe else None
        y.append(lbl)
        x.append(pct)

        effective_moe = max(moe, _MIN_MOE) if moe is not None and moe > 0 else None
        if effective_moe is not None and abs(pct) <= effective_moe:
            colors.append("#999")
        elif (effective_moe is not None and abs(pct) > effective_moe) or abs(pct) > _WARN_PCT:
            colors.append("#c33")
        else:
            colors.append("#999")

        hover_lines = [
            f"<b>{lbl}</b>",
            f"Target: {target:,.1f}",
            f"Weighted: {r['weighted_total']:,.1f}",
            f"Diff: {r['diff']:+,.1f}",
            f"Diff %: {pct:+.1f}%",
        ]
        if moe is not None:
            hover_lines.append(f"PUMS SE: \u00b1{moe:.1f}%")
        hovers.append("<br>".join(hover_lines))
        moe_values.append(moe)

    fig.add_trace(
        go.Bar(
            y=y,
            x=x,
            orientation="h",
            marker_color=colors,
            hovertext=hovers,
            hoverinfo="text",
            showlegend=False,
        ),
        row=row,
        col=col,
    )

    if has_moe and any(v is not None for v in moe_values):
        fig.add_trace(
            go.Scatter(
                y=y,
                x=[0] * len(y),
                mode="markers",
                marker={"size": 0, "color": "rgba(0,0,0,0)"},
                error_x={
                    "type": "data",
                    "array": [v if v is not None else 0 for v in moe_values],
                    "visible": True,
                    "color": "rgba(0,0,0,0.25)",
                    "thickness": 1.5,
                    "width": 3,
                },
                hoverinfo="skip",
                showlegend=False,
            ),
            row=row,
            col=col,
        )


def _format_group_name(ctrl_name: str) -> str:
    """Format control name as a compact group label for chart y-axis."""
    if ctrl_name.startswith("h_"):
        return "HH " + ctrl_name[2:].replace("_", " ").title()
    if ctrl_name.startswith("p_"):
        return "Person " + ctrl_name[2:].replace("_", " ").title()
    return ctrl_name.replace("_", " ").title()


def _add_group_prefixes(cat_labels: list[str], ctrl_per_label: list[str]) -> list[str]:
    """Add bold control group prefix to the first label of each control group."""
    ticktext = list(cat_labels)
    prev_ctrl = None
    for i, ctrl_name in enumerate(ctrl_per_label):
        if ctrl_name != prev_ctrl:
            group_label = _format_group_name(ctrl_name)
            ticktext[i] = f"<b>{group_label}:</b>  {ticktext[i]}"
            prev_ctrl = ctrl_name
    return ticktext


def fit_diverging_figure(
    fit: pl.DataFrame,
) -> go.Figure:
    """Grid of horizontal diverging bar charts (% error, one panel per zone + overall).

    Expects *fit* to contain a ``label`` column (added by
    [`apply_fit_merges`][processing.weighting.diagnostics.data.apply_fit_merges]).
    Null placeholder rows are rendered as invisible bars
    so the y-axis remains consistent across panels.

    When ``moe_pct`` is present (from PUMS replicate weights), horizontal
    error bars show the sampling margin of error on each target.
    """
    has_moe = "moe_pct" in fit.columns
    zones = sorted(fit["geo_id"].unique().to_list())

    # Overall aggregation — sum targets/weighted across zones
    agg_exprs = [pl.col("target_total").sum(), pl.col("weighted_total").sum()]
    if has_moe:
        # Propagate MOE: SE_overall = sqrt(Σ SE_i²), then moe_pct = SE / target * 100
        # SE_i = moe_pct_i / 100 * target_i
        agg_exprs.append(
            (
                ((pl.col("moe_pct") / 100 * pl.col("target_total")) ** 2).sum().sqrt()
                / pl.col("target_total").sum()
                * 100
            )
            .fill_nan(None)
            .alias("moe_pct")
        )

    overall = (
        fit.group_by("control_name", "category", "label")
        .agg(*agg_exprs)
        .with_columns(
            ((pl.col("weighted_total") - pl.col("target_total")) / pl.col("target_total") * 100)
            .fill_nan(0)
            .fill_null(0)
            .alias("diff_pct"),
            (pl.col("weighted_total") - pl.col("target_total")).alias("diff"),
        )
        .sort("control_name", "category", "label")
    )

    # Re-sort: structural controls first (h_total, p_total), then alphabetical
    _structural = {n for n, c in CONTROLS.items() if c.structural}
    overall = (
        overall.with_columns(
            pl.when(pl.col("control_name").is_in(_structural)).then(0).otherwise(1).alias("_prio")
        )
        .sort("_prio", "control_name", "category", "label")
        .drop("_prio")
    )

    panels = [*zones, "Overall"]
    n_cols = min(4, len(panels))
    n_rows = math.ceil(len(panels) / n_cols)

    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=[str(p) for p in panels],
        shared_xaxes=False,
        shared_yaxes=True,
        horizontal_spacing=0.03,
        vertical_spacing=max(0.03, 0.15 / max(n_rows, 1)),
    )

    # Consistent y-label ordering from the overall panel
    _overall_rows = list(overall.iter_rows(named=True))
    cat_labels = [r["label"] for r in _overall_rows]
    ctrl_per_label = [r["control_name"] for r in _overall_rows]

    for idx, panel in enumerate(panels):
        r_idx, c_idx = divmod(idx, n_cols)
        pdf = (
            overall
            if panel == "Overall"
            else fit.filter(pl.col("geo_id") == panel).sort("control_name", "category", "label")
        )
        _add_panel_traces(fig, pdf, has_moe, row=r_idx + 1, col=c_idx + 1)

    # Data-driven x-range: show errors at readable scale while
    # keeping typical PUMS MOE whiskers partially visible.
    _max_err = max(
        fit["diff_pct"].drop_nulls().abs().max() or 0,
        overall["diff_pct"].drop_nulls().abs().max() or 0,
    )
    _median_moe = (fit["moe_pct"].drop_nulls().median() or 0) if has_moe else 0
    _x_limit = math.ceil(max(_median_moe, _max_err * 1.5, 5) / 5) * 5

    fig.update_xaxes(
        range=[-_x_limit, _x_limit],
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
        title_text="% Error",
        matches="x",  # shared x-range across all panels
    )
    # Control group labels on the y-axis (bold prefix on first tick per group).
    # categoryarray + autorange="reversed" ensures first-in-list renders at the
    # top of the chart, so the group prefix appears above its members.
    ticktext = _add_group_prefixes(cat_labels, ctrl_per_label)
    fig.update_yaxes(
        categoryorder="array",
        categoryarray=cat_labels,
        tickvals=cat_labels,
        ticktext=ticktext,
        tickfont_size=9,
        autorange="reversed",
    )
    fig.update_layout(
        autosize=False,
        width=960,
        height=max(350, 18 * len(cat_labels) * n_rows + 40 * n_rows),
        margin={"l": 160, "r": 20, "t": 30, "b": 20},
        dragmode=False,
    )
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    return fig


def violins_figure(weighted: pl.DataFrame) -> go.Figure:
    """Violin plot of ``hh_weight`` by zone (log scale)."""
    zones = sorted(weighted["ctrl_geoid"].unique().to_list())
    fig = go.Figure()
    for z in zones:
        w = weighted.filter(pl.col("ctrl_geoid") == z)["hh_weight"].to_list()
        fig.add_trace(
            go.Violin(
                y=w,
                name=str(z),
                box_visible=True,
                meanline_visible=True,
                bandwidth=max(0.05, (max(w) - min(w)) / 40) if w else 0.1,
                spanmode="manual",
                span=[max(1e-6, min(w)), max(w)],
            )
        )
    fig.update_layout(
        showlegend=False,
        title="Household Weight Distribution by Zone",
        yaxis_title="hh_weight",
        yaxis_type="linear",
        autosize=False,
        width=960,
        height=max(400, 60 * len(zones)),
        margin={"l": 60, "r": 20, "t": 40, "b": 40},
        updatemenus=[
            {
                "type": "buttons",
                "direction": "left",
                "x": 1.0,
                "y": 1.02,
                "xanchor": "right",
                "yanchor": "bottom",
                "buttons": [
                    {"label": "Log", "method": "relayout", "args": [{"yaxis.type": "log"}]},
                    {"label": "Linear", "method": "relayout", "args": [{"yaxis.type": "linear"}]},
                ],
            }
        ],
        dragmode=False,
        xaxis_fixedrange=True,
        yaxis_fixedrange=True,
    )
    return fig


# ---------------------------------------------------------------------------
# Imputation distribution chart
# ---------------------------------------------------------------------------


def imputation_distribution_figure(
    seed_incidence: pl.DataFrame,
    pums_incidence: pl.DataFrame,
    target_names: list[str],
    imputed_controls: list[str],
    *,
    pre_imputation: pl.DataFrame | None = None,
) -> go.Figure:
    """Stacked bar chart: observed + imputed contribution vs PUMS shares.

    For each imputed control the survey bar is split into two stacked
    segments — *Observed* (pre-imputation, unweighted) and *Imputed*
    (the RF-predicted fractional contribution).  A separate PUMS bar
    provides the reference distribution.

    When *pre_imputation* is ``None`` the chart falls back to a single
    post-imputation bar (no stacking).
    """
    all_ctrls = resolve_targets(target_names)
    ctrl_map = {c.name: c for c in all_ctrls}

    categories: list[str] = []
    observed_pcts: list[float] = []
    imputed_pcts: list[float] = []
    pums_pcts: list[float] = []

    for ctrl_name in imputed_controls:
        ctrl = ctrl_map.get(ctrl_name)
        if ctrl is None or ctrl.structural:
            continue
        member_cols = [f"{ctrl.name}__{m.lower()}" for _, m in ctrl.valid_members]
        member_labels = [m.replace("_", " ").title() for _, m in ctrl.valid_members]

        present_cols = [c for c in member_cols if c in seed_incidence.columns]
        if not present_cols:
            continue

        # Post-imputation sums (unweighted)
        post_sums = seed_incidence.select(present_cols).sum().row(0)
        post_total = sum(post_sums) or 1

        # Pre-imputation sums (unweighted, zeros where null)
        if pre_imputation is not None:
            pre_cols = [c for c in present_cols if c in pre_imputation.columns]
            pre_sums = pre_imputation.select(pre_cols).sum().row(0) if pre_cols else post_sums
        else:
            pre_sums = post_sums

        # PUMS shares (WGTP-weighted if available)
        has_wgtp = "WGTP" in pums_incidence.columns
        if has_wgtp:
            pums_sums = pums_incidence.select(
                [(pl.col(c) * pl.col("WGTP")).sum().alias(c) for c in present_cols],
            ).row(0)
        else:
            pums_sums = pums_incidence.select(present_cols).sum().row(0)
        pums_total = sum(pums_sums) or 1

        for i, col in enumerate(present_cols):
            idx = member_cols.index(col)
            categories.append(f"{ctrl.name}: {member_labels[idx]}")
            obs = 100.0 * pre_sums[i] / post_total
            imp = 100.0 * (post_sums[i] - pre_sums[i]) / post_total
            observed_pcts.append(obs)
            imputed_pcts.append(imp)
            pums_pcts.append(100.0 * pums_sums[i] / pums_total)

    if not categories:
        return go.Figure()

    fig = go.Figure()
    has_imputed = any(v > 0.01 for v in imputed_pcts)  # noqa: PLR2004

    # Observed + Imputed stack in offsetgroup "survey";
    # PUMS sits in its own offsetgroup so the two groups are side-by-side.
    fig.add_trace(
        go.Bar(
            y=categories,
            x=observed_pcts,
            name="Observed (unweighted)",
            orientation="h",
            marker_color="steelblue",
            offsetgroup="survey",
        )
    )
    if has_imputed:
        fig.add_trace(
            go.Bar(
                y=categories,
                x=imputed_pcts,
                name="Imputed (RF)",
                orientation="h",
                marker_color="rgba(70,160,220,0.5)",
                offsetgroup="survey",
                base=observed_pcts,
            )
        )
    fig.add_trace(
        go.Bar(
            y=categories,
            x=pums_pcts,
            name="PUMS",
            orientation="h",
            marker_color="rgba(200,100,50,0.7)",
            offsetgroup="pums",
        )
    )
    fig.update_layout(
        barmode="group",
        xaxis_title="Share (%)",
        autosize=False,
        width=960,
        height=max(300, 28 * len(categories) + 80),
        margin={"l": 200, "r": 30, "t": 30, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        dragmode=False,
        xaxis_fixedrange=True,
        yaxis_fixedrange=True,
    )
    fig.update_yaxes(autorange="reversed")
    return fig


# ---------------------------------------------------------------------------
# Expansion-factor tradeoff chart
# ---------------------------------------------------------------------------


def ef_tradeoff_figure(
    grid_results: list,
    selected_ef: float,
) -> go.Figure:
    """Small-multiples chart: four stacked subplots sharing the x-axis.

    Hovering at an EF value on any subplot shows aligned tooltips on
    all four panels via ``hovermode="x unified"`` and spike lines.

    Args:
        grid_results: One entry per expansion-factor value with aggregate metrics.
        selected_ef: The user's chosen ``max_expansion_factor`` — shown as a vertical
            dashed marker line on every panel.
    """
    efs = [g.max_expansion_factor for g in grid_results]

    # Panel 1: Fit error metrics (all on same % scale)
    fit_traces: list[tuple[str, list[float], str, str]] = [
        ("MAPE (%)", [g.mape for g in grid_results], "#1f77b4", ",.2f"),
        ("P90 (%)", [g.p90 for g in grid_results], "#ff7f0e", ",.2f"),
        ("Max Error (%)", [g.max_error for g in grid_results], "#9467bd", ",.2f"),
    ]
    # Panels 2-3: Weight quality
    quality_panels: list[tuple[str, list[float], str, str]] = [
        ("CV", [g.cv for g in grid_results], "#d62728", ",.3f"),
        ("ESS (%)", [g.ess_pct for g in grid_results], "#2ca02c", ",.1f"),
    ]

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=["Fit Error (%)", "CV", "ESS (%)"],
    )

    # Panel 1 — three fit-error traces overlaid
    for name, values, color, fmt in fit_traces:
        fig.add_trace(
            go.Scatter(
                x=efs,
                y=values,
                mode="lines+markers",
                name=name,
                line={"color": color, "width": 2},
                marker={"size": 6},
                hovertemplate=f"{name}: %{{y:{fmt}}}<extra></extra>",
                showlegend=True,
            ),
            row=1,
            col=1,
        )

    # Panels 2-3 — CV and ESS
    for i, (name, values, color, fmt) in enumerate(quality_panels, 2):
        fig.add_trace(
            go.Scatter(
                x=efs,
                y=values,
                mode="lines+markers",
                name=name,
                line={"color": color, "width": 2},
                marker={"size": 6},
                hovertemplate=f"{name}: %{{y:{fmt}}}<extra></extra>",
                showlegend=False,
            ),
            row=i,
            col=1,
        )

    # Vertical marker on all panels
    for row in range(1, 4):
        fig.add_vline(
            x=selected_ef,
            line_dash="dot",
            line_color="#333",
            line_width=1.5,
            row=row,
            col=1,
        )

    # Only the top panel gets the annotation so it doesn't repeat
    fig.add_annotation(
        x=selected_ef,
        y=1,
        yref="y domain",
        text=f"selected EF={selected_ef}",
        showarrow=False,
        font={"size": 11, "color": "#333"},
        xanchor="left",
        xshift=5,
        row=1,
        col=1,
    )

    fig.update_xaxes(title_text="Max Expansion Factor", row=3, col=1)
    fig.update_layout(
        autosize=False,
        width=960,
        height=600,
        margin={"l": 60, "r": 30, "t": 80, "b": 50},
        hovermode="x unified",
        hoversubplots="axis",
        title={"text": "Expansion Factor Tradeoff", "y": 0.98, "yanchor": "top"},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        dragmode=False,
    )
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    return fig


# ---------------------------------------------------------------------------
# Crosswalk map
# ---------------------------------------------------------------------------

# Target zones — per-zone traces for group-aware colouring
_GROUP_FILLS = [
    "rgba(70,130,180,0.15)",  # steel blue (default / ungrouped)
    "rgba(180,100,50,0.15)",  # burnt orange
    "rgba(100,170,80,0.15)",  # olive green
    "rgba(150,80,160,0.15)",  # plum
    "rgba(200,170,50,0.15)",  # gold
    "rgba(80,170,170,0.15)",  # teal
]
_GROUP_BORDERS = [
    "steelblue",
    "rgb(180,100,50)",
    "rgb(100,170,80)",
    "rgb(150,80,160)",
    "rgb(200,170,50)",
    "rgb(80,170,170)",
]


def _build_tooltips(
    target_4326: gpd.GeoDataFrame,
    xw: pl.DataFrame,
    sample_counts: dict[str, int],
) -> dict[str, str]:
    """Build per-zone tooltips showing PUMA allocation weights."""
    tooltips: dict[str, str] = {}
    for geo_id in target_4326["study_geoid"]:
        rows = xw.filter(pl.col("study_geoid") == geo_id).sort(
            "allocation_weight",
            descending=True,
        )
        if rows.is_empty():
            tooltips[geo_id] = f"Zone {geo_id}: no PUMA overlap"
            continue
        zone_pop = rows["population"].sum()
        header = f"Zone {geo_id} (BG 2020 person pop {zone_pop:,.0f})"
        if geo_id in sample_counts:
            header += f" — {sample_counts[geo_id]:,} sample HH"
        lines = [header]
        for r in rows.iter_rows(named=True):
            aw = r["allocation_weight"] * 100
            # Drop slivers below 0.01% allocation to reduce tooltip noise (outer joins)
            if aw < 0.01:  # noqa: PLR2004
                continue
            lines.append(f"  PUMA {r['puma_id']}: {aw:.2f}%")
        tooltips[geo_id] = "<br>".join(lines)
    return tooltips


def _build_zone_labels_and_colors(
    target_4326: gpd.GeoDataFrame,
    zone_to_group_idx: dict[str, int],
    zone_to_group_name: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Build zone labels and colors for centroid labels."""
    zone_labels: list[str] = []
    zone_colors: list[str] = []
    for gid in target_4326["study_geoid"]:
        gname = zone_to_group_name.get(gid)
        label = f"Zone {gid} ({gname})" if gname else f"Zone {gid}"
        zone_labels.append(label)
        gi = zone_to_group_idx.get(gid, 0)
        zone_colors.append(_GROUP_BORDERS[gi])
    return zone_labels, zone_colors


def _build_zone_group_index(
    target_4326: gpd.GeoDataFrame,
    zone_groups: dict[str, list[str]] | None = None,
) -> dict[str, int]:
    """Build mapping from zone ID to group index."""
    zone_to_group_idx: dict[str, int] = {}
    next_idx = 1  # 0 is reserved; explicit groups start at 1
    if zone_groups:
        for zones in zone_groups.values():
            idx = next_idx % len(_GROUP_FILLS)
            for z in zones:
                zone_to_group_idx[z] = idx
            next_idx += 1

    # Assign each ungrouped zone its own cycling colour
    for _, row in target_4326.iterrows():
        gid = row["study_geoid"]
        if gid not in zone_to_group_idx:
            zone_to_group_idx[gid] = next_idx % len(_GROUP_FILLS)
            next_idx += 1

    return zone_to_group_idx


def _add_zone_traces(
    fig: go.Figure,
    target_4326: gpd.GeoDataFrame,
    zone_to_group_idx: dict[str, int],
) -> None:
    """Add target zone traces grouped by color."""
    group_rows: dict[int, list] = defaultdict(list)
    group_tooltips: dict[int, list[str]] = defaultdict(list)
    for _, row in target_4326.iterrows():
        gi = zone_to_group_idx.get(row["study_geoid"], 0)
        group_rows[gi].append(row)
        group_tooltips[gi].append(row["tooltip"])

    for gi, rows in group_rows.items():
        fill = _GROUP_FILLS[gi]
        border = _GROUP_BORDERS[gi]
        batch = gpd.GeoDataFrame(rows, crs=target_4326.crs)
        fig.add_trace(
            go.Choroplethmap(
                geojson=batch.__geo_interface__,
                locations=batch.index,
                z=[0] * len(batch),
                colorscale=[[0, fill], [1, fill]],
                marker_line_color=border,
                marker_line_width=2,
                showscale=False,
                text=group_tooltips[gi],
                hoverinfo="text",
            )
        )


def _add_zone_label_traces(
    fig: go.Figure,
    zone_centroids: gpd.GeoSeries,
    zone_labels: list[str],
    zone_colors: list[str],
) -> None:
    """Add zone label traces grouped by color."""
    label_groups: dict[str, tuple[list[float], list[float], list[str]]] = defaultdict(
        lambda: ([], [], [])
    )
    for i, pt in enumerate(zone_centroids):
        c = zone_colors[i]
        lons, lats, texts = label_groups[c]
        lons.append(pt.x)  # pyright: ignore[reportAttributeAccessIssue]
        lats.append(pt.y)  # pyright: ignore[reportAttributeAccessIssue]
        texts.append(zone_labels[i])

    for color, (lons, lats, texts) in label_groups.items():
        fig.add_trace(
            go.Scattermap(
                lon=lons,
                lat=lats,
                mode="text",
                text=texts,
                textfont={"size": 12, "color": color},
                hoverinfo="skip",
                showlegend=False,
            )
        )


def crosswalk_figure(
    puma_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    crosswalk_df: pl.DataFrame,
    households: pl.DataFrame | None = None,
    zone_groups: dict[str, list[str]] | None = None,
) -> go.Figure:
    """Build an interactive Plotly map of the crosswalk.

    Layers:
    - PUMA boundaries (dashed grey) — full extent
    - Study area outline (bold black)
    - Target zones (solid border, transparent fill) with tooltip
      showing PUMA allocation weights from the crosswalk.

    Args:
        puma_gdf: PUMA boundary polygons (must have ``puma_id`` column).
        target_gdf: Target zone polygons (must have ``study_geoid`` column).
        crosswalk_df: Crosswalk table with ``puma_id``, ``study_geoid``,
            ``population``, ``allocation_weight``.
        households: Assigned households (must contain ``study_geoid``).  When
            provided, per-zone sample counts appear in the tooltip.
        zone_groups: Optional zone group mapping.  When provided, grouped zones
            share a fill colour and labels include the group name.

    Returns:
        go.Figure
    """
    puma_4326 = puma_gdf.to_crs("EPSG:4326")
    target_4326 = target_gdf.to_crs("EPSG:4326")
    study_boundary = target_4326.dissolve()

    # Build tooltip per target zone showing allocation weights
    xw = crosswalk_df.select(
        "puma_id",
        "study_geoid",
        "population",
        "allocation_weight",
    )

    # Per-zone sample counts from assigned households (Polars only)
    sample_counts: dict[str, int] = {}
    if households is not None and "study_geoid" in households.columns:
        sample_counts = dict(
            households.filter(pl.col("study_geoid").is_not_null())
            .group_by("study_geoid")
            .len()
            .iter_rows()
        )

    tooltips = _build_tooltips(target_4326, xw, sample_counts)
    target_4326 = target_4326.copy()
    target_4326["tooltip"] = target_4326["study_geoid"].map(tooltips)

    fig = go.Figure()

    # PUMAs -- full boundaries, outline only
    fig.add_trace(
        go.Choroplethmap(
            geojson=puma_4326.__geo_interface__,
            locations=puma_4326.index,
            z=[0] * len(puma_4326),
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
            marker_line_color="rgba(100,100,100,0.7)",
            marker_line_width=1,
            showscale=False,
            hoverinfo="skip",
        )
    )

    # Study area -- bold black outline
    fig.add_trace(
        go.Choroplethmap(
            geojson=study_boundary.__geo_interface__,
            locations=study_boundary.index,
            z=[0] * len(study_boundary),
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
            marker_line_color="black",
            marker_line_width=3,
            showscale=False,
            hoverinfo="skip",
        )
    )

    # Build zone grouping
    zone_to_group_idx = _build_zone_group_index(target_4326, zone_groups)

    # Add target zone traces
    _add_zone_traces(fig, target_4326, zone_to_group_idx)

    # PUMA labels at centroids (grey)
    puma_centroids = puma_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
    fig.add_trace(
        go.Scattermap(
            lon=[p.x for p in puma_centroids],  # pyright: ignore[reportAttributeAccessIssue]
            lat=[p.y for p in puma_centroids],  # pyright: ignore[reportAttributeAccessIssue]
            mode="text",
            text=[f"PUMA {pid}" for pid in puma_4326["puma_id"]],
            textfont={"size": 10, "color": "gray"},
            hoverinfo="skip",
            showlegend=False,
        )
    )

    # Zone labels at centroids — colour-matched to group
    zone_centroids = target_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
    zone_to_group_name: dict[str, str] = {}
    if zone_groups:
        for gname, zones in zone_groups.items():
            for z in zones:
                zone_to_group_name[z] = gname

    zone_labels, zone_colors = _build_zone_labels_and_colors(
        target_4326, zone_to_group_idx, zone_to_group_name
    )

    # Add zone label traces
    _add_zone_label_traces(fig, zone_centroids, zone_labels, zone_colors)

    # Layout -- centre on PUMA extent and estimate zoom from longitude span.
    bounds = puma_4326.total_bounds  # (minx, miny, maxx, maxy)
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    lon_span = bounds[2] - bounds[0]
    # Mercator zoom: 360 / 2^z ≈ visible longitude span at the equator.
    # Adjust for map width (960 px / 256 px tile) and add padding.
    zoom = math.log2(360 / max(lon_span, 0.01)) + math.log2(960 / 256) - 0.5
    fig.update_layout(
        map={
            "style": "carto-positron",
            "center": {"lon": center_lon, "lat": center_lat},
            "zoom": max(zoom, 3),
        },
        autosize=False,
        width=960,
        height=700,
        margin={"l": 0, "r": 0, "t": 30, "b": 0},
        title="Crosswalk: PUMA to Target Zones",
    )

    return fig
