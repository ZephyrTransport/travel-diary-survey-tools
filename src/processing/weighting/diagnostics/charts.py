"""Plotly chart builders for the diagnostics report."""

import math

import numpy as np
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from processing.weighting.controls.registry import CONTROLS, resolve_targets

from .data import with_diff

_MOE_Z = 1.645  # ACS 90% margin of error = 1.645 x the replicate-weight standard error
_FLOOR_PCT = 5  # never red within this, however small the margin of error
_WARN_PCT = 15  # red threshold for a group with no standard error
_SINA_HALF_WIDTH = 0.42  # half a zone's column, reached by its densest weights
_SINA_SEED = 0  # fixed, so the same weights always draw the same chart


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

        margin = _MOE_Z * moe if moe is not None and moe > 0 else None
        is_red = abs(pct) > (max(margin, _FLOOR_PCT) if margin is not None else _WARN_PCT)
        colors.append("#c33" if is_red else "#999")

        hover_lines = [
            f"<b>{lbl}</b>",
            f"Target: {target:,.1f}",
            f"Weighted: {r['weighted_total']:,.1f}",
            f"Diff: {r['diff']:+,.1f}",
            f"Diff %: {pct:+.1f}%",
        ]
        if margin is not None:
            hover_lines.append(f"ACS 90% MOE: \u00b1{margin:.1f}%")
        else:
            hover_lines.append(f"No ACS margin of error: red past \u00b1{_WARN_PCT}%")
        hovers.append("<br>".join(hover_lines))
        moe_values.append(margin)

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


def _overall_fit(fit: pl.DataFrame) -> pl.DataFrame:
    """The fit table summed over zones, for the Overall panel."""
    has_moe = "moe_pct" in fit.columns
    # Overall aggregation — sum targets/weighted across zones
    agg_exprs = [pl.col("target_total").sum(), pl.col("weighted_total").sum()]
    if has_moe:
        # Propagate MOE: SE_overall = sqrt(Σ SE_i²), then moe_pct = SE / target * 100
        # SE_i = moe_pct_i / 100 * target_i. An area with a target but no SE leaves none.
        no_se = (pl.col("moe_pct").is_null() & pl.col("target_total").is_not_null()).any()
        agg_exprs.append(
            pl.when(no_se)
            .then(None)
            .otherwise(
                ((pl.col("moe_pct") / 100 * pl.col("target_total")) ** 2).sum().sqrt()
                / pl.col("target_total").sum()
                * 100
            )
            .fill_nan(None)
            .alias("moe_pct")
        )

    overall = with_diff(fit.group_by("control_name", "category", "label").agg(*agg_exprs)).sort(
        "control_name", "category", "label"
    )

    # Re-sort: structural controls first (h_total, p_total), then alphabetical
    _structural = {n for n, c in CONTROLS.items() if c.structural}
    return (
        overall.with_columns(
            pl.when(pl.col("control_name").is_in(_structural)).then(0).otherwise(1).alias("_prio")
        )
        .sort("_prio", "control_name", "category", "label")
        .drop("_prio")
    )


def fit_x_limit(fits: list[pl.DataFrame]) -> float:
    """One x range for every profile's fit chart, so equal errors look equal.

    Wide enough for the largest error in any zone or overall at 1.5x, and for the
    median margin of error; at least 5%, in steps of 5.
    """
    limit = 5.0
    for fit in fits:
        overall = _overall_fit(fit)
        max_err = max(
            fit["diff_pct"].drop_nulls().abs().max() or 0,
            overall["diff_pct"].drop_nulls().abs().max() or 0,
        )
        median_margin = (
            _MOE_Z * (fit["moe_pct"].drop_nulls().median() or 0) if "moe_pct" in fit.columns else 0
        )
        limit = max(limit, median_margin, max_err * 1.5)
    return math.ceil(limit / 5) * 5


def fit_diverging_figure(
    fit: pl.DataFrame,
    x_limit: float | None = None,
) -> go.Figure:
    """Grid of horizontal diverging bar charts (% error, one panel per zone + overall).

    Expects *fit* to contain a ``label`` column (added by
    [`apply_fit_merges`][processing.weighting.diagnostics.data.apply_fit_merges]).
    Null placeholder rows are rendered as invisible bars
    so the y-axis remains consistent across panels.

    When ``moe_pct`` is present (from PUMS replicate weights), horizontal
    error bars show the sampling margin of error on each target. *x_limit* is
    the range either side of zero, from `fit_x_limit`; ``None`` fits this chart alone.
    """
    has_moe = "moe_pct" in fit.columns
    zones = sorted(fit["zone_group_id"].unique().to_list())
    overall = _overall_fit(fit)

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
            else fit.filter(pl.col("zone_group_id") == panel).sort(
                "control_name", "category", "label"
            )
        )
        _add_panel_traces(fig, pdf, has_moe, row=r_idx + 1, col=c_idx + 1)

    _x_limit = x_limit if x_limit is not None else fit_x_limit([fit])

    fig.update_xaxes(
        range=[-_x_limit, _x_limit],
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
        title_text="% Error",
        matches="x",  # shared x-range across all panels
    )
    # Bold control prefix on each group's first tick; reversed, so it sits above its members
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


def _sina_offsets(values: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Sideways offsets for one zone's points, as wide as the weights are dense there.

    Each point moves a uniform random share of the density at its own value, scaled
    so the densest value fills ``_SINA_HALF_WIDTH``. A zone of under five
    households has no density to speak of, so its points get a narrow even jitter.
    """
    if len(values) < 5:  # noqa: PLR2004
        return rng.uniform(-0.1, 0.1, len(values))
    bandwidth = 1.06 * float(values.std()) * len(values) ** -0.2 or 1e-9  # Silverman's rule
    grid = np.linspace(values.min(), values.max(), 512)
    density = np.exp(-0.5 * ((grid[:, None] - values[None, :]) / bandwidth) ** 2).sum(axis=1)
    at_value = np.interp(values, grid, density)
    return rng.uniform(-1, 1, len(values)) * at_value / density.max() * _SINA_HALF_WIDTH


def weight_distribution_figure(weighted: pl.DataFrame) -> go.Figure:
    """``hh_weight`` by zone: one point per household, spread by density, and the mean.

    The spread follows the density on the scale shown: the Log and Linear buttons
    switch the axis and the points' sideways positions together.
    """
    zones = sorted(weighted["zone_group_id"].unique().to_list())
    rng = np.random.default_rng(_SINA_SEED)
    fig = go.Figure()
    x_linear: list[list[float]] = []
    x_log: list[list[float]] = []
    for i, z in enumerate(zones):
        w = weighted.filter(pl.col("zone_group_id") == z)["hh_weight"].to_numpy()
        on_linear = (i + _sina_offsets(w, rng)).tolist()
        on_log = (i + _sina_offsets(np.log(np.maximum(w, 1e-9)), rng)).tolist()
        fig.add_trace(
            go.Scatter(
                x=on_linear,
                y=w,
                mode="markers",
                name=str(z),
                marker={"size": 3, "opacity": 0.45},
                hovertemplate=f"{z}<br>weight %{{y:,.1f}}<extra></extra>",
            )
        )
        mean = float(w.mean()) if len(w) else 0.0
        edges = [i - _SINA_HALF_WIDTH, i + _SINA_HALF_WIDTH]
        fig.add_trace(
            go.Scatter(
                x=edges,
                y=[mean, mean],
                mode="lines",
                name=f"{z} mean",
                line={"color": "black", "width": 2},
                hovertemplate=f"{z}<br>mean %{{y:,.1f}}<extra></extra>",
            )
        )
        x_linear += [on_linear, edges]
        x_log += [on_log, edges]
    fig.update_layout(
        showlegend=False,
        title="Household Weight Distribution by Zone",
        yaxis_title="hh_weight",
        yaxis_type="linear",
        xaxis={
            "tickvals": list(range(len(zones))),
            "ticktext": [str(z) for z in zones],
            "range": [-0.6, len(zones) - 0.4],
        },
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
                    {
                        "label": "Log",
                        "method": "update",
                        "args": [{"x": x_log}, {"yaxis.type": "log"}],
                    },
                    {
                        "label": "Linear",
                        "method": "update",
                        "args": [{"x": x_linear}, {"yaxis.type": "linear"}],
                    },
                ],
            }
        ],
        dragmode=False,
        xaxis_fixedrange=True,
        yaxis_fixedrange=True,
    )
    return fig


def imputation_distribution_figure(
    seed_incidence: pl.DataFrame,
    pums_incidence: pl.DataFrame,
    target_names: list[str],
    imputed_controls: list[str],
    *,
    pre_imputation: pl.DataFrame,
) -> go.Figure:
    """Stacked bar chart: observed + imputed contribution vs PUMS shares.

    For each filled control the survey bar is split into two stacked
    segments — *Observed* (pre-imputation, unweighted) and *Imputed*
    (the RF-predicted fractional contribution), so the second segment
    shows how much of a category's mass is prediction rather than
    response.  A separate PUMS bar provides the reference distribution.
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
        pre_cols = [c for c in present_cols if c in pre_imputation.columns]
        pre_sums = pre_imputation.select(pre_cols).sum().row(0) if pre_cols else post_sums

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


def ef_tradeoff_figure(
    grid_results: list,
    selected_ef: float,
) -> go.Figure:
    """Small-multiples chart: three stacked subplots sharing the x-axis.

    Hovering at an EF value on any subplot shows aligned tooltips on
    all three panels via ``hovermode="x unified"`` and spike lines.

    Args:
        grid_results: One entry per expansion-factor value with aggregate metrics.
        selected_ef: The user's chosen ``max_expansion_factor`` — shown as a vertical
            dashed marker line on every panel.
    """
    efs = [g.max_expansion_factor for g in grid_results]

    # (panel row, name, values, colour, hover format): fit errors overlaid on row 1,
    # then CV and ESS on rows 2 and 3
    traces: list[tuple[int, str, list[float], str, str]] = [
        (1, "MAPE (%)", [g.mape for g in grid_results], "#1f77b4", ",.2f"),
        (1, "P90 (%)", [g.p90 for g in grid_results], "#ff7f0e", ",.2f"),
        (1, "Max Error (%)", [g.max_error for g in grid_results], "#9467bd", ",.2f"),
        (2, "CV", [g.cv for g in grid_results], "#d62728", ",.3f"),
        (3, "ESS (%)", [g.ess_pct for g in grid_results], "#2ca02c", ",.1f"),
    ]

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=["Fit Error (%)", "CV", "ESS (%)"],
    )

    for row, name, values, color, fmt in traces:
        fig.add_trace(
            go.Scatter(
                x=efs,
                y=values,
                mode="lines+markers",
                name=name,
                line={"color": color, "width": 2},
                marker={"size": 6},
                hovertemplate=f"{name}: %{{y:{fmt}}}<extra></extra>",
                showlegend=row == 1,
            ),
            row=row,
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
