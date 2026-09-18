"""Plotly builders for the validation report.

One figure per (table, kind): every comparable column of a kind becomes a panel in
a single grouped-bar / overlaid-distribution grid, so ~100 columns render as ~20
figures rather than 100. All binning and aggregation happens in polars, so no trace
carries more than a few dozen points -- the report stays small despite the coverage.

Depends only on spec (for column kinds and labels) and polars; data frames are passed
in. Legacy vs new is blue vs orange throughout.
"""

import plotly.graph_objects as go
import polars as pl
import spec
from plotly.subplots import make_subplots

LEG_C, NEW_C = "#4C78A8", "#F58518"

# Coordinates and weights are legacy-value-vs-new-value SCATTERS: agreement sits on the
# y = x diagonal, disagreement falls off it. There is no "legacy series" and "new series"
# to colour, so these panels carry no legend -- the axes say which is which. Everything
# else is a distribution comparison (two coloured series).
SCATTER_KINDS = {spec.Kind.COORD, spec.Kind.WEIGHT}

# The documented, sanity-checked encoding invariant: legacy clock columns are HHMM
# integers, the new pipeline writes minutes past midnight.
_N_BINS = 20


def _hour_expr(col: str, *, legacy: bool) -> pl.Expr:
    """Departure/clock hour from either encoding."""
    return (pl.col(col) // 100) if legacy else (pl.col(col) % 1440 // 60)


def _share(df: pl.DataFrame, keyexpr: pl.Expr, label: str) -> pl.DataFrame:
    """Value -> percentage-share frame for one pipeline."""
    out = df.select(keyexpr.alias("k")).drop_nulls().group_by("k").agg(pl.len().alias(label))
    total = out[label].sum()
    return out.with_columns((pl.col(label) / total * 100).alias(f"{label}_pct"))


def marginal_distribution(
    cs: spec.ColumnSpec, leg_df: pl.DataFrame, new_df: pl.DataFrame
) -> pl.DataFrame | None:
    """A tidy [order, label, legacy_pct, new_pct] frame comparing one column's shape.

    This is a *marginal* distribution comparison -- it does not require the two
    pipelines' records to be matched, so it works uniformly for every table. Record-
    level agreement lives in the coverage table instead.

    Categorical/count columns bucket by value; time columns by hour; numeric columns
    into shared bins over the combined range. Returns None if a side is missing the
    column or nothing plottable survives.
    """
    name = cs.name
    if name not in leg_df.columns or name not in new_df.columns:
        return None

    if cs.kind in (spec.Kind.CATEGORICAL, spec.Kind.COUNT):
        # Count tails past a sensible cap are lumped so the axis stays readable.
        # Cast to a common integer type -- legacy is int, the new pipeline sometimes
        # float, and the two must share the grouping key's dtype to join.
        cap = 10 if cs.kind is spec.Kind.COUNT else None
        key = pl.col(name).cast(pl.Int64)
        if cap is not None:
            key = pl.when(key > cap).then(cap + 1).otherwise(key)
        leg = _share(leg_df, key, "legacy")
        new = _share(new_df, key, "new")
        merged = leg.join(new, on="k", how="full", coalesce=True).fill_null(0.0).sort("k")
        if cs.labels:
            lab = pl.col("k").cast(pl.Int64).replace_strict(cs.labels, default=None)
            lab = (
                pl.when(pl.col("k") == (cap + 1 if cap else -999))
                .then(pl.lit(f"{cap}+"))
                .otherwise(lab)
                .fill_null(pl.col("k").cast(pl.String))
            )
        else:
            lab = (
                pl.when(pl.col("k") == (cap + 1 if cap else -999))
                .then(pl.lit(f"{cap}+"))
                .otherwise(pl.col("k").cast(pl.String))
            )
        return merged.with_columns(pl.col("k").alias("order"), lab.alias("label"))

    if cs.kind is spec.Kind.TIME:
        leg = _share(
            leg_df.filter(pl.col(name) >= 0), _hour_expr(name, legacy=True).cast(pl.Int64), "legacy"
        )
        new = _share(
            new_df.filter(pl.col(name) >= 0), _hour_expr(name, legacy=False).cast(pl.Int64), "new"
        )
        merged = leg.join(new, on="k", how="full", coalesce=True).fill_null(0.0).sort("k")
        return merged.with_columns(
            pl.col("k").alias("order"),
            (pl.col("k").cast(pl.Int64).cast(pl.String).str.zfill(2) + ":00").alias("label"),
        )

    # Numeric: continuous / coord / weight -> shared bins over the combined range,
    # dropping stub/missing values (negatives, and near-zero coordinates).
    if cs.kind is spec.Kind.COORD:
        lo, hi = pl.col(name).abs() > 1, pl.col(name).abs() > 1
    else:
        lo = hi = pl.col(name) >= 0
    leg_v = leg_df.filter(lo).select(pl.col(name).cast(pl.Float64)).drop_nulls()
    new_v = new_df.filter(hi).select(pl.col(name).cast(pl.Float64)).drop_nulls()
    if leg_v.height == 0 or new_v.height == 0:
        return None
    vmin = min(leg_v[name].min(), new_v[name].min())
    vmax = max(leg_v[name].max(), new_v[name].max())
    if vmin == vmax:
        return None
    edges = [vmin + (vmax - vmin) * i / _N_BINS for i in range(1, _N_BINS)]

    def _binned(v: pl.DataFrame, label: str) -> pl.DataFrame:
        b = v.with_columns(
            pl.col(name).cut(edges, labels=[str(i) for i in range(_N_BINS)]).alias("k")
        )
        out = b.group_by("k").agg(pl.len().alias(label))
        return out.with_columns((pl.col(label) / out[label].sum() * 100).alias(f"{label}_pct"))

    leg = _binned(leg_v, "legacy")
    new = _binned(new_v, "new")
    merged = (
        leg.join(new, on="k", how="full", coalesce=True)
        .fill_null(0.0)
        .with_columns(pl.col("k").cast(pl.Int32).alias("order"))
        .sort("order")
    )
    lo_edges = [vmin, *edges]
    return merged.with_columns(
        pl.col("order")
        .map_elements(lambda i: f"{lo_edges[i]:.4g}", return_dtype=pl.String)
        .alias("label")
    )


def kind_grid(
    table: str,
    kinds: spec.Kind | list[spec.Kind],
    leg_df: pl.DataFrame,
    new_df: pl.DataFrame,
    comparable: set[str],
    *,
    paired: pl.DataFrame | None = None,
    cols: int = 3,
) -> tuple[go.Figure | None, int, int]:
    """A distribution / scatter grid for every comparable column of the given kind(s).

    Coded and count columns render as grouped %-bars, numeric columns as overlaid
    translucent histograms, times as hour profiles, and coordinates as legacy-vs-new
    scatters (agreement on the y = x diagonal) built from ``paired`` -- a frame carrying
    each column as ``col`` and ``col_new`` (the direct join for hh/person/person-day, or
    the matched frame for tours/trips).

    Returns (figure, n_columns_plotted, n_flagged).
    """
    kind_list = [kinds] if isinstance(kinds, spec.Kind) else list(kinds)
    scatter_mode = bool(set(kind_list) & SCATTER_KINDS)
    panels = _build_panels(table, kind_list, leg_df, new_df, comparable, paired)
    if not panels:
        return None, 0, 0

    rows = -(-len(panels) // cols)
    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=[p[0] for p in panels],
        vertical_spacing=max(0.06, 0.5 / rows),
        horizontal_spacing=0.08,
    )
    # Dodged (grouped) bars for coded / count / continuous distributions; lines for the
    # time-of-day profile. No overlay mode -- side-by-side bars read cleaner.
    lines = kind_list[0] is spec.Kind.TIME
    n_flagged = 0
    for i, (_name, pkind, data) in enumerate(panels):
        r, c = divmod(i, cols)
        add_panel = _add_scatter_panel if pkind == "scatter" else _add_distribution_panel
        n_flagged += add_panel(fig, data, row=r + 1, col=c + 1, first=i == 0, lines=lines)

    _style_grid(fig, rows=rows, cols=cols, scatter_mode=scatter_mode)
    return fig, len(panels), n_flagged


def _build_panels(
    table: str,
    kind_list: list[spec.Kind],
    leg_df: pl.DataFrame,
    new_df: pl.DataFrame,
    comparable: set[str],
    paired: pl.DataFrame | None,
) -> list[tuple[str, str, object]]:
    """(name, panel_kind, data) for every comparable column that yields a panel."""
    specs = [
        cs for k in kind_list for cs in spec.columns_of_kind(table, k) if cs.name in comparable
    ]
    panels: list[tuple[str, str, object]] = []
    for cs in specs:
        if cs.kind in SCATTER_KINDS and paired is not None and f"{cs.name}_new" in paired.columns:
            pts = _value_pairs(paired, cs.name, coord=cs.kind is spec.Kind.COORD)
            if pts is not None:
                panels.append((cs.name, "scatter", pts))
            continue
        dist = marginal_distribution(cs, leg_df, new_df)
        if dist is not None and dist.height:
            panels.append((cs.name, "dist", dist))
    return panels


def _add_scatter_panel(
    fig: go.Figure, data: dict, *, row: int, col: int, first: bool, lines: bool
) -> int:
    """Legacy-vs-new scatter with its y = x reference. Returns 1 if flagged, else 0."""
    del first, lines  # uniform panel signature; scatters carry no legend or line mode
    lo, hi = data["lo"], data["hi"]
    fig.add_scatter(
        x=data["x"],
        y=data["y"],
        mode="markers",
        showlegend=False,
        marker={"color": NEW_C, "size": 3, "opacity": 0.4},
        row=row,
        col=col,
    )
    fig.add_scatter(
        x=[lo, hi],
        y=[lo, hi],
        mode="lines",
        showlegend=False,
        line={"color": "grey", "width": 1, "dash": "dot"},
        row=row,
        col=col,
    )
    return int(data["off_diag_pct"] > 5)


def _add_distribution_panel(
    fig: go.Figure, data: pl.DataFrame, *, row: int, col: int, first: bool, lines: bool
) -> int:
    """Legacy vs new distribution, as dodged bars or a profile line. 1 if flagged."""
    common = {"x": data["label"], "showlegend": first, "opacity": 0.85, "row": row, "col": col}
    for name, series, colour in (
        ("Legacy", data["legacy_pct"], LEG_C),
        ("New", data["new_pct"], NEW_C),
    ):
        if lines:
            fig.add_scatter(
                y=series, name=name, mode="lines", line={"color": colour, "width": 2}, **common
            )
        else:
            fig.add_bar(y=series, name=name, marker_color=colour, **common)
    gap = (data["new_pct"] - data["legacy_pct"]).abs().max()
    return int(gap is not None and gap > 5)


def _style_grid(fig: go.Figure, *, rows: int, cols: int, scatter_mode: bool) -> None:
    """Shared layout, and the axis titles that carry each mode's identity."""
    fig.update_layout(
        barmode="group",
        template="plotly_white",
        height=300 * rows,
        margin={"t": 60, "b": 50},
        legend={"orientation": "h", "x": 0.4, "y": 1.04},
    )
    fig.update_annotations(font_size=11)
    fig.update_yaxes(tickfont={"size": 8})
    if scatter_mode:
        # Axes carry the identity: x = legacy value, y = new value (label the edges only).
        fig.update_xaxes(tickfont={"size": 8})
        for c in range(1, cols + 1):
            fig.update_xaxes(title_text="legacy", title_font={"size": 9}, row=rows, col=c)
        for r in range(1, rows + 1):
            fig.update_yaxes(title_text="new", title_font={"size": 9}, row=r, col=1)
    else:
        fig.update_xaxes(tickangle=-45, tickfont={"size": 8})
        for r in range(1, rows + 1):
            fig.update_yaxes(title_text="% of records", title_font={"size": 9}, row=r, col=1)


def _value_pairs(paired: pl.DataFrame, col: str, *, coord: bool, sample: int = 3000) -> dict | None:
    """Legacy-value-vs-new-value pairs for a scatter, with an off-diagonal fraction.

    Points on the y = x diagonal agree; the further off, the larger the difference.
    Coordinates drop near-zero (missing) points; weights keep all non-negative values.
    Down-sampled deterministically (every k-th row) to keep the figure small.
    """
    ncol = f"{col}_new"
    keep = (
        ((pl.col(col).abs() > 1) & (pl.col(ncol).abs() > 1))
        if coord
        else ((pl.col(col) >= 0) & (pl.col(ncol) >= 0))
    )
    valid = paired.filter(keep & pl.col(col).is_not_null() & pl.col(ncol).is_not_null()).select(
        pl.col(col).cast(pl.Float64).alias("x"), pl.col(ncol).cast(pl.Float64).alias("y")
    )
    n = valid.height
    if n == 0:
        return None
    tol = 1e-3 if coord else 1e-6
    off = valid.filter((pl.col("x") - pl.col("y")).abs() > tol).height
    if n > sample:  # deterministic thinning (no RNG in this environment)
        valid = valid.gather_every(-(-n // sample))
    lo = min(valid["x"].min(), valid["y"].min())
    hi = max(valid["x"].max(), valid["y"].max())
    return {"x": valid["x"], "y": valid["y"], "lo": lo, "hi": hi, "off_diag_pct": 100 * off / n}


def trip_structuring_fig(stages: pl.DataFrame) -> go.Figure:
    """Four-stage trip-structuring bars on a single shared record axis.

    Stages 1-3 count linked trips; stage 4 counts tours. Both share one "records" axis
    (no deceptive dual axis) -- tours simply read smaller, which is honest. Every bar is
    annotated with its value.
    """
    fig = go.Figure()
    for label, color in (("legacy", LEG_C), ("new", NEW_C)):
        fig.add_bar(
            x=stages["stage"],
            y=stages[label],
            name=label.capitalize(),
            marker_color=color,
            opacity=0.85,
            text=stages[label],
            texttemplate="%{text:,}",
            textposition="outside",
        )
    fig.update_layout(
        barmode="group",
        template="plotly_white",
        height=460,
        margin={"t": 70, "b": 40},
        legend={"orientation": "h", "x": 0.4, "y": 1.10},
        title_text="Trip structuring: unlinked → linked → in tours → tours "
        "(stages 1-3 count trips; stage 4 counts tours)",
        yaxis_title="records",
    )
    return fig


def confusion_sankey(
    matched: pl.DataFrame, col_leg: str, col_new: str, labels: dict[int, str]
) -> go.Sankey:
    """Legacy-classification -> new-classification flow on matched records."""
    flow = matched.group_by([col_leg, col_new]).agg(pl.len().alias("n")).sort("n", descending=True)
    keys = sorted(labels)
    node_labels = [f"Legacy: {labels[k]}" for k in keys] + [f"New: {labels[k]}" for k in keys]
    idx = {k: i for i, k in enumerate(keys)}
    rows = [r for r in flow.iter_rows(named=True) if r[col_leg] in idx and r[col_new] in idx]
    return go.Sankey(
        node={"pad": 12, "thickness": 16, "label": node_labels},
        link={
            "source": [idx[r[col_leg]] for r in rows],
            "target": [idx[r[col_new]] + len(keys) for r in rows],
            "value": [r["n"] for r in rows],
        },
    )
