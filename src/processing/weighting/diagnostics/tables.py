"""HTML table builders for the diagnostics report.

This module generates the four main tables displayed in the diagnostics HTML report:

1. **Balancer Performance Table** — Per-zone convergence status, target-fit metrics
   (MAPE, P90, Max), and weight quality metrics (CV, ESS%). Combines all key
   performance indicators into a single comprehensive table.

2. **Weight Quality Table** — Per-zone weight distribution statistics (mean, median,
   std, min, max) and expansion factor stats (min/max/mean/median EF ratio).

3. **Unweighted Cell Counts** — Data sparsity matrix showing unweighted sample counts
   per control category per zone, with optional PUMS-weighted percentages for context.

4. **Crosswalk Summary Table** — Zone → HH samples mapping with optional zone group
   aggregation.

All table builders return raw HTML strings suitable for Jinja2 template insertion.
The `_html_table()` helper provides a consistent interface for simple tables and
supports grouped/spanned headers for complex layouts.
"""

import re

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.core.specs import ControlTotals, ImputationSummary, ZoneStatus

from .data import category_label_map

# ---------------------------------------------------------------------------
# HTML primitives
# ---------------------------------------------------------------------------


def _tag(name: str, text: str, **attrs: str) -> str:
    """Wrap *text* in an HTML element with optional attributes."""
    attr_str = "".join(f' {k.rstrip("_")}="{v}"' for k, v in attrs.items()) if attrs else ""
    return f"<{name}{attr_str}>{text}</{name}>"


def _wbr(text: str) -> str:
    """Insert ``<wbr>`` word-break opportunities after ``/``, ``-``, ``_``, and spaces."""
    return re.sub(r"([/_\- ])", r"\1<wbr>", text)


def _html_table(
    headers: list[str],
    rows: list[list[str]],
    *,
    group_row: list[tuple[str, int]] | None = None,
    css_class: str = "",
) -> str:
    r"""Build a ``<table>`` from *headers* and pre-formatted cell strings.

    Provides a consistent interface for generating HTML tables with single or
    multi-tier headers. Cells starting with ``<td`` are inserted as-is to support
    pre-styled cells (e.g., CSS classes for coloring convergence status).

    Args:
        headers: Column header labels for a single-row header, or sub-column labels
            when `group_row` is used.
        rows: Table body, one list per row. Each cell is a pre-formatted string.
        group_row: Optional two-tier header specification. Each tuple is
            ``(label, colspan)``. Columns with ``colspan == 1`` span both header rows
            (``rowspan="2"``). Columns with ``colspan > 1`` become group headers
            spanning multiple sub-columns (labels consumed from `headers` list).
        css_class: Optional CSS class name applied to the ``<table>`` element.

    Returns:
        Complete HTML ``<table>`` string.

    Example:
        Simple table:
        >>> _html_table(["Zone", "Count"], [["A", "100"], ["B", "200"]])
        '<table>\n<tr><th>Zone</th><th>Count</th></tr>\n...'

        Grouped header:
        >>> _html_table(
        ...     ["Target", "% Error"],  # sub-headers
        ...     [["A", "1000", "+2.5%"]],
        ...     group_row=[("Zone", 1), ("Household", 2)]  # Zone spans both rows
        ... )
    """
    cls = f' class="{css_class}"' if css_class else ""

    if group_row is not None:
        top_cells: list[str] = []
        sub_cells: list[str] = []
        for label, span in group_row:
            if span == 1:
                top_cells.append(f'<th rowspan="2">{label}</th>')
            else:
                top_cells.append(f'<th colspan="{span}">{label}</th>')
                sub_cells.extend(_tag("th", headers.pop(0)) for _ in range(span))
        head = "<tr>" + "".join(top_cells) + "</tr>\n<tr>" + "".join(sub_cells) + "</tr>"
    else:
        head = "<tr>" + "".join(_tag("th", h) for h in headers) + "</tr>"

    body = "\n".join(
        "<tr>" + "".join(c if c.startswith("<td") else _tag("td", c) for c in cells) + "</tr>"
        for cells in rows
    )
    return f"<table{cls}>\n{head}\n{body}\n</table>"


# ---------------------------------------------------------------------------
# Section 0 — Data Quality & Imputation
# ---------------------------------------------------------------------------

_HIGH_NULL_PCT = 25


def imputation_summary_table(summaries: list[ImputationSummary]) -> str:
    """Generate the imputation summary table (Section 0 of diagnostics report).

    One row per control showing null count, null share, RF cross-validated
    log-loss and F1, and an overall status indicator.
    """
    headers = [
        "Control",
        "Level",
        "Records",
        "Null",
        "Null&nbsp;%",
        "RF log_loss",
        "RF&nbsp;F1",
        "Status",
    ]
    rows: list[list[str]] = []
    for s in summaries:
        null_pct = 100.0 * s.n_null / s.n_total if s.n_total > 0 else 0.0
        ll = f"{s.log_loss:.3f}" if s.log_loss is not None else "\u2014"
        f1 = f"{s.f1_macro:.3f}" if s.f1_macro is not None else "\u2014"
        if s.n_null == 0:
            status = "\u2714"
        elif null_pct > _HIGH_NULL_PCT:
            status = f'<span style="color:#c33;font-weight:bold">\u26a0 >{_HIGH_NULL_PCT}%</span>'
        else:
            status = "\u2714 imputed"
        rows.append(
            [
                s.control,
                s.level.title(),
                f"{s.n_total:,}",
                f"{s.n_null:,}",
                f"{null_pct:.1f}%",
                ll,
                f1,
                status,
            ]
        )
    return _html_table(headers, rows)


# ---------------------------------------------------------------------------
# Section 2 — Balancer Performance (convergence + target fit)
# ---------------------------------------------------------------------------


def balancer_performance_table(
    statuses: list[ZoneStatus],
    weighted: pl.DataFrame,
    zone_fit: pl.DataFrame,
) -> str:
    """Generate the main balancer performance table (Section 2 of diagnostics report).

    Combines three categories of per-zone metrics into a single comprehensive table:

    - **Convergence:** Did the balancer converge? How many iterations?
    - **Target Fit:** How well do weighted totals match PUMS targets? (MAPE, P90, Max)
    - **Weight Quality:** How stable/dispersed are the weights? (CV, ESS%)

    Args:
        statuses: Per-zone convergence results from the balancer.
        weighted: Household seed joined with final weights (must include `ctrl_geoid`,
            `hh_weight`, `base_weight` columns).
        zone_fit: Zone-level target fit summary (output of `zone_fit_summary()`).

    Returns:
        HTML table with 13 columns: Zone, N, Conv?, Iter, Household (Target, % Error),
        Person (Target, % Error), MAPE, P90, Max, CV, ESS%.

    Note:
        Uses `_html_table()` with a two-tier grouped header. The "Household" and
        "Person" columns span their respective Target/% Error sub-columns.
    """
    status_map = {s.geo_id: s for s in statuses}
    fit_map = {r["geo_id"]: r for r in zone_fit.iter_rows(named=True)}
    zones = sorted(status_map)

    group_row = [
        ("Zone", 1),
        ("N", 1),
        ("Conv?", 1),
        ("Iter", 1),
        ("Household", 2),
        ("Person", 2),
        ("MAPE", 1),
        ("P90", 1),
        ("Max", 1),
        ("CV", 1),
        ("ESS&nbsp;%", 1),
    ]
    sub_headers = ["Target", "%&nbsp;Error", "Target", "%&nbsp;Error"]

    rows: list[list[str]] = []
    for z in zones:
        s = status_map[z]
        f = fit_map.get(z, {})
        zone_df = weighted.filter(pl.col("ctrl_geoid") == z)
        n = zone_df.height

        w = zone_df["hh_weight"]
        mean = w.mean() or 0
        sum_w = w.sum()
        sum_w2 = (w * w).sum()
        cv = f"{w.std() / mean:.3f}" if mean else "N/A"  # pyright: ignore[reportOperatorIssue]
        ess_pct = ((sum_w**2 / sum_w2) / n * 100) if sum_w2 > 0 and n > 0 else 0.0  # pyright: ignore[reportOperatorIssue]

        css = "converged" if s.converged else "failed"
        rows.append(
            [
                z,
                f"{n:,}",
                f'<td class="{css}">{"Y" if s.converged else "N"}</td>',
                str(s.iterations),
                f"{f.get('hh_target', 0):,.0f}",
                f"{f.get('hh_pct_err', 0):+.1f}%",
                f"{f.get('per_target', 0):,.0f}",
                f"{f.get('per_pct_err', 0):+.1f}%",
                f"{f.get('mape', 0):.2f}%",
                f"{f.get('p90_err', 0):.2f}%",
                f"{f.get('max_err', 0):.2f}%",
                cv,
                f"{ess_pct:.1f}%",
            ]
        )

    return _html_table(sub_headers, rows, group_row=group_row)


# ---------------------------------------------------------------------------
# Section 3 — Weight Quality (distribution + ESS + CV + expansion factors)
# ---------------------------------------------------------------------------


def _weight_stats(w: pl.Series, bw: pl.Series) -> dict[str, str]:
    """Compute weight distribution and expansion factor statistics.

    Args:
        w: Final household weights.
        bw: Base weights (pre-balancing).

    Returns:
        Dictionary of pre-formatted strings: mean, median, std, min, max, min_ef,
        max_ef, mean_ef, median_ef. EF (expansion factor) is the ratio w / bw.

    Note:
        CV and ESS are now computed inline in `balancer_performance_table()` rather
        than here, as they only appear in that table.
    """
    ratio = w / bw
    return {
        "mean": f"{w.mean() or 0:,.2f}",
        "median": f"{w.median():,.2f}",
        "std": f"{w.std():,.2f}",
        "min": f"{w.min():,.2f}",
        "max": f"{w.max():,.2f}",
        "min_ef": f"{ratio.min():.3f}",
        "max_ef": f"{ratio.max():.3f}",
        "mean_ef": f"{ratio.mean():.3f}",
        "median_ef": f"{ratio.median():.3f}",
    }


def weight_quality_table(weighted: pl.DataFrame) -> str:
    """Generate the weight quality table (Section 3 of diagnostics report).

    Shows per-zone and total weight distribution statistics (mean, median, std,
    min, max) and expansion factor ratios (min/max/mean/median EF). This table
    complements the violin plot that follows it in the report.

    Args:
        weighted: Household seed joined with final weights (must include `ctrl_geoid`,
            `hh_weight`, `base_weight` columns).

    Returns:
        HTML table with 11 columns: Zone, N, Mean, Median, Std, Min, Max, Min EF,
        Max EF, Mean EF, Median EF, plus a TOTAL row aggregating across zones.

    Note:
        CV and ESS% were removed from this table in March 2026 and moved to the
        balancer performance table for a unified view of all key metrics.
    """
    headers = [
        "Zone",
        "N",
        "Mean",
        "Median",
        "Std",
        "Min",
        "Max",
        "Min&nbsp;EF",
        "Max&nbsp;EF",
        "Mean&nbsp;EF",
        "Median&nbsp;EF",
    ]

    def _row(label: str, df: pl.DataFrame) -> list[str]:
        s = _weight_stats(df["hh_weight"], df["base_weight"])
        return [
            label,
            f"{df.height:,}",
            s["mean"],
            s["median"],
            s["std"],
            s["min"],
            s["max"],
            s["min_ef"],
            s["max_ef"],
            s["mean_ef"],
            s["median_ef"],
        ]

    zones = sorted(weighted["ctrl_geoid"].unique().to_list())
    rows = [_row(str(z), weighted.filter(pl.col("ctrl_geoid") == z)) for z in zones]
    rows.append(_row("TOTAL", weighted))
    return _html_table(headers, rows)


# ---------------------------------------------------------------------------
# Section 5 — Unweighted cell counts (data sparsity matrix)
# ---------------------------------------------------------------------------

_LOW_COUNT_THRESHOLD = 30


def _count_cell(
    count: int,
    pums_pct: float | None = None,
    *,
    dimmed: bool = False,
    na: bool = False,
) -> str:
    """Format a count cell.

    *dimmed* — constituent category absorbed by a zone merge (grey italic).
    *na* — merged category not active for this zone (em-dash).
    """
    if na:
        return '<td style="color:#aaa;text-align:center">&mdash;</td>'
    pct_html = ""
    if pums_pct is not None:
        pct_html = f' <em style="color:#888;font-weight:normal">({pums_pct:.1f}%)</em>'
    if dimmed:
        return f'<td style="color:#aaa;font-style:italic">{count}{pct_html}</td>'
    if count < _LOW_COUNT_THRESHOLD:
        return f'<td style="color:#c33;font-weight:bold">{count}{pct_html}</td>'
    return f"<td>{count}{pct_html}</td>"


def _build_zone_merge_lookups(
    merge_specs: list | None,
) -> tuple[dict[tuple[str, str], set[str]], dict[tuple[str, str], set[str]]]:
    """Parse zone-specific merges into per-cell annotation lookups.

    Returns:
    -------
    merged_zones : dict[(control, merged_label), set[zone_id]]
        Zones where this merged category is active.
    absorbed_zones : dict[(control, constituent), set[zone_id]]
        Zones where this constituent is absorbed into a merge.
    """
    merged_zones: dict[tuple[str, str], set[str]] = {}
    absorbed_zones: dict[tuple[str, str], set[str]] = {}
    for spec in merge_specs or []:
        if spec.zones is None:
            continue
        zone_set = set(spec.zones)
        for merged_label, base_members in spec.groups.items():
            if isinstance(base_members, dict):
                continue
            merged_zones[(spec.control, merged_label.lower())] = zone_set
            for m in base_members:
                key = (spec.control, m.lower())
                absorbed_zones.setdefault(key, set()).update(zone_set)
    return merged_zones, absorbed_zones


def unweighted_cell_counts(  # noqa: C901, PLR0912, PLR0915
    seed: pl.DataFrame,
    target_names: list[str],
    control_totals: ControlTotals | None = None,
    merge_specs: list | None = None,
) -> str:
    """Single matrix table: categories (rows) x zones (columns).

    Row headers are grouped by control name using ``<th rowspan>``.
    A level separator row (Household / Person) divides the two groups.

    When *control_totals* is provided, each cell also shows the
    PUMS-weighted percentage in italic parentheses so the reader can
    compare survey representation against the PUMS universe.

    Uses uniform column handling for all controls (structural unpivoted,
    non-structural pivoted).
    """
    labels = category_label_map(target_names)
    merged_zones, absorbed_zones = _build_zone_merge_lookups(merge_specs)
    zones = sorted(seed["ctrl_geoid"].unique().to_list())
    n_zone_cols = len(zones)

    # Pre-compute counts for all controls across all zones with single aggregation.
    # Discover categories from actual seed columns (includes merged,
    # excludes dropped originals).
    all_controls = resolve_targets(target_names, ControlLevel.HOUSEHOLD) + resolve_targets(
        target_names, ControlLevel.PERSON
    )

    # Build (ctrl, col_name, member_key) triples from actual seed columns
    ctrl_members: dict[str, list[tuple[str, str]]] = {}  # ctrl_name -> [(col, member_key)]
    control_cols: list[str] = []

    for ctrl in all_controls:
        members: list[tuple[str, str]] = []
        if ctrl.structural:
            col = ctrl.name
            if col in seed.columns:
                member_key = ctrl.valid_members[0][1].lower()
                members.append((col, member_key))
                control_cols.append(col)
        else:
            prefix = f"{ctrl.name}__"
            for col in sorted(c for c in seed.columns if c.startswith(prefix)):
                member_key = col[len(prefix) :]
                members.append((col, member_key))
                control_cols.append(col)
        ctrl_members[ctrl.name] = members

    # Single group_by aggregation to get all zone x control counts
    if control_cols:
        zone_counts = seed.group_by("ctrl_geoid").agg(
            [pl.col(c).sum().alias(c) for c in control_cols]
        )
        counts_by_zone: dict[str, dict[str, int]] = {}
        for row in zone_counts.iter_rows(named=True):
            zone_id = row["ctrl_geoid"]
            counts_by_zone[zone_id] = {col: int(row[col]) for col in control_cols}
    else:
        counts_by_zone = {}

    # Pre-compute PUMS share per (zone, control, category) ----------------
    pums_pct: dict[tuple[str, str, str], float] = {}
    pums_pct_total: dict[tuple[str, str], float] = {}
    if control_totals is not None:
        ct = control_totals.totals
        zone_ctrl_totals = ct.group_by(["geo_id", "control_name"]).agg(
            pl.col("target_total").sum().alias("zone_ctrl_total")
        )
        ct_with_pct = ct.join(zone_ctrl_totals, on=["geo_id", "control_name"], how="left")
        ct_with_pct = ct_with_pct.with_columns(
            (pl.col("target_total") / pl.col("zone_ctrl_total") * 100).alias("pct")
        )
        for row in ct_with_pct.iter_rows(named=True):
            pums_pct[(row["geo_id"], row["control_name"], row["category"])] = row["pct"]

        all_ctrl_totals = ct.group_by("control_name").agg(
            pl.col("target_total").sum().alias("all_ctrl_total")
        )
        ct_total = (
            ct.group_by(["control_name", "category"])
            .agg(pl.col("target_total").sum().alias("cat_total"))
            .join(all_ctrl_totals, on="control_name", how="left")
            .with_columns((pl.col("cat_total") / pl.col("all_ctrl_total") * 100).alias("pct"))
        )
        for row in ct_total.iter_rows(named=True):
            pums_pct_total[(row["control_name"], row["category"])] = row["pct"]

    header = (
        "<tr>"
        + _tag("th", "Control")
        + _tag("th", "Category")
        + "".join(_tag("th", _wbr(str(z))) for z in zones)
        + _tag("th", "Total")
        + "</tr>"
    )

    data_rows: list[str] = []

    for level in (ControlLevel.HOUSEHOLD, ControlLevel.PERSON):
        ctrls = resolve_targets(target_names, level)
        if not ctrls:
            continue
        level_label = "Household" if level == ControlLevel.HOUSEHOLD else "Person"
        data_rows.append(
            f'<tr><td colspan="{n_zone_cols + 3}" '
            f'style="background:#e8e8e8;font-weight:bold;text-align:left">{level_label}</td></tr>'
        )
        for ctrl in ctrls:
            members = ctrl_members.get(ctrl.name, [])
            for i, (col, member_key) in enumerate(members):
                cells = ""
                if i == 0:
                    cells += f'<th rowspan="{len(members)}">{ctrl.name}</th>'

                # Zone merge annotations
                active_in = merged_zones.get((ctrl.name, member_key))
                absorbed_in = absorbed_zones.get((ctrl.name, member_key))
                lbl = labels.get((ctrl.name, member_key), member_key.replace("_", " ").title())
                cells += f'<td style="text-align:left">{lbl}</td>'

                row_total = 0
                for z in zones:
                    count = counts_by_zone.get(z, {}).get(col, 0)
                    pct = pums_pct.get((z, ctrl.name, member_key))
                    is_na = (active_in is not None and z not in active_in) or (
                        absorbed_in is not None and z in absorbed_in
                    )
                    if not is_na:
                        row_total += count
                    cells += _count_cell(count, pct, na=is_na)
                total_pct = pums_pct_total.get((ctrl.name, member_key))
                cells += _count_cell(row_total, total_pct)
                data_rows.append(f"<tr>{cells}</tr>")

    return '<table class="sparsity">\n' + header + "\n" + "\n".join(data_rows) + "\n</table>"


# ---------------------------------------------------------------------------
# Section 1b — Crosswalk summary table
# ---------------------------------------------------------------------------


def crosswalk_summary_table(crosswalk_df: pl.DataFrame, seed: pl.DataFrame) -> str:  # noqa: C901, PLR0912
    """Compact Zone -> HH Samples table with optional Zone Group column."""
    # study_geoid is the raw crosswalk geography; ctrl_geoid may be grouped.
    sample_counts = dict(
        seed.filter(pl.col("study_geoid").is_not_null()).group_by("study_geoid").len().iter_rows()
    )

    # Zone group mapping (study_geoid → ctrl_geoid when they differ)
    zone_to_group: dict[str, str] = {}
    if "ctrl_geoid" in seed.columns and "study_geoid" in seed.columns:
        pairs = (
            seed.filter(pl.col("study_geoid") != pl.col("ctrl_geoid"))
            .select("study_geoid", "ctrl_geoid")
            .unique()
        )
        if pairs.height > 0:
            zone_to_group = dict(pairs.iter_rows())
    has_groups = bool(zone_to_group)

    raw_zones = (
        crosswalk_df.select("study_geoid")
        .unique()
        .filter(pl.col("study_geoid").is_not_null())
        .to_series()
        .to_list()
    )
    # Sort by zone group then descending HH samples so group members stay
    # contiguous (needed for correct rowspan) and larger zones appear first.
    zones = sorted(
        raw_zones,
        key=lambda g: (zone_to_group.get(g, ""), -(sample_counts.get(g, 0))),
    )

    headers = []
    if has_groups:
        headers += ["Zone Group", "Grouped HH Samples"]
    headers += ["Zone", "HH Samples"]
    header = "<tr>" + "".join(_tag("th", h) for h in headers) + "</tr>"

    # Build runs of consecutive zones sharing the same group for rowspan
    group_runs: list[tuple[str, int]] = []  # (group_name, span)
    if has_groups:
        for geo in zones:
            grp = zone_to_group.get(geo, "")
            if group_runs and group_runs[-1][0] == grp and grp:
                group_runs[-1] = (grp, group_runs[-1][1] + 1)
            else:
                group_runs.append((grp, 1))

    # Grouped sample totals
    group_sample_totals: dict[str, int] = {}
    if has_groups:
        for geo, grp in zone_to_group.items():
            group_sample_totals[grp] = group_sample_totals.get(grp, 0) + sample_counts.get(geo, 0)

    body_rows: list[str] = []
    run_idx = 0
    pos_in_run = 0
    for geo in zones:
        cells = ""
        if has_groups:
            grp = zone_to_group.get(geo, "")
            if grp and group_runs:
                _, span = group_runs[run_idx]
                if pos_in_run == 0:
                    rs = f' rowspan="{span}"' if span > 1 else ""
                    cells += f"<td{rs}>{grp}</td>"
                    total = group_sample_totals.get(grp, 0)
                    cells += f"<td{rs}>{total:,}</td>"
                pos_in_run += 1
                if pos_in_run >= span:
                    run_idx += 1
                    pos_in_run = 0
            else:
                cells += _tag("td", "")
                cells += _tag("td", "")
        cells += f'<td style="text-align:left">{geo}</td>'
        cells += _tag("td", f"{sample_counts.get(geo, 0):,}")
        body_rows.append(f"<tr>{cells}</tr>")

    return f"<table>\n{header}\n" + "\n".join(body_rows) + "\n</table>"
