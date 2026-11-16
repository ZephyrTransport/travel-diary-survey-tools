"""HTML table builders for the diagnostics report.

This module generates six of the tables displayed in the diagnostics HTML report:

1. **Balancer Performance Table** — Per-zone convergence status, target-fit metrics
   (MAPE, P90, Max), and weight quality metrics (CV, ESS%). Combines all key
   performance indicators into a single comprehensive table.

2. **Weight Quality Table** — Per-zone weight distribution statistics (mean, median,
   std, min, max) and expansion factor stats (min/max/mean/median EF ratio).

3. **Unweighted Cell Counts** — Data sparsity matrix showing unweighted sample counts
   per control category per zone, with optional PUMS-weighted percentages for context,
   and each control's importance.

4. **Crosswalk Summary Table** — Zone → HH samples mapping with optional zone group
   aggregation.

5. **Imputation Summary Table** — Per control, how many seed rows had an empty
   incidence block, and the cross-validated log-loss and F1 of the PUMS-trained
   model that filled them.

6. **Survey Share Table** — Each group's share of its area's survey against its
   share of the area's PUMS.

All table builders return raw HTML strings suitable for Jinja2 template insertion,
built with the helpers in ``table_html``. The per-profile tables are in
``profile_tables``.
"""

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.core.specs import (
    ControlTotals,
    ImputationSummary,
    ZoneStatus,
)
from processing.weighting.data_prep.incidence import control_columns
from processing.weighting.validation.survey_shares import table_cells

from .data import category_label_map
from .glossary import term
from .table_html import html_table, tag, wbr

_HIGH_NULL_PCT = 25
_LOW_COUNT_THRESHOLD = 30


def imputation_summary_table(summaries: list[ImputationSummary]) -> str:
    """Generate the fractional seed imputation table.

    One row per control showing how many seed rows had an empty incidence block
    for it, plus the cross-validated log-loss and F1 of the PUMS-trained model
    that filled them.  Describes the seed the balancer was handed -- not the
    survey ``imputation`` step, which writes discrete values to canonical data.
    """
    headers = [
        "Control",
        "Level",
        "Records",
        "Null",
        "Null&nbsp;%",
        term("logloss", "RF&nbsp;log_loss"),
        term("f1", "RF&nbsp;F1"),
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
    return html_table(headers, rows)


def balancer_performance_table(
    statuses: list[ZoneStatus],
    weighted: pl.DataFrame,
    zone_fit: pl.DataFrame,
) -> str:
    """Generate the main balancer performance table.

    Combines three categories of per-zone metrics into a single comprehensive table:

    - **Convergence:** Did the balancer converge? How many iterations?
    - **Target Fit:** How well do weighted totals match PUMS targets? (MAPE, P90, Max)
    - **Weight Quality:** How stable/dispersed are the weights? (CV, ESS%)

    Args:
        statuses: Per-zone convergence results from the balancer.
        weighted: Household seed joined with final weights (must include `zone_group_id`,
            `hh_weight`, `base_weight` columns).
        zone_fit: Zone-level target fit summary (output of `zone_fit_summary()`).

    Returns:
        HTML table with 13 columns: Zone, N, Conv?, Iter, Household (Target, % Error),
        Person (Target, % Error), MAPE, P90, Max, CV, ESS%.

    Note:
        Uses `html_table()` with a two-tier grouped header. The "Household" and
        "Person" columns span their respective Target/% Error sub-columns.
    """
    status_map = {s.zone_group_id: s for s in statuses}
    fit_map = {r["zone_group_id"]: r for r in zone_fit.iter_rows(named=True)}
    zones = sorted(status_map)

    group_row = [
        ("Zone", 1),
        ("N", 1),
        ("Conv?", 1),
        ("Iter", 1),
        ("Household", 2),
        ("Person", 2),
        (term("mape"), 1),
        (term("p90"), 1),
        (term("maxerr"), 1),
        (term("cv"), 1),
        (term("ess", "ESS&nbsp;%"), 1),
    ]
    pct_error = term("pcterr", "%&nbsp;Error")
    sub_headers = ["Target", pct_error, "Target", pct_error]

    rows: list[list[str]] = []
    for z in zones:
        s = status_map[z]
        f = fit_map.get(z, {})
        zone_df = weighted.filter(pl.col("zone_group_id") == z)
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

    return html_table(sub_headers, rows, group_row=group_row)


def _weight_stats(w: pl.Series, bw: pl.Series) -> list[str]:
    """Weight distribution and expansion factor statistics, formatted.

    Args:
        w: Final household weights.
        bw: Base weights (pre-balancing).

    Returns:
        Mean, median, std, min and max of *w*, then the min, max, mean and
        median EF (expansion factor, the ratio w / bw).
    """
    ratio = w / bw
    return [
        f"{w.mean() or 0:,.2f}",
        f"{w.median():,.2f}",
        f"{w.std():,.2f}",
        f"{w.min():,.2f}",
        f"{w.max():,.2f}",
        f"{ratio.min():.3f}",
        f"{ratio.max():.3f}",
        f"{ratio.mean():.3f}",
        f"{ratio.median():.3f}",
    ]


def weight_quality_table(weighted: pl.DataFrame) -> str:
    """Generate the weight quality table.

    Shows per-zone and total weight distribution statistics (mean, median, std,
    min, max) and expansion factor ratios (min/max/mean/median EF). This table
    complements the weight chart that follows it in the report.

    Args:
        weighted: Household seed joined with final weights (must include `zone_group_id`,
            `hh_weight`, `base_weight` columns).

    Returns:
        HTML table with 11 columns: Zone, N, Mean, Median, Std, Min, Max, Min EF,
        Max EF, Mean EF, Median EF, plus a TOTAL row aggregating across zones.
    """
    headers = [
        "Zone",
        "N",
        "Mean",
        "Median",
        "Std",
        "Min",
        "Max",
        term("ef", "Min&nbsp;EF"),
        term("ef", "Max&nbsp;EF"),
        term("ef", "Mean&nbsp;EF"),
        term("ef", "Median&nbsp;EF"),
    ]

    def _row(label: str, df: pl.DataFrame) -> list[str]:
        return [label, f"{df.height:,}", *_weight_stats(df["hh_weight"], df["base_weight"])]

    zones = sorted(weighted["zone_group_id"].unique().to_list())
    rows = [_row(str(z), weighted.filter(pl.col("zone_group_id") == z)) for z in zones]
    rows.append(_row("TOTAL", weighted))
    return html_table(headers, rows)


def _count_cell(count: int, pums_pct: float | None = None, *, na: bool = False) -> str:
    """Format a count cell; *na* marks a category not active in this zone (em-dash)."""
    if na:
        return '<td style="color:#aaa;text-align:center">&mdash;</td>'
    pct_html = ""
    if pums_pct is not None:
        pct_html = f' <em style="color:#888;font-weight:normal">({pums_pct:.1f}%)</em>'
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
            merged_zones[(spec.control, merged_label.lower())] = zone_set
            for m in base_members:
                key = (spec.control, m.lower())
                absorbed_zones.setdefault(key, set()).update(zone_set)
    return merged_zones, absorbed_zones


def _importance_cell(control: str, used: dict[str, float], moe: dict[str, float] | None) -> str:
    """The importance balancing used, with the MOE-computed one in parentheses."""
    if moe is None:
        return f"{used[control]:.0f}"
    computed = moe.get(control)
    shown = "&mdash;" if computed is None else f"{computed:.0f}"
    return f'{used[control]:.0f} <em style="color:#888;font-weight:normal">({shown})</em>'


def _seed_counts(
    seed: pl.DataFrame, controls: list
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, dict[str, int]]]:
    """Each control's ``(column, category)`` pairs in *seed*, and every column's count per zone.

    Categories come from the seed's own columns, so merged ones are included and
    the originals a merge dropped are not.
    """
    ctrl_members = {c.name: list(control_columns(c, seed.columns).items()) for c in controls}
    control_cols = [col for members in ctrl_members.values() for col, _ in members]
    if not control_cols:
        return ctrl_members, {}
    zone_counts = seed.group_by("zone_group_id").agg(
        [pl.col(c).sum().alias(c) for c in control_cols]
    )
    counts_by_zone = {
        row["zone_group_id"]: {col: int(row[col]) for col in control_cols}
        for row in zone_counts.iter_rows(named=True)
    }
    return ctrl_members, counts_by_zone


def _pums_shares(
    ct: pl.DataFrame,
) -> tuple[dict[tuple[str, str, str], float], dict[tuple[str, str], float]]:
    """Each category's share of its control's PUMS total, per zone and over all zones."""
    zone_ctrl_totals = ct.group_by(["zone_group_id", "control_name"]).agg(
        pl.col("target_total").sum().alias("zone_ctrl_total")
    )
    ct_with_pct = ct.join(zone_ctrl_totals, on=["zone_group_id", "control_name"], how="left")
    ct_with_pct = ct_with_pct.with_columns(
        (pl.col("target_total") / pl.col("zone_ctrl_total") * 100).alias("pct")
    )
    pums_pct = {
        (row["zone_group_id"], row["control_name"], row["category"]): row["pct"]
        for row in ct_with_pct.iter_rows(named=True)
    }

    all_ctrl_totals = ct.group_by("control_name").agg(
        pl.col("target_total").sum().alias("all_ctrl_total")
    )
    ct_total = (
        ct.group_by(["control_name", "category"])
        .agg(pl.col("target_total").sum().alias("cat_total"))
        .join(all_ctrl_totals, on="control_name", how="left")
        .with_columns((pl.col("cat_total") / pl.col("all_ctrl_total") * 100).alias("pct"))
    )
    pums_pct_total = {
        (row["control_name"], row["category"]): row["pct"] for row in ct_total.iter_rows(named=True)
    }
    return pums_pct, pums_pct_total


def unweighted_cell_counts(  # noqa: C901
    seed: pl.DataFrame,
    target_names: list[str],
    control_totals: ControlTotals | None = None,
    merge_specs: list | None = None,
    *,
    moe_importance: dict[str, float] | None = None,
    importance_used: dict[str, float] | None = None,
) -> str:
    """Single matrix table: categories (rows) x zones (columns).

    Row headers are grouped by control name using ``<th rowspan>``.
    A level separator row (Household / Person) divides the two groups.

    With *importance_used*, each control also shows the importance balancing
    gave it, with what its PUMS MOE gave (*moe_importance*) in parentheses, or
    a dash where the MOE could not be computed. The parentheses are left out
    when importance was not MOE-based.

    When *control_totals* is provided, each cell also shows the
    PUMS-weighted percentage in italic parentheses so the reader can
    compare survey representation against the PUMS universe.

    Uses uniform column handling for all controls (structural unpivoted,
    non-structural pivoted).
    """
    labels = category_label_map(target_names)
    merged_zones, absorbed_zones = _build_zone_merge_lookups(merge_specs)
    zones = sorted(seed["zone_group_id"].unique().to_list())
    n_zone_cols = len(zones)
    all_controls = resolve_targets(target_names, ControlLevel.HOUSEHOLD) + resolve_targets(
        target_names, ControlLevel.PERSON
    )
    ctrl_members, counts_by_zone = _seed_counts(seed, all_controls)

    pums_pct: dict[tuple[str, str, str], float] = {}
    pums_pct_total: dict[tuple[str, str], float] = {}
    if control_totals is not None:
        ct = control_totals.totals
        pums_pct, pums_pct_total = _pums_shares(ct)

        # A category is shown if, in some zone where it is a target, it has a target or
        # records. One zone merges replaced everywhere is a target nowhere with either.
        targets = {
            (r["zone_group_id"], r["control_name"], r["category"]): r["target_total"]
            for r in ct.iter_rows(named=True)
        }
        ctrl_members = {
            name: [
                (col, key)
                for col, key in members
                if any(
                    (z, name, key) in targets
                    and (targets[(z, name, key)] or counts_by_zone.get(z, {}).get(col))
                    for z in zones
                )
            ]
            for name, members in ctrl_members.items()
        }

    importance_cols: list[str] = []
    if importance_used is not None:
        importance_cols = ["Importance" if moe_importance is None else "Importance (MOE-computed)"]

    header = (
        "<tr>"
        + tag("th", "Control")
        + "".join(tag("th", c) for c in importance_cols)
        + tag("th", "Category")
        + "".join(tag("th", wbr(str(z))) for z in zones)
        + tag("th", "Total")
        + "</tr>"
    )

    data_rows: list[str] = []

    for level in (ControlLevel.HOUSEHOLD, ControlLevel.PERSON):
        ctrls = resolve_targets(target_names, level)
        if not ctrls:
            continue
        level_label = "Household" if level == ControlLevel.HOUSEHOLD else "Person"
        data_rows.append(
            f'<tr><td colspan="{n_zone_cols + 3 + len(importance_cols)}" '
            f'style="background:#e8e8e8;font-weight:bold;text-align:left">{level_label}</td></tr>'
        )
        for ctrl in ctrls:
            members = ctrl_members.get(ctrl.name, [])
            for i, (col, member_key) in enumerate(members):
                cells = ""
                if i == 0:
                    span = f'rowspan="{len(members)}"'
                    cells += f"<th {span}>{ctrl.name}</th>"
                    if importance_used is not None:
                        shown = _importance_cell(ctrl.name, importance_used, moe_importance)
                        cells += f"<td {span}>{shown}</td>"

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


def crosswalk_summary_table(
    crosswalk_df: pl.DataFrame,
    seed: pl.DataFrame,
    counties: dict[str, str],
) -> str:
    """Households per zone, in their balancing groups, with the Census county each covers.

    The county names come from the Census rather than the config, so a zone ID put
    in the wrong group shows up as a county the group's name doesn't match.
    Households outside every zone get their own row: they cannot be weighted.

    Args:
        crosswalk_df: The PUMA-to-zone allocation, with ``target_zone_id`` and
            ``zone_group_id``.
        seed: One profile's household seed, with ``target_zone_id``.
        counties: Zone ID to its Census county name or names.
    """
    counts = dict(
        seed.filter(pl.col("target_zone_id").is_not_null())
        .group_by("target_zone_id")
        .len()
        .iter_rows()
    )
    outside = seed.filter(pl.col("target_zone_id").is_null()).height
    zones = (
        crosswalk_df.select("target_zone_id", "zone_group_id")
        .unique()
        .filter(pl.col("target_zone_id").is_not_null())
        .sort("zone_group_id", "target_zone_id")
    )
    grouped = zones.filter(pl.col("target_zone_id") != pl.col("zone_group_id")).height > 0

    headers = (["Zone group"] if grouped else []) + ["Zone", "Census county", "Households"]
    pad = [""] if grouped else []
    body: list[list[str]] = []
    for (group,), members in zones.group_by("zone_group_id", maintain_order=True):
        for i, zone in enumerate(members["target_zone_id"].to_list()):
            name = [str(group) if i == 0 else ""] if grouped else []
            body.append(
                [*name, _left(zone), _left(counties.get(zone, "")), f"{counts.get(zone, 0):,}"]
            )
        if grouped and members.height > 1:
            total = sum(counts.get(z, 0) for z in members["target_zone_id"].to_list())
            body.append(["", "", _left("<b>Group</b>"), f"<b>{total:,}</b>"])
    body.append(["<b>Outside all zones</b>", *pad, "", f"<b>{outside:,}</b>"])
    body.append(["<b>Total</b>", *pad, "", f"<b>{seed.height:,}</b>"])
    return html_table(headers, body)


def _left(text: str) -> str:
    """A left-aligned cell, for names in a table that right-aligns its numbers."""
    return f'<td style="text-align:left">{text}</td>'


def survey_share_table_html(shares: pl.DataFrame) -> str:
    """Every group's share of its area's survey against its share of the area's PUMS.

    Smallest ratio first, so the groups nearest the stop are at the top. The
    columns are the stop message's, so the two read the same way.
    """
    headers = [
        "Area",
        "Target",
        "Group",
        "Survey",
        "PUMS",
        "%&nbsp;of area's survey",
        "%&nbsp;of area's PUMS",
        "Ratio",
    ]
    rows = []
    for r in shares.sort("ratio").iter_rows(named=True):
        cells = table_cells(r)
        cells[2] = wbr(cells[2])  # the group name, which can be long
        rows.append(cells)
    return html_table(headers, rows)
