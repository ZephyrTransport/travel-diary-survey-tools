"""The report's per-profile tables: how each fit compares, and what carries weight.

One row or column per profile, so the fits can be read side by side: seed and
weight sums, the cascade of usable records, and how far kept records were
scaled up to carry dropped ones. The records the cleaner excluded sit beside the
cascade, the same under every profile.
"""

from .data import (
    RAISE_BANDS,
    CascadeRow,
    ExclusionRow,
    ProfileSummary,
    RedistributionRow,
)
from .glossary import term
from .table_html import html_table, profile_label

# How a parent level is named in a row label, as in "persons, from their household".
_PARENT_NOUN = {
    "households": "household",
    "persons": "person",
    "person_days": "day",
    "linked_trips": "linked trip",
    "tours": "tour",
    "joint_trips": "joint trip",
    "joint_tours": "joint tour",
}


def profile_comparison_table(rows: list[ProfileSummary]) -> str:
    """Compare every fit in the run, one row each.

    Each profile is balanced to the same controls over its own universe, so the
    totals should land on the same targets. Divergence is the usability gate
    changing what the survey can reach, not the balancer failing -- which is why
    this table only exists once all the fits are done.
    """
    headers = [
        "Profile",
        "Households&nbsp;in&nbsp;sample",
        "&Sigma;&nbsp;hh&nbsp;weight",
        "&Sigma;&nbsp;person&nbsp;weight",
        "&Sigma;&nbsp;day&nbsp;weight",
        term("ess", "ESS&nbsp;%"),
        term("cv"),
        term("maxmed"),
        term("mape"),
        "Converged",
    ]
    body: list[list[str]] = []
    for r in rows:
        converged = f"{r.zones_converged} / {r.zones_total}"
        css = "converged" if r.zones_converged == r.zones_total else "failed"
        body.append(
            [
                profile_label(r.profile),
                f"{r.seed_households:,}",
                f"{r.weight_sums.get('households', 0.0):,.0f}",
                f"{r.weight_sums.get('persons', 0.0):,.0f}",
                f"{r.weight_sums.get('person_days', 0.0):,.0f}",
                f"{r.ess_pct:.1f}%",
                f"{r.cv:.3f}",
                f"{r.max_over_median:.1f}",
                f"{r.mape:.2f}%",
                f'<td class="{css}">{converged}</td>',
            ]
        )
    return html_table(headers, body)


def cascade_table(cascades: dict[str | None, list[CascadeRow]]) -> str:
    """Records each profile keeps at each level, profiles side by side.

    Only the kept count is shown: every kept record carries a weight and no other
    record does, or the run stops before writing the report.
    """
    if not cascades:
        return ""
    profiles = list(cascades)
    by_table: dict[str, dict[str | None, CascadeRow]] = {}
    for profile, rows in cascades.items():
        for row in rows:
            by_table.setdefault(row.table, {})[profile] = row

    headers = ["Level", "Records", *(profile_label(p) for p in profiles)]
    body: list[list[str]] = []
    for table, per_profile in by_table.items():
        any_row = next(iter(per_profile.values()))
        cells = [table.replace("_", " "), f"{any_row.rows:,}"]
        cells += [f"{r.usable:,}" if (r := per_profile.get(p)) else "—" for p in profiles]
        body.append(cells)
    return html_table(headers, body)


def exclusion_table(rows: list[ExclusionRow]) -> str:
    """Records the project cleaner excluded, by level and reason."""
    if not rows:
        return ""
    body = [[r.table.replace("_", " "), r.reason, f"{r.records:,}"] for r in rows]
    return html_table(["Level", "Reason", "Records"], body)


def redistribution_view(rows: list[RedistributionRow]) -> str:
    """One profile's kept records: how many carry the record above's weight unchanged.

    The rest were scaled up to carry dropped siblings. A bar per level shows the
    split at a glance, and the table gives the shares by how far each was raised.
    """
    if not rows:
        return ""
    bars: list[str] = []
    body: list[list[str]] = []
    for r in rows:
        child = r.table.replace("_", " ")
        parent = _PARENT_NOUN.get(r.parent, r.parent.replace("_", " "))
        segments = [r.unchanged, *r.bands]
        bars.append(
            f'<div class="bar-row"><span class="bar-lab">{child}</span><span class="bar">'
            + "".join(
                f'<span class="seg s{i}" style="width:{share:.2f}%"></span>'
                for i, share in enumerate(segments)
            )
            + "</span></div>"
        )
        body.append(
            [
                f"{child}, from their {parent}",
                f"{r.records:,}",
                *(f"{share:.1f}%" for share in segments),
                f"{r.maximum:.1f}&times;",
            ]
        )
    labels = ["Unchanged", *(label for label, _, _ in RAISE_BANDS)]
    legend = "".join(
        f'<span class="key s{i}"></span>{label.lower()}' for i, label in enumerate(labels)
    )
    headers = ["Kept records", "Number", *labels, "Largest"]
    return (
        f'<div class="bars">{"".join(bars)}<div class="legend">{legend}</div></div>'
        + html_table(headers, body)
    )
