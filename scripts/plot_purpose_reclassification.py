"""Draw the hierarchy-vs-scoring tour-purpose Sankey for the Extract Tours docs.

Reads the aggregate purpose-to-purpose tour counts in
``docs/assets/data/purpose_reclassification_bats2023.csv`` (a BATS-2023
snapshot: hierarchy purpose -> scoring purpose -> tour count) and renders
``docs/assets/images/purpose_reclassification.png``.

The figure is drawn directly in matplotlib -- no browser/kaleido dependency --
so it regenerates from the committed counts alone, without the survey data:

    uv run python scripts/plot_purpose_reclassification.py

Refresh the CSV (and rerun this) if the scoring defaults change materially; the
counts come from running extract_tours both ways on BATS-2023.
"""

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import PathPatch
from matplotlib.path import Path as MplPath

_ROOT = Path(__file__).parent.parent
DATA = _ROOT / "docs" / "assets" / "data" / "purpose_reclassification_bats2023.csv"
OUTPUT = _ROOT / "docs" / "assets" / "images" / "purpose_reclassification.png"

# Display order (top to bottom) and colour per purpose.
ORDER = [
    "Work",
    "School",
    "Escort",
    "Shop",
    "Meal",
    "Social/rec",
    "Errand",
    "Overnight",
    "Other",
    "Work-related",
    "School-related",
    "Change-mode",
]
COLOR = {
    "Work": "#1f77b4",
    "Work-related": "#1f77b4",
    "School": "#17becf",
    "School-related": "#17becf",
    "Escort": "#d62728",
    "Shop": "#2ca02c",
    "Meal": "#ff7f0e",
    "Social/rec": "#9467bd",
    "Errand": "#8c564b",
    "Overnight": "#e377c2",
    "Other": "#7f7f7f",
    "Change-mode": "#7f7f7f",
}
GAP = 0.012  # vertical gap between stacked nodes, as a fraction of the column
BAR = 0.018  # node bar width, as a fraction of the x-axis


def _load_flows() -> list[tuple[str, str, int]]:
    """Read (hierarchy, scoring, tours) rows from the committed CSV."""
    with DATA.open(newline="") as handle:
        return [(r["hierarchy"], r["scoring"], int(r["tours"])) for r in csv.DictReader(handle)]


def _node_spans(totals: dict[str, int], grand: int) -> dict[str, tuple[float, float]]:
    """Vertical (bottom, top) span of each node, stacked top-down in ORDER."""
    present = [p for p in ORDER if totals.get(p)]
    usable = 1.0 - GAP * (len(present) - 1)
    spans, cursor = {}, 1.0
    for purpose in present:
        height = totals[purpose] / grand * usable
        spans[purpose] = (cursor - height, cursor)
        cursor -= height + GAP
    return spans


def _ribbon(
    x0: float, x1: float, y0: tuple[float, float], y1: tuple[float, float], color: str
) -> PathPatch:
    """A filled flow ribbon from left span y0 to right span y1 (cubic sides)."""
    mid = (x0 + x1) / 2
    verts = [
        (x0, y0[0]),
        (mid, y0[0]),
        (mid, y1[0]),
        (x1, y1[0]),  # bottom edge L->R
        (x1, y1[1]),
        (mid, y1[1]),
        (mid, y0[1]),
        (x0, y0[1]),  # top edge R->L
        (x0, y0[0]),
    ]
    codes = [
        MplPath.MOVETO,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.LINETO,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.CLOSEPOLY,
    ]
    return PathPatch(MplPath(verts, codes), facecolor=color, edgecolor="none", alpha=0.55)


def main() -> None:
    """Render the reclassification Sankey from the committed counts."""
    flows = _load_flows()
    left_totals: dict[str, int] = defaultdict(int)
    right_totals: dict[str, int] = defaultdict(int)
    for hier, score, tours in flows:
        left_totals[hier] += tours
        right_totals[score] += tours
    grand = sum(left_totals.values())

    left = _node_spans(left_totals, grand)
    right = _node_spans(right_totals, grand)

    n_nodes = len([p for p in ORDER if left_totals.get(p)])
    usable = 1.0 - GAP * (n_nodes - 1)

    fig, ax = plt.subplots(figsize=(9, 7))
    # Draw ribbons largest-first so thin flows stay visible on top.
    left_off = {p: span[1] for p, span in left.items()}
    right_off = {p: span[1] for p, span in right.items()}
    for hier, score, tours in sorted(flows, key=lambda r: -r[2]):
        if hier not in left or score not in right:
            continue
        h = tours / grand * usable
        ly = (left_off[hier] - h, left_off[hier])
        ry = (right_off[score] - h, right_off[score])
        left_off[hier] -= h
        right_off[score] -= h
        color = "#cccccc" if hier == score else COLOR.get(score, "#7f7f7f")
        ax.add_patch(_ribbon(BAR, 1 - BAR, ly, ry, color))

    # Node bars and labels.
    for purpose, (bottom, top) in left.items():
        ax.add_patch(
            plt.Rectangle((0, bottom), BAR, top - bottom, color=COLOR.get(purpose, "#555"))
        )
        ax.text(-0.01, (bottom + top) / 2, purpose, ha="right", va="center", fontsize=9)
    for purpose, (bottom, top) in right.items():
        ax.add_patch(
            plt.Rectangle((1 - BAR, bottom), BAR, top - bottom, color=COLOR.get(purpose, "#555"))
        )
        ax.text(1.01, (bottom + top) / 2, purpose, ha="left", va="center", fontsize=9)

    ax.set_xlim(-0.16, 1.16)
    ax.set_ylim(-0.02, 1.04)
    ax.axis("off")
    ax.text(BAR / 2, 1.03, "Hierarchy", ha="center", fontsize=11, fontweight="bold")
    ax.text(1 - BAR / 2, 1.03, "Scoring", ha="center", fontsize=11, fontweight="bold")
    ax.set_title("Tour purpose: hierarchy → scoring (BATS-2023)", fontsize=12, pad=18)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUTPUT, dpi=130, bbox_inches="tight")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
