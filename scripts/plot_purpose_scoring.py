"""Plot the tour-purpose scoring functions from the TourConfig defaults.

Regenerates ``docs/assets/images/purpose_scoring_functions.png``, the figure in
the Extract Tours documentation. The curves are read straight from
``TourConfig`` so the picture cannot drift from the actual defaults: rerun this
script after changing ``purpose_score_weights`` or ``purpose_score_halfmax``.

    uv run python scripts/plot_purpose_scoring.py
"""

import sys
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_canon.codebook.tours import PersonCategory
from data_canon.codebook.trips import PurposeCategory
from processing.tours.tour_configs import TourConfig

_REPO_ROOT = Path(__file__).parent.parent
OUTPUT = _REPO_ROOT / "docs" / "assets" / "images" / "purpose_scoring_functions.png"
MAX_DURATION_MIN = 480

# Purpose display: (label, style, colour). Solid = mandatory, dashed =
# discretionary, dotted = suppressed. Order controls legend order.
_DISPLAY: list[tuple[PurposeCategory, str, str, str]] = [
    (PurposeCategory.WORK, "-", "#1f77b4", "Work"),
    (PurposeCategory.SCHOOL, "-", "#17becf", "School"),
    (PurposeCategory.SHOP, "--", "#2ca02c", "Shop"),
    (PurposeCategory.MEAL, "--", "#ff7f0e", "Meal"),
    (PurposeCategory.ERRAND, "--", "#8c564b", "Errand"),
    (PurposeCategory.SOCIALREC, "--", "#9467bd", "Social/rec"),
    (PurposeCategory.ESCORT, ":", "#d62728", "Escort"),
    (PurposeCategory.OVERNIGHT, ":", "#7f7f7f", "Overnight"),
]


def main() -> None:
    """Render the scoring-function figure for the default worker weights."""
    config = TourConfig()
    weights = config.purpose_score_weights[PersonCategory.WORKER]
    halfmax = config.purpose_score_halfmax

    duration = np.linspace(0, MAX_DURATION_MIN, num=600)
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for purpose, style, colour, label in _DISPLAY:
        weight = weights[purpose]
        h = halfmax[purpose]
        score = weight * duration / (duration + h)
        ax.plot(
            duration,
            score,
            style,
            color=colour,
            linewidth=2,
            label=f"{label}  (W={weight:g}, h={h:g})",
        )

    ax.set_xlabel("activity duration at destination (min)")
    ax.set_ylabel("purpose score   W · x / (x + h)")
    ax.set_title("Tour purpose scoring functions (worker weights)")
    ax.set_xlim(0, MAX_DURATION_MIN)
    ax.set_ylim(0, max(weights.values()) * 1.05)
    ax.grid(alpha=0.3)
    ax.legend(loc="center right", fontsize=9, framealpha=0.95)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUTPUT, dpi=130)
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
