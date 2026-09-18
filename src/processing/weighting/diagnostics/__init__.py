"""Diagnostics sub-package — HTML report generation for weighting results.

Produces one self-contained interactive HTML report per *run* (Plotly + Jinja2,
no external dependencies), covering every profile the run fitted. The crosswalk
geometry, control totals and PUMS incidence are built once however many profiles
are fitted -- and on a regional run that geometry is the great majority of the
file -- so one document per run embeds them once rather than once per profile.

A sidebar outlines the document and holds the profile toggle. Every statistic is
defined, with its formula, in a Definitions section at the top (see
``glossary``), and abbreviated headers link to their entry. Each section shows a
one-line summary, with the longer guidance collapsed beneath it.

# Run-level sections, always visible

1. **Profile Comparison** — one row per fit: seed size, weight sums by level,
   ESS%, CV, max/median, MAPE and convergence. Two profiles matching exactly
   means two balancing runs produced one answer.
2. **Crosswalk Map** — geographic crosswalk visualization, with every profile's
   seed count in each zone's tooltip. Boundaries are simplified to a few metres.
3. **Weight Cascade** — per level, what each profile admitted and what carries
   weight, all profiles side by side; plus the redistribution each survivor
   absorbs, whether each weight set's lower levels were copied, redistributed
   or adjusted per record, the day-split identity, and control-geography
   coverage.
4. **Weight Set Comparer** — any two weight sets compared at any level: this
   run's fits against each other, or against weights supplied with the survey
   and named in ``diagnostics.compare_weights``. Run-level because a comparison
   names two profiles. Shows a scatter with a reduced-major-axis fit, the
   median, p90 and maximum fold difference, rank correlation and R-squared, and
   the spread of ln(A/B) by decile. No verdict is drawn: a comparison is not a
   validation, and the section says so above the plot.

# Per-profile sections, behind the profile toggle

5. **Fractional Seed Imputation** — per-control null rate in the seed incidence
   matrix and the quality of the PUMS-trained model that filled it. Shown only
   when something was filled. This describes the seed handed to the balancer,
   not the survey ``imputation`` pipeline step.
6. **Balancer Performance** — per-zone convergence status, target fit (MAPE,
   P90, Max), CV and ESS%.
7. **Weight Quality** — per-zone weight and expansion-factor statistics, with
   violin plots of ``final_weight / base_weight``.
8. **Expansion Factor Calibration** — MAPE vs CV across a grid of
   ``max_expansion_factor`` values. Enabled by setting ``expansion_factor_grid``
   in the weighting config.
9. **Target Fit (% Error)** — diverging bar charts per control category per
   zone, with PUMS replicate-weight whiskers.
10. **Unweighted Cell Counts (Data Sparsity)** — seed counts per control category
   per zone.

Each per-profile heading names its profile, so a screenshot cropped out of the
document still says which fit it describes.

Section numbers are assigned to the sections a run actually renders, so they run
consecutively from 1 whether or not the optional ones are present -- the numbers
above are a full run. A per-profile section is numbered once for the whole
document rather than per pane: the number is an address into the report, so a
profile that omits a section leaves a gap in its own pane rather than shifting
every heading below it when the reader presses the toggle.

# Configuration (YAML)

```yaml
diagnostics:
  output_path: "{{ output_dir }}/weighting_diagnostics.html"
  compare_weights:            # optional; weight sets supplied with the survey
    vendor:
      label: vendor
      columns: {households: hh_weight, unlinked_trips: trip_weight}
```

When `output_path` is omitted the report is written to
``<cache_dir>/diagnostics.html``. One file per run, with no profile suffix.
"""

from .charts import crosswalk_figure
from .report import generate_report

__all__ = ["crosswalk_figure", "generate_report"]
