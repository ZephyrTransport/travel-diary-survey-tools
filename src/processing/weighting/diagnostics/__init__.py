"""Diagnostics sub-package — HTML report generation for weighting results.

Produces a self-contained interactive HTML report (Plotly + Jinja2, no
external dependencies) with the following sections:

1. **Crosswalk Map** — geographic crosswalk visualization.
2. **Convergence & Weight Summary** — per-zone convergence status,
   weight sums, ESS%, and CV.
3. **Target Fit** — per-zone fit metrics (HH/person targets, MAPE,
   P90, Max error).
4. **Weight Distribution** — violin / jitter plots of
   ``final_weight / base_weight`` per zone, with summary statistics.
5. **Target Fit (% Error)** — diverging bar charts per zone.
6. **Unweighted Cell Counts (Data Sparsity)** — seed counts per
   control category per zone.

7. **Expansion Factor Calibration** — MAPE vs CV across a grid of
   ``max_expansion_factor`` values.  Enabled by setting
   ``expansion_factor_grid`` in the weighting config.

# Configuration (YAML)

```yaml
diagnostics:
  output_path: "{{ output_dir }}/weighting_diagnostics.html"
```

When `output_path` is omitted the report is, written to ``<cache_dir>/diagnostics.html``.
"""

from .charts import crosswalk_figure
from .report import generate_report

__all__ = ["crosswalk_figure", "generate_report"]
