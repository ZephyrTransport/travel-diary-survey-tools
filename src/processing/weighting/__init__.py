"""Survey weighting module.

This module provides two pipeline options for attaching expansion weights to
survey tables:

1. **``add_existing_weights``** -- load pre-computed weights from CSV files and
   join them to tables, optionally deriving missing weights by propagating
   values through the survey hierarchy.
2. **``compute_weights``** -- compute weights from scratch using PUMS / ACS
   microdata as population controls via maximum-entropy balancing.

The ``compute_weights`` step orchestrates the full pipeline — see
``compute_weights.py`` for the detailed step-by-step description.

# Weight hierarchy

Every weight is written once per profile, suffixed with its name
(``hh_weight_<profile>``, ``person_weight_<profile>``, ...):

    hh_weight
      └─ person_weight        (household's weight, shared among its usable persons)
          └─ person_day_weight        (split: a person's usable days sum to their weight)
              └─ unlinked_trip_weight  (day's weight, shared within the day)
                  ├─ linked_trip_weight   (mean agg via linked_trip_id)
                  │   └─ joint_trip_weight (SUM agg via joint_trip_id)
                  └─ tour_weight          (mean agg via tour_id)
                      └─ joint_tour_weight (SUM agg via joint_tour_id)

# Module structure

    weighting/
    ├── existing_weights.py       # attach pre-computed weights
    ├── existing/                 # its settings and redistribution onto usable records
    ├── compute_weights.py        # single @step() entry point for full weighting pipeline
    ├── core/                     # WeightingPipeline, the hierarchy, propagation, settings
    ├── controls/                 # control variable definitions & registry
    ├── data_prep/                # PUMS I/O, control totals, survey seed, geography
    ├── balancing/                # balancer, base weights, importance
    ├── diagnostics/              # interactive HTML report (Plotly + Jinja2)
    └── validation/               # post-balancing sanity checks
"""

from .compute_weights import compute_weights
from .existing_weights import add_existing_weights

__all__ = [
    "add_existing_weights",
    "compute_weights",
]
