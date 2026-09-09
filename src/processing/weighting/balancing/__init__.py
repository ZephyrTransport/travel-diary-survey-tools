"""Balancing sub-package (balancer, base weights, importance).

Orchestrates the core balancing loop:

1. **Base weights** ([`base_weights`][processing.weighting.balancing.base_weights]) --
   compute initial expansion factors per zone: ``target_hh_pop / n_responses``.
2. **Importance** ([`importance`][processing.weighting.balancing.importance]) --
   derive MOE-based per-control importance from PUMS replicate weights, with explicit
   YAML overrides.
3. **Balancer** ([`balancer`][processing.weighting.balancing.balancer]) --
   maximum-entropy list balancing via PopulationSim's ``np_balancer_numba``.
   Runs independently per geography zone.

Household weights leave here for [`processing.weighting.core.propagation`]
[processing.weighting.core.propagation], which carries them down the canonical
hierarchy.
"""
