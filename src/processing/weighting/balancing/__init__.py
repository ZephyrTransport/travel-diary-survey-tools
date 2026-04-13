"""Balancing sub-package (balancer, base weights, weight propagation).

Orchestrates the core balancing loop:

1. **Base weights** ([`base_weights`][processing.weighting.balancing.base_weights]) --
   compute initial expansion factors per zone: ``target_hh_pop / n_responses``.
2. **Importance** ([`importance`][processing.weighting.balancing.importance]) --
   derive MOE-based per-control importance from PUMS replicate weights, with explicit
   YAML overrides.
3. **Balancer** ([`balancer`][processing.weighting.balancing.balancer]) --
   maximum-entropy list balancing via PopulationSim's ``np_balancer_numba``.
   Runs independently per geography zone.
4. **Weight propagation** ([`weight_propagation`][processing.weighting.balancing.weight_propagation]) --
   carry final household weights down through the canonical table hierarchy.
"""  # noqa: E501
