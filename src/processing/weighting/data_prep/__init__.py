"""Data-preparation toolbox for the weighting workflow.

This sub-package does not orchestrate the weighting pipeline itself.
Instead, it provides the reusable building blocks that the weighting
pipeline uses to prepare geography, PUMS inputs, control totals, and
survey seed data.

# Modules

1. **Census geography** ([`census_geo`][processing.weighting.data_prep.census_geo]) --
   download and cache TIGER PUMA and block shapefiles via pygris, including block-level
   population inputs.
2. **Geography crosswalk** ([`crosswalk`][processing.weighting.data_prep.crosswalk]) --
   construct a population-weighted PUMA-to-target-zone allocation table and assign/allocate
   records across project geographies.
3. **PUMS data** ([`pums_data`][processing.weighting.data_prep.pums_data]) -- fetch
   ACS 1-year PUMS microdata from the Census API or load local extracts, with
   chunking and caching helpers.
4. **Control data** ([`control_data`][processing.weighting.data_prep.control_data]) --
   recode PUMS variables into weighting control categories and aggregate them into
   zone-level control totals.
5. **Seed data** ([`seed_data`][processing.weighting.data_prep.seed_data]) --
   recode canonical survey variables into the same control categories used by the
   PUMS-based controls.

Together these modules provide the shared data-preparation utilities used
by [`WeightingPipeline`][processing.weighting.core.pipeline.WeightingPipeline]
before balancing begins.
"""
