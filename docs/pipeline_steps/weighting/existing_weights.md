# Existing Weights

This path skips the compute-weighting pipeline and instead attaches externally produced weights to the canonical survey tables.

Use this path when you already have weights from another system, a prior weighting run, or an external model workflow.

Core behavior:

- load weight CSVs keyed to canonical IDs
- join the weights to the corresponding tables
- optionally derive missing downstream weights through the survey hierarchy
- validate that the provided weight set is internally consistent

::: processing.weighting.existing_weights
    options:
      show_root_heading: true
      members:
        - ExistingWeightConfig
        - add_existing_weights
