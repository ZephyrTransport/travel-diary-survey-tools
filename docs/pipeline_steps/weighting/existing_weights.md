# Existing Weights

This path skips the compute-weighting pipeline and instead attaches externally produced weights to the canonical survey tables.

Use this path when you already have weights from another system, a prior weighting run, or an external model workflow.

Core behavior:

- load weight CSVs keyed to canonical IDs
- join the weights to the corresponding tables
- redistribute each weight onto the records the model can actually use, preserving the supplied total
- optionally derive missing downstream weights through the survey hierarchy
- validate that the provided weight set is internally consistent

## Supplied totals are preserved

Unlike the computed path, the anchor cannot be re-balanced here: the supplied weights already sum
to whoever produced them's population estimate, so dropping unusable records must not shrink that
total. Each weight is first conserved within its declared scope (see
[Balancing](balancing.md)); whatever that leaves stranded — scopes with no
usable record at all — is closed with a single table-wide factor, which is logged.

::: processing.weighting.existing_weights
    options:
      show_root_heading: true
      members:
        - ExistingWeightConfig
        - add_existing_weights
