"""Shared machinery behind the two weighting entry points.

Neither module here is a pipeline step. The steps live one level up --
[`compute_weights`][processing.weighting.compute_weights] (balance from
controls) and [`add_existing_weights`][processing.weighting.existing_weights]
(attach supplied weights) -- and both are built from:

* [`hierarchy`][processing.weighting.core.hierarchy] -- the weight hierarchy,
  declared once.
* [`propagation`][processing.weighting.core.propagation] -- the walk that moves
  weights along it.
* [`specs`][processing.weighting.core.specs] -- configuration and result types.
* [`pipeline`][processing.weighting.core.pipeline] -- orchestration for the
  computed path.

Kept import-free so it cannot close a cycle with the sub-packages that read it.
"""
