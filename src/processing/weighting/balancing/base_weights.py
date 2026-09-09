"""Initial (base) expansion weights for the balancer.

Before balancing, each survey household needs a
starting weight that reflects its basic expansion factor — the number of
real-world households it represents.  Without meaningful initial weights,
the Newton-Raphson solver starts from ``1.0`` and must bridge the gap to
expansion factors of hundreds or thousands, quickly slamming into the
expansion-factor constraints.

This module provides two paths:

1. **Response inversion** (default) — ``target_hh_pop / n_responses``
   per zone.  Works whenever PUMS-derived control totals are available
   (always, in our pipeline).

2. **Sample plan** — a [`SamplePlan`][processing.weighting.balancing.base_weights.SamplePlan]
   object mapping Census block groups
   to sampling strata (segments).  Block-group populations are sourced
   from the crosswalk (Census block population summed to BG level).
   Each household is assigned a block group (via spatial join), mapped
   to a segment via the plan, and receives a segment-level initial
   weight: ``segment_pop / segment_responses``.

The public entry point is
[`compute_base_weights`][processing.weighting.balancing.base_weights.compute_base_weights],
which adds a ``base_weight`` column to the seed table.
"""

import logging
from pathlib import Path

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import CONTROLS
from processing.weighting.core.specs import ControlTotals, SamplePlan

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------
def load_sample_plan(path: str | Path) -> SamplePlan:
    """Read a sample-plan CSV into a [`SamplePlan`][processing.weighting.core.specs.SamplePlan].

    Args:
        path: Path to a CSV file.  Must contain at minimum ``bg_geo_id``
            and ``sample_segment``.  Block-group population totals are
            sourced from the crosswalk, not from the CSV.

    Returns:
        [`SamplePlan`][processing.weighting.core.specs.SamplePlan].

    Raises:
        FileNotFoundError: If *path* does not exist.
        ValueError: If required columns are missing (raised by
            ``SamplePlan``).
    """
    p = Path(path)
    if not p.exists():
        msg = f"Sample plan file not found: {p}"
        raise FileNotFoundError(msg)
    df = pl.read_csv(p, schema_overrides={"bg_geo_id": pl.Utf8})
    # Census block-group FIPS codes are 12 characters; zero-pad in case
    # the CSV was saved without preserving leading zeros.
    df = df.with_columns(pl.col("bg_geo_id").str.zfill(12))
    logger.info("Loaded sample plan from %s (%d rows)", p, len(df))
    return SamplePlan(strata=df)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def compute_base_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    geo_col: str = "ctrl_geoid",
    *,
    sample_plan: str | Path | SamplePlan | None = None,
    bg_populations: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Add ``base_weight`` column to the seed table.

    Args:
        seed: Household seed table from ``build_seed_table``.  Must
            contain ``hh_id`` and *geo_col*.  When using a sample plan,
            must also contain ``bg_geo_id`` (assigned via
            [`PumaCrosswalk.assign_block_groups`][processing.weighting.data_prep.crosswalk.PumaCrosswalk.assign_block_groups]).
        control_totals: PUMS-derived targets from
            ``build_control_totals``.
        targets: Control registry names (used to identify the master HH
            control).
        geo_col: Geography column on *seed*.
        sample_plan: Optional sample plan.  Accepts a file path (loaded
            via [`load_sample_plan`][processing.weighting.balancing.base_weights.load_sample_plan])
            or an already-loaded
            [`SamplePlan`][processing.weighting.core.specs.SamplePlan].
            When ``None``, default response inversion is used.
        bg_populations: Census block-group population totals with columns
            ``[bg_geo_id, bg_population]``.  Required when *sample_plan*
            is provided; sourced from
            [`PumaCrosswalk.block_group_populations`][processing.weighting.data_prep.crosswalk.PumaCrosswalk.block_group_populations].

    Returns:
        *seed* with an additional ``base_weight`` column (Float64).

    Raises:
        ValueError: If no household-level control is found in *targets*,
            or if a zone has zero survey responses.
    """
    if sample_plan is not None:
        plan = sample_plan if isinstance(sample_plan, SamplePlan) else load_sample_plan(sample_plan)
        if bg_populations is None:
            msg = (
                "bg_populations is required when using a sample plan. "
                "Pass PumaCrosswalk.block_group_populations from the crosswalk step."
            )
            raise ValueError(msg)
        hh_weights = _hh_weights_from_plan(seed, plan, bg_populations)
        return seed.join(hh_weights, on="hh_id", how="left")

    logger.info("Using PUMS-target response inversion for base weights")
    zone_weights = _zone_weights_from_response_inversion(seed, control_totals, targets, geo_col)

    return seed.join(
        zone_weights,
        left_on=pl.col(geo_col).cast(pl.Utf8),
        right_on="geo_id",
        how="left",
    )


# ---------------------------------------------------------------------------
# Strategy: response inversion (default)
# ---------------------------------------------------------------------------
def _zone_weights_from_response_inversion(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    geo_col: str,
) -> pl.DataFrame:
    """Return ``(geo_id, base_weight)`` via zone_target_hh_pop / zone_n_responses.

    Uses the first household-level control to derive the total-HH
    population target per zone — the same logic that ``_build_incidence``
    uses for the total control row.
    Total control var is just used as a reference point to get the zone-level population targets;
    the actual base weights are derived from the total HH population per zone,
    not the category-level targets.
    """
    hh_names = [t for t in targets if CONTROLS[t].level == ControlLevel.HOUSEHOLD]
    if not hh_names:
        msg = (
            f"Cannot compute base weights: no household-level control found in targets={targets!r}"
        )
        raise ValueError(msg)

    ref_var_name = hh_names[0]

    # Total HH population target per zone (sum across categories)
    zone_targets = (
        control_totals.totals.filter(pl.col("control_name") == ref_var_name)
        .group_by("geo_id")
        .agg(pl.col("target_total").sum().alias("_zone_target_hh"))
    )

    # Count survey responses per zone
    zone_counts = (
        seed.group_by(geo_col)
        .agg(pl.len().alias("_zone_n_responses"))
        .rename({geo_col: "geo_id"})
        .with_columns(pl.col("geo_id").cast(pl.Utf8))
    )

    # Join and derive base weight
    zone_weights = zone_targets.join(zone_counts, on="geo_id", how="inner")

    zeros = zone_weights.filter(pl.col("_zone_n_responses") == 0)
    if len(zeros) > 0:
        bad = zeros["geo_id"].to_list()
        msg = f"Zones with zero survey responses: {bad}"
        raise ValueError(msg)

    zone_weights = zone_weights.with_columns(
        (pl.col("_zone_target_hh") / pl.col("_zone_n_responses")).alias("base_weight")
    )

    # Per-zone and region-level diagnostics
    region_target = zone_weights["_zone_target_hh"].sum()
    region_responses = zone_weights["_zone_n_responses"].sum()
    region_weighted = (zone_weights["base_weight"] * zone_weights["_zone_n_responses"]).sum()

    zw = zone_weights.sort("geo_id").with_columns(
        (pl.col("base_weight") * pl.col("_zone_n_responses")).alias("_init_total")
    )
    zw_wide = pl.concat(
        [
            zw.select(
                "geo_id", "_zone_target_hh", "_zone_n_responses", "base_weight", "_init_total"
            ),
            pl.DataFrame(
                {
                    "geo_id": ["REGION"],
                    "_zone_target_hh": [region_target],
                    "_zone_n_responses": [region_responses],
                    "base_weight": [None],
                    "_init_total": [region_weighted],
                }
            ),
        ],
        how="diagonal_relaxed",
    ).rename(
        {
            "geo_id": "Zone",
            "_zone_target_hh": "Target HH",
            "_zone_n_responses": "Responses",
            "base_weight": "Base Wt",
            "_init_total": "Init Total",
        }
    )
    logger.info(
        "Base weights (response inversion, reference=%s):\n%s",
        ref_var_name,
        zw_wide,
    )

    zone_weights = zone_weights.select("geo_id", "base_weight")

    return zone_weights


# ---------------------------------------------------------------------------
# Strategy: explicit sample plan (block-group level)
# ---------------------------------------------------------------------------
def _hh_weights_from_plan(
    seed: pl.DataFrame,
    plan: SamplePlan,
    bg_populations: pl.DataFrame,
) -> pl.DataFrame:
    """Return ``(hh_id, base_weight)`` from a block-group-level sample plan.

    The sample plan maps Census block groups to sampling segments.
    Block-group populations come from the crosswalk (Census blocks
    summed to BG).  Each survey household must already have a
    ``bg_geo_id`` column (from
    [`PumaCrosswalk.assign_block_groups`][processing.weighting.data_prep.crosswalk.PumaCrosswalk.assign_block_groups]).

    Steps:

    1. Join BG populations onto the plan by ``bg_geo_id``.
    2. Sum BG populations per segment → segment target population.
    3. Map each HH to a segment via its ``bg_geo_id``.
    4. Count survey responses per segment.
    5. ``base_weight = segment_pop / segment_responses``.
    6. Return ``(hh_id, base_weight)``.
    """
    if "bg_geo_id" not in seed.columns:
        msg = (
            "Seed table is missing 'bg_geo_id' column. "
            "Call PumaCrosswalk.assign_block_groups() before computing base weights."
        )
        raise ValueError(msg)

    # BG → segment lookup from the plan
    bg_segment = plan.strata.select(
        pl.col("bg_geo_id").cast(pl.Utf8),
        pl.col("sample_segment").cast(pl.Utf8),
    )

    # Attach BG populations
    bg_with_pop = bg_segment.join(
        bg_populations.with_columns(pl.col("bg_geo_id").cast(pl.Utf8)),
        on="bg_geo_id",
        how="left",
    )
    missing_pop = bg_with_pop.filter(pl.col("bg_population").is_null())
    if len(missing_pop) > 0:
        bad = missing_pop["bg_geo_id"].to_list()[:10]
        msg = f"Block groups in sample plan have no Census population: {bad}"
        raise ValueError(msg)

    # Segment-level target population (sum BG pops within each segment)
    seg_pop = bg_with_pop.group_by("sample_segment").agg(
        pl.col("bg_population").sum().alias("_seg_target_pop")
    )

    # Map each HH to its segment via bg_geo_id
    seed_with_seg = seed.select("hh_id", "bg_geo_id").join(
        bg_segment,
        left_on=pl.col("bg_geo_id").cast(pl.Utf8),
        right_on="bg_geo_id",
        how="left",
    )

    unmatched = seed_with_seg.filter(pl.col("sample_segment").is_null())
    if len(unmatched) > 0:
        n = len(unmatched)
        logger.warning(
            "%d / %d households have bg_geo_id not in sample plan — they will get null base_weight",
            n,
            len(seed),
        )

    # Count actual survey responses per segment
    seg_counts = (
        seed_with_seg.filter(pl.col("sample_segment").is_not_null())
        .group_by("sample_segment")
        .agg(pl.len().alias("_seg_n_responses"))
    )

    # Derive weight per segment
    seg_weights = seg_pop.join(seg_counts, on="sample_segment", how="left")

    zeros = seg_weights.filter(
        pl.col("_seg_n_responses").is_null() | (pl.col("_seg_n_responses") == 0)
    )
    if len(zeros) > 0:
        bad = zeros["sample_segment"].to_list()
        msg = f"Sample plan segments with zero survey responses: {bad}"
        raise ValueError(msg)

    seg_weights = seg_weights.with_columns(
        (pl.col("_seg_target_pop") / pl.col("_seg_n_responses")).alias("base_weight")
    ).select("sample_segment", "base_weight")

    logger.info(
        "Base weights (sample plan): min=%.1f, mean=%.1f, median=%.1f, max=%.1f across %d segments",
        seg_weights["base_weight"].min(),
        seg_weights["base_weight"].mean(),
        seg_weights["base_weight"].median(),
        seg_weights["base_weight"].max(),
        len(seg_weights),
    )

    # HH → segment → weight
    return seed_with_seg.join(seg_weights, on="sample_segment", how="left").select(
        "hh_id", "base_weight"
    )
