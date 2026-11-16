"""Initial (base) expansion weights for the balancer.

Before balancing, each survey household needs a
starting weight that reflects its basic expansion factor — the number of
real-world households it represents.  Without meaningful initial weights,
the Newton-Raphson solver starts from ``1.0`` and must bridge the gap to
expansion factors of hundreds or thousands, quickly slamming into the
expansion-factor constraints.

This module provides two paths:

1. **Response inversion** (default) — the zone's ``h_total`` target
   divided by its responding households.  Works whenever PUMS-derived control totals are available
   (always, in our pipeline).

2. **Sample plan** — a [`SamplePlan`][processing.weighting.core.specs.SamplePlan]
   object mapping Census block groups
   to sampling strata (segments).  Household counts come from the
   decennial Census by block, grown to the PUMS year by PUMA
   ([`grow_block_households`][processing.weighting.balancing.base_weights.grow_block_households]).
   Each household is assigned a block group (via spatial join), mapped
   to a segment via the plan, and receives the initial weight of its
   segment x zone cell: ``cell_households / cell_responses``, so every
   zone's base weights add up to its households.

The public entry point is
[`compute_base_weights`][processing.weighting.balancing.base_weights.compute_base_weights],
which adds a ``base_weight`` column to the seed table.
"""

import logging
from pathlib import Path

import polars as pl

from processing.weighting.core.specs import ControlTotals, SamplePlan

logger = logging.getLogger(__name__)

# A block group's GEOID is the first 12 characters of its blocks' GEOIDs.
_BLOCK_GROUP_ID_LENGTH = 12


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------
def load_sample_plan(path: str | Path) -> SamplePlan:
    """Read a sample-plan CSV into a [`SamplePlan`][processing.weighting.core.specs.SamplePlan].

    Args:
        path: Path to a CSV file.  Must contain at minimum ``bg_geo_id``
            and ``sample_segment``.  Block-group household totals come
            from the decennial Census, not from the CSV.

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
    df = df.with_columns(pl.col("bg_geo_id").str.zfill(_BLOCK_GROUP_ID_LENGTH))
    logger.info("Loaded sample plan from %s (%d rows)", p, len(df))
    return SamplePlan(strata=df)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def compute_base_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    *,
    sample_plan: str | Path | SamplePlan | None = None,
    block_households: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Add ``base_weight`` column to the seed table.

    Args:
        seed: Household seed table from ``build_incidence_table``.  Must
            contain ``hh_id`` and ``zone_group_id``.  When using a sample plan,
            must also contain ``bg_geo_id`` (assigned via
            [`PumaCrosswalk.assign_block_groups`][processing.weighting.data_prep.crosswalk.PumaCrosswalk.assign_block_groups]).
        control_totals: PUMS-derived targets from
            ``aggregate_control_totals``; response inversion reads ``h_total``.
        sample_plan: Optional sample plan.  Accepts a file path (loaded
            via [`load_sample_plan`][processing.weighting.balancing.base_weights.load_sample_plan])
            or an already-loaded
            [`SamplePlan`][processing.weighting.core.specs.SamplePlan].
            When ``None``, default response inversion is used.
        block_households: Census households per block, grown to the PUMS year,
            with columns ``[block_id, zone_group_id, households]``. Required when
            *sample_plan* is provided; built by
            [`grow_block_households`][processing.weighting.balancing.base_weights.grow_block_households].

    Returns:
        *seed* with an additional ``base_weight`` column (Float64).

    Raises:
        ValueError: If *control_totals* has no ``h_total`` target, or if a zone,
            or a segment within a zone, has zero survey responses.
    """
    if sample_plan is not None:
        plan = sample_plan if isinstance(sample_plan, SamplePlan) else load_sample_plan(sample_plan)
        if block_households is None:
            msg = (
                "block_households is required when using a sample plan. "
                "Pass the result of grow_block_households."
            )
            raise ValueError(msg)
        hh_weights = _hh_weights_from_plan(seed, plan, block_households)
        return seed.join(hh_weights, on="hh_id", how="left")

    logger.info("Using PUMS-target response inversion for base weights")
    zone_weights = _zone_weights_from_response_inversion(seed, control_totals)

    return seed.join(
        zone_weights,
        left_on=pl.col("zone_group_id").cast(pl.Utf8),
        right_on="zone_group_id",
        how="left",
    )


# ---------------------------------------------------------------------------
# Strategy: response inversion (default)
# ---------------------------------------------------------------------------
def _zone_weights_from_response_inversion(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
) -> pl.DataFrame:
    """Return ``(zone_group_id, base_weight)``: each zone's ``h_total`` over its responses."""
    h_total = control_totals.totals.filter(pl.col("control_name") == "h_total")
    if h_total.is_empty():
        msg = "Cannot compute base weights: the control totals have no h_total target"
        raise ValueError(msg)

    zone_targets = h_total.group_by("zone_group_id").agg(
        pl.col("target_total").sum().alias("_zone_target_hh")
    )

    # Count survey responses per zone
    zone_counts = (
        seed.group_by("zone_group_id")
        .agg(pl.len().alias("_zone_n_responses"))
        .with_columns(pl.col("zone_group_id").cast(pl.Utf8))
    )

    # Join and derive base weight
    zone_weights = zone_targets.join(zone_counts, on="zone_group_id", how="inner")

    zeros = zone_weights.filter(pl.col("_zone_n_responses") == 0)
    if len(zeros) > 0:
        bad = zeros["zone_group_id"].to_list()
        msg = f"Zones with zero survey responses: {bad}"
        raise ValueError(msg)

    zone_weights = zone_weights.with_columns(
        (pl.col("_zone_target_hh") / pl.col("_zone_n_responses")).alias("base_weight")
    )

    # Per-zone and region-level diagnostics
    region_target = zone_weights["_zone_target_hh"].sum()
    region_responses = zone_weights["_zone_n_responses"].sum()
    region_weighted = (zone_weights["base_weight"] * zone_weights["_zone_n_responses"]).sum()

    zw = zone_weights.sort("zone_group_id").with_columns(
        (pl.col("base_weight") * pl.col("_zone_n_responses")).alias("_init_total")
    )
    zw_wide = pl.concat(
        [
            zw.select(
                "zone_group_id",
                "_zone_target_hh",
                "_zone_n_responses",
                "base_weight",
                "_init_total",
            ),
            pl.DataFrame(
                {
                    "zone_group_id": ["REGION"],
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
            "zone_group_id": "Zone",
            "_zone_target_hh": "Target HH",
            "_zone_n_responses": "Responses",
            "base_weight": "Base Wt",
            "_init_total": "Init Total",
        }
    )
    logger.info("Base weights (response inversion, from h_total):\n%s", zw_wide)

    return zone_weights.select("zone_group_id", "base_weight")


# ---------------------------------------------------------------------------
# Strategy: explicit sample plan (block-group level)
# ---------------------------------------------------------------------------
def grow_block_households(
    blocks: pl.DataFrame,
    pums_households: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Grow each block's decennial Census households to the PUMS year, by PUMA.

    The Census counts households by block but only every ten years; the PUMS
    counts them for the survey year but only by PUMA. Blocks nest in PUMAs, so
    each PUMA's ``growth = PUMS households / Census households`` scales its blocks.

    Args:
        blocks: One row per block, with ``block_id``, ``puma_id``, ``zone_group_id``
            (null outside the zone groups) and ``households`` (Census count).
        pums_households: PUMS households with ``PUMA`` and ``WGTP``, weighted as
            the household targets are.

    Returns:
        ``(grown, growth)``: *grown* has ``[block_id, zone_group_id, households]``
        for blocks inside the zones, counts grown; *growth* has one row per
        PUMA holding such a block, with ``[puma_id, census_households,
        pums_households, growth]``.

    Raises:
        ValueError: A PUMA holding zone blocks has no PUMS households.
    """
    census = blocks.group_by("puma_id").agg(pl.col("households").sum().alias("census_households"))
    pums = pums_households.group_by(pl.col("PUMA").cast(pl.Utf8).alias("puma_id")).agg(
        pl.col("WGTP").sum().cast(pl.Float64).alias("pums_households")
    )
    in_zones = blocks.filter(pl.col("zone_group_id").is_not_null())
    growth = (
        census.filter(pl.col("puma_id").is_in(in_zones["puma_id"].unique().implode()))
        .join(pums, on="puma_id", how="left")
        .with_columns((pl.col("pums_households") / pl.col("census_households")).alias("growth"))
        .sort("puma_id")
    )
    missing = growth.filter(pl.col("pums_households").is_null())["puma_id"].to_list()
    if missing:
        msg = f"PUMAs inside the zone groups have no PUMS households: {missing}"
        raise ValueError(msg)

    grown = in_zones.join(growth.select("puma_id", "growth"), on="puma_id", how="left").select(
        "block_id",
        pl.col("zone_group_id").cast(pl.Utf8),
        (pl.col("households") * pl.col("growth").fill_null(0.0)).alias("households"),
    )
    logger.info(
        "Census households grown to the PUMS year across %d PUMAs: growth min=%.3f, "
        "median=%.3f, max=%.3f",
        len(growth),
        growth["growth"].min(),
        growth["growth"].median(),
        growth["growth"].max(),
    )
    return grown, growth


def _hh_weights_from_plan(
    seed: pl.DataFrame,
    plan: SamplePlan,
    block_households: pl.DataFrame,
) -> pl.DataFrame:
    """Return ``(hh_id, base_weight)`` from a block-group-level sample plan.

    The sample plan maps Census block groups to sampling segments. A segment
    can span zone groups, so the stratum is segment x zone: each cell's
    households are spread over that cell's responses, and every zone's base
    weights add up to its households. Each survey household must already have
    a ``bg_geo_id`` column (from
    [`PumaCrosswalk.assign_block_groups`][processing.weighting.data_prep.crosswalk.PumaCrosswalk.assign_block_groups]).

    Steps:

    1. Map each block to a segment via its block group.
    2. Sum block households per segment x zone.
    3. Map each HH to a segment via its ``bg_geo_id``.
    4. Count survey responses per segment x zone.
    5. ``base_weight = cell_households / cell_responses``.
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
    cell = ["sample_segment", "zone_group_id"]

    # Households per segment x zone; blocks outside the plan belong to no cell
    cell_households = (
        block_households.with_columns(
            pl.col("block_id").str.slice(0, _BLOCK_GROUP_ID_LENGTH).alias("bg_geo_id"),
            pl.col("zone_group_id").cast(pl.Utf8),
        )
        .join(bg_segment, on="bg_geo_id", how="inner")
        .group_by(cell)
        .agg(pl.col("households").sum().alias("_cell_households"))
    )

    # Map each HH to its segment via bg_geo_id
    seed_with_seg = seed.select("hh_id", "bg_geo_id", pl.col("zone_group_id").cast(pl.Utf8)).join(
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

    # Count actual survey responses per segment x zone
    cell_counts = (
        seed_with_seg.filter(pl.col("sample_segment").is_not_null())
        .group_by(cell)
        .agg(pl.len().alias("_cell_n_responses"))
    )

    # Derive weight per cell; a cell with households but no responses, or
    # responses but no households, cannot be weighted
    cell_weights = cell_households.join(cell_counts, on=cell, how="full", coalesce=True)
    bad = cell_weights.filter(
        pl.col("_cell_n_responses").is_null() | (pl.col("_cell_households").fill_null(0) <= 0)
    )
    if len(bad) > 0:
        rows = bad.sort(cell).select(*cell, "_cell_households", "_cell_n_responses").rows()
        msg = (
            f"{len(rows)} sample plan segments have zero survey responses, or responses but "
            "no Census households, within a zone. First 10 (segment, zone, households, "
            f"responses): {rows[:10]}"
        )
        raise ValueError(msg)

    cell_weights = cell_weights.with_columns(
        (pl.col("_cell_households") / pl.col("_cell_n_responses")).alias("base_weight")
    ).select(*cell, "base_weight")

    logger.info(
        "Base weights (sample plan): min=%.1f, mean=%.1f, median=%.1f, max=%.1f across "
        "%d segment x zone cells",
        cell_weights["base_weight"].min(),
        cell_weights["base_weight"].mean(),
        cell_weights["base_weight"].median(),
        cell_weights["base_weight"].max(),
        len(cell_weights),
    )

    # HH → cell → weight
    return seed_with_seg.join(cell_weights, on=cell, how="left").select("hh_id", "base_weight")
