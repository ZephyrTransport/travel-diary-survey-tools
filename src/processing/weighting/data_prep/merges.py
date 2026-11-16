"""Category merging for incidence tables and control totals.

Cross-tab merges are applied when the cross-tab is registered, so only 1-D
merges are handled here:

1. **1-D merges** — :func:`apply_1d_merges` handles both global
   (drop originals) and zone-specific (keep originals) merge specs
   based on ``spec.zones``.
2. **Control-total merges** — :func:`merge_control_totals` adjusts
   aggregated target rows for zone-specific merges.
"""

import logging
from dataclasses import replace

import polars as pl

from processing.weighting.core.specs import ControlTotals, MergeSpec

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1-D merges (global + zone-specific)
# ---------------------------------------------------------------------------


def apply_1d_merges(
    df: pl.DataFrame,
    merges: list[MergeSpec],
) -> pl.DataFrame:
    """Collapse or add incidence columns for 1-D merge specs.

    * **Global** (``zones=None``): sums constituent
      ``{ctrl}__{member}`` columns into ``{ctrl}__{merged_label}``
      and drops the originals.
    * **Zone-specific** (``zones`` set): adds the merged column but
      keeps the originals — non-merged zones still need them.  The
      per-zone selection happens in control totals, not here.
    """
    for spec in merges:
        for merged_label, base_members in spec.groups.items():
            src_cols = [f"{spec.control}__{m}" for m in base_members]
            present = [c for c in src_cols if c in df.columns]
            if not present:  # a group of one still merges, as a rename
                continue
            merged_col = f"{spec.control}__{merged_label}"
            merged = pl.sum_horizontal(*[pl.col(c) for c in present]).alias(merged_col)

            if spec.zones is None:
                # Global: sum + drop originals
                df = df.with_columns(merged).drop(present)
                logger.debug("Merged %s: %s → %s", spec.control, present, merged_col)
            elif merged_col not in df.columns:
                # Zone-specific: add column, keep originals
                df = df.with_columns(merged)
                logger.debug(
                    "Zone merge %s (zones %s): added %s",
                    spec.control,
                    spec.zones,
                    merged_col,
                )
    return df


def merge_control_totals(
    control_totals: ControlTotals,
    merges: list[MergeSpec],
) -> ControlTotals:
    """Collapse control-total rows for zone-specific merges.

    For each zone merge the constituent category rows are summed into a
    single merged row and the originals removed — but only for the
    specified zones.  Other zones keep their original rows.
    """
    totals = control_totals.totals
    for spec in merges:
        if spec.zones is None:
            continue
        for merged_label, base_members in spec.groups.items():
            member_names = [m.lower() for m in base_members]
            is_match = (
                (pl.col("control_name") == spec.control)
                & pl.col("category").is_in(member_names)
                & pl.col("zone_group_id").is_in(spec.zones)
            )
            to_merge = totals.filter(is_match)
            if to_merge.is_empty():  # a group of one still merges, as a rename
                continue
            keep = totals.filter(~is_match)
            merged_rows = (
                to_merge.group_by("zone_group_id")
                .agg(pl.col("target_total").sum())
                .with_columns(
                    pl.lit(spec.control).alias("control_name"),
                    pl.lit(merged_label.lower()).alias("category"),
                )
                .select("zone_group_id", "control_name", "category", "target_total")
            )
            totals = pl.concat([keep, merged_rows])
            logger.debug(
                "Merged control totals %s zones=%s: %s → %s",
                spec.control,
                spec.zones,
                member_names,
                merged_label,
            )

    return replace(control_totals, totals=totals)
