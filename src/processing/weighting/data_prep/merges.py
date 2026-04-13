"""Category merging for incidence tables and control totals.

Three concerns:

1. **Crosstab merges** — :func:`apply_crosstab_merges` collapses
   composite ``{xtab}__{dim0}_{dim1}`` columns along one or more
   dimensions, dropping originals.  Applied before 1-D merges.
2. **1-D merges** — :func:`apply_1d_merges` handles both global
   (drop originals) and zone-specific (keep originals) merge specs
   based on ``spec.zones``.
3. **Control-total merges** — :func:`merge_control_totals` adjusts
   aggregated target rows for zone-specific merges.
"""

import logging
from itertools import product

import polars as pl

from processing.weighting.controls.registry import resolve_targets
from processing.weighting.specs import ControlTotals, MergeSpec

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
            if len(present) < 2:  # noqa: PLR2004
                continue
            merged_col = f"{spec.control}__{merged_label}"

            if spec.zones is None:
                # Global: sum + drop originals
                df = df.with_columns(
                    pl.sum_horizontal(*[pl.col(c) for c in present]).alias(merged_col)
                ).drop(present)
                logger.debug("Merged %s: %s → %s", spec.control, present, merged_col)
            elif merged_col not in df.columns:
                # Zone-specific: add column, keep originals
                df = df.with_columns(
                    pl.sum_horizontal(*[pl.col(c) for c in present]).alias(merged_col)
                )
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
                & pl.col("geo_id").is_in(spec.zones)
            )
            to_merge = totals.filter(is_match)
            if to_merge.is_empty() or len(to_merge) < 2:  # noqa: PLR2004
                continue
            keep = totals.filter(~is_match)
            merged_rows = (
                to_merge.group_by("geo_id")
                .agg(pl.col("target_total").sum())
                .with_columns(
                    pl.lit(spec.control).alias("control_name"),
                    pl.lit(merged_label.lower()).alias("category"),
                )
                .select("geo_id", "control_name", "category", "target_total")
            )
            totals = pl.concat([keep, merged_rows])
            logger.debug(
                "Merged control totals %s zones=%s: %s → %s",
                spec.control,
                spec.zones,
                member_names,
                merged_label,
            )

    return ControlTotals(
        totals=totals,
        pums_hh_count=control_totals.pums_hh_count,
        pums_person_count=control_totals.pums_person_count,
        geo_ids=control_totals.geo_ids,
    )


# ---------------------------------------------------------------------------
# Crosstab (N-D) merges
# ---------------------------------------------------------------------------


def apply_crosstab_merges(
    df: pl.DataFrame,
    merges: list[MergeSpec],
) -> pl.DataFrame:
    """Collapse composite incidence columns along crosstab dimensions.

    Each spec's ``groups`` is ``{dim_name: {merged_label: [sources]}}``.
    For each dimension merge, enumerates combinations of the other
    dimensions' current members, sums constituent columns, and drops
    originals.  Dimensions within one spec are processed sequentially
    so that a merge in one dimension sees already-merged columns from
    another.
    """
    for spec in merges:
        ctrl = resolve_targets([spec.control])[0]
        if not hasattr(ctrl, "dim_controls"):
            logger.warning(
                "Control '%s' is not a crosstab, skipping N-D merges",
                spec.control,
            )
            continue

        n_dims = len(ctrl.dim_controls)
        dim_name_to_idx = {dc.name: i for i, dc in enumerate(ctrl.dim_controls)}

        # Track current member names per dimension (start with originals)
        dim_members: list[list[str]] = [
            [name.lower() for _, name in dc.valid_members] for dc in ctrl.dim_controls
        ]

        prefix = f"{spec.control}__"

        for dim_name, dim_merge_groups in spec.groups.items():
            df = _apply_one_dim_merge(
                df, prefix, n_dims, dim_name_to_idx, dim_members, dim_name, dim_merge_groups
            )

    return df


def _apply_one_dim_merge(
    df: pl.DataFrame,
    prefix: str,
    n_dims: int,
    dim_name_to_idx: dict[str, int],
    dim_members: list[list[str]],
    dim_name: str,
    dim_merge_groups: dict[str, list[str]],
) -> pl.DataFrame:
    """Merge one dimension of a crosstab, updating *dim_members* in place."""
    dim_idx = dim_name_to_idx.get(dim_name)
    if dim_idx is None:
        logger.warning("Unknown dimension '%s' in crosstab merge, skipping", dim_name)
        return df

    other_indices = [i for i in range(n_dims) if i != dim_idx]
    other_members = [dim_members[i] for i in other_indices]

    for merged_label, source_members in dim_merge_groups.items():
        source_lower = [m.lower() for m in source_members]
        merged_lower = merged_label.lower()

        for other_combo in product(*other_members):
            src_cols = [
                _composite_col(prefix, n_dims, dim_idx, m, other_indices, other_combo)
                for m in source_lower
            ]
            target_col = _composite_col(
                prefix, n_dims, dim_idx, merged_lower, other_indices, other_combo
            )

            present = [c for c in src_cols if c in df.columns]
            if not present:
                continue

            df = df.with_columns(
                pl.sum_horizontal(*[pl.col(c) for c in present]).alias(target_col)
            ).drop(present)

        # Update tracked members: remove sources, append merged label
        removed = set(source_lower)
        dim_members[dim_idx] = [m for m in dim_members[dim_idx] if m not in removed] + [
            merged_lower
        ]

        logger.debug(
            "Crosstab merge dim %s: %s → %s",
            dim_name,
            source_lower,
            merged_lower,
        )
    return df


def _composite_col(
    prefix: str,
    n_dims: int,
    dim_idx: int,
    member: str,
    other_indices: list[int],
    other_combo: tuple[str, ...],
) -> str:
    """Build ``{prefix}{dim0_member}_{dim1_member}_...`` column name."""
    parts: list[str] = [""] * n_dims
    parts[dim_idx] = member
    for oi, oidx in enumerate(other_indices):
        parts[oidx] = other_combo[oi]
    return f"{prefix}{'_'.join(parts)}"
