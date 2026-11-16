"""Control configuration and data validation.

Validates control configurations before the balancer runs, and warns on sparse
cross-tab cells in the seed. These checks catch configuration errors early
(e.g., too many categories) rather than failing deep in the balancing loop.

Functions:

``validate_total_control_categories``
    Validates total category count across all controls. Warns above 100
    categories, raises error above 200. Prevents balancer performance issues
    from excessive categories.

``warn_crosstab_sparsity``
    Warns on cross-tab cells with few unweighted seed records in some zone.
"""

import logging

import polars as pl

from processing.weighting.controls.base import (
    INFO_TOTAL_CATEGORIES,
    MAX_TOTAL_CATEGORIES,
    ControlTarget,
    CrosstabControlTarget,
)

logger = logging.getLogger(__name__)

_SPARSE_CELL_THRESHOLD = 30


# ---------------------------------------------------------------------------
# Control configuration validation
# ---------------------------------------------------------------------------


def validate_total_control_categories(controls: list[ControlTarget]) -> None:
    """Validate total category count across all controls.

    Counts the total number of valid (non-sentinel) categories across all
    controls and logs a breakdown. Logs a warning if count exceeds
    INFO_TOTAL_CATEGORIES (100) or raises an error if count exceeds
    MAX_TOTAL_CATEGORIES (200).

    Parameters
    ----------
    controls : list[ControlTarget]
        List of control instances to validate.

    Raises:
    -------
    ValueError
        If total category count exceeds MAX_TOTAL_CATEGORIES.
    """
    # Count categories per control
    control_sizes = [(ctrl.name, len(ctrl.valid_members)) for ctrl in controls]
    total_categories = sum(size for _, size in control_sizes)

    # Always log breakdown
    control_sizes.sort(key=lambda x: x[1], reverse=True)
    breakdown = "\n".join(f"  - {name}: {size}" for name, size in control_sizes)
    logger.info("Total control categories: %d cells\n%s", total_categories, breakdown)

    # Error if exceeds hard limit
    if total_categories > MAX_TOTAL_CATEGORIES:
        msg = (
            f"Total category count ({total_categories}) exceeds maximum ({MAX_TOTAL_CATEGORIES}).\n"
            f"This will cause balancer performance issues and potentially poor convergence.\n"
            f"Solution: Pre-merge categories for cross-tabs or remove controls."
        )
        raise ValueError(msg)

    # Warn if exceeds soft threshold
    if total_categories > INFO_TOTAL_CATEGORIES:
        logger.warning(
            "Category count (%d) exceeds recommended threshold (%d). "
            "Monitor balancer performance in small zones.",
            total_categories,
            INFO_TOTAL_CATEGORIES,
        )


# ---------------------------------------------------------------------------
# Cross-tab sparsity check
# ---------------------------------------------------------------------------


def warn_crosstab_sparsity(
    seed_incidence: pl.DataFrame,
    ctrl_instances: list[ControlTarget],
    *,
    threshold: int = _SPARSE_CELL_THRESHOLD,
) -> None:
    """Log warnings for cross-tab cells with low unweighted sample counts.

    For each cross-tab control, sums the binary incidence columns per
    zone and reports any cells below *threshold*.  This catches data
    sparsity that may cause the balancer to produce extreme weights.

    Only cross-tab controls are checked — 1-D controls are already
    covered by the diagnostics sparsity table.

    Parameters
    ----------
    seed_incidence : pl.DataFrame
        Household-level incidence matrix with ``zone_group_id`` and
        binary ``{control}__{category}`` columns.
    ctrl_instances : list[ControlTarget]
        Resolved control instances (may include both 1-D and cross-tab).
    threshold : int
        Minimum unweighted count per cell.  Cells below this trigger a
        warning log message.
    """
    for ctrl in (c for c in ctrl_instances if isinstance(c, CrosstabControlTarget)):
        prefix = f"{ctrl.name}__"
        inc_cols = sorted(c for c in seed_incidence.columns if c.startswith(prefix))
        if not inc_cols:
            continue

        # Sum incidence per zone → unweighted cell counts
        counts = seed_incidence.group_by("zone_group_id").agg([pl.col(c).sum() for c in inc_cols])

        sparse_cells: list[str] = []
        for col in inc_cols:
            min_count = counts[col].min()
            if min_count is not None and min_count < threshold:
                cell_label = col[len(prefix) :]
                zones_below = counts.filter(pl.col(col) < threshold)["zone_group_id"].to_list()
                sparse_cells.append(
                    f"  {cell_label}: min={min_count} "
                    f"({len(zones_below)} zone(s) below {threshold})"
                )

        if sparse_cells:
            logger.warning(
                "Cross-tab '%s' has sparse cells (<%d unweighted records):\n%s\n"
                "Consider pre-merging categories to reduce sparsity.",
                ctrl.name,
                threshold,
                "\n".join(sparse_cells),
            )
