"""Control configuration and data validation.

Validates control configurations and cross-tab marginal consistency before
the balancer runs. These checks catch configuration errors early (e.g.,
too many categories, inconsistent marginals) rather than failing deep in
the balancing loop.

Functions:

``validate_total_control_categories``
    Validates total category count across all controls. Logs info at 100
    categories, raises error at 200. Prevents balancer performance issues
    from excessive categories.

``validate_crosstab_margins``
    Validates that cross-tab marginals sum to match 1D dimension control
    targets. Ensures joint distributions are consistent with marginals.
"""

import logging
from itertools import product

import polars as pl

from processing.weighting.controls.base import (
    INFO_TOTAL_CATEGORIES,
    MAX_TOTAL_CATEGORIES,
    ControlTarget,
    CrosstabControlTarget,
)
from processing.weighting.specs import ControlSpec

logger = logging.getLogger(__name__)


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
# Cross-tab margin validation
# ---------------------------------------------------------------------------


def validate_crosstab_margins(
    totals_df: pl.DataFrame,
    ctrl_instances: list[ControlTarget],
    specs: list[ControlSpec],
) -> None:
    """Validate that cross-tab marginals match 1D dimension control targets.

    For each cross-tab control, checks if any of its dimension controls are
    also present in the control set. If so, computes marginals by summing
    cross-tab cells for each dimension value and compares to 1D targets.

    Parameters
    ----------
    totals_df : pl.DataFrame
        Combined totals from all controls (geo_id, control_name, category, target_total).
    ctrl_instances : list[ControlTarget]
        List of ControlTarget instances.
    specs : list[ControlSpec]
        List of control specifications (for names).

    Raises:
    -------
    ValueError
        If cross-tab marginals don't match 1D dimension control targets within tolerance.
    """
    active_names = {spec.name for spec in specs}

    # Find cross-tab controls
    crosstab_ctrls = [ctrl for ctrl in ctrl_instances if isinstance(ctrl, CrosstabControlTarget)]

    for xtab in crosstab_ctrls:
        # Check which dimension controls are also active
        active_dims = [dim for dim in xtab.dim_controls if dim.name in active_names]

        if not active_dims:
            # No dimension controls active, nothing to validate
            continue

        logger.info(
            "Validating margins for crosstab '%s' against dimension controls: %s",
            xtab.name,
            [dim.name for dim in active_dims],
        )

        # Get cross-tab totals
        xtab_data = totals_df.filter(pl.col("control_name") == xtab.name)

        if len(xtab_data) == 0:
            # No data for this cross-tab (shouldn't happen but handle gracefully)
            logger.warning("No totals found for crosstab '%s'", xtab.name)
            continue

        # For each dimension control, validate marginals
        for dim_ctrl in active_dims:
            _validate_single_margin(xtab, dim_ctrl, xtab_data, totals_df)


def _validate_single_margin(
    xtab: CrosstabControlTarget,
    dim_ctrl: ControlTarget,
    xtab_data: pl.DataFrame,
    totals_df: pl.DataFrame,
) -> None:
    """Validate one dimension's marginals for a cross-tab control.

    Parameters
    ----------
    xtab : CrosstabControlTarget
        The cross-tab control instance.
    dim_ctrl : ControlTarget
        The dimension control to validate.
    xtab_data : pl.DataFrame
        Cross-tab totals (filtered to xtab.name).
    totals_df : pl.DataFrame
        All control totals.
    """
    # Get 1D dimension control totals
    dim_data = totals_df.filter(pl.col("control_name") == dim_ctrl.name)

    if len(dim_data) == 0:
        msg = f"Dimension control '{dim_ctrl.name}' has no totals"
        raise ValueError(msg)

    # Find dimension index in cross-tab
    try:
        dim_idx = xtab.dim_controls.index(dim_ctrl)
    except ValueError:
        msg = f"Dimension control '{dim_ctrl.name}' not found in crosstab '{xtab.name}'"
        raise ValueError(msg)  # noqa: B904

    # Build mapping: crosstab_category -> dimension_value
    # Cross-tab categories are sequential integers (0, 1, 2, ...)
    category_to_dim: dict[int, int] = {}
    for xtab_cat, member_combo in enumerate(product(*[c.valid_members for c in xtab.dim_controls])):
        dim_value = member_combo[dim_idx][0]  # (value, name) tuple
        category_to_dim[xtab_cat] = dim_value

    # Compute marginals: sum cross-tab totals by dimension value
    xtab_with_dim = xtab_data.with_columns(
        pl.col("category")
        .map_elements(category_to_dim.get, return_dtype=pl.Int16)
        .alias("dim_value")
    )

    xtab_marginals = (
        xtab_with_dim.group_by(["geo_id", "dim_value"])
        .agg(pl.col("target_total").sum().alias("xtab_marginal"))
        .sort(["geo_id", "dim_value"])
    )

    # Join with 1D dimension totals
    dim_totals_by_geo = (
        dim_data.rename({"category": "dim_value", "target_total": "dim_total"})
        .select(["geo_id", "dim_value", "dim_total"])
        .sort(["geo_id", "dim_value"])
    )

    comparison = xtab_marginals.join(dim_totals_by_geo, on=["geo_id", "dim_value"], how="outer")

    # Check for mismatches (allowing small floating-point tolerance)
    tolerance = 1e-2  # 0.01 tolerance for floating-point rounding
    comparison = comparison.with_columns(
        pl.coalesce("xtab_marginal", pl.lit(0.0)).alias("xtab_marginal"),
        pl.coalesce("dim_total", pl.lit(0.0)).alias("dim_total"),
    )

    comparison = comparison.with_columns(
        (pl.col("xtab_marginal") - pl.col("dim_total")).abs().alias("abs_diff")
    )

    mismatches = comparison.filter(pl.col("abs_diff") > tolerance)

    if len(mismatches) > 0:
        # Format error message
        mismatch_summary = mismatches.head(10).to_dicts()
        mismatch_str = "\n".join(
            f"  Zone {r['geo_id']}, dim_value {r['dim_value']}: "
            f"xtab={r['xtab_marginal']:.2f}, 1D={r['dim_total']:.2f}, "
            f"diff={r['abs_diff']:.2f}"
            for r in mismatch_summary
        )
        msg = (
            f"Crosstab '{xtab.name}' marginals don't match dimension control '{dim_ctrl.name}'.\n"
            f"This indicates a bug in the cross-tab computation or data inconsistency.\n"
            f"Mismatches (showing first 10 of {len(mismatches)}):\n{mismatch_str}"
        )
        raise ValueError(msg)

    logger.info(
        "✓ Crosstab '%s' marginals match dimension control '%s' (max diff: %.4f)",
        xtab.name,
        dim_ctrl.name,
        comparison["abs_diff"].max(),
    )


# ---------------------------------------------------------------------------
# Cross-tab sparsity check
# ---------------------------------------------------------------------------

_SPARSE_CELL_THRESHOLD = 30


def warn_crosstab_sparsity(
    seed_incidence: pl.DataFrame,
    ctrl_instances: list[ControlTarget],
    *,
    geo_col: str = "ctrl_geoid",
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
        Household-level incidence matrix with ``ctrl_geoid`` and
        binary ``{control}__{category}`` columns.
    ctrl_instances : list[ControlTarget]
        Resolved control instances (may include both 1-D and cross-tab).
    geo_col : str
        Geography column in the incidence table.
    threshold : int
        Minimum unweighted count per cell.  Cells below this trigger a
        warning log message.
    """
    crosstab_ctrls = [c for c in ctrl_instances if isinstance(c, CrosstabControlTarget)]
    if not crosstab_ctrls:
        return

    for ctrl in crosstab_ctrls:
        prefix = f"{ctrl.name}__"
        inc_cols = sorted(c for c in seed_incidence.columns if c.startswith(prefix))
        if not inc_cols:
            continue

        # Sum incidence per zone → unweighted cell counts
        counts = seed_incidence.group_by(geo_col).agg([pl.col(c).sum() for c in inc_cols])

        sparse_cells: list[str] = []
        for col in inc_cols:
            min_count = counts[col].min()
            if min_count is not None and min_count < threshold:
                cell_label = col[len(prefix) :]
                zones_below = counts.filter(pl.col(col) < threshold)[geo_col].to_list()
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
