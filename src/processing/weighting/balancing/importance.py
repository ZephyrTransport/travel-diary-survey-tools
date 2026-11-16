"""MOE-based importance weight calculation.

Uses PUMS successive-difference replicate weights (WGTP1-80 / PWGTP1-80)
to estimate the Margin of Error (MOE) for each weighted control total,
then converts to importance weights inversely proportional to the
coefficient of variation (CV).

Controls with higher sampling uncertainty (larger CV) receive *lower*
importance so the balancer doesn't chase noisy targets.  Controls absent
from the returned dict (e.g. structural totals whose MOE is meaningless)
fall back to the balancer's default importance.
"""

import logging

import numpy as np
import polars as pl

from processing.weighting.controls.base import (
    ControlLevel,
    ControlTarget,
    CrosstabControlTarget,
)
from processing.weighting.controls.registry import CONTROLS

logger = logging.getLogger(__name__)

DEFAULT_IMPORTANCE = 100.0

_N_REPLICATES = 80


def compute_control_moe(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    target_names: list[str],
) -> pl.DataFrame:
    """Compute per-cell MOE from PUMS replicate weights for all controls.

    Args:
        hh_df: Crosswalk-allocated PUMS households with ``_xw_WGTP`` and
            ``_xw_WGTP1`` … ``_xw_WGTP80`` columns (plus recoded
            controls).
        person_df: Crosswalk-allocated PUMS persons with ``_xw_PWGTP``
            and ``_xw_PWGTP1`` … ``_xw_PWGTP80`` columns (plus recoded
            controls).
        target_names: Control registry names to compute the MOE for.

    Returns:
        A DataFrame with columns ``[zone_group_id, control_name, category,
        target_total, se, moe_pct]``.  Controls with no PUMS records are
        skipped.

    Raises:
        ValueError: If a target name is not in the control registry, or
            if required replicate weight / control columns are missing
            from the data.
    """
    frames: list[pl.DataFrame] = []
    for name in target_names:
        ctrl = CONTROLS.get(name)
        if ctrl is None:
            msg = f"Unknown control {name!r} — not in registry"
            raise ValueError(msg)
        cell_df = _control_cell_moe(ctrl, hh_df, person_df)
        if cell_df is not None:
            frames.append(cell_df)

    if not frames:
        return pl.DataFrame(
            schema={
                "zone_group_id": pl.Utf8,
                "control_name": pl.Utf8,
                "category": pl.Utf8,
                "target_total": pl.Float64,
                "se": pl.Float64,
                "moe_pct": pl.Float64,
            }
        )
    return pl.concat(frames)


def compute_moe_importance(control_moe: pl.DataFrame, target_names: list[str]) -> dict[str, float]:
    """Compute per-control importance weights from the per-cell MOE.

    Args:
        control_moe: Per-cell MOE from
            [`compute_control_moe`][processing.weighting.balancing.importance.compute_control_moe].
        target_names: Control registry names to compute importance for.

    Returns:
        ``{control_name: importance}`` for controls where MOE could be
        computed.  Controls with no PUMS records (data sparsity) are
        omitted — the balancer will apply its default importance.
    """
    control_cvs: dict[str, float] = {}
    for name in target_names:
        cv = _control_cv(control_moe.filter(pl.col("control_name") == name))
        if cv is not None:
            control_cvs[name] = cv

    importance = _normalize_cvs(control_cvs)

    logger.info(
        "MOE importance: %d controls computed, median=%.1f",
        len(control_cvs),
        np.median(list(importance.values())) if importance else 0,
    )
    return importance


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _control_cell_moe(
    ctrl: ControlTarget,
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
) -> pl.DataFrame | None:
    """Compute per-cell SE and MOE% for one control across all zones.

    Returns a DataFrame with columns ``[zone_group_id, control_name, category,
    target_total, se, moe_pct]``, or ``None`` if no PUMS records exist.
    """
    if ctrl.level == ControlLevel.HOUSEHOLD:
        source, wt_col = hh_df, "_xw_WGTP"
    else:
        source, wt_col = person_df, "_xw_PWGTP"

    rep_cols = [f"{wt_col}{i}" for i in range(1, _N_REPLICATES + 1)]
    missing = [c for c in rep_cols if c not in source.columns]

    if missing:
        msg = f"Replicate weight columns missing for {ctrl.name}: {missing[:3]}..."
        raise ValueError(msg)

    # Cross-tab composite columns don't exist in raw PUMS, but we can
    # derive them from the dimension columns using pums_expr().
    if ctrl.name not in source.columns:
        if isinstance(ctrl, CrosstabControlTarget):
            source = source.with_columns(ctrl.pums_expr().alias(ctrl.name))
        else:
            msg = f"Control column {ctrl.name!r} not found in data"
            raise ValueError(msg)

    # Map int enum values → category string names
    value_to_name = {v: name.lower() for v, name in ctrl.valid_members}
    valid_values = list(value_to_name.keys())

    cells = (
        source.filter(pl.col(ctrl.name).is_in(valid_values))
        .group_by(["zone_group_id", ctrl.name])
        .agg([pl.col(wt_col).sum(), *(pl.col(c).sum() for c in rep_cols)])
    )
    if len(cells) == 0:
        return None

    estimates = cells[wt_col].to_numpy()
    rep_matrix = cells.select(rep_cols).to_numpy()  # (n_cells, 80)

    # Successive-difference variance, vectorised across all cells
    diff = rep_matrix - estimates[:, np.newaxis]
    se = np.sqrt((4.0 / _N_REPLICATES) * np.sum(diff**2, axis=1))

    # Build result DataFrame with string category names
    zone_group_ids = cells["zone_group_id"].to_list()
    cat_ints = cells[ctrl.name].to_list()
    cat_names = [value_to_name.get(v, str(v)) for v in cat_ints]
    moe_pct = np.where(estimates > 0, se / estimates * 100, 0.0)

    return pl.DataFrame(
        {
            "zone_group_id": zone_group_ids,
            "control_name": [ctrl.name] * len(zone_group_ids),
            "category": cat_names,
            "target_total": estimates.tolist(),
            "se": se.tolist(),
            "moe_pct": moe_pct.tolist(),
        }
    )


def _control_cv(cells: pl.DataFrame) -> float | None:
    """Compute a representative CV for one control across all zones.

    Args:
        cells: The control's rows of ``compute_control_moe``.

    Returns:
        The *median CV across cells* (zone x category), or ``None`` if no
        cell has a positive estimate.
    """
    targets = cells["target_total"].to_numpy()
    ses = cells["se"].to_numpy()
    mask = targets > 0
    if not mask.any():
        return None

    return float(np.median(ses[mask] / targets[mask]))


def _normalize_cvs(control_cvs: dict[str, float]) -> dict[str, float]:
    """Convert per-control CVs to importance weights.

    Lower CV → higher importance.  Normalized so that the median
    importance equals ``DEFAULT_IMPORTANCE``.

    Uses ``importance = 1 / sqrt(CV)`` to dampen extreme ratios.
    """
    if not control_cvs:
        return {}

    raw = {name: 1.0 / np.sqrt(max(cv, 1e-12)) for name, cv in control_cvs.items()}
    median_raw = float(np.median(list(raw.values())))
    scale = DEFAULT_IMPORTANCE / median_raw
    return {name: val * scale for name, val in raw.items()}
