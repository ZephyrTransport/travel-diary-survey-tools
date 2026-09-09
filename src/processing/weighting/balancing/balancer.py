r"""Maximum-entropy list balancer.

Thin Polars→numpy bridge around PopulationSim's ``np_balancer_numba``.
Runs independently per geography zone.

# Algorithm

Find weight vector **w** closest to seed weights **w₀** (KL-divergence)
subject to marginal constraints:

$$
\min \sum_i w_i \ln(w_i / w_{0i})
\quad \text{s.t.} \quad Aw = t,\; w_i \ge 0
$$

where **A** is the incidence matrix and **t** is the target totals
vector.

# Implementation

Calls ``populationsim.balancing.balancers_numba.np_balancer_numba``
directly — a pure ``@njit`` function (~120 lines) taking numpy arrays.
No PopulationSim pipeline infrastructure involved.  Zones are
independent and parallelisable via ``ThreadPoolExecutor``.

# Configuration (YAML)

```yaml
max_iterations: 1000
convergence_threshold: 0.001
max_expansion_factor: 10    # upper bound = initial_weight x factor
min_expansion_factor: 0.1   # lower bound = initial_weight x factor
```
"""

import logging
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import polars as pl

from processing.weighting.balancing._np_balancer import np_balancer_numba
from processing.weighting.controls.registry import CONTROLS
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlTotals,
    GridPoint,
    ImportanceConfig,
    ZoneInput,
    ZoneResult,
    ZoneStatus,
)
from processing.weighting.diagnostics.data import compute_weighted_totals, fit_table

logger = logging.getLogger(__name__)


# -- Public API ------------------------------------------------------------


def balance_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    balancing: BalancingConfig | None = None,
    importance: ImportanceConfig | None = None,
    *,
    verbose: bool = True,
) -> tuple[pl.DataFrame, list[ZoneStatus]]:
    """Balance household weights to match control totals per zone.

    Args:
        seed: Incidence table with ``hh_id``, ``ctrl_geoid``,
            ``base_weight``, and pivoted control columns
            (``{ctrl}__{member}`` or structural).  All merges (global
            and zone-specific) must already be applied.
        control_totals: Per-zone targets (with merges already applied).
        targets: Control registry names.
        balancing: Solver bounds, iteration limits, and parallelism
            (defaults apply).
        importance: Per-control importance weights (defaults apply).
        verbose: Log per-zone convergence (default ``True``).

    Returns:
        Tuple of ``(weights, statuses)`` where *weights* is a DataFrame
        with columns ``hh_id``, ``hh_weight``, ``geo_id``, and
        *statuses* is one [`ZoneStatus`][processing.weighting.core.specs.ZoneStatus]
        entry per zone with convergence info.
    """
    bal = balancing or BalancingConfig()
    imp = importance or ImportanceConfig()
    geo_col = "ctrl_geoid"

    # Build per-zone inputs
    zone_inputs: list[ZoneInput] = []
    for geo_id in control_totals.geo_ids:
        zone_seed = seed.filter(pl.col(geo_col).cast(pl.Utf8) == geo_id)
        if len(zone_seed) == 0:
            logger.warning("Zone %s: no seed records, skipping", geo_id)
            continue

        zone_totals = control_totals.totals.filter(pl.col("geo_id") == geo_id)
        zone_inputs.append(
            _prepare_zone(
                zone_seed,
                zone_totals,
                targets,
                geo_id,
                importance=imp.explicit or None,
                default_importance=imp.default,
                min_expansion_factor=bal.min_expansion_factor,
                max_expansion_factor=bal.max_expansion_factor,
                min_weight=bal.min_weight,
                max_weight=bal.max_weight,
                max_iterations=bal.max_iterations,
                verbose=verbose,
            )
        )

    # Run balancer — numba @njit releases the GIL so threads parallelize
    if bal.n_workers > 1 and len(zone_inputs) > 1:
        with ThreadPoolExecutor(max_workers=bal.n_workers) as pool:
            results = list(pool.map(_balance_zone, zone_inputs))
    else:
        results = [_balance_zone(z) for z in zone_inputs]

    # Assemble output
    weight_frames = [r.weights for r in results]
    statuses = [r.status for r in results]

    weights = (
        pl.concat(weight_frames)
        if weight_frames
        else pl.DataFrame(schema={"hh_id": pl.Int64, "hh_weight": pl.Float64, "geo_id": pl.Utf8})
    )

    n_fail = sum(not s.converged for s in statuses)
    if verbose:
        logger.info("Balancing: %d zones, %d failed", len(statuses), n_fail)
    return weights, statuses


def grid_search_expansion_factor(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    ef_grid: list[float],
    selected_ef: float,
    balancing: BalancingConfig | None = None,
    importance: ImportanceConfig | None = None,
) -> list[GridPoint]:
    """Re-run the balancer across a grid of ``max_expansion_factor`` values.

    Returns one [`GridPoint`][processing.weighting.core.specs.GridPoint]
    per EF value with aggregate fit and weight-quality metrics.
    The *selected_ef* is automatically included in the grid if not already present.

    This is a **diagnostics-only** function — it does not modify the
    production weights.
    """
    bal = balancing or BalancingConfig()
    grid = sorted(set(ef_grid) | {selected_ef})
    if not grid:
        return []

    results: list[GridPoint] = []
    for i, ef in enumerate(grid, 1):
        logger.info("Grid search: EF=%.1f (%d/%d)", ef, i, len(grid))
        ef_bal = BalancingConfig(
            max_expansion_factor=ef,
            min_expansion_factor=bal.min_expansion_factor,
            min_weight=bal.min_weight,
            max_weight=bal.max_weight,
            max_iterations=bal.max_iterations,
            n_workers=bal.n_workers,
        )
        weights, statuses = balance_weights(
            seed,
            control_totals,
            targets,
            balancing=ef_bal,
            importance=importance,
            verbose=False,
        )

        # -- Fit metrics (MAPE, P90) -----------------------------------
        wt = compute_weighted_totals(seed, weights, targets)
        ft = fit_table(control_totals, wt)
        abs_pct = ft["diff_pct"].abs()
        mape = float(abs_pct.mean() or 0) if len(abs_pct) > 0 else 0.0  # pyright: ignore[reportArgumentType]
        p90 = float(abs_pct.quantile(0.9) or 0) if len(abs_pct) > 0 else 0.0
        max_error = float(abs_pct.max() or 0) if len(abs_pct) > 0 else 0.0  # pyright: ignore[reportArgumentType]

        # -- Weight quality (CV, ESS%) ---------------------------------
        w = weights["hh_weight"]
        n = len(w)
        if n > 0:
            mean_w = float(w.mean() or 0)  # pyright: ignore[reportArgumentType]
            std_w = float(w.std() or 0)  # pyright: ignore[reportArgumentType]
            cv = std_w / mean_w if mean_w > 0 else 0.0
            sum_w = float(w.sum())
            sum_w2 = float((w * w).sum())
            ess_pct = (sum_w**2 / (n * sum_w2) * 100) if sum_w2 > 0 else 0.0
        else:
            cv, ess_pct = 0.0, 0.0

        results.append(
            GridPoint(
                max_expansion_factor=ef,
                converged_zones=sum(s.converged for s in statuses),
                total_zones=len(statuses),
                mape=mape,
                p90=p90,
                max_error=max_error,
                cv=cv,
                ess_pct=ess_pct,
            )
        )

    return results


# -- Zone preparation -------------------------------------------------------


def _prepare_zone(
    zone_seed: pl.DataFrame,
    zone_totals: pl.DataFrame,
    targets: list[str],
    geo_id: str,
    *,
    importance: dict[str, float] | None,
    default_importance: float,
    min_expansion_factor: float,
    max_expansion_factor: float,
    min_weight: float | None,
    max_weight: float | None,
    max_iterations: int,
    verbose: bool,
) -> ZoneInput:
    """Build ``ZoneInput`` arrays for a single geography zone."""
    incidence, ctrl_targets, master_idx, row_labels = _build_incidence(
        zone_seed,
        zone_totals,
        targets,
    )

    # Importance: default for every row, then apply overrides
    overrides = importance or {}
    imp_vec = np.full(len(row_labels), default_importance, dtype=np.float64)
    for i, (ctrl_name, _member) in enumerate(row_labels):
        if ctrl_name in overrides:
            imp_vec[i] = overrides[ctrl_name]

    if "base_weight" not in zone_seed.columns:
        msg = (
            "Seed table missing 'base_weight' column. "
            "Run compute_base_weights() before balance_weights()."
        )
        raise ValueError(msg)
    initial = zone_seed["base_weight"].to_numpy().astype(np.float64)

    # Bounds: expansion-factor scaling, then optional absolute clipping
    total = initial.sum()
    ratio = float(ctrl_targets[master_idx]) / total if total > 0 else 1.0
    lb = np.maximum(initial * min_expansion_factor * ratio, 0.0)
    ub = np.maximum(initial * max_expansion_factor * ratio, 1.0)
    if min_weight is not None:
        lb = np.maximum(lb, min_weight)
    if max_weight is not None:
        ub = np.minimum(ub, max_weight)

    return ZoneInput(
        hh_ids=zone_seed["hh_id"],
        incidence=incidence,
        initial=initial,
        lb=lb,
        ub=ub,
        targets=ctrl_targets,
        importance=imp_vec,
        master_idx=master_idx,
        max_iterations=max_iterations,
        geo_id=geo_id,
        verbose=verbose,
    )


# -- Balancing --------------------------------------------------------------


def _balance_zone(zone: ZoneInput) -> ZoneResult:
    """Run ``np_balancer_numba`` for a single zone and return results."""
    n = len(zone.initial)

    w_final, _relax, status = np_balancer_numba(
        sample_count=n,
        control_count=zone.incidence.shape[0],
        master_control_index=zone.master_idx,
        incidence=zone.incidence,
        weights_initial=zone.initial,
        weights_lower_bound=zone.lb,
        weights_upper_bound=zone.ub,
        controls_constraint=zone.targets,
        controls_importance=zone.importance,
        max_iterations=zone.max_iterations,
    )

    converged, iters, delta, gamma = status
    zs = ZoneStatus(zone.geo_id, bool(converged), int(iters), float(delta), float(gamma))

    if zone.verbose:
        level = logging.INFO if converged else logging.WARNING
        logger.log(
            level,
            "Zone %s: %s in %d iters (delta=%.2e)",
            zone.geo_id,
            "converged" if converged else "NOT CONVERGED",
            iters,
            delta,
        )

    frame = pl.DataFrame(
        {
            "hh_id": zone.hh_ids,
            "hh_weight": w_final,
            "geo_id": [zone.geo_id] * n,
        }
    )
    return ZoneResult(frame, zs)


# -- Incidence matrix -------------------------------------------------------


def _build_incidence(
    zone_seed: pl.DataFrame,
    zone_totals: pl.DataFrame,
    targets: list[str],
) -> tuple[np.ndarray, np.ndarray, int, list[tuple[str, str]]]:
    """Build incidence matrix + target vector for one zone.

    Expects seed table with:
    - Pivoted columns for non-structural controls: ``{control_name}__{member_name}``
    - Unpivoted columns for structural controls: ``h_total``, ``p_total``

    ``"h_total"`` must be present in *targets* and serves as the master
    control (incidence = 1 for every household).

    Returns:
    -------
    incidence : np.ndarray
        Shape ``(n_controls, n_households)``.
    targets : np.ndarray
        Target totals, one per control row.
    master_control_index : int
        Row index of ``h_total``.
    row_labels : list[tuple[str, str]]
        ``(control_name, member_name)`` for each row.
    """
    if "h_total" not in targets:
        msg = "'h_total' must be included in the controls list."
        raise ValueError(msg)

    n = len(zone_seed)
    rows: list[np.ndarray] = []
    tgt: list[float] = []
    labels: list[tuple[str, str]] = []
    master_idx = -1

    for name in targets:
        ctrl = CONTROLS[name]
        ctrl_rows = zone_totals.filter(pl.col("control_name") == name)
        if len(ctrl_rows) == 0:
            continue

        for member, target_val in zip(
            ctrl_rows["category"].to_list(),
            ctrl_rows["target_total"].to_list(),
            strict=True,
        ):
            # Structural controls use unpivoted column name,
            # all others use {control_name}__{member_name} pattern
            col_name = name if ctrl.structural else f"{name}__{member}"

            if col_name in zone_seed.columns:
                row = zone_seed[col_name].fill_null(0).to_numpy().astype(np.float64)
            else:
                row = np.zeros(n, dtype=np.float64)

            rows.append(row)
            labels.append((name, member))
            tgt.append(float(target_val))

            if name == "h_total" and master_idx == -1:
                master_idx = len(labels) - 1

    return np.vstack(rows), np.array(tgt, dtype=np.float64), master_idx, labels
