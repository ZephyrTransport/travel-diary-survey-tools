r"""Maximum-entropy list balancer.

Thin Polars→numpy bridge around ``np_balancer_numba``, vendored from PopulationSim.
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

Calls ``np_balancer_numba`` from ``_np_balancer``, vendored from PopulationSim
v0.10.0 -- a pure ``@njit`` function taking numpy arrays, so PopulationSim is
not a dependency.  Zones are independent and run in parallel via
``ThreadPoolExecutor``.

# Configuration (YAML)

The ``balancing:`` block of ``compute_weights`` (see ``BalancingConfig``):

```yaml
balancing:
  max_expansion_factor: 5.0     # upper bound = base_weight x factor
  min_expansion_factor: 0.01    # lower bound = base_weight x factor
  max_base_weight_gap: 0.005    # stop if a zone's base weights miss its target by more
  min_weight: 1                 # absolute floor
  # max_weight: null            # absolute ceiling (optional)
  max_iterations: 10000
  n_workers: 8                  # zones balanced in parallel
  default_importance: 100
  moe_based_importance: true
  min_survey_share_ratio: 0.25  # stop if a group is too small a share of its area's survey
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
    balancing: BalancingConfig,
    importance: dict[str, float],
    *,
    verbose: bool = True,
) -> tuple[pl.DataFrame, list[ZoneStatus]]:
    """Balance household weights to match control totals per zone.

    Args:
        seed: Incidence table with ``hh_id``, ``zone_group_id``,
            ``base_weight``, and pivoted control columns
            (``{ctrl}__{member}`` or structural).  All merges (global
            and zone-specific) must already be applied.
        control_totals: Per-zone targets (with merges already applied).
        targets: Control registry names.
        balancing: Solver bounds, iteration limits, and parallelism.
        importance: Importance of each control in *targets*, by name.
        verbose: Log per-zone convergence (default ``True``).

    Returns:
        Tuple of ``(weights, statuses)`` where *weights* is a DataFrame
        with columns ``hh_id``, ``hh_weight``, ``zone_group_id``, and
        *statuses* is one [`ZoneStatus`][processing.weighting.core.specs.ZoneStatus]
        entry per zone with convergence info.
    """
    zone_inputs: list[ZoneInput] = []
    for zone_group_id in control_totals.zone_group_ids:
        zone_seed = seed.filter(pl.col("zone_group_id").cast(pl.Utf8) == zone_group_id)
        if len(zone_seed) == 0:
            logger.warning("Zone %s: no seed records, skipping", zone_group_id)
            continue

        zone_totals = control_totals.totals.filter(pl.col("zone_group_id") == zone_group_id)
        zone_inputs.append(
            _prepare_zone(
                zone_seed,
                zone_totals,
                targets,
                zone_group_id,
                importance=importance,
                min_expansion_factor=balancing.min_expansion_factor,
                max_expansion_factor=balancing.max_expansion_factor,
                max_base_weight_gap=balancing.max_base_weight_gap,
                min_weight=balancing.min_weight,
                max_weight=balancing.max_weight,
                max_iterations=balancing.max_iterations,
                verbose=verbose,
            )
        )

    # Run balancer — numba @njit releases the GIL so threads parallelize
    if balancing.n_workers > 1 and len(zone_inputs) > 1:
        with ThreadPoolExecutor(max_workers=balancing.n_workers) as pool:
            results = list(pool.map(_balance_zone, zone_inputs))
    else:
        results = [_balance_zone(z) for z in zone_inputs]

    # Assemble output
    weight_frames = [r.weights for r in results]
    statuses = [r.status for r in results]

    weights = (
        pl.concat(weight_frames)
        if weight_frames
        else pl.DataFrame(
            schema={"hh_id": pl.Int64, "hh_weight": pl.Float64, "zone_group_id": pl.Utf8}
        )
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
    balancing: BalancingConfig,
    importance: dict[str, float],
) -> list[GridPoint]:
    """Re-run the balancer across a grid of ``max_expansion_factor`` values.

    Returns one [`GridPoint`][processing.weighting.core.specs.GridPoint]
    per EF value with aggregate fit and weight-quality metrics.
    The *selected_ef* is automatically included in the grid if not already present.

    This is a **diagnostics-only** function — it does not modify the
    production weights.

    Grid points are independent, so with ``balancing.n_workers > 1`` they run
    in parallel, each balancing its zones in turn. That keeps the thread count
    at ``n_workers`` while using it fully: a run has few zones, and zone-level
    parallelism alone left most workers idle.
    """
    grid = sorted(set(ef_grid) | {selected_ef})

    def run(ef: float) -> GridPoint:
        # Zones run one at a time here; the grid points share the worker pool
        ef_bal = balancing.model_copy(update={"max_expansion_factor": ef, "n_workers": 1})
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
        mape = float(abs_pct.mean() or 0)  # pyright: ignore[reportArgumentType]
        p90 = float(abs_pct.quantile(0.9) or 0)
        max_error = float(abs_pct.max() or 0)  # pyright: ignore[reportArgumentType]

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

        return GridPoint(
            max_expansion_factor=ef,
            converged_zones=sum(s.converged for s in statuses),
            total_zones=len(statuses),
            mape=mape,
            p90=p90,
            max_error=max_error,
            cv=cv,
            ess_pct=ess_pct,
        )

    logger.info("Grid search: EF %s on %d worker(s)", grid, balancing.n_workers)
    if balancing.n_workers > 1 and len(grid) > 1:
        with ThreadPoolExecutor(max_workers=balancing.n_workers) as pool:
            return list(pool.map(run, grid))
    return [run(ef) for ef in grid]


# -- Zone preparation -------------------------------------------------------


def _prepare_zone(
    zone_seed: pl.DataFrame,
    zone_totals: pl.DataFrame,
    targets: list[str],
    zone_group_id: str,
    *,
    importance: dict[str, float],
    min_expansion_factor: float,
    max_expansion_factor: float,
    max_base_weight_gap: float,
    min_weight: float | None,
    max_weight: float | None,
    max_iterations: int,
    verbose: bool,
) -> ZoneInput:
    """Build ``ZoneInput`` arrays for a single geography zone.

    Raises:
        ValueError: The zone's base weights miss its household target by more
            than *max_base_weight_gap*.
    """
    incidence, ctrl_targets, master_idx, row_labels = _build_incidence(
        zone_seed,
        zone_totals,
        targets,
    )

    imp_vec = np.array([importance[name] for name, _member in row_labels], dtype=np.float64)

    if "base_weight" not in zone_seed.columns:
        msg = (
            "Seed table missing 'base_weight' column. "
            "Run compute_base_weights() before balance_weights()."
        )
        raise ValueError(msg)
    initial = zone_seed["base_weight"].to_numpy().astype(np.float64)

    # The limits are multiples of the base weights, so those must add up to the target
    total = float(initial.sum())
    target = float(ctrl_targets[master_idx])
    gap = abs(total / target - 1) if target > 0 else float("inf")
    if gap > max_base_weight_gap:
        msg = (
            f"Zone {zone_group_id}: base weights add up to {total:,.0f} against a household "
            f"target of {target:,.0f}, {gap:.2%} off, more than max_base_weight_gap "
            f"({max_base_weight_gap:.2%}). Check the base weights for this zone."
        )
        raise ValueError(msg)

    # Bounds: expansion factors of the base weight, then optional absolute clipping
    lb = np.maximum(initial * min_expansion_factor, 0.0)
    ub = np.maximum(initial * max_expansion_factor, 1.0)
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
        zone_group_id=zone_group_id,
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
    zs = ZoneStatus(zone.zone_group_id, bool(converged), int(iters), float(delta), float(gamma))

    if zone.verbose:
        level = logging.INFO if converged else logging.WARNING
        logger.log(
            level,
            "Zone %s: %s in %d iters (delta=%.2e)",
            zone.zone_group_id,
            "converged" if converged else "NOT CONVERGED",
            iters,
            delta,
        )

    frame = pl.DataFrame(
        {
            "hh_id": zone.hh_ids,
            "hh_weight": w_final,
            "zone_group_id": [zone.zone_group_id] * n,
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
