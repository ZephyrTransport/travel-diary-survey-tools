# ruff: noqa: C901, PLR0912, PLR0915, PLR1730
"""Vendored from PopulationSim v0.10.0 (MIT License).

Source: populationsim/balancing/balancers_numba.py
Commit: 7826bbfa9cd2801cdbed9af0492a7308672c0ef2

Only np_balancer_numba and the constants it references are included.
"""

import logging

import numpy as np
from numba import njit

# --- constants (from populationsim/balancing/constants.py) ---
DEFAULT_MAX_ITERATIONS = 10000
MAX_DELTA32 = 1.0e-5
MAX_GAMMA = 1.0e-5
MIN_GAMMA = 1.0e-10
IMPORTANCE_ADJUST = 2
IMPORTANCE_ADJUST_COUNT = 100
MIN_IMPORTANCE = 1.0
MAX_RELAXATION_FACTOR = 1000000
MIN_CONTROL_VALUE = 0.1
ALT_MAX_DELTA = 1.0e-14


logger = logging.getLogger(__name__)


@njit(fastmath=True, cache=True)
def np_balancer_numba(
    sample_count: int,
    control_count: int,
    master_control_index: int,
    incidence: np.ndarray,
    weights_initial: np.ndarray,
    weights_lower_bound: np.ndarray,
    weights_upper_bound: np.ndarray,
    controls_constraint: np.ndarray,
    controls_importance: np.ndarray,
    max_iterations: int = DEFAULT_MAX_ITERATIONS,
) -> tuple[np.ndarray, np.ndarray, tuple[bool, int, float, float]]:
    # Upcast key scalars to float64 for stability
    weights_final = weights_initial.copy()
    relaxation_factors = np.empty(control_count, dtype=np.float64)

    for i in range(control_count):
        relaxation_factors[i] = 1.0

    # Precompute incidence squared
    incidence2 = incidence * incidence
    importance_adjustment = 1.0

    # Manual control reordering
    control_indexes = np.empty(control_count, dtype=np.int32)
    k = 0
    for i in range(control_count):
        if i != master_control_index:
            control_indexes[k] = i
            k += 1
    if master_control_index >= 0:
        control_indexes[k] = master_control_index

    for it in range(max_iterations):
        delta = 0.0  # float64
        gamma = np.ones(control_count, dtype=np.float64)

        if it > 0 and it % IMPORTANCE_ADJUST_COUNT == 0:
            importance_adjustment /= IMPORTANCE_ADJUST

        for i in range(control_count):
            c = control_indexes[i]
            xx = 0.0
            yy = 0.0
            for j in range(sample_count):
                w = float(weights_final[j])
                inc = float(incidence[c, j])
                xx += w * inc
                yy += w * float(incidence2[c, j])

            imp = (
                float(controls_importance[c])
                if c == master_control_index
                else max(
                    float(controls_importance[c]) * importance_adjustment,
                    MIN_IMPORTANCE,
                )
            )

            if xx > 0.0:
                relaxed = float(controls_constraint[c]) * relaxation_factors[c]
                if relaxed < MIN_CONTROL_VALUE:
                    relaxed = MIN_CONTROL_VALUE

                gamma_val = 1.0 - (xx - relaxed) / (yy + relaxed / imp)
                gamma_val = max(gamma_val, MIN_GAMMA)
                gamma[c] = gamma_val
                log_gamma = np.log(gamma_val)

                for j in range(sample_count):
                    w_old = float(weights_final[j])
                    inc = float(incidence[c, j])
                    new_w = w_old * np.exp(log_gamma * inc)

                    lb = float(weights_lower_bound[j])
                    ub = float(weights_upper_bound[j])
                    new_w = min(max(new_w, lb), ub)

                    delta += abs(new_w - w_old)
                    weights_final[j] = new_w

                relax_factor = relaxation_factors[c] * (1.0 / gamma_val) ** (1.0 / imp)
                if relax_factor > MAX_RELAXATION_FACTOR:
                    relax_factor = MAX_RELAXATION_FACTOR
                relaxation_factors[c] = relax_factor

        delta /= sample_count
        max_gamma_dif = 0.0
        for i in range(control_count):
            g_dif = abs(gamma[i] - 1.0)
            if g_dif > max_gamma_dif:
                max_gamma_dif = g_dif

        converged = delta < MAX_DELTA32 and max_gamma_dif < MAX_GAMMA
        no_progress = delta < ALT_MAX_DELTA

        if converged or no_progress:
            return weights_final, relaxation_factors, (True, it, delta, max_gamma_dif)

    return (
        weights_final,
        relaxation_factors,
        (False, max_iterations, delta, max_gamma_dif),
    )
