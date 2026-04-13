[← Back to Main README](../../../README.md)

# Weighting Pipeline — Development Plan

Remaining features for the weighting module. For documentation of the implemented pipeline, see [README.md](README.md).

---

## Remaining Work

The core pipeline (geography crosswalk → PUMS controls → seed prep → base weights → max-entropy balancing → weight propagation → HTML diagnostics) is fully implemented and operational. The items below extend the pipeline to handle more complex weighting scenarios.

---

### 1. Custom Trip-Level Targets [ ]

**Status:** Not implemented. The current system controls only household and person-level variables.

**Goal:** Support trip-level aggregate targets such as transit mode share, VMT, average trip distance, or total linked trips by mode. These are not marginal cell counts — they are weighted sums or ratios computed from the trip table.

**Design considerations:**
- Trip targets operate at a different level than household/person controls: the "population" being matched is trips (or trip-miles), not households.
- Each trip target requires: (a) a filter expression identifying which trips contribute (e.g., `mode == "transit"`), (b) a metric (count, sum of distance, etc.), and (c) an external target value (from NTDL, FHWA, or regional counts).
- These cannot be expressed as incidence matrix rows in the standard balancer — they are non-linear constraints (weighted sum of trip attributes, where trip count per household varies).
- **Approach options:**
  - **Post-balancing raking adjustment:** Run the marginal balancer first, then apply iterative proportional fitting (raking) to nudge weights toward trip targets. Simple but may degrade marginal fit.
  - **Augmented incidence matrix:** Pre-compute per-household trip aggregates (e.g., "number of transit trips made by this household") and treat them as continuous incidence values. This linearizes the constraint but changes the interpretation of the incidence row from 0/1 to arbitrary counts.
  - **Two-stage weighting:** Household-level balancing first, then a trip-level calibration layer. More complex but cleanly separates the two constraint types.
- External targets must be supplied in the YAML config (not derived from PUMS).

```yaml
trip_targets:
  - name: transit_trips
    filter: "mode == 'transit'"
    metric: count             # count | sum(field_name)
    target: 425000            # external target value
  - name: total_vmt
    metric: "sum(trip_distance)"
    filter: "mode in ['drive_alone', 'shared_ride_2', 'shared_ride_3plus']"
    target: 89000000          # total VMT from HPMS or regional model
```

**Files affected:** New module (e.g., `balancing/trip_targets.py`), `weighting.py`, config schema, diagnostics.

---

### 2. Day-of-Week Structuring [ ]

**Status:** Not implemented. The development plan spec above describes the design. The `dow.py` module is planned but not yet created.

**Goal:** Expand each household into household-day records so that the balancer assigns separate weights per travel day, correcting for day-of-week sampling imbalance (e.g., surveys that over-sample weekdays relative to weekends).

**Design considerations:**
- Each household-day record inherits the household's stratum variables and is tagged with a `dow_group` label (e.g., "weekday", "weekend").
- The balancer seeds become household-day rows instead of household rows. All existing incidence, importance, and bounding logic applies unchanged — the row count simply increases.
- `dow_groups` are fully configurable in YAML (see spec above).
- Base weight scaling: `base_weight *= (target_days_in_group / observed_days_in_group_for_hh)`.
- Weight propagation must route day-specific weights correctly: `day_weight` comes from the household-day seed; trip weights inherit from the day, not the household.
- This interacts with the trip-targets feature: trip-level targets would need to be evaluated per day-group (e.g., separate weekday vs weekend VMT targets).

```yaml
dow_weighting: true
dow_groups:
  weekday: [1, 2, 3, 4, 5]   # day_of_week: 1=Mon
  weekend: [6, 7]
```

**Files affected:** New `balancing/dow.py`, `data_prep/seed_data.py` (expansion), `balancing/base_weights.py` (scaling), `balancing/weight_propagation.py` (day-specific routing), `weighting.py`.

---

### 3. Platform / Mode Bias Adjustment [ ]

**Status:** Not implemented. Not currently in the development plan.

**Goal:** Correct for systematic biases introduced by the survey collection method — respondents who complete the survey via different platforms (web browser, mobile app, phone interview, paper mail-back) may exhibit different travel behaviors, response rates, and demographic profiles. If the platform mix in the sample doesn't match the population, weighted estimates will be biased.

**Design considerations:**
- This is typically handled as an **additional control variable** if external benchmarks for platform share exist, or as a **propensity-score stratification** if they don't.
- **Option A — Platform as a control:** Add a `survey_platform` control with categories (browser, mobile, phone, paper) and external targets for the share of each. Requires an external source for population-level platform adoption rates (may not exist). Simple to implement — it's just another marginal control.
- **Option B — Propensity weighting:** Model P(platform = X | demographics) using a logistic regression on the survey data. Weight inversely by the propensity to correct for differential response. This is a pre-balancing adjustment to the base weights, not a control target.
- **Option C — Post-stratification raking:** After marginal balancing, add a raking step that adjusts weights to match assumed platform shares. Similar to trip-targets raking.
- For travel diary surveys, the main concern is that phone/paper respondents tend to under-report trips (especially short ones) relative to app-tracked respondents. This is a **measurement bias**, not a sampling bias — weighting alone cannot fix under-reporting, only adjust for differential inclusion.
- Platform information must be present in the survey data (a `survey_mode` or `platform` field).

```yaml
platform_adjustment:
  enabled: true
  method: control          # control | propensity | raking
  field: survey_platform   # canonical field name
  # Only for method: control — external target shares
  targets:
    browser: 0.35
    mobile: 0.25
    phone: 0.30
    paper: 0.10
```

**Files affected:** Depends on method. `control` → just add a new `ControlTarget` subclass + YAML entry. `propensity` → new module (e.g., `balancing/propensity.py`) adjusting base weights. `raking` → post-balancing adjustment in `weighting.py`.

---

### 4. Replicate Weights for Computed Survey Weights [ ]

**Status:** Not implemented. Current replicate handling is used for control-importance MOE logic, not for generating replicate versions of final survey weights.

**Goal:** Support replicate-weight generation on the **computed survey weights** themselves (household/person/day/trip), so users can evaluate weighting methodology stability and produce design-based variance estimates from replicate runs.

**Design considerations:**
- This feature is explicitly about recomputing the weighting pipeline per replicate (base-weight adjustments, balancing/calibration, trimming), not PUMS-target uncertainty alone.
- Add configurable replicate methods: `jk1`, `brr`, `fay_brr`, `bootstrap` (method availability depends on sample design metadata such as strata/PSU).
- Replicate weights should be generated at the household seed stage, then propagated through existing weight propagation (household → person → day → trip).
- Diagnostics should summarize methodology stability across replicates:
  - convergence success rate by zone
  - control fit dispersion (e.g., CV or percentile spread of residuals)
  - weight quality dispersion (CV, ESS%, trimming-hit rate)
  - optional DEFF-style summaries for key outputs
- Performance/storage impact is substantial (`R` replicate runs and `R` extra weight columns). Feature should be gated by config and allow reduced `n_replicates` for exploratory QA.
- Backward compatibility: default behavior remains single final weight columns (`hh_weight`, `person_weight`, etc.), with replicate columns emitted only when enabled.

```yaml
replicate_weights:
  enabled: true
  method: jk1              # jk1 | brr | fay_brr | bootstrap
  n_replicates: 80
  random_seed: 2026
  output_replicate_columns: true
```

**Files affected:** `weighting.py` (replicate orchestration), `balancing/base_weights.py` (replicate seed/base adjustments), new module (e.g., `balancing/replicate_weights.py`), `balancing/weight_propagation.py` (replicate column propagation), diagnostics modules/tables/charts, config schema in `specs.py`.

---

### Priority and Dependencies

| # | Feature | Status | Complexity | Impact |
|---|---------|--------|------------|--------|
| 1 | Custom trip targets | [ ] | Medium | High — useful for matching to NTD/FHWA but adds architectural complexity |
| 2 | DOW structuring | [ ] | High | High — provides day-of-week weight granularity |
| 3 | Platform bias | [ ] | Low-High | Medium — important for mixed-mode surveys |
| 4 | Replicate weights on computed weights | [ ] | Medium-High | High — enables methodology stability checks and replicate-based variance |

`[ ]` Not started · `[x]` Complete

Features 1 and 3 are independent and can be developed in parallel. Feature 2 is architecturally distinct (non-linear constraints). Feature 4 is orthogonal — it modifies inputs (base weights or controls) rather than the balancer itself.

---

## Previously Completed

- **Cross-Tab Targets** (March 2026) — `CrosstabControlTarget` in `controls/base.py`, `make_crosstab_enum()` in `controls/enums.py`, `register_crosstab()` / `register_crosstabs_from_config()` in `controls/registry.py`, pre-merge at registration via `_build_dim_value_groups()`, N-D merges in `data_prep/merges.py`, margin validation in `validation/control_validation.py`. Human-readable cross-tab labels ("by" separator) in `diagnostics/data.py`. Control group headers in fit chart y-axis (`diagnostics/charts.py`). Tests in `tests/test_crosstab_controls.py`.
- **Expansion Factor Grid Search** (March 2026) — `grid_search_expansion_factor()` in `balancing/balancer.py`, `GridPoint` in `specs.py`, `ef_tradeoff_figure()` in `diagnostics/charts.py`, wired into `generate_report()` and `weighting.py`. Tests in `tests/test_grid_search.py`.
- **Diagnostics Table Refactoring** (March 2026) — `_html_table()` `group_row` support, CV/ESS% consolidated into `balancer_performance_table()`, simplified `weight_quality_table()`. Files: `diagnostics/tables.py`, `diagnostics/diagnostics_template.html`.
- **Diagnostics Output Path Wiring** (April 2026) — `generate_diagnostics()` in `weighting_pipeline.py` now accepts an `output_path` parameter; `compute_weights.py` passes `diagnostics.get("output_path")` through from the YAML config. Both project configs (`bats_2019`, `bats_2023`) have a `diagnostics: output_path:` block. Unimplemented config keys (`enabled`, `fit_error_thresholds`, etc.) were removed from docs and configs.
