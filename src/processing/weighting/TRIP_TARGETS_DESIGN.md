# Exogenous Trip-Level Targets — Design Document

[← Development Plan](DEVELOPMENT_PLAN.md) · [← Weighting README](README.md)

---

## 1. Overview

### Problem

The weighting pipeline currently matches household and person characteristics
to Census (PUMS) marginal distributions. This corrects demographic sampling
bias but does not guarantee that aggregate travel metrics — total transit
ridership, vehicle-miles traveled, mode shares — match known external
benchmarks.

Transit agencies publish unlinked boarding counts (NTD), highway agencies
publish VMT estimates (HPMS/Caltrans), and regional planning agencies track
mode shares. These provide ground-truth aggregate targets that the survey
weights should ideally reproduce.

### Solution

Add an **optional `trip_targets` configuration block** that lets users
specify exogenous aggregate targets (transit linked trips, VMT, etc.).
Trip-level metrics are pre-aggregated to the household level and injected as
**continuous-valued rows** in the existing maximum-entropy incidence matrix.
The balancer then jointly optimises household weights to fit both demographic
marginals and trip aggregate targets in a single stage.

### Key Properties

| Property | Decision | Rationale |
|----------|----------|-----------|
| Integration approach | **Augmented incidence matrix** (single-stage) | Avoids two-stage complexity; the max-entropy solver naturally handles mixed binary + continuous rows |
| Target granularity | **Scalar totals** (region-wide or per `ctrl_geoid`) | Start simple; distribution targets (by purpose/TOD) achievable by specifying multiple targets |
| VMT definition | **Person-trip miles** (sum of distance × trips) | Matches survey data directly; avoids occupancy inference |
| Transit boardings | **Single region-wide `boardings_per_trip` factor** | Converts NTD unlinked boardings to linked trip equivalents before entering the balancer |
| Config placement | **Separate `trip_targets:` block** (not under `controls:`) | Different data source (exogenous vs PUMS), different fields, keeps existing control infra untouched |

---

## 2. Transit Boardings → Linked Transit Trips

### The Problem

Transit boarding counts from NTD are **unlinked boardings** — each vehicle
boarding is a separate count. A passenger who takes BART then transfers to
Muni Metro registers as 2 boardings but made 1 linked transit trip.

The survey records trips at the **linked** level: a multi-modal transit
journey (walk → BART → Muni Metro → walk) appears as one linked trip with
`mode_type = TRANSIT`, composed of multiple unlinked trip segments.

Matching raw NTD boardings directly to survey linked trips would overcount
the target. We need a conversion factor.

### Conversion Approach

```
linked_transit_trip_target = ntd_annual_boardings / boardings_per_trip
```

**`boardings_per_trip`** is the average number of transit vehicle boardings
per linked transit trip in the region. Typical values:

| Region Type | Factor | Notes |
|-------------|--------|-------|
| Bus-only system | 1.1–1.3 | Few transfers |
| Rail + bus with transfers | 1.4–1.8 | Cross-system transfers common |
| Closed-system rail (BART) + feeder bus | 1.5–2.0 | BART internal transfers invisible; feeder connections add boardings |
| Bay Area (MTC/SFCTA) | ~1.7 | Estimate from BATS transfer rates |

The user supplies this factor in the config. It is applied **before** zone
allocation — the target entering the balancer is already in linked-trip
units.

### Data Flow

```
NTD annual boardings (e.g., 425,000/day)
  ÷ boardings_per_trip (e.g., 1.7)
  = linked transit trip target (e.g., 250,000/day)
  → allocate to zones by survey base-weighted shares
  → enter incidence matrix as target for t_transit_linked_trips column
```

### What Counts as a Linked Transit Trip

A linked trip counts toward the transit target if its canonical
`mode_type` column equals `ModeType.TRANSIT` (value 13). This includes all
trips where **any** unlinked segment used a transit mode — the trip linking
algorithm already assigns `mode_type = TRANSIT` whenever a transit-mode
segment is present.

The `FERRY` mode type (12) and `LONG_DISTANCE` mode type (14) are
**excluded** by default — users who want to include ferry or intercity rail
should add them to the filter list explicitly.

---

## 3. Config Schema

### YAML Structure

```yaml
steps:
  - name: compute_weights
    params:
      # ... existing params (state_fips, pums_year, geography, controls, etc.)

      trip_targets:
        - name: transit_linked_trips
          table: linked_trips          # default; also accepts "unlinked_trips"
          filter:
            mode_type: [13]            # ModeType.TRANSIT
          metric: count
          target: 250000               # linked trips per day (post-conversion)
          boardings_per_trip: 1.7      # NTD boardings ÷ this = linked trip target
          geography: region            # "region" or "ctrl_geoid"
          importance: 50

        - name: auto_pmt
          table: linked_trips
          filter:
            mode_type: [8]             # ModeType.CAR
          metric: "sum(distance_meters)"
          target: 143000000000         # person-meters per day
          geography: region
          importance: 25
```

### Field Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | **yes** | — | Unique identifier. Used as the incidence column name (`t_{name}`) and in diagnostics. |
| `table` | str | no | `"linked_trips"` | Which survey table to aggregate. Must be `"linked_trips"` or `"unlinked_trips"`. |
| `filter` | dict | no | `{}` (all rows) | Column → value(s) filter. Each key is a column name, each value is a list of valid values. Rows must match **all** filters (AND logic). |
| `metric` | str | **yes** | — | `"count"` for row count, or `"sum(column_name)"` to sum a numeric column. |
| `target` | float | **yes** | — | The exogenous aggregate target value (region-wide, or see `target_by_zone`). |
| `boardings_per_trip` | float | no | `None` | If set, divides `target` by this factor before use. Intended for converting NTD unlinked boardings to linked trips. Applied once at parse time. |
| `geography` | str | no | `"region"` | `"region"` → single target pro-rated to zones. `"ctrl_geoid"` → per-zone targets from `target_by_zone`. |
| `target_by_zone` | dict | no | `None` | Required when `geography: ctrl_geoid`. Maps zone ID → target value. |
| `importance` | float | no | `50.0` | Importance weight in the balancer. Should generally be **lower** than demographic controls (which default to 100) to avoid distorting marginal fit. |

### Why Separate from `controls:`

The existing `controls:` block entries share a common structure:

- Reference a **registered `ControlTarget`** (with `pums_expr()`, `survey_expr()`, enum categories)
- Derive targets from **PUMS aggregation** (`aggregate_control_totals()`)
- Produce **binary/integer incidence** columns (0/1 or person counts per HH)
- Support **merges** (category grouping) and **zone_merges**

Trip targets share **none** of these properties:

- Targets are **exogenous** (user-supplied scalar), not derived from PUMS
- Incidence values are **continuous** (trip counts, meters, etc.)
- No enum categories, no survey/pums recoding expressions
- No merges (a scalar target has nothing to merge)
- Need a **filter** and **metric** instead

Mixing them would require "skip-if-trip" guards in 6+ existing functions
(`aggregate_control_totals`, `recode_pums_households`,
`recode_survey_households`, `pivot_hh_controls`, `resolve_targets`,
`from_yaml`). The only convergence point is the numpy incidence matrix
inside the balancer — and that's exactly where we join them.

---

## 4. Architecture

### Data Flow Diagram

```
                    ┌──────────────────────────────────────────────────┐
                    │           Existing Pipeline (unchanged)          │
                    │                                                  │
 YAML controls ──► │ register → fetch_pums → recode → pivot → merge  │
                    │  → aggregate_totals → resolve_importance         │
                    │                                                  │
                    │  Output: seed_incidence (hh_id × ctrl columns)  │
                    │          control_totals (geo × ctrl × target)   │
                    └──────────────────┬───────────────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │          NEW: Trip Target Preparation            │
                    │                                                  │
 YAML trip_targets  │  1. compute_trip_incidence()                    │
    + linked_trips ─┤     filter trip table → aggregate to HH level  │
    + unlinked_trips│     → columns: t_transit_linked_trips, t_auto_pmt│
                    │                                                  │
                    │  2. join onto seed_incidence                     │
                    │     seed now has ctrl columns + trip columns     │
                    │                                                  │
                    │  3. allocate_trip_targets()                      │
                    │     region target → zone shares → per-zone targets│
                    │     (boardings_per_trip applied before allocation)│
                    │                                                  │
                    │  Output: augmented seed, trip zone targets       │
                    └──────────────────┬───────────────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │        Balancer (_build_incidence modified)      │
                    │                                                  │
                    │  Standard control rows (binary/integer):        │
                    │    h_total      [1  1  1  1  1  ...  1]  tgt=N  │
                    │    h_size__1    [1  0  0  1  0  ...  0]  tgt=X  │
                    │    p_gender__m  [2  1  0  1  1  ...  1]  tgt=Y  │
                    │    ...                                           │
                    │                                                  │
                    │  + Trip target rows (continuous):                │
                    │    t_transit    [0  3  1  0  2  ...  0]  tgt=Z  │
                    │    t_auto_pmt  [47 0  82 23 0  ... 15]  tgt=W  │
                    │                                                  │
                    │  → max-entropy solver jointly fits all rows      │
                    └──────────────────┬───────────────────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │        Weight Propagation (unchanged)            │
                    │  hh_weight → person → day → trip → tour         │
                    └─────────────────────────────────────────────────-┘
```

### Pipeline Phase Insertion

The weighting pipeline currently runs these phases in order:

```
1. setup()                    — register controls, build crosswalk
2. fetch_pums()               — load PUMS microdata
3. recode_and_pivot()         — recode both datasets, build incidence
4. assign_zones()             — spatial join, zone allocation
5. apply_merges()             — collapse merged categories
6. aggregate_totals()         — PUMS → per-zone control totals
7. resolve_importance()       — compute final importance dict
8. balance()                  — base weights + solver + optional grid search
9. propagate()                — carry-forward weights to all tables
10. generate_diagnostics()    — HTML report
```

Trip target preparation inserts as a **new phase between 6 and 7**:

```
6.  aggregate_totals()
6b. prepare_trip_targets()      ← NEW PHASE
7.  resolve_importance()
```

This timing works because:
- **Depends on** seed_incidence (from phase 3–5) and base_weights (from
  early phase 8, but base weights are computed before the solver runs —
  they just need zone assignments from phase 4). We may need to split
  base weight computation out of `balance()` into its own sub-step, or
  compute a preliminary base-weight estimate for zone allocation.
- **Does not affect** PUMS fetching, recoding, or control-total
  aggregation — those are pure demographic operations.

**Alternative timing**: compute trip incidence in phase 6b but defer zone
allocation to early in phase 8 (after base weights are computed). This
avoids splitting base-weight computation.

---

## 5. Implementation Details

### 5.1 New Data Structure: `TripTargetSpec`

**File**: `src/processing/weighting/specs.py`

```python
@dataclass
class TripTargetSpec:
    """Specification for an exogenous trip-level aggregate target.

    Parsed from the ``trip_targets`` YAML block. Each spec defines:
    - Which trip table and rows to include (table + filter)
    - What metric to compute (count or sum)
    - The external target value
    - How to allocate across zones (region-wide or per-zone)
    - Importance weight for the balancer

    The resulting per-household aggregate becomes a continuous-valued
    row in the incidence matrix alongside the binary demographic
    controls.
    """

    name: str
    table: str = "linked_trips"
    filter: dict[str, list] = field(default_factory=dict)
    metric: str = "count"
    target: float = 0.0
    boardings_per_trip: float | None = None
    geography: str = "region"
    target_by_zone: dict[str, float] | None = None
    importance: float = 50.0

    def __post_init__(self) -> None:
        if self.table not in ("linked_trips", "unlinked_trips"):
            raise ValueError(f"trip target '{self.name}': table must be "
                             f"'linked_trips' or 'unlinked_trips', got '{self.table}'")
        if self.metric != "count" and not self.metric.startswith("sum("):
            raise ValueError(f"trip target '{self.name}': metric must be "
                             f"'count' or 'sum(column_name)', got '{self.metric}'")
        if self.geography == "ctrl_geoid" and not self.target_by_zone:
            raise ValueError(f"trip target '{self.name}': geography='ctrl_geoid' "
                             f"requires target_by_zone dict")
        if self.boardings_per_trip is not None:
            if self.boardings_per_trip <= 0:
                raise ValueError(f"trip target '{self.name}': "
                                 f"boardings_per_trip must be positive")
            self.target = self.target / self.boardings_per_trip

    @property
    def incidence_col(self) -> str:
        """Column name in the seed incidence table."""
        return f"t_{self.name}"

    @property
    def sum_field(self) -> str | None:
        """Extract field name from 'sum(field)' metric, or None for count."""
        if self.metric.startswith("sum(") and self.metric.endswith(")"):
            return self.metric[4:-1]
        return None

    @classmethod
    def from_yaml(cls, raw: dict) -> "TripTargetSpec":
        """Parse a single trip_targets YAML entry."""
        return cls(**raw)
```

### 5.2 New Module: `trip_incidence.py`

**File**: `src/processing/weighting/data_prep/trip_incidence.py`

Two public functions:

#### `compute_trip_incidence()`

Aggregates trip-table rows to household-level values for each target.

```python
def compute_trip_incidence(
    trip_targets: list[TripTargetSpec],
    tables: dict[str, pl.DataFrame],   # {"linked_trips": ..., "unlinked_trips": ...}
    hh_ids: pl.Series,
) -> pl.DataFrame:
    """Compute per-household trip aggregates for each trip target.

    Returns a DataFrame with one row per hh_id and one column per
    trip target (named t_{spec.name}). Missing households get 0.
    """
```

**Algorithm per target**:

1. Select `tables[spec.table]`
2. Apply filters: for each `(column, values)` in `spec.filter`, keep rows
   where `column.is_in(values)`. All filters are ANDed.
3. Compute metric:
   - `"count"` → group by `hh_id`, count rows
   - `"sum(col)"` → group by `hh_id`, sum the named column
4. Right-join onto full `hh_ids` to fill missing HHs with 0
5. Rename result column to `t_{spec.name}`

Output: `pl.DataFrame` with columns `[hh_id, t_target1, t_target2, ...]`

#### `allocate_trip_targets()`

Distributes region-wide targets to zones proportionally.

```python
def allocate_trip_targets(
    trip_targets: list[TripTargetSpec],
    seed: pl.DataFrame,          # must have hh_id, ctrl_geoid, base_weight, t_{name} columns
    geo_col: str = "ctrl_geoid",
) -> pl.DataFrame:
    """Allocate trip targets to zones.

    For region-wide targets: pro-rate by zone's base-weighted share
    of the metric. For per-zone targets: use target_by_zone directly.

    Returns tidy DataFrame: [geo_id, target_name, target_total]
    """
```

**Algorithm for `geography == "region"`**:

1. Compute weighted metric per zone:
   `zone_total = sum(t_{name} * base_weight)` grouped by `ctrl_geoid`
2. Compute region total: `region_total = sum(zone_totals)`
3. Zone share: `share = zone_total / region_total`
4. Allocated target: `zone_target = spec.target * share`
5. If `region_total == 0` (no survey trips of this type anywhere):
   log warning, skip this target for all zones

**Algorithm for `geography == "ctrl_geoid"`**:

1. Directly use `spec.target_by_zone` dict
2. Warn about zones present in crosswalk but missing from dict

### 5.3 Modifications to Balancer

**File**: `src/processing/weighting/balancing/balancer.py`

#### `balance_weights()` — New Parameter

```python
def balance_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    balancing: BalancingConfig | None = None,
    importance: ImportanceConfig | None = None,
    *,
    trip_targets: list[TripTargetSpec] | None = None,     # NEW
    trip_zone_targets: pl.DataFrame | None = None,          # NEW
    verbose: bool = True,
) -> tuple[pl.DataFrame, list[ZoneStatus]]:
```

- `trip_targets` — parsed specs (needed for importance values and column
  names)
- `trip_zone_targets` — tidy DataFrame `[geo_id, target_name,
  target_total]` from `allocate_trip_targets()`
- Both are passed through to `_prepare_zone()` → `_build_incidence()`

#### `_build_incidence()` — Append Trip Rows

After building the standard control rows, append one row per trip target:

```python
# After standard control rows are built...
if trip_targets and trip_zone_targets is not None:
    zt = trip_zone_targets.filter(pl.col("geo_id") == geo_id)
    for spec in trip_targets:
        col = spec.incidence_col  # "t_transit_linked_trips"
        target_row = zt.filter(pl.col("target_name") == spec.name)
        if target_row.is_empty():
            continue  # no target for this zone (logged at allocation time)

        target_val = target_row["target_total"][0]

        if col in zone_seed.columns:
            row = zone_seed[col].fill_null(0).to_numpy().astype(np.float64)
        else:
            row = np.zeros(n_hh, dtype=np.float64)

        # Skip if all zeros (can't meet any nonzero target)
        if row.sum() == 0 and target_val > 0:
            logger.warning(
                "Zone '%s': trip target '%s' has zero survey incidence "
                "but nonzero target (%.1f) — skipping.",
                geo_id, spec.name, target_val,
            )
            continue

        rows.append(row)
        tgt.append(target_val)
        labels.append((spec.name, "total"))
```

#### `_prepare_zone()` — Wire Importance

Trip target importance is set from `TripTargetSpec.importance`, appended
after the standard control importance vector:

```python
# After standard importance assignment...
for spec in trip_targets:
    if (spec.name, "total") in row_labels:
        importance_vec.append(spec.importance)
```

### 5.4 Pipeline Orchestration

**File**: `src/processing/weighting/weighting_pipeline.py`

Add a `prepare_trip_targets()` phase method:

```python
def prepare_trip_targets(self) -> None:
    """Phase 6b: Compute trip incidence and allocate targets to zones.

    Requires: seed_incidence (from recode_and_pivot + apply_merges),
              base_weight column on seed (from assign_zones or balance setup).
    Produces: augmented seed_incidence with t_{name} columns,
              trip_zone_targets DataFrame for the balancer.
    """
    if not self.trip_target_specs:
        return  # no-op when trip_targets not configured

    tables = {}
    if self.data.linked_trips is not None:
        tables["linked_trips"] = self.data.linked_trips
    if self.data.unlinked_trips is not None:
        tables["unlinked_trips"] = self.data.unlinked_trips

    hh_ids = self.seed_incidence["hh_id"]
    incidence = compute_trip_incidence(self.trip_target_specs, tables, hh_ids)
    self.seed_incidence = self.seed_incidence.join(incidence, on="hh_id", how="left")

    self.trip_zone_targets = allocate_trip_targets(
        self.trip_target_specs,
        self.seed_incidence,
        geo_col="ctrl_geoid",
    )
```

### 5.5 Entry Point

**File**: `src/processing/weighting/compute_weights.py`

Add `trip_targets` parameter:

```python
@step()
def compute_weights(
    # ... existing params ...
    trip_targets: list[dict] | None = None,    # NEW
    # ... existing table params ...
) -> dict[str, pl.DataFrame]:
```

Parse and pass to pipeline:

```python
trip_target_specs = (
    [TripTargetSpec.from_yaml(t) for t in trip_targets]
    if trip_targets
    else []
)
# pass trip_target_specs to WeightingPipeline
```

---

## 6. Incidence Matrix: Binary vs Continuous

### Current: Binary/Integer Rows

Standard demographic controls produce rows where each cell is a non-negative
integer — typically 0 or 1 for household-level controls, or a person count
(0, 1, 2, ...) for person-level controls:

```
                 HH1  HH2  HH3  HH4  HH5
h_total           1    1    1    1    1     ← always 1
h_size__1         1    0    0    1    0     ← 0/1 binary
h_size__2         0    1    0    0    1
p_gender__male    2    1    0    1    1     ← integer (count of males in HH)
p_gender__female  0    0    1    1    0
```

### New: Continuous Trip Rows

Trip target rows have arbitrary non-negative continuous values:

```
                      HH1   HH2   HH3   HH4   HH5
t_transit_trips        0     3     1     0     2    ← integer (count)
t_auto_pmt         47200     0  82100 23400     0    ← continuous (meters)
```

### Why This Works with Max-Entropy

The PopulationSim max-entropy solver minimises:

$$\min_w \sum_i w_i \ln(w_i / w_{0i}) \quad \text{s.t.} \quad Aw = t, \quad w \geq 0$$

The incidence matrix $A$ and target vector $t$ are unconstrained — $A$ can
contain any real values, not just 0/1. The solver's Newton-Raphson iteration
adjusts $\gamma$ factors per constraint row:

$$w_j \leftarrow w_j \cdot \exp\!\bigl(\ln(\gamma_c) \cdot A_{c,j}\bigr)$$

For binary rows ($A_{c,j} \in \{0,1\}$), this scales only the affected HHs.
For continuous rows, the exponential adjustment is proportional to the
incidence value — HHs with more transit trips get a larger multiplicative
nudge. This is mathematically well-defined and converges under the same
conditions.

### Scale Considerations

Large-magnitude incidence values (e.g., VMT in meters) can cause numerical
issues — the $\gamma$ exponent becomes very large. **Recommendation**: if
convergence is poor, divide both the incidence column and the target by a
common scale factor (e.g., 1000 for distances). This is equivalent from
the solver's perspective but improves numerical stability.

Future extension: add an optional `scale_factor` field to `TripTargetSpec`
if empirical testing reveals scale-sensitivity issues.

---

## 7. Zone Allocation for Region-Wide Targets

### Problem

The balancer operates per-zone. A region-wide target of "250,000 daily
linked transit trips" must be decomposed into per-zone sub-targets (e.g.,
San Francisco gets 38%, Alameda 22%, etc.).

### Approach: Base-Weighted Survey Share

```
zone_share = sum(t_{name} * base_weight, zone) / sum(t_{name} * base_weight, region)
zone_target = region_target * zone_share
```

**Why base-weighted**: Using unweighted survey counts would bias toward
zones with higher response rates. Using base weights (pre-balancing) provides
a crude but consistent population-proportional estimate.

**Why not PUMS**: PUMS has no trip data — only socioeconomic characteristics.
The survey is the only source for trip-pattern geographic distribution.

### Edge Cases

| Scenario | Handling |
|----------|----------|
| Zone has 0 survey transit trips | `zone_share = 0` → `zone_target = 0`. Incidence row is all-zero. Skip this target row for this zone (log warning). |
| Zone has survey trips but tiny share | Normal operation — small zone target. Balancer may deprioritize via importance weighting. |
| All zones have 0 transit trips | `region_total = 0`. Log warning, skip entire target. No rows added to any zone's incidence matrix. |
| Per-zone targets don't sum to any particular region total | Fine — each zone has an independent target. No consistency constraint. |

---

## 8. Importance and Convergence

### Why Lower Importance for Trip Targets

Trip targets should generally receive **lower importance** (e.g., 25–50) than
demographic controls (default 100, structural 200) because:

1. **Demographic marginals are more reliable** — Census-derived, precise.
   Trip targets are estimates with substantial uncertainty (NTD sampling,
   boardings-per-trip conversion).
2. **Avoid distorting marginals** — the household weights primarily serve
   demographic expansion. Trip targets are supplementary calibration.
3. **Flexibility** — with lower importance, the solver will partially fit
   trip targets while preserving demographic fit. Users can increase
   importance if trip accuracy is more critical for their application.

### Convergence Interactions

Adding trip target rows may:
- **Slow convergence** — more constraints to satisfy simultaneously.
- **Prevent convergence** — if trip targets conflict strongly with
  demographic marginals (e.g., demanding high transit trips in a zone with
  few transit-using households). Lower importance mitigates this.
- **Increase weight dispersion** — weights pulled in more directions may have
  higher variance. Monitor ESS% in diagnostics.

**Recommendation**: Run with and without trip targets, compare weight
quality metrics (ESS%, CV, trimming rate, marginal fit), and tune importance
accordingly.

---

## 9. Diagnostics Extensions

### Trip Target Fit Table

Add a section to the existing HTML diagnostic report showing per-zone fit
for each trip target:

| Zone | Target Name | Target | Achieved | Pct Error | Status |
|------|-------------|--------|----------|-----------|--------|
| 075 | transit_linked_trips | 95,000 | 93,200 | -1.9% | ✓ |
| 001 | transit_linked_trips | 55,000 | 58,100 | +5.6% | ⚠ |
| REGION | transit_linked_trips | 250,000 | 248,300 | -0.7% | ✓ |
| REGION | auto_pmt | 143B | 141B | -1.4% | ✓ |

**"Achieved"** = `sum(t_{name} * hh_weight)` per zone, summed across all
households.

### Grid Search Interaction

If `expansion_factor_grid` is configured, trip target fit should be included
in the grid-point metrics. The MAPE calculation should incorporate trip
target deviations alongside demographic control deviations.

---

## 10. Files Affected

### New Files

| File | Description |
|------|-------------|
| `src/processing/weighting/data_prep/trip_incidence.py` | `compute_trip_incidence()`, `allocate_trip_targets()` |
| `tests/test_weighting_trip_targets.py` | Unit tests for all trip target logic |

### Modified Files

| File | Changes |
|------|---------|
| `src/processing/weighting/specs.py` | Add `TripTargetSpec` dataclass |
| `src/processing/weighting/compute_weights.py` | Add `trip_targets: list[dict] \| None` parameter, parse to `TripTargetSpec` |
| `src/processing/weighting/weighting_pipeline.py` | Add `prepare_trip_targets()` phase, store trip target state, pass to balancer |
| `src/processing/weighting/balancing/balancer.py` | Modify `balance_weights()`, `_prepare_zone()`, `_build_incidence()` to accept and append trip target rows |
| `src/processing/weighting/diagnostics/` | Add trip target fit table to HTML report |
| `projects/bats_2023/config.yaml` | Add commented-out `trip_targets` example |

### Unchanged Files

| File | Why unchanged |
|------|---------------|
| `src/processing/weighting/controls/` | Trip targets bypass the control registry entirely |
| `src/processing/weighting/data_prep/control_data.py` | PUMS aggregation logic untouched |
| `src/processing/weighting/data_prep/seed_data.py` | Survey recoding untouched |
| `src/processing/weighting/data_prep/incidence.py` | HH-level pivot untouched |
| `src/processing/weighting/balancing/weight_propagation.py` | Weight propagation unchanged — operates on HH weights regardless of how they were computed |
| `src/processing/weighting/balancing/_np_balancer.py` | Numba solver untouched — it only sees numpy arrays |

---

## 11. Testing Strategy

### Unit Tests (`tests/test_weighting_trip_targets.py`)

| Test | What it verifies |
|------|------------------|
| `test_trip_target_spec_from_yaml` | Parse valid YAML → correct fields, boardings_per_trip division applied |
| `test_trip_target_spec_validation` | Invalid table, metric, geography raise ValueError |
| `test_compute_trip_incidence_count` | Count metric: correct per-HH transit trip counts |
| `test_compute_trip_incidence_sum` | Sum metric: correct per-HH distance totals |
| `test_compute_trip_incidence_filter` | Multi-filter AND logic works correctly |
| `test_compute_trip_incidence_missing_hh` | HHs with no matching trips get 0 |
| `test_allocate_region_targets` | Region target pro-rated correctly by zone share |
| `test_allocate_zone_targets` | Per-zone targets pass through directly |
| `test_allocate_zero_region_trips` | All-zero incidence → warning, target skipped |
| `test_balancer_with_trip_rows` | End-to-end: trip rows in incidence matrix, solver converges, weighted total within tolerance |
| `test_no_trip_targets_unchanged` | `trip_targets=None` → identical weights to baseline (backward compat) |

### Integration Verification

1. Run BATS 2023 pipeline **without** trip targets → save weights
2. Run BATS 2023 **with** trip targets → compare weights
3. Verify: demographic marginal fit degradation < 2% (MAPE)
4. Verify: trip target fit within 5% per zone
5. Verify: no convergence failures in zones that previously converged

---

## 12. Future Extensions

### Distribution Targets

Multiple `TripTargetSpec` entries can approximate distribution targets:

```yaml
trip_targets:
  - name: transit_hbw
    filter: { mode_type: [13], o_purpose_category: [1] }
    metric: count
    target: 80000
  - name: transit_hbo
    filter: { mode_type: [13], o_purpose_category: [2, 3] }
    metric: count
    target: 120000
```

This is equivalent to matching a transit-by-purpose distribution without
any framework changes.

### Day-of-Week Interaction

If day-of-week structuring (DEVELOPMENT_PLAN item 2) is implemented, trip
targets would need to be evaluated per DOW group. The `filter` mechanism
already supports this: add `travel_dow: [1,2,3,4,5]` for weekday targets.

### Per-Zone Boarding Factors

Some regions might benefit from different `boardings_per_trip` factors by
zone (urban core vs suburban). This could be added as
`boardings_per_trip_by_zone: dict[str, float]` without changing the core
architecture — just a per-zone target override.
