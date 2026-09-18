# SFCTA Legacy Pipeline Validation (BATS 2023)

Compares DaySim-format outputs from the **legacy SFCTA scripts**
(`archive/survey_processing/SFCTA/`) against the **new config-driven
pipeline**. The comparison is *exhaustive and spec-driven*: **every column of
every DaySim table** (enumerated from the pydantic models in
`src/data_canon/models/daysim.py`, never hand-listed) gets a legacy-vs-new
comparison, and every comparable column gets a plot. It covers record-level
column agreement, aggregate distributions (weighted with the legacy vendor
weights, and unweighted), record-level tour and **trip** matching, and
side-by-side person-diary traces through every stage of both pipelines. The
goal is to demonstrate the new pipeline can retire the bespoke legacy scripts.

The new pipeline's own raked weighting is intentionally out of scope here (a
future standalone report); this one holds weighting fixed at the legacy vendor
weights so differences reflect processing logic, not weighting method.

**Deliverable:** [`validation_report.html`](validation_report.html) — a
self-contained rendered document; open it directly, no data access or Python
environment required.

## Contents

| File | Role |
|---|---|
| `validation_report.qmd` | The report (Quarto source) |
| `spec.py` | The DaySim spec as data: enumerates every column from the pydantic models, classifies each (categorical / count / time / continuous / coord / zone / weight) from its field metadata, resolves value labels from the codebook enums, and builds the coverage & agreement table. Also the divergence registry (every flagged column's documented cause). **Pure** — no data or I/O. |
| `plots.py` | Plotly builders: one grouped-bar / distribution grid per (table, kind), the four-stage trip-structuring figure, and classification Sankeys |
| `helpers.py` | Loaders (with local parquet mirror of network CSVs), semantic normalizations, legacy code remap, weight derivations, raw-trip crosswalk, the record-level trip bridge + tour/trip matchers, aggregate builders, sanity checks |
| `diary.py` | Deterministic exemplar selection + stage-by-stage diary traces, rendered side-by-side (legacy vs new) |
| `run_vendor_weights.py` | Re-runs the new pipeline with the vendor weights the legacy scripts consume (`add_existing_weights` in place of `compute_weights`) — the apples-to-apples weighted basis |
| `.cache/` (gitignored) | Parquet mirror of M: files, crosswalk, trip bridge, vendor-run outputs |

## Reproducing

Requires access to the MTC `M:` share
(`\\models.ad.mtc.ca.gov\data\models`) and the raked-weights pipeline outputs
at `bats_2023/output/` (produced by `projects/bats_2023/run.py`). From the
repo root:

```bash
# 1. One-time: vendor-weights pipeline run (~10 min; reuses the step cache
#    at projects/bats_2023/.cache/bats_2023)
uv run python scripts/validate_sfcta/run_vendor_weights.py

# 2. Column classification (pure; prints how every spec column is compared)
uv run python scripts/validate_sfcta/spec.py

# 3. Sanity checks (also warms the local parquet mirror + trip bridge; ~2 min first time)
uv run python scripts/validate_sfcta/helpers.py

# 4. Render
quarto render scripts/validate_sfcta/validation_report.qmd
```

Set `VALIDATE_SFCTA_REFRESH=1` to re-mirror the network CSVs and rebuild the
crosswalk.

## Key methodology notes

- **The spec is the contract.** Every column comes from the DaySim pydantic
  models; the render *fails* (a sanity check) if either pipeline starts or stops
  emitting a spec column, so the report cannot silently fall out of date. Value
  labels come from the codebook enums, so they can't drift either.
- **Record linkage per table.** Households, persons and person-days join
  directly on the surviving IDs (`hhno` / `hhno,pno` / `hhno,pno,day`). Tours are
  matched on person-day plus all three anchor times. Trips are matched through a
  **linked-group bridge**: the legacy pipeline drops the survey's unique
  `trip_id`/`person_id` in `02a-reformat`, so trips are bridged by the raw-trip
  overlap of every legacy↔new linked-trip pair (mutual-best pairs), carrying both
  pipelines' full trip records so any column compares record-to-record.
- **Normalizations before comparison.** Legacy HHMM → minutes; TM2 → TM1
  `TAZ1454` for the household/person zone columns (trip/tour zones stay id-space,
  their geography validated via coordinates); legacy's `9` → `-1` missing code in
  `hownrent`/`hrestype`; and the legacy drive-to-transit two-row trip collapsed to
  one linked `mode=7` row (an invariant-checked transform).
- **Every flagged column has a documented cause** in the divergence registry
  (`spec.DIVERGENCES`), surfaced in both the coverage tables and the Divergences
  section — imputation (`pgend`, `hhincome`), the weight-source difference
  (`pdexpfac`/`trexpfac`/`toexpfac`), unimplemented skim-derived fields, the
  purpose-specific person-day counters the new pipeline zeroes, and tour
  primary-destination selection.
- **Directly-mapped vs derived.** Each column is tagged `mapped` (a 1:1 survey
  passthrough) or `derived` (computed). A mapping-integrity check compares the mapped
  attributes on non-missing legacy values, where they must agree exactly; shortfalls
  are surfaced for investigation (currently `hownrent`/`hrestype`).
- **The report is adversarial and has already paid for itself.** Being exhaustive
  caught two genuine DaySim-formatter bugs — `wbtours` wrongly equal to `hbtours`
  (subtour predicate) and `tripsh1`/`tripsh2` off by one — both since fixed in
  `src/processing/formatting/daysim/` (the canonical pipeline and CT-RAMP were
  unaffected).
- The legacy weight transform (`pdexpfac = person_weight /
  n_complete_TueThu_days`, zero-masked outside Tue–Thu) reconstructs exactly
  (100.00%) from raw survey columns, and its aggregate expansion is reproduced by
  the new pipeline via `add_existing_weights` configuration alone.

This work supersedes the exploratory comparison in
`projects/bats_2023/compare/daysim/`.
