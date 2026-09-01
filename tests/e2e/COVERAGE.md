# E2E Synthetic-Data Edge-Case Coverage

The synthetic dataset in `generate_toy_data.py` is a **branch-coverage fixture**: each
household is authored to exercise specific pipeline classification outputs, so the
end-to-end run guards the offline data-processing path (`load → link → detect_joint →
imputation → tours → cascade_completeness → add_zone_ids → format_ctramp → format_daysim →
write_data`) across a representative spread of scenarios.

**Weighting is excluded** — it needs PUMS microdata and control totals, so it is not
hermetic — and is covered by `tests/test_weighting_*.py` instead. That deferral only holds
while those tests reach the *entry points* the pipeline actually calls. They did not once:
`weight_sanity_checks` had no test, so when `_check_hierarchy` grew a required
`usability_flag_col` its caller was never updated and both projects died on the last line of
`compute_weights` with the suite green. `TestNoCallerOmitsTheFlag` in
`tests/test_weighting_sanity_checks.py` now walks the source for calls that drop that
argument, because a seam no test enters cannot be guarded by coverage.

Coverage is **enforced** by `TestEdgeCaseCoverage` in `test_e2e_pipeline.py` — those tests
fail if a scenario is removed and a bucket disappears.

## Bit-exact output baseline

`test_e2e_baseline.py` fingerprints every canonical table on the `full` profile and
compares it to `baselines/full.json`. **Any** change in any column fails, including a
correct one — that is the point. The suite otherwise answers "does it run" and "does
behaviour X hold", neither of which notices that a number moved; a batch of PRs altered
production output while the suite stayed green, and only a hand-run against real data
surfaced it weeks later.

Promoting an intended change:

```
E2E_BASELINE_UPDATE=1 uv run pytest tests/e2e/test_e2e_baseline.py
```

That rewrites the baseline; committing it puts the movement in the PR diff where a
reviewer can judge it.

**No record-level values are stored.** Each column contributes a hash plus aggregates —
null count, distinct count, numeric range and sum, and for low-cardinality
non-identifier columns a value-count histogram. The hash detects the change; the
aggregates make the failure readable (`tours.analysis_usable: counts {'True': 37} ->
{'True': 33}`) without putting data in the repository. Identifier and coordinate columns
never get a histogram. The fixture is synthetic, but the rule holds regardless so the
harness stays safe to point at real output.

Tables are sorted by their own unique key before hashing (`PRIMARY_KEYS` in
`baseline.py`). A non-unique sort key leaves ties in arbitrary order and reports drift
where the rows are the same set — worth knowing before adding a table.

Two limits found by deliberately breaking the pipeline and checking the gate fires: the
toy fixture does not discriminate `household_day_needs: all_members` from `nothing` (no
household has a partially-reporting day), and at `tour_closes_at: primary_home` the
`tour_category` term masks changes to the admitted quality codes. Scenarios covering
either would strengthen the fixture.

## Step-toggle profiles (parametrized)

Rather than fixed "2019 / 2023" configs, the suite builds the pipeline config
programmatically from a set of *enabled optional steps* and parametrizes over a
**leave-one-out matrix** (`conftest.PROFILES`). `TestStepToggling` runs for every profile and
verifies that toggling an optional step off/on does **not** break the downstream steps (the
pipeline still completes with valid, referentially-consistent output).

- **Mandatory** (always run): `load_data`, `link_trips`, `detect_joint_trips`, `extract_tours`,
  `add_zone_ids`, `write_data`.
- **Optional** (toggled): `detect_joint_trips`, `imputation`, `format_ctramp`, `format_daysim`.
- Profiles: `full`, `no_joint`, `no_imputation`, `no_ctramp`, `no_daysim`.

**Discovered step dependency:** `format_ctramp` consumes the `joint_trips` table (and emits
joint-tour/joint-trip CT-RAMP outputs), so it **requires** `detect_joint_trips`. Encoded in
`conftest._REQUIRES`; the `no_joint` profile therefore also drops `format_ctramp`. (`format_daysim`
and `extract_tours` both tolerate an absent `joint_trips`.) Without `detect_joint_trips`,
`format_ctramp._drop_missing_taz` raises `TypeError` on `len(joint_trips)` — a latent robustness
gap that is never hit in production because both real configs always run joint detection.

Feature-specific behaviours (imputation stash, edge-case coverage below) are asserted on the
`full_result` fixture (all optional steps on).

## Scenario → edge-case map

| HH | Scenario | Edge cases exercised |
|----|----------|----------------------|
| 1 | Simple car commuter | VALID/COMPLETE tour, WORK purpose, FT worker, activity M |
| 2 | Transit with mode-change | trip linking (4→2), access/egress legs, TRANSIT mode |
| 3 | 2-person joint (adults) | joint detection, composition ADULTS_ONLY |
| 4 | Multi-stop errands | non-mandatory day (activity N), inbound/outbound stops |
| 5 | Escort + school child (proxy) | ESCORT, child non-driving, proxy respondent |
| 6 | Work subtour (lunch) | at-work subtour: tour_type WORK_BASED, COMPLETE against its work anchor, emitted AT_WORK in CT-RAMP |
| 7 | Single-trip tour (no return) | tour_data_quality PARTIAL_DIARY_EDGE, SOCIALREC |
| 8 | Weekend 2 retirees | weekend day, joint, Retired person type |
| 9 | TNC user | TNC mode, zero-vehicle household |
| 10 | Bikeshare + part-time student | BIKESHARE mode, PT student |
| 11 | Multi-day traveler | 2 travel days for one person |
| 12 | Incomplete household | missing sentinels (gender/income/residence/race/eth/work_park), imputation input |
| 13 | Senior retiree couple (65+) | age 65-74 & 75-84, TAXI mode, under-25k income bin |
| 14 | 4-person family + teen + preschooler | 3+ person HH, age under-5 & 16-17, SCHOOL_BUS, self-employed, university-vs-school split |
| 15 | Car-free bike commuter | BIKE mode |
| 16 | University student + non-traveler | person_type UNIVERSITY_STUDENT, student_category College, activity M **and** H (person with a day but no trips) |
| 17 | PT worker with stops | PART_TIME_WORKER, outbound + inbound intermediate stops |
| 18 | Home→home loop | tour_data_quality NO_DESTINATION |
| 19 | Change-mode-only stops | tour_data_quality NO_DESTINATION, CHANGE_MODE purpose, >180 min no-link |
| 20 | Day never touches home | tour_data_quality PARTIAL_DIARY_EDGE, category PARTIAL_BOTH |
| 21 | Starts away, ends home | tour_category PARTIAL_START |
| 22 | Home→work→social | tour_category PARTIAL_END |
| 23 | 3-person joint with a child | 3-person clique, composition ADULTS_AND_CHILDREN |
| 24 | Two children to school together | composition CHILDREN_ONLY |
| 25 | Same trip 6h apart | joint negative control (temporal non-overlap → NOT joint) |

## Classification buckets covered (verified in outputs)

- **CT-RAMP person_type**: all 8 (FT worker, PT worker, university student, nonworker, retired, child driving-age, child non-driving, child under-5).
- **StudentCategory**: all 3 (College or higher, Grade or high school, Not a student).
- **activity_pattern**: M, N, H.
- **tour_data_quality**: VALID, PARTIAL_DIARY_EDGE, NO_DESTINATION (3 of 6). PARTIAL_OTHER_HOME, PARTIAL_DAY_SPLIT and SPATIAL_GAP need travel the generator does not produce and are unit-tested.
- **tour_category**: COMPLETE, PARTIAL_END, PARTIAL_START, PARTIAL_BOTH (all 4).
- **joint tour_composition**: ADULTS_ONLY, CHILDREN_ONLY, ADULTS_AND_CHILDREN (all 3).
- **modes**: walk, bike, bikeshare, taxi, TNC, car, school-bus, transit.

## Intentionally NOT covered (documented gaps)

- **tour_data_quality INDETERMINATE (4)** — a "cause unknown" diagnostic bucket that
  requires a contradictory first trip (tour-start detection failing on otherwise-valid
  data). Not representable with clean synthetic input.
- **Single geography** — one TAZ/MAZ polygon; no cross-zone trips or out-of-region/null-zone
  handling. Coordinates all fall in the one zone.
- **Some canonical `tour_purpose` values** (ESCORT/SOCIALREC/ERRAND as the *primary* purpose)
  are masked by higher-priority WORK/SCHOOL stops in the current tours; they are still
  exercised as intermediate stops and via the CT-RAMP purpose segmentation.
- **Rare modes** (ferry, carshare, scooter-share, long-distance) and the **85+** age band.
- **Imputation** exercises `income_bin` only (RF); other imputable columns carry a single
  missing value each. Imputation *quality* is covered by `tests/test_imputation.py`.
