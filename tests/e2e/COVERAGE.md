# E2E coverage

`generate_toy_data.py` builds a synthetic survey in which each household exercises particular
pipeline outputs; its `# HH n` comments say which. `compute_weights` is not run, because it
fetches PUMS microdata; `add_existing_weights` is, from a household weight file.

## Classification buckets covered

`TestEdgeCaseCoverage` in `test_e2e_pipeline.py` fails if a bucket disappears.

- **CT-RAMP person type**: all 8.
- **StudentCategory**: all 3.
- **activity_pattern**: M, N, H.
- **tour_data_quality**: VALID, PARTIAL_DIARY_EDGE, NO_DESTINATION, SPATIAL_GAP (4 of 6).
- **tour_category**: COMPLETE, PARTIAL_END, PARTIAL_START, PARTIAL_BOTH (all 4).
- **joint tour_composition**: ADULTS_ONLY, CHILDREN_ONLY, ADULTS_AND_CHILDREN (all 3).
- **modes**: walk, bike, bikeshare, taxi, TNC, car, school bus, transit.

## Known gaps

- **A joint tour demoted below quorum.** It needs two members whose tours the clique detection
  groups yet whose verdicts differ; tours alike enough to be grouped share a verdict.
  Household 30 covers the trip-level case. Covered by the joint-quorum unit tests in
  `tests/test_usability_edge_cases.py`.
- **A tour whose endpoints have zones and whose middle leg has none.** It needs control over
  which stop becomes the primary destination. Covered by `tests/test_zone_coverage_cascade.py`.

## Intentionally not covered

- **tour_data_quality OTHER_HOME and PARTIAL_DAY_SPLIT**: a second home, and a trip chain
  resuming across diary days. The generator cannot represent either; both are unit-tested.
- **More than one zone**: there is one TAZ/MAZ polygon, so no trip crosses zones. Households
  27 and 28 sit outside it, which exercises the out-of-region handling.
- **ESCORT, SOCIALREC and ERRAND as a tour's primary purpose**: masked by WORK or SCHOOL stops
  in the current tours; they appear as intermediate stops.
- **Rare modes** (ferry, carshare, scooter share, long distance) and the **85+** age band.
- **Imputation of columns other than `income_bin`**. Imputation quality is covered by
  `tests/test_imputation.py`.

## Baseline

`test_e2e_baseline.py` compares a fingerprint of every canonical and formatted table of the
`full` profile with `baselines/full.json`. To promote an intended change:

```
E2E_BASELINE_UPDATE=1 uv run pytest tests/e2e/test_e2e_baseline.py
```

Two known limits: the toy survey does not tell `household_day_needs: all_members` from
`nothing` (no household has a partially reporting day), and at `tour_closes_at: primary_home`
the `tour_category` term masks changes to the admitted quality codes.
