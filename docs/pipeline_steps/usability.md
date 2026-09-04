# Completeness & Usability

The single place the canonical completeness and usability flags are computed.
The module docstring below carries the flag definitions, the derivation-order
diagram, and the per-level rule table; the pipeline step
(`cascade_completeness`) stamps them once so every downstream consumer only
reads the result.

Two kinds of flag, computed side by side:

- `complete` — survey reporting completeness (vendor-provided at the day level,
  possibly adjusted by the project cleaner), cascaded through
  household ↔ person ↔ day ↔ trip/tour so it is internally consistent. One
  answer per run, never configurable.
- **one column per usability profile** — the subset of `complete` that profile
  admits. A project names its profiles in config, so several standards can
  coexist in one run and a column's meaning reads off the config.

A profile answers two axes, both explicitly, neither with a default:

| axis | values | meaning |
|------|--------|---------|
| `tour_closes_at` | `primary_home` | only a tour that returns to the home it left |
| | `any_home` | also one closing at another home of this person's |
| | `anywhere` | also the diary-edge and day-split open ends |
| `household_day_needs` | `all_members` | every surveyable member's day complete on that date |
| | `nothing` | no household-date requirement |

No setting admits a tour with *missing data* (`NO_DESTINATION`, `SPATIAL_GAP`):
those are not open ends, and no tolerance makes a missing leg present.

```yaml
usability_profiles:
  ctramp_usable:
    tour_closes_at: primary_home
    household_day_needs: all_members
  analysis_usable:
    tour_closes_at: anywhere
    household_day_needs: nothing
```

Downstream consumers name the profile they honour via `usability_flag_col` —
the weighting, the CT-RAMP formatter and the DaySim formatter each take it, and
none re-derive the verdict: the column is read, and its absence raises.

The name is also the address of that profile's *weights*. A weighting run given
`weight_profiles` fits each profile separately and suffixes its columns with the
profile's name (`hh_weight_ctramp_usable`), so `usability_flag_col` settles both
which records a consumer keeps and which weights it reads — there is no second
setting that could disagree with the first. A consumer naming a profile the
weighting was not asked to fit raises rather than falling back to an unsuffixed
column, since that column describes some other universe, or none.

::: processing.completeness
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - cascade_completeness
        - parse_usability_profiles
        - UsabilityProfile
        - cascade_complete
        - stamp_usable
        - compute_usability
        - rollup_completeness
        - flag_household_day_complete
      filters:
        - "!^logger$"
        - "!^_"
