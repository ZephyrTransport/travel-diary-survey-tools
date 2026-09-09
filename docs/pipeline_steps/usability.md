# Completeness & Usability

The single place the canonical completeness and usability flags are computed.
The module docstring below carries the flag definitions, the derivation-order
diagram, and the per-level rule table; the pipeline step
(`cascade_completeness`) stamps them once so every downstream consumer only
reads the result.

Two kinds of flag, computed side by side:

- `survey_complete` — survey reporting completeness (vendor-provided at the day level,
  possibly adjusted by the project cleaner), cascaded through
  household ↔ person ↔ day ↔ trip/tour so it is internally consistent. One
  answer per run, never configurable.
- **one column per usability profile** — the subset of `survey_complete` that profile
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
  ctramp:
    tour_closes_at: primary_home
    household_day_needs: all_members
  analysis:
    tour_closes_at: anywhere
    household_day_needs: nothing
```

## One profile, several column families

A profile is a name. Every column belonging to it is that name suffixed onto a
family, so the family says what kind of thing a column is and the suffix says
whose:

| family | column | written by |
|---|---|---|
| `usable_` | `usable_ctramp` | the per-record verdict, from this step |
| `hh_day_` | `hh_day_ctramp` | whether every surveyable member's day passed, that date |
| `hh_weight_`, `day_weight_`, … | `hh_weight_ctramp` | the weight fitted over that profile's universe |
| `base_weight_` | `base_weight_ctramp` | the seed weight its fit started from |

Two profiles can therefore never write the same column, whatever they are
called: the families have disjoint prefixes, and a name is unique within one.

`survey reporting completeness` sits outside all of it. `survey_complete` and
`hh_day_survey_complete` record what the survey collected rather than what a model will
take, so they carry no family prefix and are reserved — a profile may not take
either name.

Downstream consumers name the profile they honour via `usability_profile` — the
weighting, the CT-RAMP formatter and the DaySim formatter each take it, and none
re-derive the verdict: the column is read, and its absence raises.

That one name is the whole address. It resolves to `usable_<profile>` for the
records a consumer keeps and to `hh_weight_<profile>` for the weights it reads,
so there is no second setting that could disagree with the first. A consumer
naming a profile the weighting was not asked to fit raises rather than falling
back to an unsuffixed column, since that column describes some other universe, or
none.

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
