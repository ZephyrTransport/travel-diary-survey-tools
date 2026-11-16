# Completeness & Usability

The single place the canonical completeness and usability flags are computed.
"Flag definitions" below carries the definitions, the derivation-order
diagram, and the per-level rule table; the pipeline step
(`cascade_usability`) stamps them once so every downstream consumer only
reads the result.

Two kinds of flag, computed side by side:

- `survey_complete` — survey reporting completeness (vendor-provided at the day level,
  possibly adjusted by the project cleaner), cascaded through
  household ↔ person ↔ day ↔ trip/tour so it is internally consistent. One
  answer per run, never configurable.
- **one column per usability profile** — the subset of `survey_complete` that profile
  admits. A project names its profiles in config, so several standards can
  coexist in one run and a column's meaning reads off the config.

A profile answers five axes, all explicitly, none with a default:

| axis | values | meaning |
|------|--------|---------|
| `tour_closes_at` | `primary_home` | only a tour that returns to the home it left |
| | `any_home` | also one closing at another home of this person's |
| | `anywhere` | also the diary-edge and day-split open ends |
| `household_day_needs` | `all_members` | every surveyable member's day complete on that date |
| | `nothing` | no household-date requirement |
| `zone_coverage` | a `zone_name` that `add_zone_ids` builds | every location on the record has a zone in that system |
| | `none` | no geographic requirement |
| `joint_tours` | `fully_joint` | everyone on a joint tour shares every leg, as activity-based models draw it; work, school and escort tours are never joint |
| | `partial` | the joint groups the survey detected, as they are |
| `travel_days` | a list of day names, `monday` to `sunday` | only days on these days of the week; a day on any other is unusable, with its tours and trips |

No setting admits a tour with *missing data* (`NO_DESTINATION`, `SPATIAL_GAP`):
those are not open ends, and no tolerance makes a missing leg present.

```yaml title="Example"
- name: cascade_usability
  params:
    usability_profiles:
      ctramp:
        tour_closes_at: primary_home
        household_day_needs: all_members
        zone_coverage: TAZ1454
        joint_tours: fully_joint
        travel_days: [tuesday, wednesday, thursday]
      analysis:
        tour_closes_at: anywhere
        household_day_needs: nothing
        zone_coverage: none
        joint_tours: partial
        travel_days: [monday, tuesday, wednesday, thursday, friday, saturday, sunday]
```

## One profile, several column families

A profile is a name. Every column belonging to it is that name suffixed onto a
family, so the family says what kind of thing a column is and the suffix says
whose:

| family | column | written by |
|---|---|---|
| `usable_` | `usable_ctramp` | the per-record verdict on every table, joint tables included, from this step. On `household_days` it says whether every surveyable member's day passed that date. |
| `joint_tour_id_`, `joint_trip_id_` | `joint_tour_id_ctramp` | the joint group a record is on for that profile, null where the profile splits it off |
| `hh_weight_`, `person_day_weight_`, … | `hh_weight_ctramp` | the weight fitted over that profile's universe |
| `base_weight_` | `base_weight_ctramp` | the seed weight its fit started from |

Two profiles can therefore never write the same column, whatever they are
called: the families have disjoint prefixes, and a name is unique within one.

`survey reporting completeness` sits outside all of it. `survey_complete` records
what the survey collected rather than what a model will take, so it carries no
family prefix and is reserved — a profile may not take the name.

## Household-days

A household-day is one household's travel date: every member's person-day on it.
The vendor sends no household-day file, so each project's cleaner builds the
`household_days` table from the person-days, with
`utils.create_ids.create_household_days`. Its id, `hh_day_id`, is
`hh_id * 100 + day_num`, the household's counterpart of `person_day_id`. Every
person-day, joint trip and joint tour carries the `hh_day_id` it belongs to.

This step writes two kinds of column onto it:

| column | meaning |
|---|---|
| `survey_complete` | every surveyable member's day on that date is complete, as the vendor judged each one. False when no surveyable member has a day on it. |
| `usable_<profile>` | every surveyable member's day on that date is usable under the profile |

A profile with `household_day_needs: all_members` reads them: a person-day is
usable only on a complete household-day, and a household only with a usable one.
An unsurveyable member's own day takes no part in the household-day and keeps its
own verdict. The table carries no weight.

Downstream consumers name the profiles they honour: the weighting takes
`weight_profiles`, one weight set per profile, and the CT-RAMP and DaySim
formatters each take `usability_profile`. None re-derive the verdict: the
column is read, and its absence raises.

That one name is the whole address. It resolves to `usable_<profile>` for the
records a consumer keeps and to `hh_weight_<profile>` for the weights it reads,
so there is no second setting that could disagree with the first. A consumer
naming a profile the weighting was not asked to fit raises rather than falling
back to an unsuffixed column, since that column describes some other universe, or
none.

## Exclusions

A project cleaner drops a household, person or day by giving it an
`exclusion_reason`. An excluded record is unusable under every profile, and so
is everything below it; a household left with no usable day falls too.
`survey_complete` is untouched, since it records what the survey collected.

Under `household_day_needs: all_members`, an excluded day also makes its
household-day incomplete, so the household's other days on that date are
unusable. An excluded person's days fail the household-day's usable check, but
the other members' days on that date keep their own verdict.

## Flag definitions

::: processing.usability
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

## The step

::: processing.usability.cascade_usability.cascade_usability
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Settings

<!-- step-settings: cascade_usability -->

Code: [processing.usability](../reference/processing/usability.md)
