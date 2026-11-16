[← Back to Main README](../../../README.md)

# Habitual Locations

Each person's anchor places: their home(s), workplace(s) and school(s). Tour
extraction reads them to decide where tours start and end, and which stops a
subtour leaves from.

Full API documentation: [Detect Habitual Locations](https://bayareametro.github.io/travel-diary-survey-tools/pipeline_steps/detect_habitual_locations/)

## Where it runs

```
clean_<project>            delivers habitual_locations: the reported locations
link_trips
detect_habitual_locations  appends the observed locations; writes habitual_location_days
extract_tours              reads habitual_locations
```

## What survey cleaning delivers

Each project's cleaning step returns `habitual_locations` built with
`reported_habitual_locations(households, persons, extra=...)`:

- The survey's home (household), work and school (person) coordinates become each
  person's primary locations.
- `extra` takes any further reported locations, such as a vendor's second
  home. One within the buffer of its kind's primary is dropped.
- Every row is `REPORTED`, numbered and identified.

The primary rows repeat the coordinate columns (`home_lat/lon`, `work_*`,
`school_*`), which other steps still read. Validation fails if they disagree.

## What the step adds

| Kind | Evidence | Rule |
|---|---|---|
| Workplace | Stops with a work purpose | 90 min or longer for the primary-workplace purpose, 4 h for work-related, clustered within the buffer |
| School | Stops with a school purpose | 90 min or longer (college 45), clustered within the buffer |
| Home | Days the respondent said began or ended at home or their other home | Only with `include_observed_homes: true`. Placed at that day's first origin or last destination, clustered within the buffer |

- Observed homes are an analyst's choice, with no default. Off, the only homes
  are the reported primary and second homes. On, a "home" answer away from the
  reported home becomes another home too, and tours touching it are graded
  `OTHER_HOME`.
- A cluster within the buffer of a reported location of the same kind is that
  location, and is not added.
- Homes are never found from travel alone.
- A work-related stop of a few hours is a meeting, not a place of work; only a
  stay the length of a working day counts.
- Observed rows are numbered after the delivered ones, in the order the person
  was first seen there. Delivered rows are never changed.
- The step refuses a delivered row that is not `REPORTED`, and two primaries of
  one kind.

## When a trip end is at a location

Within the buffer **and** its purpose agrees with the location's kind. An
unknown purpose agrees with nothing. A work-related purpose agrees with a
workplace: it says the person is working, and the distance says which workplace.
At a day's first origin and last destination, the respondent's answer to where
the day began or ended also counts, for homes only.

One exception: within `at_address_meters` (100 m) of their *primary* home, any
purpose is at that home. At their own door the purpose describes the activity —
a walk, working from home, dropping someone off — not a different place, and
refusing it welds two tours into one. Other homes are placed from trip ends in
the first place, so they still need the purpose or the day answer.

Nothing is recoded: a work-related stop stays work-related.

`extract_tours` asks the same question through `match_trip_ends`.

## Configuration

Each step has its own `buffer_meters`: one for finding habitual locations, one for
matching tour trip ends to them. They are separate settings. A project that wants
them equal sets both from one template value, as bats_2023 does:

```yaml
habitual_buffer_meters: "300"
steps:
  - name: detect_habitual_locations
    params:
      buffer_meters: "{{ habitual_buffer_meters }}"
      include_observed_homes: true  # required: add homes from stated day starts/ends
      # at_address_meters: 100
      # min_dwell_minutes: 90
      # min_dwell_minutes_by_purpose: {COLLEGE: 45, WORK_ACTIVITY: 240}
  - name: extract_tours
    params:
      buffer_meters: "{{ habitual_buffer_meters }}"  # extract_tours' habitual_match
```

## Outputs

| Table | Grain |
|---|---|
| `habitual_locations` | One row per person, kind and number: reported first, then observed |
| `habitual_location_days` | One row per location per day the person was there |

## Modules

| Module | Holds |
|---|---|
| `detect_habitual_locations.py` | The pipeline step |
| `habitual_location_configs.py` | The rules |
| `reported.py` | The delivered table, built from the survey's coordinates |
| `episodes.py` | Trips as stays |
| `observed.py` | Clustering, and the observed locations |
| `numbering.py` | Primacy, numbering and identifiers |
| `matching.py` | The one "at a location" test |
| `location_days.py` | The per-day table |
