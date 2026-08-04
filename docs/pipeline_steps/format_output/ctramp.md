# CT-RAMP Formatting

## Overview

The `format_ctramp` step transforms canonical survey data (households, persons, tours, and trips)
into the file format used by
[CT-RAMP](https://github.com/BayAreaMetro/modeling-website/wiki/DataDictionary)
(Coordinated Travel – Regional Activity Modeling Platform), MTC's activity-based travel demand
model for the nine-county Bay Area.

This formatted output enables **direct comparison between observed survey behavior and
model-simulated behavior**: the survey tables and model output tables share an identical schema,
so any summary statistic computed on model output can be replicated on the survey to measure
how well the model reproduces observed travel patterns.

---

## Background: BATS vs. CT-RAMP

BATS (Bay Area Travel Study) is a GPS-tracked travel diary survey.  Respondents carry a device for
several days and their trips are logged automatically.  Raw survey data is therefore organized
around **individual trips**: each record is a single movement from one place to another.

CT-RAMP is a **tour-based** model.  Rather than simulating each trip in isolation, it groups trips
into *tours* — sequences of trips that begin and end at a fixed anchor location (usually home, or
the work place for at-work sub-tours).  Every stop along a tour is a trip in the raw survey, but
CT-RAMP treats the entire tour as the unit of analysis.

Bridging these two representations is the core job of the pipeline steps that precede
`format_ctramp`:

| Pipeline step | What it does |
|---|---|
| `load_data` | Reads raw BATS household / person / trip tables |
| `link_trips` | Resolves GPS traces into semantically distinct linked trips |
| `detect_joint_trips` | Identifies trips shared among household members |
| `extract_tours` | Groups linked trips into home-based and work-based tours |
| **`format_ctramp`** | **Translates canonical tours/trips into CT-RAMP schema** |

By the time `format_ctramp` runs, the canonical data is already in a tour-based structure that
mirrors CT-RAMP's expectations.  The remaining work is renaming columns, encoding categorical
variables, applying income-based segmentation, and filtering out households/persons that cannot be
meaningfully represented in the model.

---

## Output Tables

`format_ctramp` produces seven tables that exactly mirror the CT-RAMP model output files.
See [CT-RAMP Data Models](../../models/ctramp.md) for the table listing and field-level schema
documentation.

---

## Key Transformations

### Income segmentation

BATS collects income as a categorical bracket (e.g. `$50,000–$74,999`).  CT-RAMP encodes work tour
purpose as a function of household income — `work_low`, `work_med`, `work_high`, or
`work_very_high` — because higher-income workers have different mode choice sensitivities.

The pipeline converts the categorical bracket to a **dollar midpoint** (in survey-year 2023
dollars), converts it to year-2000 dollars (multiplying by `income_survey_year_to_ctramp_year`, since
CT-RAMP income categories are in year-2000 dollars), then classifies it against configurable
thresholds (set in the config via `income_low_threshold`, `income_med_threshold`,
`income_high_threshold`; see [CTRAMPConfig][processing.formatting.ctramp.ctramp_config.CTRAMPConfig]).
The thresholds are in year-2000 dollars, matching MTC CT-RAMP
`INCOME_SEGMENT_DOLLAR_LIMITS = {30000, 60000, 100000, MAX}`.

### Person type

CT-RAMP distinguishes eight person types that drive the model's choice structure (e.g., children
of non-driving age make no mode choice; retired persons have different activity patterns than
full-time workers).  Person type is derived from three BATS fields:

| BATS fields | CT-RAMP person type |
|---|---|
| `employment = EMPLOYED_FULLTIME` or `EMPLOYED_SELF` | Full-time worker (1) |
| `employment = EMPLOYED_PARTTIME` | Part-time worker (2) |
| University / vocational student (not employed FT/PT) | University student (3) |
| Not employed, not student, not retired, age ≥ 18 | Non-worker (4) |
| `employment = RETIRED` | Retired (5) |
| High-school student, age 16–17 | Student of driving age (6) |
| Elementary / middle-school student, or age 5–15 | Student of non-driving age (7) |
| Age < 5 | Child too young for school (8) |

### Purpose mapping

BATS uses a single `PurposeCategory` for all trips.  CT-RAMP needs more detail:

- **Work trips** are further split by income bracket (see above).
- **School trips** split into grade school, high school, and university based on `school_type`.
- **At-work sub-tours** (tours that begin and end at the work place rather than home) use dedicated
  CT-RAMP purposes: `atwork_business`, `atwork_eat`, `atwork_maint`.
- **Escort trips** split into `escort_kids` vs. `escort_no_kids` depending on whether the
  household contains school-age children.
- All remaining BATS purposes (`ERRAND`, `SHOP`, `MEAL`, `SOCIALREC`, etc.) map to direct
  CT-RAMP equivalents.

### Mode mapping

BATS records a single `ModeType` per trip.  CT-RAMP encodes 21 mode codes that distinguish
drive-alone, shared ride (2 or 3+), walk/bike, transit access/egress combinations (walk-to-bus,
drive-to-rail, etc.), taxi, and TNC.  Transit modes are further refined using access-egress
information recorded in the linked trip data.

### Joint tours

Tours detected by `detect_joint_trips` as shared among two or more household members are written to
the **joint** tour and trip tables rather than the individual ones.  The `joint_tours_ctramp` table
records tour composition (adults only, children only, or mixed) and the participating person list.

### Spatial fields (TAZ)

CT-RAMP operates on Transportation Analysis Zones (TAZ).  Home, work, and school locations are
snapped to TAZ centroids by the upstream `add_zone_ids` step.  `format_ctramp` selects which
TAZ geography to use (configurable via `taz_field`, e.g. `TAZ1454`) and optionally drops
households whose home location falls outside the model region (`drop_missing_taz: true`).

---

## Fields Not Derivable from Survey Data

Several CT-RAMP output fields are **model artefacts** that cannot be reconstructed from survey
responses.  These are omitted from the survey-formatted tables (left as `null`) and should be
ignored when using the survey files for calibration:

| Field | Why it is model-only |
|---|---|
| `walk_subzone` | Computed from network skims at run time |
| `humanVehicles` / `autonomousVehicles` | AV fleet composition is a model scenario input |
| `auto_suff` | Incorrectly coded in original spec; always ignored |
| `ao_rn`, `cdap_rn`, `imtf_rn`, … (all `*_rn` fields) | Random-number seeds set during Monte Carlo simulation |
| `value_of_time` | Calculated internally by the model from income and person type coefficients |
| `sampleRate` | Set equal to `1 / hh_weight` for survey records; actual model value is determined by the synthetic population sampling rate |

---

## Using the Output for Calibration

Once `format_ctramp` has run, the output CSVs can be loaded alongside CT-RAMP model output files
and compared field-by-field.  Common calibration checks include:

- **Activity pattern shares** (`activity_pattern` in `persons_ctramp`): compare % with mandatory /
  non-mandatory / home patterns between survey and model.
- **Tour frequency** (`imf_choice`, `inmf_choice`): check distributions of mandatory and
  non-mandatory tour counts per person.
- **Joint tour frequency** (`jtf_choice` in `households_ctramp`): compare households by joint tour
  type and count.
- **Mode shares** (`tour_mode`, `trip_mode`): compare mode distributions by purpose and person type.
- **Time-of-day** (`start_hour`, `end_hour` in tour tables): compare departure and arrival time
  distributions.

The `sampleRate` field in `households_ctramp` (= `1 / hh_weight`) allows these comparisons to be
made on a **weighted** basis, matching what the model sees from its synthetic population.

---

## Configuration Reference

See [CTRAMPConfig][processing.formatting.ctramp.ctramp_config.CTRAMPConfig] for all
configuration parameters.  Key parameters from `config_tm17_calibration.yaml`:

```yaml
- name: format_ctramp
  params:
    income_low_threshold: 30000     # below → work_low ($2000)
    income_med_threshold: 60000     # below → work_med ($2000)
    income_high_threshold: 100000   # below → work_high, above → work_very_high ($2000)
    income_survey_year_to_ctramp_year: 0.5319148936  # = 1 / 1.88
    age_adult: 4                    # AgeCategory enum value; 4 = AGE_18_TO_24+
    gender_default_for_missing: "f" # CT-RAMP requires binary gender
    taz_field: "TAZ1454"            # Which zone geography to use
    drop_missing_taz: true
    filter_zero_weight: true
```

---

## API Reference

::: processing.formatting.ctramp.format_ctramp
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_ctramp
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.ctramp_config
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - CTRAMPConfig
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_households
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_households
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_persons
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_persons
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_mandatory_location
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_mandatory_location
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_tours
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_individual_tour
        - format_joint_tour
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_trips
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_individual_trip
        - format_joint_trip
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.mappings
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - GENDER_MAP
        - EMPLOYMENT_TO_CTRAMP
        - PURPOSECATEGORY_TO_JTF_GROUP
        - ctramp_purpose_category_expression
        - ctramp_mode_expression
        - ctramp_person_type_expression
        - ctramp_student_category_expression
        - log_person_type_warnings
        - log_student_category_warnings
      filters:
        - "!^logger$"
        - "!^_"
