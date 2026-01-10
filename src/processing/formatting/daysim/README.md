[← Back to Main README](../../../../README.md)

# DaySim Formatting Pipeline Steps

This module transforms canonical survey data into DaySim activity-based travel demand model format, applying model-specific coding schemes and data structures.

## Pipeline Steps

### `format_daysim`

Converts canonical survey tables to DaySim model specification format.

**Inputs:**
- `persons`: Person attributes in canonical format (pl.DataFrame)
- `households`: Household attributes in canonical format (pl.DataFrame)
- `unlinked_trips`: Individual trip segments (pl.DataFrame)
- `linked_trips`: Journey records (pl.DataFrame)
- `tours`: Tour records (pl.DataFrame)
- `days`: Person-day records (pl.DataFrame)
- `drop_partial_tours`: Remove incomplete tours (default: True)
- `drop_missing_taz`: Remove records without TAZ assignments (default: True)
- `drop_invalid_tours`: Remove tours failing validation (default: True)

**Outputs:**
- Dictionary containing DaySim-formatted tables:
  - `persons`: With person type, day pattern, and completeness flags
  - `households`: With household composition and income categories
  - `trips`: With DaySim mode, path type, and driver/passenger codes
  - `tours`: With DaySim purpose codes and timing
  - `days`: With day-level summaries

**Core Algorithm:**

This step orchestrates specialized formatting modules for each table type:

**1. Person Formatting** (`format_persons`)
- **Person Type Classification:**
  - Full-time worker, part-time worker, university student, etc.
  - Based on age, employment status, student status
- **Day Completeness:**
  - Mark complete travel days vs partial reporting
  - Calculate usual work/school days per week
- **Activity Patterns:**
  - Work at home frequency
  - School location type

**2. Household Formatting** (`format_households`)
- **Household Composition:**
  - Aggregate person types within household
  - Count workers, students, children by age
- **Income Processing:**
  - Categorize household income into DaySim bins
  - Handle missing values
- **Size and Type:**
  - Household size from person count
  - Household type from composition

**3. Trip Formatting** (`format_linked_trips`)
- **Mode Codes:**
  - Map canonical mode_type to DaySim mode codes
  - Distinguish auto modes (drive alone, shared ride)
- **Path Type:**
  - Derive path type from mode and occupancy
  - Special handling for transit access/egress
- **Driver/Passenger:**
  - Code driver vs passenger for auto trips
  - Link to household vehicle information

**4. Tour Formatting** (`format_tours`)
- **Purpose Mapping:**
  - Map canonical tour purposes to DaySim purpose codes
  - Person-type specific mappings (work, school, other)
- **Timing:**
  - Departure and arrival time categories
  - Duration calculation
- **Location:**
  - Map origin/destination to TAZ/MAZ
  - Handle missing locations

**5. Day Formatting** (`format_days`)
- **Day-Level Summaries:**
  - Tour count by purpose
  - Trip count by mode
  - Total travel time and distance

**Data Quality Filters:**
- **Partial Tours:** Optionally drop tours without return home
- **Missing TAZ:** Remove records without spatial assignment (required for model)
- **Invalid Tours:** Filter out tours failing validation rules
  - Zero distance tours
  - Negative duration
  - Tours with data quality flags

**Notes:**
- DaySim requires specific integer codes for categorical variables
- Formatting maintains referential integrity across tables
- TAZ (Traffic Analysis Zone) assignment critical for model application
- Person type classification affects downstream choice model applicability
- Mode/purpose hierarchies ensure consistent coding
- Output validates against DaySim data specifications
