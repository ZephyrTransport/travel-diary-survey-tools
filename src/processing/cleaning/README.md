[← Back to Main README](../../../README.md)

# Cleaning Pipeline Steps

This module contains project-specific data cleaning operations that prepare raw survey data for processing.

## Pipeline Steps

### `clean_2023_bats`

Custom cleaning and preprocessing steps for the 2023 Bay Area Travel Survey (BATS) dataset.

**Inputs:**
- `households`: Household table (pl.DataFrame)
- `persons`: Person table (pl.DataFrame)
- `days`: Person-day table (pl.DataFrame)
- `unlinked_trips`: Individual trip records (pl.DataFrame)

**Outputs:**
- Dictionary containing cleaned versions of:
  - `households`: With residence attributes added from persons
  - `persons`: Original persons table
  - `unlinked_trips`: Corrected and standardized trip records
  - `days`: With dummy days added for persons missing travel diary days

**Core Algorithm:**

1. **Trip Data Corrections:**
   - Rename `arrive_second` to `arrive_seconds` for consistency
   - Add time component columns if missing (hours, minutes, seconds)
   - Swap depart/arrive times when depart > arrive (reverses incorrect data entry)
   - Replace `-1` purpose codes with missing value code (996)
   - Recalculate missing distances using haversine formula from lat/lon coordinates
   - Recalculate missing durations from depart/arrive time differences

2. **Add Missing Person-Days:**
   - Identify persons without any day records in `days` table
   - Look up household travel day-of-week (`travel_dow`) from other household members
   - Create default day records with synthetic `day_id = person_id * 100 + travel_dow`
   - Append dummy days to ensure all persons have at least one day record

3. **Move Household Attributes:**
   - Extract `residence_rent_own` and `residence_type` from persons table
   - Aggregate at household level (mode of non-missing values)
   - Join household-level attributes to households table
   - Ensures household characteristics stored in correct table

**Notes:**
- This step handles survey-specific data quality issues before canonical processing
- Dummy days ensure pipeline compatibility for persons who didn't complete travel diaries
- Time/distance recalculation fills gaps in reported data
- Residence attribute migration corrects data structure for proper household-level analysis
