[← Back to Main README](../../../README.md)

# Tours Pipeline Steps

This module extracts hierarchical tour structures from linked trip data, identifying home-based tours, work-based subtours, and aggregating tour-level attributes.

## Pipeline Steps

### `extract_tours`

Builds tour and subtour structures from linked trip sequences using spatial and temporal patterns.

**Inputs:**
- `persons`: Person attributes including work/school locations (pl.DataFrame)
- `households`: Household attributes including home locations (pl.DataFrame)
- `unlinked_trips`: Individual trip segments (pl.DataFrame) - for reference
- `linked_trips`: Journey records with coordinates and timing (pl.DataFrame)
- `**kwargs`: Configuration parameters for TourConfig:
  - `distance_thresholds`: Dict of location type → distance threshold (meters)
  - `person_type_mapping`: Person type classification rules
  - `mode_hierarchy`: Mode priority for tour mode assignment
  - `purpose_hierarchy`: Purpose priority by person type

**Outputs:**
- Dictionary containing:
  - `linked_trips`: Trips with added `tour_id`, `subtour_id`, `half_tour` columns
  - `tours`: Aggregated tour records with purpose, mode, timing, and trip counts

**Core Algorithm:**

**Phase 1: Location Classification**
1. Prepare person location cache with home, work, and school coordinates
2. For each trip endpoint (origin and destination):
   - Calculate haversine distance to home location (all persons have home)
   - Calculate distance to work location (if person has workplace defined)
   - Calculate distance to school location (if person is student with school defined)
3. Classify location as HOME, WORK, SCHOOL, or OTHER based on distance thresholds
4. Add boolean flags: `o_is_home`, `d_is_home`, `o_is_work`, `d_is_work`, etc.

**Phase 2: Home-Based Tour Identification**
1. Sort trips by person_id, day_id, depart_time
2. Detect tour boundaries:
   - Tour starts: Departure from home (`o_is_home=True`, `d_is_home=False`)
   - Tour ends: Return to home (`o_is_home=False`, `d_is_home=True`)
   - Also consider day boundaries (first trip of person-day)
3. Assign sequential tour IDs within each person-day
4. Format: `tour_id = (day_id * 100) + tour_sequence_number`

**Phase 3: Anchor Period Expansion (for Subtour Detection)**
1. For tours visiting usual anchor locations (work or school):
   - Find first arrival at anchor location
   - Find last departure from anchor location
   - Mark all trips in between as "at anchor period"
2. Uses Polars window functions to expand anchor presence across trip sequences
3. Prevents subtours from being detected during travel TO/FROM anchor
4. Generalizable design supports work, school, or future anchor types

**Phase 4: Anchor-Based Subtour Detection**
1. Within anchor periods, identify subtour boundaries:
   - Subtour starts: Departure from anchor (`o_at_anchor=True`, `d_at_anchor=False`)
   - Subtour ends: Return to anchor (`o_at_anchor=False`, `d_at_anchor=True`)
2. Assign hierarchical subtour IDs
3. Format: `subtour_id = (tour_id * 10) + subtour_sequence_number`
4. Currently supports work-based subtours (extensible to school-based)

**Phase 5: Tour Attribute Aggregation**
1. Group trips by tour_id (and subtour_id for subtours)
2. Compute tour-level attributes:
   - `tour_purpose`: Highest priority destination purpose (person-type specific hierarchy)
   - `tour_mode`: Highest priority travel mode (from configurable mode hierarchy)
   - `origin_depart_time`: First trip's departure time
   - `dest_arrive_time`: Last trip's arrival time
   - `trip_count`: Number of trips in tour
   - `stop_count`: Number of intermediate stops (trip_count - 1)
3. Assign half-tour classification:
   - "outbound": Trips before primary destination
   - "inbound": Trips after primary destination
   - "subtour": Work-based subtour trips

**Configuration via TourConfig:**
- `distance_thresholds`: Controls location classification sensitivity
  - home: 100m, work: 200m, school: 200m (defaults)
- `person_type_mapping`: Rules for classifying person types (full-time worker, student, etc.)
- `mode_hierarchy`: Priority order for determining tour mode from multiple trip modes
- `purpose_hierarchy`: Purpose priority by person type (work trips prioritized for workers, etc.)

**Notes:**
- Hierarchical tour structure: Home-based tours → Work-based subtours
- Handles incomplete tours (no return home) at end of travel day
- Location classification robust to GPS/geocoding errors via distance thresholds
- Tour purpose reflects primary activity, not intermediate stops
- Extensible design allows future additions (school-based subtours, other anchor types)
