[← Back to Main README](../../../README.md)

# Link Pipeline Steps

This module links individual trip segments into complete journey records (linked trips), aggregating sequential trips made during mode changes or transfers.

## Pipeline Steps

### `link_trips`

Links unlinked trip segments into complete journey records by detecting mode changes and aggregating trip chains.

**Inputs:**
- `unlinked_trips`: Individual trip records (pl.DataFrame)
  - Required columns: person_id, day_id, depart_time, arrive_time, o/d locations, o/d purposes, mode_type
- `change_mode_code`: Purpose code indicating a mode change (int)
- `transit_mode_codes`: List of mode codes that count as transit (list[int])
- `max_dwell_time`: Maximum time gap between trips to link them, in minutes (default: 120)
- `dwell_buffer_distance`: Maximum spatial distance between trips to link, in meters (default: 100)

**Outputs:**
- Dictionary containing:
  - `unlinked_trips`: Original trips with added `linked_trip_id` column
  - `linked_trips`: Aggregated journey records with combined attributes

**Core Algorithm:**

**Phase 1: Link Trip IDs**
1. Sort unlinked trips by person, day, and departure time
2. For each person-day sequence:
   - If previous trip's destination purpose is `change_mode_code`, continue the current linked trip
   - Validate spatial/temporal continuity:
     - Time gap between trips ≤ `max_dwell_time` minutes
     - Distance between previous destination and current origin ≤ `dwell_buffer_distance` meters
   - Otherwise, start a new linked trip
3. Assign globally unique `linked_trip_id` = (day_id * 1000) + sequence_number

**Phase 2: Aggregate Linked Trips**
1. Group unlinked trips by `linked_trip_id`
2. For each linked trip, aggregate:
   - **Origin/Destination:** First trip's origin, last trip's destination
   - **Timing:** First depart_time, last arrive_time
   - **Distance:** Sum of all trip distances
   - **Duration:** Sum of all trip durations (including dwell time)
   - **Purposes:** First origin purpose, last destination purpose
   - **Mode Logic:**
     - If any trip uses transit → `mode_type = TRANSIT`
     - Otherwise, use mode of longest distance trip
   - **Transit Details:** Count boarding/alighting, aggregate access/egress modes
   - **Driver/Passenger:** Aggregate from component trips
3. Create `trip_list` array containing all component trip IDs

**Notes:**
- Links trips when travelers make intermediate stops for mode changes or transfers
- Preserves full trip detail in `unlinked_trips` while creating journey-level `linked_trips`
- Transit detection ensures multi-modal journeys classified correctly
- Access/egress mode mapping converts trip modes to transit-specific codes
- Spatial/temporal thresholds prevent false linkages across separate journeys
