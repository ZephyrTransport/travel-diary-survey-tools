[← Back to Main README](../../../README.md)

# Weighting Pipeline Steps

This module attaches existing weight values to survey data tables, with optional hierarchical weight derivation for tables without provided weights.

## Pipeline Steps

### `add_existing_weights`

Loads weight files and joins them to survey data tables. Can optionally derive missing weights by propagating values from upstream tables in the survey hierarchy.

**Inputs:**
- Survey tables (pl.DataFrame, all optional):
  - `households`: Household records
  - `persons`: Person records
  - `days`: Day records (if available)
  - `unlinked_trips`: Individual trip segments
  - `linked_trips`: Aggregated journey records
  - `joint_trips`: Joint household travel records
  - `tours`: Tour records
- `weights`: Dictionary mapping config keys to weight file specifications (dict[str, dict[str, str]])
  - Config keys: `household_weights`, `person_weights`, `day_weights`, `unlinked_trip_weights`, `linked_trip_weights`, `joint_trip_weights`, `tour_weights`
  - Each config must contain:
    - `weight_path`: Path to CSV file containing weights (required)
    - `id_col`: ID column name in main table (optional, uses default from mapping)
    - `weight_id_col`: ID column name in weight file (optional, defaults to `id_col`)
    - `weight_col`: Weight column name (optional, uses default from mapping)
- `derive_missing_weights`: Whether to derive weights for tables without provided weight files (bool, default: False)

**Outputs:**
- Dictionary containing all input tables with weight columns attached:
  - Weight column names: `hh_weight`, `person_weight`, `day_weight`, `unlinked_trip_weight`, `linked_trip_weight`, `joint_trip_weight`, `tour_weight`

**Weight Hierarchy:**

```
household_weight
  └─ person_weight (carry forward via hh_id)
      └─ day_weight (carry forward via person_id)
          └─ unlinked_trip_weight (carry forward via day_id)
              ├─ linked_trip_weight (mean aggregation via linked_trip_id)
              ├─ joint_trip_weight (mean aggregation via joint_trip_id)
              └─ tour_weight (mean aggregation via tour_id)
```

**Core Algorithm:**

**Phase 1: Load and Join Weights**
1. For each provided weight config:
   - Validate config key matches allowed table types
   - Load weight CSV file from `weight_path`
   - Validate required ID and weight columns exist
   - Handle ID column name mismatches between tables and weight files:
     - Rename weight file ID column to match table if needed
   - Left join weights to table on ID column
   - Track which tables now have weights

**Phase 2: Derive Missing Weights** (if `derive_missing_weights=True`)
1. **Hierarchical carry-forward** for household → person → day → unlinked_trip:
   - For each child table without provided weights:
     - Validate parent table has weights (error if missing - indicates gap)
     - Select parent's ID and weight columns
     - Rename parent weight to child weight name
     - Left join to child table on hierarchical key

2. **Aggregated weights** for linked_trip, joint_trip, tour:
   - For each target table without provided weights:
     - Skip if source table lacks weights or doesn't exist
     - Calculate mean weight per group (linked_trip_id, joint_trip_id, tour_id)
     - Exclude null and zero weights from mean calculation
     - Left join aggregated weights to target table

**Configuration Example:**

```yaml
- name: add_existing_weights
  params:
    derive_missing_weights: true
    weights:
      household_weights:
        weight_path: "weights/hh_weights.csv"
        # Uses defaults: id_col='hh_id', weight_col='hh_weight'

      person_weights:
        weight_path: "weights/person_weights.csv"

      unlinked_trip_weights:
        weight_path: "weights/trip_weights.csv"
        id_col: "trip_id"              # Custom ID in main table
        weight_id_col: "unlinked_trip_id"  # Custom ID in weight file
        weight_col: "trip_weight"       # Custom weight column name
```

**Notes:**
- **ID Column Flexibility:** Supports different ID column names between tables and weight files
- **Weight Column Customization:** Override default weight column names via config
- **Hierarchical Derivation:** Ensures consistent weights across related tables when only top-level weights provided
- **Gap Detection:** Raises errors if middle-tier weights missing (e.g., have household + trip but not person/day)
- **Aggregation Strategy:** Uses mean for deriving weights from multiple source records, excluding zeros and nulls
- **Weight Validation:** All required columns validated before joining to catch config errors early
