# 2023 Transit Analysis

Quick-start example for analyzing transit trips from survey data.

## Running the Analysis

1. **Configure your data paths** in `config.yaml`:
   - Set `survey_dir` to your input data location
   - Set `output_dir` for results
   - Set `weights_dir` if using external weights

2. **Adjust trip linking parameters** (optional):
   ```yaml
   - name: link_trips
     params:
       change_mode_code: 11          # Purpose code for transfers
       transit_mode_codes: [12, 13, 14]  # Ferry, transit, long distance
       max_dwell_time: 180           # Max minutes between segments
       dwell_buffer_distance: 100    # Max meters between trip endpoints
   ```

3. **Run the pipeline**:
   ```bash
   python run.py
   ```

   The script reads `config.yaml` and executes the pipeline steps in order:
   - Loads data from configured paths
   - Cleans 2023 BATS data
   - Links multi-leg trips
   - Summarizes transit trips and generates O-D matrix

## What It Does

### 1. Trip Linking

Combines multi-leg journeys into single trips (e.g., walk → bus → walk = 1 linked trip).

Segments are linked when:
- Previous trip destination purpose is "change mode" (code 11)
- Time gap ≤ 180 minutes
- Distance between trip endpoints ≤ 100 meters

Creates `linked_trips` with aggregated distance, duration, and mode from all segments.

### 2. Transit Summarization

Calculates transit metrics:
- **Total weighted transit trips** (expected ~971,588)
- **Average boardings per trip** (counts transit mode segments per linked trip)
- **Origin-destination matrix** by county with row/column totals

Outputs summary statistics to console and returns `transit_summary` DataFrame.

## Pipeline Configuration

The `config.yaml` file controls the pipeline. Key sections:

```yaml
# Data paths (customize these)
survey_dir: "path/to/survey/data"
output_dir: "path/to/output"
weights_dir: "path/to/weights"

# Pipeline steps run in order
steps:
  - name: load_data
    params:
      input_paths:
        households: "{{ survey_dir }}/hh.csv"
        persons: "{{ survey_dir }}/person.csv"
        # ... (uses survey_dir variable)

  - name: link_trips
    params:
      change_mode_code: 11
      transit_mode_codes: [12, 13, 14]  # Ferry, transit, long distance
      max_dwell_time: 180
      dwell_buffer_distance: 100

  - name: summarize_transit_trips
    params:
      transit_mode_codes: [12, 13]  # Exclude long distance from analysis
```

The `run.py` script loads this config and executes each step using the Pipeline framework.
