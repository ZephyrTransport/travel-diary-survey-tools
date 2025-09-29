# Daysim Pipeline - Travel Survey Data Processing

This directory contains a comprehensive pipeline for processing travel diary survey data into Daysim activity-based travel demand model inputs. The pipeline transforms raw survey responses through multiple stages of data cleaning, geographic processing, and pattern recognition to generate model-ready datasets.

## Overview

The Daysim pipeline processes multi-day travel survey data to extract daily travel patterns and tours (round trips) that serve as inputs for activity-based travel demand modeling. The pipeline is designed to handle complex survey data with multiple transportation modes, incomplete responses, and varying geographic coverage.

## Pipeline Steps

### Step 0: Data Preprocessing (00-preprocess.py)
**Purpose**: Initial data cleaning and standardization
- Copies raw CSV files with minor modifications
- Adds calculated time fields (depart_time, arrive_time) to trip data
- Links person_id references between trip and location tables
- Ensures consistent data formats for downstream processing

**Input**: Raw survey CSV files (trip, location, day, household, person, vehicle)
**Output**: Standardized CSV files in 00-preprocess/ directory

### Step 1: Spatial Joining (01-taz_spatial_join.py)
**Purpose**: Geographic integration with transportation analysis zones
- Spatially joins survey locations to Traffic Analysis Zones (TAZ) and Micro Analysis Zones (MAZ)
- Supports multiple agency models (SFCTA CHAMP with TAZ/MAZ, MTC TM1 with TAZ only)
- Uses nearest neighbor matching with distance buffers to handle geographic gaps
- Adds zone identifiers to households (home), persons (work/school), and trips (origin/destination)

**Input**: Preprocessed CSV files + geographic zone files
**Output**: Spatially joined CSV files in 01-taz_spatial_join/ directory

### Step 2a: Data Reformatting (02a-reformat.py)
**Purpose**: Transform survey data to Daysim model format
- Maps survey categories to Daysim coding schemes for demographics, employment, and travel
- Applies Bay Area geographic filters (9-county region)
- Processes income, dwelling types, and person types according to Daysim specifications
- Converts travel modes, purposes, and timing to Daysim codes
- Calculates survey completeness metrics for weighting

**Input**: Spatially joined CSV files
**Output**: Daysim-formatted CSV files in 02a-reformat/ directory

### Step 2b: Trip Linking (02b-link_trips_week.py)
**Purpose**: Link related trips representing single journeys
- Identifies and merges trips that are part of multi-modal journeys
- Handles access/egress trips for transit (park-and-ride, walk-to-transit)
- Links sequential trips with "change mode" purposes based on activity duration thresholds
- Preserves highest-level transportation mode for linked trips
- Generates continuous trip numbering after linking

**Input**: Reformatted trip data from step 02a
**Output**: Linked trip data in 02b-link_trips_week/ directory

### Step 3a: Tour Extraction (03a-tour_extract_week.py)
**Purpose**: Extract travel patterns and organize into tours
- Identifies tours (round trips starting/ending at same location)
- Classifies home-based tours and work-based subtours
- Determines primary tour purposes and timing patterns
- Organizes trips into tour stops and calculates tour-level characteristics
- Generates comprehensive person-day activity patterns
- Creates weighted outputs for model estimation

**Input**: Linked trip data from step 02b + reformatted person/household data
**Output**: Tour, trip, person-day, person, and household files in 03a-tour_extract_week/ directory

### Step 3b: Day Assignment and Weighting (03b-assign_day.py)
**Purpose**: Assign specific days and apply survey weights
- Assigns day-of-week to tours based on constituent trip patterns
- Applies person-day weights based on survey completeness
- Focuses on representative weekdays (typically Tuesday-Thursday)
- Generates final weighted datasets for model estimation

**Input**: Tour/trip data from step 03a + person completeness data
**Output**: Day-assigned, weighted Daysim input files in 03b-assign_day/ directory

## Configuration

The pipeline uses TOML configuration files that specify:
- Input/output directory paths for each step
- Survey file names and formats
- Agency-specific model parameters (SFCTA_CHAMP vs MTC_TM1)
- Geographic zone file paths
- Weighting options and parameters

Example configuration files:
- `pipeline_config_sfcta.toml` - SFCTA CHAMP model configuration
- `pipeline_config_mtc.toml` - MTC TM1 model configuration

## Usage

Run each script in sequence with the appropriate configuration file:

```bash
python 00-preprocess.py pipeline_config_mtc.toml
python 01-taz_spatial_join.py pipeline_config_mtc.toml
python 02a-reformat.py pipeline_config_mtc.toml
python 02b-link_trips_week.py pipeline_config_mtc.toml
python 03a-tour_extract_week.py pipeline_config_mtc.toml
python 03b-assign_day.py pipeline_config_mtc.toml
```

## Key Concepts

**Tours**: Round trips that start and end at the same location, typically home. Tours represent complete activity episodes and are the fundamental unit of analysis in activity-based modeling.

**Person Types**: Demographic/employment categories used in Daysim:
1. Full-time worker
2. Part-time worker  
3. Non-working adult (65+)
4. Non-working adult (<65)
5. University student
6. High school student (16+)
7. Child (5-15)
8. Child (0-4)

**Mode Hierarchy**: Transportation modes in Daysim priority order:
1. Walk
2. Bike
3. Drive alone (SOV)
4. Drive with others (HOV2)
5. Drive with 3+ people (HOV3+)
6. Walk-to-transit
7. Drive-to-transit
8. School bus
9. Transportation Network Company (TNC)

**Survey Weighting**: Person-day weights account for differential response rates across survey days and demographic groups, ensuring representative samples for model estimation.

## Output Files

The final output includes standard Daysim input files:
- **hh.csv**: Household characteristics (income, vehicles, dwelling type)
- **person.csv**: Person demographics and usual locations (work, school)
- **personday.csv**: Daily activity patterns and tour summaries
- **tour.csv**: Tour-level characteristics (purpose, timing, mode, stops)
- **trip.csv**: Individual trip records with origins, destinations, and attributes

## Dependencies

- pandas: Data manipulation and analysis
- polars: High-performance data processing (used in reformatting)
- geopandas: Spatial data operations
- numpy: Numerical computing
- tomllib: TOML configuration file parsing
- pathlib: Cross-platform path handling

## Notes

- The pipeline is optimized for Bay Area travel surveys but can be adapted for other regions
- Algorithm parameters (time thresholds, distance buffers) may need adjustment for different survey designs
- Survey weighting focuses on weekday patterns (typically Tue-Thu) for representative travel demand modeling
- Complex transit trips may be split into access/main transit/egress components during processing