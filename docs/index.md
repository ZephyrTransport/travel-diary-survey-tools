# Travel Diary Survey Tools

Documentation for travel diary survey data processing tools.

## Overview

This project provides tools to process and analyze travel diary survey data with standardized data models and validation.

## Documentation Structure

### [Codebook](codebook.md)
Enumerated value labels and coding schemes for survey data fields. Includes definitions for:

- Trip purposes, modes, and characteristics
- Person demographics and employment
- Household attributes
- Tour patterns
- Model-specific codes (DaySim, CTRAMP)

### [Data Models](models/index.md)
Pydantic data models for validation and processing:

- Survey data models (households, persons, trips, tours)
- Model-specific output formats (DaySim, CTRAMP)
- Validation rules and constraints

### [Pipeline Steps](pipeline_steps/index.md)
Documentation for each processing step, in the order the project configs run them:

- [Load Data](pipeline_steps/read_write.md)
- [Cleaning](pipeline_steps/cleaning.md)
- [Imputation](pipeline_steps/imputation.md)
- [Link Trips](pipeline_steps/link_trips.md)
- [Detect Habitual Locations](pipeline_steps/detect_habitual_locations.md)
- [Detect Joint Trips](pipeline_steps/detect_joint_trips.md)
- [Extract Tours](pipeline_steps/extract_tours.md)
- [Add Zone IDs](pipeline_steps/add_zone_ids.md)
- [Completeness & Usability](pipeline_steps/usability.md)
- [Weighting](pipeline_steps/weighting/index.md)
- [Final Check](pipeline_steps/final_check.md)
- Formatting
    - [CT-RAMP](pipeline_steps/format_output/ctramp.md)
    - [DaySim](pipeline_steps/format_output/daysim.md)
- [Write Data](pipeline_steps/read_write.md)

## Quick Links

- [Project README](https://github.com/BayAreaMetro/travel-diary-survey-tools/blob/main/README.md)
- [Column Requirements](COLUMN_REQUIREMENTS.md)
- [Validation Documentation](VALIDATION_README.md)
