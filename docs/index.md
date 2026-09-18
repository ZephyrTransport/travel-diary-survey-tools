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
Documentation for individual data processing steps, organized by pipeline stage.

 - Load Data
 - Link Trips
 - Joint Trips Detection
 - Extract Tours
 - Add zone IDs
 - Weighting
 - Formatting
    - DaySim
    - CTRAMP
    - ActivitySim [in development]

## Quick Links

- [Project README](https://github.com/BATS/travel-diary-survey-tools/blob/main/README.md)
- [Column Requirements](COLUMN_REQUIREMENTS.md)
- [Validation Documentation](VALIDATION_README.md)
