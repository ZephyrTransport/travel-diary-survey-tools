[← Back to Main README](../../../../README.md)

# DaySim Formatting

This module transforms canonical survey data into DaySim activity-based travel demand model format, applying model-specific coding schemes and data structures.

For detailed API documentation including function signatures, parameters and component descriptions, see: [DaySim Formatting API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/pipeline_steps/format_output/daysim/)

The documentation includes:

- `format_daysim()` - Convert canonical data to DaySim model specification
- Complete component descriptions (Person, Household, Trip, Tour, Day formatting)
- Record selection: the step keeps the records its `usability_profile` admits, so partial tours and records without a TAZ are left out by the profile, not by the formatter
- Implementation notes (integer codes, referential integrity, TAZ assignment)
- Usage examples
