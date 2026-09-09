[← Back to Main README](../../../../README.md)

# DaySim Formatting

This module transforms canonical survey data into DaySim activity-based travel demand model format, applying model-specific coding schemes and data structures.

For detailed API documentation including function signatures, parameters, component descriptions, and data quality filters, see: [DaySim Formatting API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/pipeline_steps/format_output/daysim/)

The documentation includes:

- `format_daysim()` - Convert canonical data to DaySim model specification
- Complete component descriptions (Person, Household, Trip, Tour, Day formatting)
- Data quality filters (partial tours, missing TAZ, invalid tours)
- Implementation notes (integer codes, referential integrity, TAZ assignment)
- Usage examples
