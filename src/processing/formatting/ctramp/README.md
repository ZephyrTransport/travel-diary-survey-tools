[← Back to Main README](../../../../README.md)

# CT-RAMP Formatting

This module transforms canonical survey data into CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) format for use with activity-based travel demand models.


For detailed API documentation including function signatures, parameters, component descriptions, and data flow diagrams, see [CT-RAMP Formatting API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/pipeline_steps/format_output/ctramp/)

The documentation includes:

- `format_ctramp()` - Transform canonical data to CT-RAMP model specification
- Component descriptions for the eleven output tables (households, persons, mandatory locations, individual and joint tours and trips, all tours, all trips, CDAP and auto ownership results)
- Data flow and execution stages
- Configuration parameters (`usability_profile`, income thresholds, TAZ field, age thresholds, gender defaults). Which records are written is the profile's choice: the step keeps the records its `usability_profile` admits
- Implementation notes (excluded fields, placeholder values, empty data handling)
- Usage examples

Also see the [CT-RAMP Data Models Documentation](https://bayareametro.github.io/travel-diary-survey-tools/models/ctramp/)
