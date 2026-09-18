[← Back to Main README](../../../README.md)

# Tours Pipeline Steps

This module extracts hierarchical tour structures from linked trip data, identifying home-based tours, work-based subtours, and aggregating tour-level attributes.

For detailed API documentation including function signatures, parameters, and the complete algorithm, see: [Tour Extraction API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/processing/#tour-extraction)

The documentation includes:

- `extract_tours()` - Build tour and subtour structures from linked trip sequences
- Seven-phase algorithm: Location Classification, Home-Based Tour Identification, Anchor Period Expansion, Anchor-Based Subtour Detection, Tour Attribute Aggregation, Joint Tour Identification, and Tour Validation
- Configuration via TourConfig for distance thresholds, mode/purpose hierarchies, and person categorization
- Hierarchical tour structure (home-based tours → work-based subtours)
