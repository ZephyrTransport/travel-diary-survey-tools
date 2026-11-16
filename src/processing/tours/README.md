[← Back to Main README](../../../README.md)

# Tours Pipeline Steps

This module extracts hierarchical tour structures from linked trip data, identifying home-based tours, work- and school-based subtours, and joint tours, and aggregating tour-level attributes.

For detailed API documentation including function signatures, parameters, and the complete algorithm, see: [Tour Extraction API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/pipeline_steps/extract_tours/)

The documentation includes:

- `extract_tours()` - Build tour and subtour structures from linked trip sequences
- The algorithm, step by step, from locating each trip end to stamping each tour's data quality
- Configuration: `TourConfig` (`extraction`) for the purpose and mode rules and person
  categories, and `MatchConfig` (`habitual_match`) for the distance at which a trip end is
  at a habitual location
- Hierarchical tour structure (home-based tours → work- and school-based subtours)
