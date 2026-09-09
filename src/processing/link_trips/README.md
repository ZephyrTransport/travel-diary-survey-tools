[← Back to Main README](../../../README.md)

# Link Pipeline Steps

This module links individual trip segments into complete journey records (linked trips), aggregating sequential trips made during mode changes or transfers.

For detailed API documentation including function signatures, parameters, and algorithms, see: [Trip Linking API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/processing/#trip-linking)

The documentation includes:
- `link_trips()` - Link unlinked trip segments into complete journey records
- Two-phase algorithm: Link Trip IDs and Aggregate Linked Trips
- Transit detection and access/egress mode mapping
