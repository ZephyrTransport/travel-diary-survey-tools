[← Back to Main README](../../../README.md)

# Joint Trips Pipeline Steps

This module detects joint trips where multiple household members travel together by identifying trips with similar spatial and temporal characteristics.

For detailed API documentation including function signatures, parameters, and the complete algorithm, see: [Joint Trip Detection API Documentation](https://bayareametro.github.io/travel-diary-survey-tools/processing/#joint-trip-detection)

The documentation includes:

- `detect_joint_trips()` - Identify shared household trips using similarity matching
- Five-phase algorithm: Household Pre-filtering, Pairwise Distance Calculation, Similarity Filtering, Clique Detection, and Joint Trip Aggregation
- Both buffer and Mahalanobis detection methods
- Examples with tables and graphs showing the detection process
