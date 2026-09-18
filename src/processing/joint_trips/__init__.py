"""Joint trip processing utilities.

This module provides functionality for detecting shared trips among household
members using spatiotemporal similarity matching and
graph-based clique detection.
"""

from .detect_joint_trips import detect_joint_trips
from .joint_trip_configs import (
    JointTripConfig,
    estimate_covariance_from_detected_pairs,
)

__all__ = [
    "JointTripConfig",
    "detect_joint_trips",
    "estimate_covariance_from_detected_pairs",
]
