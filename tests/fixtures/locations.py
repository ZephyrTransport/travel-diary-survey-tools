"""Centralized location definitions for test fixtures.

This module provides a comprehensive set of predefined locations with
coordinates (lat/lon) and zone IDs (TAZ/MAZ) for use in test scenarios.
These locations are used by the mock spatial join to assign TAZ/MAZ IDs
based on coordinates.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Location:
    """A location with coordinates and zone IDs.

    Attributes:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)
        taz: Traffic Analysis Zone ID
        maz: Micro Analysis Zone ID (optional, None for TAZ-only models)
    """

    lat: float
    lon: float
    taz: int
    maz: int | None = None


# Comprehensive location definitions for test scenarios
# Using San Francisco Bay Area coordinate range for realism

# Residential locations
HOME_LOCATION = Location(lat=37.7000, lon=-122.4000, taz=100, maz=1000)
HOME_2_LOCATION = Location(lat=37.7100, lon=-122.4100, taz=101, maz=1010)
HOME_3_LOCATION = Location(lat=37.7200, lon=-122.4200, taz=102, maz=1020)

# Work locations
WORK_LOCATION = Location(lat=37.7500, lon=-122.4500, taz=200, maz=2000)
WORK_2_LOCATION = Location(lat=37.7600, lon=-122.4600, taz=201, maz=2010)
WORK_3_LOCATION = Location(lat=37.7700, lon=-122.4700, taz=202, maz=2020)

# School locations
SCHOOL_LOCATION = Location(lat=37.7300, lon=-122.4300, taz=300, maz=3000)
SCHOOL_HIGH_LOCATION = Location(lat=37.7350, lon=-122.4350, taz=301, maz=3010)
SCHOOL_COLLEGE_LOCATION = Location(lat=37.7400, lon=-122.4400, taz=302, maz=3020)

# Transit access points (BART stations)
BART_HOME_LOCATION = Location(lat=37.7100, lon=-122.4100, taz=100, maz=1001)
BART_WORK_LOCATION = Location(lat=37.7400, lon=-122.4400, taz=200, maz=2001)

# Transit stops (bus stops, same TAZ as origin/destination)
TRANSIT_STOP_HOME_LOCATION = Location(lat=37.7050, lon=-122.4050, taz=100, maz=1002)
TRANSIT_STOP_WORK_LOCATION = Location(lat=37.7550, lon=-122.4550, taz=200, maz=2002)

# Activity locations
SHOPPING_LOCATION = Location(lat=37.7250, lon=-122.4250, taz=400, maz=4000)
RESTAURANT_LOCATION = Location(lat=37.7280, lon=-122.4280, taz=400, maz=4001)
RECREATION_LOCATION = Location(lat=37.7320, lon=-122.4320, taz=401, maz=4010)
MEDICAL_LOCATION = Location(lat=37.7380, lon=-122.4380, taz=402, maz=4020)

# Locations without MAZ (for testing TAZ-only models)
RURAL_HOME_LOCATION = Location(lat=37.8000, lon=-122.5000, taz=900, maz=None)
RURAL_WORK_LOCATION = Location(lat=37.8500, lon=-122.5500, taz=901, maz=None)

# Build lookup dictionary: (lat, lon) -> Location
LOCATIONS: dict[tuple[float, float], Location] = {
    (loc.lat, loc.lon): loc
    for loc in [
        HOME_LOCATION,
        HOME_2_LOCATION,
        HOME_3_LOCATION,
        WORK_LOCATION,
        WORK_2_LOCATION,
        WORK_3_LOCATION,
        SCHOOL_LOCATION,
        SCHOOL_HIGH_LOCATION,
        SCHOOL_COLLEGE_LOCATION,
        BART_HOME_LOCATION,
        BART_WORK_LOCATION,
        TRANSIT_STOP_HOME_LOCATION,
        TRANSIT_STOP_WORK_LOCATION,
        SHOPPING_LOCATION,
        RESTAURANT_LOCATION,
        RECREATION_LOCATION,
        MEDICAL_LOCATION,
        RURAL_HOME_LOCATION,
        RURAL_WORK_LOCATION,
    ]
}


def lookup_location(lat: float | None, lon: float | None) -> Location | None:
    """Look up location by coordinates.

    Args:
        lat: Latitude
        lon: Longitude

    Returns:
        Location object if found, None otherwise
    """
    if lat is None or lon is None:
        return None

    return LOCATIONS.get((lat, lon))
