"""The places test scenarios use: coordinates with the zone ids the mock zone join assigns."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Location:
    """Coordinates and their zone ids; ``maz`` is None where only TAZ is modelled."""

    lat: float
    lon: float
    taz: int
    maz: int | None = None


# Residential locations
HOME_LOCATION = Location(lat=37.7000, lon=-122.4000, taz=100, maz=1000)

# Work locations
WORK_LOCATION = Location(lat=37.7500, lon=-122.4500, taz=200, maz=2000)
WORK_2_LOCATION = Location(lat=37.7600, lon=-122.4600, taz=201, maz=2010)
WORK_3_LOCATION = Location(lat=37.7700, lon=-122.4700, taz=202, maz=2020)

# School locations
SCHOOL_HIGH_LOCATION = Location(lat=37.7350, lon=-122.4350, taz=301, maz=3010)
SCHOOL_COLLEGE_LOCATION = Location(lat=37.7400, lon=-122.4400, taz=302, maz=3020)

# Transit access points (BART stations)
BART_HOME_LOCATION = Location(lat=37.7100, lon=-122.4100, taz=100, maz=1001)
BART_WORK_LOCATION = Location(lat=37.7400, lon=-122.4400, taz=200, maz=2001)

# Activity locations
SHOPPING_LOCATION = Location(lat=37.7250, lon=-122.4250, taz=400, maz=4000)
RESTAURANT_LOCATION = Location(lat=37.7280, lon=-122.4280, taz=400, maz=4001)

LOCATIONS: dict[tuple[float, float], Location] = {
    (loc.lat, loc.lon): loc
    for loc in [
        HOME_LOCATION,
        WORK_LOCATION,
        WORK_2_LOCATION,
        WORK_3_LOCATION,
        SCHOOL_HIGH_LOCATION,
        SCHOOL_COLLEGE_LOCATION,
        BART_HOME_LOCATION,
        BART_WORK_LOCATION,
        SHOPPING_LOCATION,
        RESTAURANT_LOCATION,
    ]
}
