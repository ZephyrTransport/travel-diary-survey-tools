"""Trip record builders for unlinked, linked, and processed trips.

This module provides builders for all trip types in the canonical format.
Uses field_utils for simplified purpose field resolution.
"""

from datetime import UTC, datetime, time, timedelta

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import (
    AccessEgressMode,
    Driver,
    Mode,
    ModeType,
    Purpose,
    PurposeCategory,
    PurposeToCategoryMap,
)

from .field_utils import add_optional_fields_batch


def _default_times(depart_time, arrive_time, default_depart_hour=8, travel_minutes=30):
    """Set default departure and arrival times if not provided."""
    if depart_time is None:
        depart_time = datetime.combine(datetime.now(tz=UTC).date(), time(default_depart_hour, 0))
    if arrive_time is None:
        arrive_time = depart_time + timedelta(minutes=travel_minutes)
    return depart_time, arrive_time


def create_unlinked_trip(
    trip_id: int = 10001,
    _linked_trip_id: int | None = None,
    person_id: int = 101,
    hh_id: int = 1,
    day_id: int = 1,
    person_num: int = 1,
    day_num: int = 1,
    o_lat: float | None = None,
    o_lon: float | None = None,
    d_lat: float | None = None,
    d_lon: float | None = None,
    depart_time: datetime | None = None,
    arrive_time: datetime | None = None,
    travel_time: int = 30,
    duration_minutes: float | None = None,
    distance_meters: float = 1000.0,
    trip_weight: float = 1.0,
    transit_access: int = 0,
    transit_egress: int = 0,
    travel_dow: TravelDow = TravelDow.MONDAY,
    mode_type: ModeType = ModeType.CAR,
    mode_1: Mode | None = None,
    mode_2: Mode | None = None,
    mode_3: Mode | None = None,
    mode_4: Mode | None = None,
    o_purpose: Purpose = Purpose.HOME,
    o_purpose_category: PurposeCategory | None = None,
    d_purpose: Purpose = Purpose.OTHER,
    d_purpose_category: PurposeCategory | None = None,
    purpose: Purpose | None = None,
    purpose_category: PurposeCategory | None = None,
    driver: Driver = Driver.DRIVER,
    access_mode: AccessEgressMode | None = None,
    egress_mode: AccessEgressMode | None = None,
    num_travelers: int = 1,
    change_mode: bool = False,
    **overrides,
) -> dict:
    """Create an unlinked trip record (raw trip segment before linking).

    Unlinked trips represent individual trip segments as reported by the
    traveler, before they are linked into journeys and organized into tours.
    These are the inputs to the link_trips processing step.

    Args:
        trip_id: Trip ID
        person_id: Person ID
        hh_id: Household ID
        day_id: Day ID (links trip to a specific day)
        person_num: Person number within household
        day_num: Day number in survey period
        o_lat: Origin latitude (optional)
        o_lon: Origin longitude (optional)
        d_lat: Destination latitude (optional)
        d_lon: Destination longitude (optional)
        depart_time: Departure time (defaults to 8 AM)
        arrive_time: Arrival time (defaults to 8:30 AM)
        travel_time: Travel time in minutes
        duration_minutes: Trip duration in minutes (defaults to travel_time)
        distance_meters: Trip distance in meters
        trip_weight: Trip expansion weight
        transit_access: Transit access flag
        transit_egress: Transit egress flag
        travel_dow: Day of week enum
        mode_type: Mode type enum (car/transit/walk/bike)
        mode_1: First trip segment mode (optional, for multi-segment trips)
        mode_2: Second trip segment mode (optional, for multi-segment trips)
        mode_3: Third trip segment mode (optional, for multi-segment trips)
        mode_4: Fourth trip segment mode (optional, for multi-segment trips)
        o_purpose_category: Origin purpose category enum (for link_trips)
        d_purpose_category: Destination purpose category enum (for link_trips)
        purpose_category: Optional purpose category for fallback
        o_purpose: Optional origin purpose for fallback
        d_purpose: Optional destination purpose for fallback
        purpose: Optional purpose for fallback
        driver: Driver status enum
        access_mode: Access mode enum for transit (optional)
        egress_mode: Egress mode enum for transit (optional)
        num_travelers: Number of travelers
        change_mode: Whether this is a change mode location (for linking)
        **overrides: Override any default values

    Returns:
        Complete unlinked trip record dict (no tour_id, trip_num,
        tour_direction)
    """
    # Default times if not provided
    depart_time, arrive_time = _default_times(
        depart_time,
        arrive_time,
        default_depart_hour=8,
        travel_minutes=travel_time,
    )

    record = {
        "trip_id": trip_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "day_id": day_id,
        "person_num": person_num,
        "day_num": day_num,
        "depart_time": depart_time,
        "arrive_time": arrive_time,
        "travel_time": travel_time,
        "duration_minutes": (
            duration_minutes if duration_minutes is not None else float(travel_time)
        ),
        "distance_meters": distance_meters,
        "trip_weight": trip_weight,
        "transit_access": transit_access,
        "transit_egress": transit_egress,
        "travel_dow": travel_dow.value,
        "mode_type": mode_type.value,
        "mode_1": mode_1.value if mode_1 else None,
        "mode_2": mode_2.value if mode_2 else None,
        "mode_3": mode_3.value if mode_3 else None,
        "mode_4": mode_4.value if mode_4 else None,
        "driver": driver.value,
        "num_travelers": num_travelers,
        "change_mode": change_mode,
    }

    # Add purpose fields - derive category from detailed purpose
    # Simple one-way flow: Purpose → PurposeCategory
    if o_purpose_category is None:
        o_purpose_category = PurposeToCategoryMap.get_category(o_purpose)
    if d_purpose_category is None:
        d_purpose_category = PurposeToCategoryMap.get_category(d_purpose)

    purpose_fields = {
        "o_purpose": o_purpose.value,
        "o_purpose_category": o_purpose_category.value,
        "d_purpose": d_purpose.value,
        "d_purpose_category": d_purpose_category.value,
    }

    if purpose is not None:
        purpose_fields["purpose"] = purpose.value
    if purpose_category is not None:
        purpose_fields["purpose_category"] = purpose_category.value

    add_optional_fields_batch(record, **purpose_fields)

    # Always include lat/lon fields (link_trips requires them even if None)
    record["o_lat"] = o_lat
    record["o_lon"] = o_lon
    record["d_lat"] = d_lat
    record["d_lon"] = d_lon

    # Add optional transit fields
    add_optional_fields_batch(
        record,
        access_mode=access_mode.value if access_mode else None,
        egress_mode=egress_mode.value if egress_mode else None,
    )

    return {**record, **overrides}


def create_linked_trip(
    linked_trip_id: int = 1,
    person_id: int = 101,
    hh_id: int = 1,
    person_num: int = 1,
    day_id: int = 1,
    day_num: int = 1,
    travel_dow: TravelDow = TravelDow.MONDAY,
    linked_trip_num: int = 1,
    tour_id: int = 1,
    depart_time: datetime | None = None,
    arrive_time: datetime | None = None,
    o_lat: float = 37.70,
    o_lon: float = -122.40,
    o_taz: int | None = 100,
    o_maz: int | None = None,
    o_purpose: PurposeCategory = PurposeCategory.HOME,
    d_lat: float = 37.75,
    d_lon: float = -122.45,
    d_taz: int | None = 200,
    d_maz: int | None = None,
    d_purpose: PurposeCategory = PurposeCategory.WORK,
    mode_type: ModeType = ModeType.CAR,
    driver: Driver = Driver.DRIVER,
    num_travelers: int = 1,
    distance_meters: float = 8046.72,
    num_unlinked_trips: int = 1,
    tour_direction: TourDirection = TourDirection.OUTBOUND,
    access_mode: AccessEgressMode | None = None,
    egress_mode: AccessEgressMode | None = None,
    **overrides,
) -> dict:
    """Create a complete canonical linked trip record.

    Args:
        linked_trip_id: Linked trip ID
        person_id: Person ID
        hh_id: Household ID
        person_num: Person number
        day_id: Day ID (optional)
        day_num: Day number
        travel_dow: Day of week enum
        linked_trip_num: Linked trip number
        tour_id: Parent tour ID
        depart_time: Departure datetime
        arrive_time: Arrival datetime
        o_lat: Origin latitude
        o_lon: Origin longitude
        o_taz: Origin TAZ (optional, added via spatial join)
        o_maz: Origin MAZ (optional, added via spatial join)
        o_purpose: Origin purpose category enum
        d_lat: Destination latitude
        d_lon: Destination longitude
        d_taz: Destination TAZ (optional, added via spatial join)
        d_maz: Destination MAZ (optional, added via spatial join)
        d_purpose: Destination purpose category enum
        mode_type: Mode type enum (car/transit/walk/bike)
        driver: Driver status enum
        num_travelers: Number of travelers
        distance_meters: Trip distance in meters
        num_unlinked_trips: Number of component unlinked trips
        tour_direction: Tour direction enum (OUTBOUND/INBOUND)
        access_mode: Transit access mode enum (for transit trips)
        egress_mode: Transit egress mode enum (for transit trips)
        **overrides: Override any default values

    Returns:
        Complete linked trip record dict
    """
    # Default times if not provided
    depart_time, arrive_time = _default_times(
        depart_time, arrive_time, default_depart_hour=8, travel_minutes=30
    )

    record = {
        "linked_trip_id": linked_trip_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "person_num": person_num,
        "day_num": day_num,
        "travel_dow": travel_dow.value,
        "linked_trip_num": linked_trip_num,
        "tour_id": tour_id,
        "depart_time": depart_time,
        "arrive_time": arrive_time,
        "duration_minutes": int((arrive_time - depart_time).total_seconds() / 60),
        "o_lat": o_lat,
        "o_lon": o_lon,
        "o_taz": o_taz,
        "o_maz": o_maz,
        "o_purpose_category": o_purpose.value,
        "d_lat": d_lat,
        "d_lon": d_lon,
        "d_taz": d_taz,
        "d_maz": d_maz,
        "d_purpose_category": d_purpose.value,
        "mode_type": mode_type.value,
        "driver": driver.value,
        "num_travelers": num_travelers,
        "distance_meters": distance_meters,
        "num_unlinked_trips": num_unlinked_trips,
        "tour_direction": tour_direction.value,
        "access_mode": access_mode.value if access_mode else None,
        "egress_mode": egress_mode.value if egress_mode else None,
        "joint_trip_id": None,  # Required for extract_tours step
    }

    # Add optional fields
    add_optional_fields_batch(record, day_id=day_id)

    return {**record, **overrides}
