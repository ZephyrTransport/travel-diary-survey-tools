"""Tour record builders for canonical tour data.

This module provides builders for tour records.
"""

from datetime import UTC, datetime, time

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import Mode, Purpose

from .field_utils import add_optional_fields_batch


def create_tour(
    tour_id: int = 1001,
    person_id: int = 101,
    hh_id: int = 1,
    person_num: int = 1,
    day_id: int = 1,
    day_num: int = 1,
    tour_num: int = 1,
    tour_purpose: Purpose = Purpose.PRIMARY_WORKPLACE,
    tour_category: TourCategory = TourCategory.COMPLETE,
    o_taz: int = 100,
    d_taz: int = 200,
    o_maz: int | None = None,
    d_maz: int | None = None,
    o_lat: float | None = None,
    o_lon: float | None = None,
    d_lat: float | None = None,
    d_lon: float | None = None,
    o_location_type: int = 0,
    d_location_type: int = 0,
    origin_depart_time: datetime | None = None,
    origin_arrive_time: datetime | None = None,
    dest_depart_time: datetime | None = None,
    dest_arrive_time: datetime | None = None,
    travel_dow: TravelDow = TravelDow.MONDAY,
    num_trips: int = 2,
    num_travelers: int = 1,
    tour_mode: Mode = Mode.MISSING,
    student_category: str = "Not student",
    data_quality: TourDataQuality = TourDataQuality.VALID,
    joint_tour_id: int | None = None,
    parent_tour_id: int | None = None,
    subtour_num: int = 0,
    **overrides,
) -> dict:
    """Create a complete canonical tour record.

    Args:
        tour_id: Tour ID
        person_id: Person ID
        hh_id: Household ID
        person_num: Person number
        day_id: Day ID
        day_num: Day number
        tour_num: Tour number within the day
        tour_purpose: Tour purpose enum
        tour_category: Tour category enum (mandatory/non-mandatory/at-work)
        o_taz: Origin TAZ
        d_taz: Destination TAZ
        o_maz: Origin MAZ (optional, for Daysim)
        d_maz: Destination MAZ (optional, for Daysim)
        o_lat: Origin latitude (optional)
        o_lon: Origin longitude (optional)
        d_lat: Destination latitude (optional)
        d_lon: Destination longitude (optional)
        o_location_type: Origin location type
        d_location_type: Destination location type
        origin_depart_time: Origin departure time (defaults to 8 AM)
        origin_arrive_time: Origin arrival time (defaults to 5 PM)
        dest_depart_time: Destination departure time
        dest_arrive_time: Destination arrival time
        travel_dow: Day of week enum
        num_trips: Number of trips on tour
        num_travelers: Number of people (1 individual, 2+ joint)
        tour_mode: Primary tour mode enum
        student_category: Student category for work/school tours
        data_quality: Data quality flag enum
        joint_tour_id: Joint tour ID (None for individual tours)
        parent_tour_id: Parent tour (None for primary tours)
        subtour_num: Subtour number (0 for primary tours)
        **overrides: Override any default values

    Returns:
        Complete tour record dict
    """
    # Default times if not provided - use defaults from canonical schema
    default_depart = datetime.combine(datetime.now(tz=UTC).date(), time(8, 0))
    default_arrive = datetime.combine(datetime.now(tz=UTC).date(), time(17, 0))

    if origin_depart_time is None:
        origin_depart_time = default_depart
    if origin_arrive_time is None:
        origin_arrive_time = default_arrive
    if dest_depart_time is None:
        dest_depart_time = origin_depart_time + (origin_arrive_time - origin_depart_time) / 2
    if dest_arrive_time is None:
        dest_arrive_time = origin_depart_time + (origin_arrive_time - origin_depart_time) / 2

    record = {
        "tour_id": tour_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "person_num": person_num,
        "day_id": day_id,
        "day_num": day_num,
        "tour_num": tour_num,
        "tour_purpose": tour_purpose.value,
        "tour_category": tour_category.value,
        "o_taz": o_taz,
        "d_taz": d_taz,
        "o_lat": o_lat,
        "o_lon": o_lon,
        "d_lat": d_lat,
        "d_lon": d_lon,
        "o_location_type": o_location_type,
        "d_location_type": d_location_type,
        "origin_depart_time": origin_depart_time,
        "origin_arrive_time": origin_arrive_time,
        "dest_depart_time": dest_depart_time,
        "dest_arrive_time": dest_arrive_time,
        "travel_dow": travel_dow.value,
        "num_trips": num_trips,
        "num_travelers": num_travelers,
        "tour_mode": tour_mode.value,
        "student_category": student_category,
        "data_quality": data_quality.value,
        "joint_tour_id": joint_tour_id,
        "parent_tour_id": parent_tour_id,
        "subtour_num": subtour_num,
    }

    # Add optional MAZ fields
    add_optional_fields_batch(record, o_maz=o_maz, d_maz=d_maz)

    return {**record, **overrides}


def get_tour_schema() -> dict[str, type]:
    """Get Polars schema for tour DataFrames with optional int fields.

    Use this when creating tour DataFrames to ensure columns with None
    values get the correct Int64 type instead of Null type.

    Example:
        tours = pl.DataFrame(
            [create_tour(...)],
            schema=get_tour_schema()
        )
    """
    return {
        "tour_id": pl.Int64,
        "person_id": pl.Int64,
        "hh_id": pl.Int64,
        "person_num": pl.Int64,
        "day_id": pl.Int64,
        "day_num": pl.Int64,
        "tour_num": pl.Int64,
        "tour_purpose": pl.Int64,
        "tour_category": pl.Int64,
        "o_taz": pl.Int64,
        "d_taz": pl.Int64,
        "origin_depart_time": pl.Datetime,
        "origin_arrive_time": pl.Datetime,
        "dest_depart_time": pl.Datetime,
        "dest_arrive_time": pl.Datetime,
        "travel_dow": pl.Int64,
        "num_trips": pl.Int64,
        "num_travelers": pl.Int64,
        "tour_mode": pl.Int64,
        "student_category": pl.String,
        "data_quality": pl.Int64,
        "joint_tour_id": pl.Int64,
        "parent_tour_id": pl.Int64,
        "subtour_num": pl.Int64,
    }
