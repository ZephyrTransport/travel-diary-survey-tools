"""Run tour extraction the way the pipeline does: locations first.

``extract_tours`` reads a finished ``habitual_locations`` table, which survey
cleaning delivers and ``detect_habitual_locations`` completes. Tests that start
from persons, households and trips go through the same three stages here.
"""

from datetime import datetime
from typing import Any

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import AgeCategory, Employment, Student
from data_canon.codebook.trips import (
    Driver,
    ModeType,
    Purpose,
    PurposeCategory,
    PurposeToCategoryMap,
)
from processing import link_trips
from processing.habitual_locations import (
    HabitualLocationConfig,
    MatchConfig,
    detect_habitual_locations,
    reported_habitual_locations,
)
from processing.tours import extract_tours
from processing.tours.tour_configs import TourConfig

# The habitual-location buffer, the value the BATS configs give both steps.
BUFFER_METERS = 300.0

HOME = (37.80, -122.40)
WORK = (37.85, -122.45)
LUNCH = (37.86, -122.46)
SHOP = (37.82, -122.42)

HOUSEHOLDS = pl.DataFrame({"hh_id": [1], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})


def at(hour: int, minute: int = 0, day: int = 1) -> datetime:
    """A time on diary day *day*; day 1 is Wednesday 17 January 2024."""
    return datetime(2024, 1, 16 + day, hour, minute)


def person(work: tuple[float, float] | None = WORK, **fields: Any) -> pl.DataFrame:
    """Person 1 of household 1: a full-time worker at *work* unless *fields* say otherwise."""
    return pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [work[0] if work else None],
            "work_lon": [work[1] if work else None],
            "school_lat": [None],
            "school_lon": [None],
            **{name: [value] for name, value in fields.items()},
        },
        schema_overrides={
            "school_type": pl.Int64,
            "work_lat": pl.Float64,
            "work_lon": pl.Float64,
            "school_lat": pl.Float64,
            "school_lon": pl.Float64,
        },
    )


def leg(
    o: tuple[float, float],
    d: tuple[float, float],
    o_purpose: Purpose,
    d_purpose: Purpose,
    depart: datetime,
    arrive: datetime,
    **columns: Any,
) -> dict:
    """One unlinked trip by car for person 1 on day 1; *columns* override any field.

    Purpose categories are derived, so a leg cannot state a purpose and a
    category that disagree. The reported duration is read off the timestamps.
    """
    return {
        "person_day_id": 1,
        "person_id": 1,
        "hh_id": 1,
        "travel_dow": TravelDow.WEDNESDAY.value,
        "depart_time": depart,
        "arrive_time": arrive,
        "o_purpose": o_purpose.value,
        "d_purpose": d_purpose.value,
        "o_purpose_category": PurposeToCategoryMap.PURPOSE_TO_CATEGORY[o_purpose].value,
        "d_purpose_category": PurposeToCategoryMap.PURPOSE_TO_CATEGORY[d_purpose].value,
        "mode_type": ModeType.CAR.value,
        "o_lat": o[0],
        "o_lon": o[1],
        "d_lat": d[0],
        "d_lon": d[1],
        "unlinked_trip_weight": 1.0,
        "distance_meters": 5000.0,
        "duration_minutes": (arrive - depart).total_seconds() / 60,
        "num_travelers": 1,
        "driver": Driver.DRIVER.value,
        **columns,
    }


def unlinked_trips(legs: list[dict]) -> pl.DataFrame:
    """The legs as an unlinked_trips frame, numbered from 1."""
    return pl.DataFrame([{"unlinked_trip_id": i, **row} for i, row in enumerate(legs, start=1)])


def link(unlinked: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Link trips with the BATS change-mode and transit codes, not splitting on occupancy."""
    return link_trips(
        unlinked_trips=unlinked,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
        split_on_occupancy=False,
    )


def tour_config(**overrides: Any) -> TourConfig:
    """Tour rules as the BATS configs state them, with *overrides* on top."""
    return TourConfig(**{"spatial_gap_threshold_meters": 1000.0, **overrides})


def days_not_asked(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Days for the trips, from a survey that did not ask where days began or ended.

    Each day's ``travel_date`` is the date its first trip departs, so members of
    one household travelling on the same date share it, and so share a
    household-day, numbered in date order.
    """
    return (
        linked_trips.group_by(pl.col("person_day_id").cast(pl.Int64))
        .agg(
            pl.col("hh_id").first(),
            pl.col("depart_time").min().dt.date().alias("travel_date"),
        )
        .sort("person_day_id")
        .with_columns(
            (
                pl.col("hh_id") * 100
                + pl.col("travel_date").rank("dense").over("hh_id").cast(pl.Int64)
            ).alias("hh_day_id"),
            pl.lit(None, dtype=pl.Int64).alias("begin_day"),
            pl.lit(None, dtype=pl.Int64).alias("end_day"),
        )
    )


def locate_and_extract_tours(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    *,
    joint_trips: pl.DataFrame | None = None,
    person_days: pl.DataFrame | None = None,
    extra: pl.DataFrame | None = None,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]:
    """Deliver the reported locations, detect the observed ones, then extract tours.

    By default joint detection found nothing and the survey did not ask where
    days began or ended. *extra* adds reported locations (a second home);
    *kwargs* are ``TourConfig`` settings over the BATS ones. Returns the
    ``extract_tours`` outputs plus both location tables.
    """
    person_days = person_days if person_days is not None else days_not_asked(linked_trips)
    if joint_trips is None:
        joint_trips = pl.DataFrame(schema={"joint_trip_id": pl.Int64})
        if "joint_trip_id" not in linked_trips.columns:
            linked_trips = linked_trips.with_columns(
                pl.lit(None, dtype=pl.Int64).alias("joint_trip_id")
            )
    located = detect_habitual_locations(
        habitual_locations=reported_habitual_locations(households, persons, extra=extra),
        linked_trips=linked_trips,
        person_days=person_days,
        locations=HabitualLocationConfig(buffer_meters=BUFFER_METERS, include_observed_homes=True),
    )
    tours = extract_tours(
        persons=persons,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        habitual_locations=located["habitual_locations"],
        person_days=person_days,
        joint_trips=joint_trips,
        extraction=tour_config(**kwargs),
        habitual_match=MatchConfig(buffer_meters=BUFFER_METERS),
    )
    return {**tours, **located}


def link_and_extract_tours(
    persons: pl.DataFrame, households: pl.DataFrame, legs: list[dict], **kwargs: Any
) -> dict[str, pl.DataFrame]:
    """Link the legs, then run ``locate_and_extract_tours`` on the result."""
    linked = link(unlinked_trips(legs))
    return locate_and_extract_tours(
        persons, households, linked["unlinked_trips"], linked["linked_trips"], **kwargs
    )
