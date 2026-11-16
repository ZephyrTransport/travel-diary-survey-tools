"""Profiles, tables and a stamping helper shared by the usability-cascade tests."""

from datetime import datetime

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.usability import (
    NO_ZONE_COVERAGE,
    HouseholdDayNeeds,
    JointTours,
    TourClosesAt,
    UsabilityProfile,
    cascade_complete,
    stamp_usable,
)
from processing.usability.profiles import ALL_DAYS

from .schema_utils import add_missing_tables

DATE = datetime(2023, 5, 1)


def make_profile(
    name: str = "test",
    closes_at: TourClosesAt = TourClosesAt.PRIMARY_HOME,
    household: HouseholdDayNeeds = HouseholdDayNeeds.ALL_MEMBERS,
    **axes: object,
) -> UsabilityProfile:
    """A profile on the given axes; the rest take no zones, partial joint tours, every day."""
    axes = {
        "zone_coverage": NO_ZONE_COVERAGE,
        "joint_tours": JointTours.PARTIAL,
        "travel_days": ALL_DAYS,
        **axes,
    }
    return UsabilityProfile(name, closes_at, household, **axes)


STRICT = make_profile()
LOOSE = make_profile(household=HouseholdDayNeeds.NOTHING)
WIDEST = make_profile("widest", TourClosesAt.ANYWHERE, HouseholdDayNeeds.NOTHING)


def config_axes(
    closes_at: str = TourClosesAt.PRIMARY_HOME,
    household: str = HouseholdDayNeeds.ALL_MEMBERS,
) -> dict:
    """One profile's config block, every axis stated."""
    return {
        "tour_closes_at": closes_at,
        "household_day_needs": household,
        "zone_coverage": NO_ZONE_COVERAGE,
        "joint_tours": JointTours.PARTIAL,
        "travel_days": [d.name.lower() for d in TravelDow],
    }


def stamp(tables: dict, *profiles: UsabilityProfile) -> dict[str, pl.DataFrame]:
    """Run ``cascade_complete`` over a copy of *tables*, then each profile in turn."""
    working: dict[str, pl.DataFrame | None] = dict(tables)
    cascade_complete(add_missing_tables(working))
    for each in profiles:
        stamp_usable(working, each)
    return {name: df for name, df in working.items() if df is not None}


def household(n: int = 1, **tours: list) -> dict[str, pl.DataFrame]:
    """Household 1 with *n* members, each with a complete day on DATE and one clean tour.

    Person p has day p and tour 10 * p; *tours* replaces or adds tour columns.
    """
    people = list(range(1, n + 1))
    return {
        "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
        "persons": pl.DataFrame(
            {
                "person_id": people,
                "hh_id": [1] * n,
                "survey_complete": [True] * n,
                "surveyable": [True] * n,
            }
        ),
        "person_days": pl.DataFrame(
            {
                "person_day_id": people,
                "person_id": people,
                "hh_id": [1] * n,
                "travel_date": [DATE] * n,
                "survey_complete": [True] * n,
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": [10 * p for p in people],
                "person_day_id": people,
                "person_id": people,
                "survey_complete": [True] * n,
                "parent_tour_id": [10 * p for p in people],
                "tour_data_quality": [TourDataQuality.VALID.value] * n,
                "tour_category": [TourCategory.COMPLETE.value] * n,
                **tours,
            }
        ),
    }
