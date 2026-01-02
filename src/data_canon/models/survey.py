"""Data models for trip linking and tour building.

This module uses Pydantic for data validation.

Models represent individual records (rows) rather than entire DataFrames.
Use the validate_* functions to validate Polars DataFrames by iterating
through rows.
"""

from datetime import datetime

from pydantic import BaseModel, model_validator

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import LocationType
from data_canon.codebook.households import ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    PersonType,
    SchoolType,
    Student,
    WorkParking,
)
from data_canon.codebook.tours import TourCategory, TourDirection
from data_canon.codebook.trips import (
    AccessEgressMode,
    Driver,
    Mode,
    ModeType,
    Purpose,
    PurposeCategory,
)
from data_canon.core.step_field import step_field


# Data Models ------------------------------------------------------------------
class HouseholdModel(BaseModel):
    """Household attributes (minimal for tour building)."""

    hh_id: int = step_field(ge=1, unique=True, required_in_steps=["extract_tours"])
    home_lat: float = step_field(ge=-90, le=90, required_in_steps=["extract_tours"])
    home_lon: float = step_field(ge=-180, le=180, required_in_steps=["extract_tours"])
    residence_rent_own: ResidenceRentOwn = step_field(required_in_steps=["format_daysim"])
    residence_type: ResidenceType = step_field(required_in_steps=["format_daysim"])


class PersonModel(BaseModel):
    """Person attributes for tour building."""

    person_id: int = step_field(ge=1, unique=True, required_in_steps=["extract_tours"])
    hh_id: int = step_field(
        ge=1,
        fk_to="households.hh_id",
        required_child=True,
    )
    age: AgeCategory = step_field(required_in_steps=["extract_tours"])
    # These fields can be None if person is not employed or in school
    work_lat: float | None = step_field(ge=-90, le=90, required_in_steps=["extract_tours"])
    work_lon: float | None = step_field(ge=-180, le=180, required_in_steps=["extract_tours"])
    school_lat: float | None = step_field(ge=-90, le=90, required_in_steps=["extract_tours"])
    school_lon: float | None = step_field(ge=-180, le=180, required_in_steps=["extract_tours"])
    person_type: PersonType = step_field(required_in_steps=[])
    employment: Employment = step_field(required_in_steps=["extract_tours"])
    student: Student = step_field(required_in_steps=["extract_tours"])
    school_type: SchoolType | None = step_field(
        required_in_steps=["extract_tours"],
    )
    work_park: WorkParking | None = step_field(
        required_in_steps=["format_daysim"],
    )
    work_mode: Mode | None = step_field(
        required_in_steps=["format_daysim"],
    )
    is_proxy: bool = step_field(required_in_steps=["format_daysim"])
    num_days_complete: int = step_field(ge=0, default=0)


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = step_field(
        ge=1,
        fk_to="persons.person_id",
        required_child=True,
    )
    day_id: int = step_field(ge=1, unique=True)
    hh_id: int = step_field(ge=1, fk_to="households.hh_id")
    travel_dow: TravelDow


class UnlinkedTripModel(BaseModel):
    """Trip data model for validation."""

    trip_id: int = step_field(ge=1, unique=True)
    day_id: int = step_field(ge=1, fk_to="days.day_id")
    person_id: int = step_field(ge=1, fk_to="persons.person_id")
    hh_id: int = step_field(ge=1, fk_to="households.hh_id")
    linked_trip_id: int = step_field(
        ge=1,
        fk_to="linked_trips.linked_trip_id",
        required_in_steps=["extract_tours"],
    )
    tour_id: int = step_field(
        ge=1,
        fk_to="tours.tour_id",
        required_in_steps=["format_daysim"],
    )
    depart_date: datetime
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: datetime
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_lon: float = step_field(ge=-180, le=180, required_in_steps=["link_trips"])
    o_lat: float = step_field(ge=-90, le=90, required_in_steps=["link_trips"])
    d_lon: float = step_field(ge=-180, le=180, required_in_steps=["link_trips"])
    d_lat: float = step_field(ge=-90, le=90, required_in_steps=["link_trips"])
    o_purpose: Purpose
    d_purpose: Purpose
    o_purpose_category: PurposeCategory = step_field(required_in_steps=["link_trips"])
    d_purpose_category: PurposeCategory = step_field(required_in_steps=["link_trips"])
    mode_type: ModeType = step_field(required_in_steps=["link_trips"])
    mode_1: Mode | None
    mode_2: Mode | None
    mode_3: Mode | None
    mode_4: Mode | None
    duration_minutes: float = step_field(ge=0)
    distance_meters: float = step_field(ge=0)

    depart_time: datetime | None = step_field(required_in_steps=["link_trips", "extract_tours"])
    arrive_time: datetime | None = step_field(required_in_steps=["link_trips", "extract_tours"])

    # You can add custom row-level validators here
    # Don't confuse with the constom DataFrame-level validators elsewhere
    @model_validator(mode="after")
    def validate_arrival_after_departure(self) -> "UnlinkedTripModel":
        """Ensure arrive_time is after depart_time.

        Raises:
            ValueError: If arrival time is before or equal to departure time
        """
        if (
            self.arrive_time is not None
            and self.depart_time is not None
            and self.arrive_time < self.depart_time
        ):
            msg = (
                f"Trip {self.trip_id}: arrive_time ({self.arrive_time}) "
                f"must be after depart_time ({self.depart_time})"
            )
            raise ValueError(msg)
        return self


class LinkedTripModel(BaseModel):
    """Linked Trip data model for validation."""

    day_id: int = step_field(ge=1, fk_to="days.day_id", required_in_steps=["extract_tours"])
    person_id: int = step_field(ge=1, fk_to="persons.person_id")
    hh_id: int = step_field(ge=1, fk_to="households.hh_id")

    linked_trip_id: int = step_field(ge=1, unique=True)
    joint_trip_id: int | None = step_field(
        ge=1,
        fk_to="joint_trips.joint_trip_id",
        default=None,
    )
    tour_id: int = step_field(ge=1, fk_to="tours.tour_id", required_in_steps=["format_daysim"])
    travel_dow: TravelDow = step_field(required_in_steps=["extract_tours"])
    depart_date: datetime = step_field()
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: datetime = step_field()
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_purpose_category: int = step_field()
    o_lat: float = step_field(ge=-90, le=90, required_in_steps=["detect_joint_trips"])
    o_lon: float = step_field(ge=-180, le=180, required_in_steps=["detect_joint_trips"])
    d_purpose_category: int = step_field(required_in_steps=["extract_tours"])
    d_lat: float = step_field(ge=-90, le=90, required_in_steps=["detect_joint_trips"])
    d_lon: float = step_field(ge=-180, le=180, required_in_steps=["detect_joint_trips"])
    mode_type: ModeType = step_field(required_in_steps=["extract_tours"])
    driver: Driver = step_field(required_in_steps=["link_trips", "format_daysim"])
    num_travelers: int = step_field(ge=1)
    access_mode: AccessEgressMode | None = step_field(
        required_in_steps=["format_daysim"], default=None
    )
    egress_mode: AccessEgressMode | None = step_field(
        required_in_steps=["format_daysim"], default=None
    )

    duration_minutes: float = step_field(ge=0)
    distance_meters: float = step_field(ge=0)
    depart_time: datetime = step_field(required_in_steps=["detect_joint_trips"])
    arrive_time: datetime = step_field(required_in_steps=["detect_joint_trips"])
    tour_direction: TourDirection = step_field(required_in_steps=["format_daysim"])


class TourModel(BaseModel):
    """Tour-level records with clear, descriptive step_field names."""

    tour_id: int = step_field(ge=1, unique=True)
    person_id: int = step_field(ge=1, fk_to="persons.person_id")
    day_id: int = step_field(ge=1, fk_to="days.day_id")
    tour_num: int = step_field(ge=1)
    subtour_num: int = step_field(ge=0)
    parent_tour_id: int = step_field(ge=1, fk_to="tours.tour_id")

    tour_purpose: PurposeCategory | None = step_field(default=None)
    tour_category: TourCategory = step_field()
    single_trip_tour: bool = step_field(default=False)

    # Timing
    origin_depart_time: datetime = step_field()
    origin_arrive_time: datetime = step_field()
    dest_arrive_time: datetime | None = step_field(default=None)
    dest_depart_time: datetime | None = step_field(default=None)

    # Helpful foreign keys to linked trips
    origin_linked_trip_id: int = step_field(
        ge=1,
        fk_to="linked_trips.linked_trip_id",
        required_in_steps=["format_daysim"],
    )
    dest_linked_trip_id: int | None = step_field(
        ge=1,
        fk_to="linked_trips.linked_trip_id",
        required_in_steps=["format_daysim"],
        default=None,
    )

    # Locations
    o_lat: float = step_field(ge=-90, le=90)
    o_lon: float = step_field(ge=-180, le=180)
    d_lat: float = step_field(ge=-90, le=90)
    d_lon: float = step_field(ge=-180, le=180)
    o_location_type: LocationType = step_field()
    d_location_type: LocationType = step_field()

    # Mode hierarchical
    tour_mode: ModeType = step_field()
    outbound_mode: ModeType | None = step_field()
    inbound_mode: ModeType | None = step_field()

    @model_validator(mode="after")
    def validate_complete_tours(self) -> "TourModel":
        """Validate that complete tours have all required fields.

        Single-trip tours (where person made one trip but didn't return home)
        are allowed to have null tour_purpose, destination times, and
        dest_linked_trip_id. Complete tours must have these fields populated.
        """
        if not self.single_trip_tour:
            if self.tour_purpose is None:
                msg = f"Tour {self.tour_id}: Complete tours must have tour_purpose (non-null)"
                raise ValueError(msg)
            if self.dest_arrive_time is None:
                msg = f"Tour {self.tour_id}: Complete tours must have dest_arrive_time (non-null)"
                raise ValueError(msg)
            if self.dest_depart_time is None:
                msg = f"Tour {self.tour_id}: Complete tours must have dest_depart_time (non-null)"
                raise ValueError(msg)
            if self.dest_linked_trip_id is None:
                msg = (
                    f"Tour {self.tour_id}: Complete tours must have dest_linked_trip_id (non-null)"
                )
                raise ValueError(msg)
        return self


class JointTripModel(BaseModel):
    """Joint trip group containing multiple linked trips from same household.

    Represents a detected shared trip where multiple household members traveled
    together. Each joint trip has a unique ID and aggregated spatiotemporal
    attributes from its member trips.
    """

    joint_trip_id: int = step_field(ge=1, unique=True)
    hh_id: int = step_field(ge=1, fk_to="households.hh_id")
    day_id: int = step_field(ge=1, fk_to="days.day_id")
    num_joint_travelers: int = step_field(
        ge=2, description="Number of travelers in this joint trip"
    )
    o_lat_mean: float = step_field(
        ge=-90, le=90, description="Mean origin latitude across member trips"
    )
    o_lon_mean: float = step_field(
        ge=-180, le=180, description="Mean origin longitude across member trips"
    )
    d_lat_mean: float = step_field(
        ge=-90,
        le=90,
        description="Mean destination latitude across member trips",
    )
    d_lon_mean: float = step_field(
        ge=-180,
        le=180,
        description="Mean destination longitude across member trips",
    )
    depart_time_mean: datetime = step_field(description="Mean departure time across member trips")
    depart_arrive_mean: datetime = step_field(description="Mean arrival time across member trips")
