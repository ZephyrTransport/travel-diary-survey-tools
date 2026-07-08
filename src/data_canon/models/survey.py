"""Data models for trip linking and tour building.

This module uses Pydantic for data validation.

Models represent individual records (rows) rather than entire DataFrames.
Use the validate_* functions to validate Polars DataFrames by iterating
through rows.

Any field without None as an allowed type is considered required core data.
"""

from datetime import datetime

from pydantic import BaseModel, model_validator

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import BooleanYesNo, LocationType
from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteFreq,
    Employment,
    Ethnicity,
    Gender,
    JobType,
    Race,
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
from data_canon.core.schema_field import schema_field


# Data Models ------------------------------------------------------------------
class HouseholdModel(BaseModel):
    """Household attributes (minimal for tour building)."""

    hh_id: int = schema_field(ge=1, unique=True)
    home_lat: float = schema_field(ge=-90, le=90)
    home_lon: float = schema_field(ge=-180, le=180)
    residence_rent_own: ResidenceRentOwn = schema_field()
    residence_type: ResidenceType = schema_field()
    income: int | None = schema_field(ge=0, default=None)
    income_bin: IncomeBroad = schema_field()
    hh_weight: float | None = schema_field(ge=0)
    num_vehicles: int = schema_field(ge=0)
    complete: bool = schema_field()


class PersonModel(BaseModel):
    """Person attributes for tour building."""

    person_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(
        ge=1,
        fk_to="households.hh_id",
        required_child=True,
    )
    person_num: int = schema_field(ge=1)
    age: AgeCategory = schema_field()
    gender: Gender = schema_field()
    # These fields can be None if person is not employed or in school
    work_lat: float | None = schema_field(ge=-90, le=90)
    work_lon: float | None = schema_field(ge=-180, le=180)
    school_lat: float | None = schema_field(ge=-90, le=90)
    school_lon: float | None = schema_field(ge=-180, le=180)
    job_type: JobType | None = schema_field(default=None)
    employment: Employment = schema_field()
    student: Student = schema_field()
    school_type: SchoolType | None = schema_field()
    work_park: WorkParking | None = schema_field()
    work_mode: Mode | None = schema_field()
    race: Race | None = schema_field(default=None)
    ethnicity: Ethnicity | None = schema_field(default=None)
    telework_freq: CommuteFreq | None = schema_field(default=None)
    commute_freq: CommuteFreq | None = schema_field(default=None)
    # NOTE: These commute subsidy fields are only used in CTRAMP format
    # But might be useful elsewhere, consider standardizing to be less vague
    # and/or moved into a data model extension.
    commute_subsidy_use_3: BooleanYesNo | None = schema_field(default=None)
    commute_subsidy_use_4: BooleanYesNo | None = schema_field(default=None)
    # NOTE: is proxy is vague.
    # Better and more flexible would be to have proxy_person_id on the proxied person
    # This allows for multiple proxy reporters and is more explicit.
    is_proxy: bool | None = schema_field(default=None)
    num_days_complete: int = schema_field(ge=0, default=0)
    complete: bool | None = schema_field(default=None)
    person_weight: float | None = schema_field(default=None, ge=0)


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = schema_field(
        ge=1,
        fk_to="persons.person_id",
        required_child=True,
    )
    day_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    travel_date: datetime = schema_field()
    travel_dow: TravelDow = schema_field()
    complete: bool | None = schema_field(default=False)
    day_weight: float | None = schema_field(default=None, ge=0)


class UnlinkedTripModel(BaseModel):
    """Trip data model for validation."""

    unlinked_trip_id: int = schema_field(ge=1, unique=True)
    day_id: int = schema_field(ge=1, fk_to="days.day_id")
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    linked_trip_id: int = schema_field(ge=1, fk_to="linked_trips.linked_trip_id")
    tour_id: int | None = schema_field(ge=1, fk_to="tours.tour_id")
    o_lon: float = schema_field(ge=-180, le=180)
    o_lat: float = schema_field(ge=-90, le=90)
    d_lon: float = schema_field(ge=-180, le=180)
    d_lat: float = schema_field(ge=-90, le=90)
    o_purpose: Purpose
    d_purpose: Purpose
    o_purpose_category: PurposeCategory = schema_field()
    d_purpose_category: PurposeCategory = schema_field()
    mode_type: ModeType = schema_field()
    mode_1: Mode | None
    mode_2: Mode | None
    mode_3: Mode | None
    mode_4: Mode | None
    duration_minutes: float = schema_field(ge=0)
    distance_meters: float = schema_field(ge=0)
    depart_time: datetime | None = schema_field()
    arrive_time: datetime | None = schema_field()
    num_travelers: int = schema_field(ge=1)
    complete: bool | None = schema_field(default=None)
    unlinked_trip_weight: float | None = schema_field(default=None, ge=0)

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
                f"Trip {self.unlinked_trip_id}: arrive_time ({self.arrive_time}) "
                f"must be after depart_time ({self.depart_time})"
            )
            raise ValueError(msg)
        return self


class LinkedTripModel(BaseModel):
    """Linked Trip data model for validation."""

    day_id: int = schema_field(ge=1, fk_to="days.day_id")
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")

    linked_trip_id: int = schema_field(ge=1, unique=True)
    joint_trip_id: int | None = schema_field(
        ge=1,
        fk_to="joint_trips.joint_trip_id",
        default=None,
    )

    tour_id: int = schema_field(ge=1, fk_to="tours.tour_id")
    travel_dow: TravelDow = schema_field()
    o_purpose: Purpose = schema_field()
    o_purpose_category: PurposeCategory = schema_field()
    o_lat: float = schema_field(ge=-90, le=90)
    o_lon: float = schema_field(ge=-180, le=180)
    d_purpose: Purpose = schema_field()
    d_purpose_category: PurposeCategory = schema_field()
    d_lat: float = schema_field(ge=-90, le=90)
    d_lon: float = schema_field(ge=-180, le=180)
    mode_type: ModeType = schema_field()
    driver: Driver = schema_field()
    num_travelers: int = schema_field(ge=1)
    access_mode: AccessEgressMode | None = schema_field(default=None)
    egress_mode: AccessEgressMode | None = schema_field(default=None)
    duration_minutes: float = schema_field(ge=0)
    distance_meters: float = schema_field(ge=0)
    depart_time: datetime = schema_field()
    arrive_time: datetime = schema_field()
    tour_direction: TourDirection = schema_field()
    complete: bool | None = schema_field(default=None)
    linked_trip_weight: float | None = schema_field(default=None, ge=0)


class TourModel(BaseModel):
    """Tour-level records with clear, descriptive schema_field names."""

    tour_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    day_id: int = schema_field(ge=1, fk_to="days.day_id")
    tour_num: int = schema_field(ge=1)
    subtour_num: int = schema_field(ge=0)
    parent_tour_id: int = schema_field(ge=1, fk_to="tours.tour_id")
    joint_tour_id: int | None = schema_field(ge=1, default=None)

    tour_purpose: PurposeCategory | None = schema_field(default=None)
    tour_category: TourCategory = schema_field()
    single_trip_tour: bool = schema_field(default=False)

    # Timing
    origin_depart_time: datetime = schema_field()
    origin_arrive_time: datetime = schema_field()
    dest_arrive_time: datetime | None = schema_field(default=None)
    dest_depart_time: datetime | None = schema_field(default=None)

    # Helpful foreign keys to linked trips
    origin_linked_trip_id: int = schema_field(
        ge=1,
        fk_to="linked_trips.linked_trip_id",
    )
    dest_linked_trip_id: int | None = schema_field(
        ge=1,
        fk_to="linked_trips.linked_trip_id",
        default=None,
    )

    # Locations
    o_lat: float = schema_field(ge=-90, le=90)
    o_lon: float = schema_field(ge=-180, le=180)
    d_lat: float = schema_field(ge=-90, le=90)
    d_lon: float = schema_field(ge=-180, le=180)
    o_location_type: LocationType = schema_field()
    d_location_type: LocationType = schema_field()

    # Mode hierarchical
    tour_mode: ModeType = schema_field()
    outbound_mode: ModeType | None = schema_field()
    inbound_mode: ModeType | None = schema_field()
    num_travelers: int = schema_field(ge=1, default=1)
    complete: bool | None = schema_field(default=None)
    tour_weight: float | None = schema_field(default=None, ge=0)

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

    joint_trip_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    day_id: int = schema_field(ge=1, fk_to="days.day_id")
    num_joint_travelers: int = schema_field(
        ge=2, description="Number of travelers in this joint trip"
    )
    o_lat_mean: float = schema_field(
        ge=-90, le=90, description="Mean origin latitude across member trips"
    )
    o_lon_mean: float = schema_field(
        ge=-180, le=180, description="Mean origin longitude across member trips"
    )
    d_lat_mean: float = schema_field(
        ge=-90,
        le=90,
        description="Mean destination latitude across member trips",
    )
    d_lon_mean: float = schema_field(
        ge=-180,
        le=180,
        description="Mean destination longitude across member trips",
    )
    depart_time_mean: datetime = schema_field(description="Mean departure time across member trips")
    depart_arrive_mean: datetime = schema_field(description="Mean arrival time across member trips")
    complete: bool | None = schema_field(default=None)
    joint_trip_weight: float | None = schema_field(default=None, ge=0)
