"""Data models for trip linking and tour building.

This module uses Pydantic for data validation.

Models represent individual records (rows) rather than entire DataFrames.
Use the validate_* functions to validate Polars DataFrames by iterating
through rows.

Any field without None as an allowed type is considered required core data.

# Completeness vs. model admissibility

Two flags answer two different questions and must not be conflated:

* ``complete`` -- *did we collect this record?* Survey reporting completeness,
  cascaded from household down. Partial and overnight tours stay ``True``: they
  are valid survey data, useful for survey analysis.
* ``model_usable`` -- *do the travel models take this record?* Reporting
  completeness AND an admissible tour structure. Stamped once by the
  ``cascade_completeness`` step and read by the CT-RAMP/DaySim drop and the
  weighting.

Gate on ``model_usable``; ``complete`` is the descriptor it derives from. See
[`processing.completeness`][processing.completeness] for the per-level rules.
"""

from datetime import datetime

from pydantic import BaseModel, model_validator

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import BooleanYesNo, LocationSource, LocationType
from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteFreq,
    Employment,
    Ethnicity,
    Gender,
    Industry,
    JobType,
    Occupation,
    Race,
    SchoolType,
    Student,
    WorkParking,
)
from data_canon.codebook.tours import TourCategory, TourDirection, TourType
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
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND part of a "
            "well-formed tour structure. Stamped by the cascade_completeness step; "
            "gates the CT-RAMP/DaySim drop and the weighting. Not a data-quality "
            "verdict - see `complete`."
        ),
    )


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
    industry: Industry | None = schema_field(default=None)
    occupation: Occupation | None = schema_field(default=None)
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
    surveyable: bool | None = schema_field(
        default=None,
        description=(
            "The survey could collect this person's travel at all. Unrelated "
            "household members (e.g. roommates) are enumerated for household "
            "composition and weighting but file no travel, and the vendor gives "
            "them no day rows whatsoever. Read by the completeness cascade, "
            "which excludes them from the household-day reductions so they "
            "cannot veto a date they were never asked about, and by the "
            "persons->days required-child constraint. Null counts as "
            "surveyable."
        ),
    )
    num_days_complete: int = schema_field(ge=0, default=0)
    complete: bool | None = schema_field(default=None)
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND part of a "
            "well-formed tour structure. Stamped by the cascade_completeness step; "
            "gates the CT-RAMP/DaySim drop and the weighting. Not a data-quality "
            "verdict - see `complete`."
        ),
    )
    person_weight: float | None = schema_field(default=None, ge=0)


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = schema_field(
        ge=1,
        fk_to="persons.person_id",
        required_child=True,
        # Only surveyable persons must have a day. Unrelated household members
        # file no travel and the vendor gives them no day rows, so requiring
        # one of them can only be satisfied by fabricating a day that reads as
        # a genuine no-travel day.
        required_child_when="surveyable",
    )
    day_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    travel_date: datetime = schema_field()
    travel_dow: TravelDow = schema_field()
    complete: bool | None = schema_field(default=False)
    hh_day_complete: bool | None = schema_field(
        default=None,
        description=(
            "Household-day coherence: every member of the household reported a "
            "complete day on this travel_date. Stamped by the cascade_completeness "
            "step as an ALL reduction over member-days. A day is only "
            "model_usable within a complete household-day."
        ),
    )
    hh_day_usable: bool | None = schema_field(
        default=None,
        description=(
            "Usable-side mirror of hh_day_complete: every member's day on this "
            "travel_date is model_usable. Stamped by cascade_completeness; a "
            "household is admissible only with >=1 usable household-day."
        ),
    )
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND a coherent "
            "household-day AND part of a well-formed tour structure. Stamped by "
            "the cascade_completeness step; gates the CT-RAMP/DaySim drop and the "
            "weighting. Not a data-quality verdict - see `complete`."
        ),
    )
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
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND part of a "
            "well-formed tour structure. Stamped by the cascade_completeness step; "
            "gates the CT-RAMP/DaySim drop and the weighting. Not a data-quality "
            "verdict - see `complete`."
        ),
    )
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
    # This field is derivable, and tour aggregation already recomputes it with
    # different semantics rather than reading it. See issue #81 for whether it
    # should stay on the schema at all.
    d_activity_duration: int = schema_field(
        description=(
            "Whole minutes spent at the trip destination before the next departure "
            "(activity duration at the destination). Sentinel values: -1 when the "
            "destination is home; -2 for the last trip of a person-day (no "
            "subsequent departure)."
        ),
    )
    tour_direction: TourDirection = schema_field()
    o_location_type: LocationType = schema_field(
        description="Classified location type of the trip origin (Home/Work/School/Other).",
    )
    d_location_type: LocationType = schema_field(
        description="Classified location type of the trip destination (Home/Work/School/Other).",
    )
    complete: bool | None = schema_field(default=None)
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND part of a "
            "well-formed tour structure. Stamped by the cascade_completeness step; "
            "gates the CT-RAMP/DaySim drop and the weighting. Not a data-quality "
            "verdict - see `complete`."
        ),
    )
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
    tour_type: TourType = schema_field(
        description=(
            "What the tour is anchored on: home for a home-based tour, the "
            "workplace or campus for a subtour. Orthogonal to `tour_category`, "
            "which grades how completely the tour reaches that anchor."
        ),
    )
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
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND part of a "
            "well-formed tour structure. Stamped by the cascade_completeness step; "
            "gates the CT-RAMP/DaySim drop and the weighting. Not a data-quality "
            "verdict - see `complete`."
        ),
    )
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
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: survey-complete AND part of a "
            "well-formed tour structure. Stamped by the cascade_completeness step; "
            "gates the CT-RAMP/DaySim drop and the weighting. Not a data-quality "
            "verdict - see `complete`."
        ),
    )
    joint_trip_weight: float | None = schema_field(default=None, ge=0)


class JointTourModel(BaseModel):
    """Joint tour group containing the tours of several household members.

    A joint tour is a *meta-entity*: the participants' individual tours remain
    the real records and keep their own ``tour_weight``. This table exists so the
    group can be counted and weighted once, rather than inferred by grouping
    tours on ``joint_tour_id`` at every point of use.

    Its weight is the mean of its member tours, so it sits on the same footing as
    a tour (the mean of its linked trips). Membership requires at least two
    participants -- a group of one is not joint.
    """

    joint_tour_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    day_id: int = schema_field(ge=1, fk_to="days.day_id")
    num_participants: int = schema_field(
        ge=2, description="Number of household members on this joint tour"
    )
    complete: bool | None = schema_field(default=None)
    model_usable: bool | None = schema_field(
        default=None,
        description=(
            "Admissible to the tour-based model: at least two member tours are "
            "themselves model-usable, so the group is still joint. Stamped by the "
            "cascade_completeness step; gates the CT-RAMP/DaySim drop and the "
            "weighting. Not a data-quality verdict - see `complete`."
        ),
    )
    joint_tour_weight: float | None = schema_field(default=None, ge=0)


class HabitualLocationModel(BaseModel):
    """A place a person habitually uses: their home, workplace or school.

    This is the tall replacement for the wide scalar ``work_lat/work_lon``,
    ``school_lat/school_lon`` columns: one row per known location, so a person
    can hold multiple locations of the same kind (e.g. a primary workplace plus
    a recurring alternate worksite) with per-row provenance.

    The table is deliberately *not* a catalogue of every place a person went —
    discretionary stops are not habitual locations and do not appear here. What
    distinguishes a habitual location is the person's standing relationship to
    it, which is what ``source`` and ``is_primary`` record.

    ``location_num`` numbers locations of the same kind for a person, with the
    primary (when known) at ``1``. An alternate worksite is therefore just a
    ``WORK`` row with ``location_num > 1`` and ``source = OBSERVED`` — there is
    no separate ``ALTERNATE_WORK`` kind. An "other home" (second home, other
    parent's house) is likewise a ``HOME`` row with ``location_num > 1``.

    Per-day use of a location lives in
    [`HabitualLocationDayModel`][data_canon.models.survey.HabitualLocationDayModel],
    keyed on ``habitual_location_id``. A location exists here whether or not it
    was visited during the diary period — a reported workplace belonging to
    someone who teleworked all week is still their workplace — which is why the
    two are separate tables.
    """

    habitual_location_id: int = schema_field(
        ge=1,
        unique=True,
        description=(
            "Identifier for this location, formed as "
            "person_id * 1000 + location_type * 100 + location_num."
        ),
    )
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    location_type: LocationType = schema_field(
        description="Kind of location (Home/Work/School/Other)."
    )
    location_num: int = schema_field(
        ge=1,
        description=(
            "Sequence number of this location within its kind for the person "
            "(1 = primary when a primary is known)."
        ),
    )
    is_primary: bool | None = schema_field(
        default=None,
        description=(
            "Whether this is the person's primary location of its kind, as stated in the survey. "
            "None when primacy is unknown (e.g. a multi-site worker with no single "
            "usual workplace)."
        ),
    )
    lat: float = schema_field(ge=-90, le=90)
    lon: float = schema_field(ge=-180, le=180)
    source: LocationSource = schema_field(
        description="How this location was established (Reported/Observed/Imputed)."
    )


class HabitualLocationDayModel(BaseModel):
    """One person-day's use of one habitual location.

    The per-day companion to
    [`HabitualLocationModel`][data_canon.models.survey.HabitualLocationModel]:
    a row exists for every day the person was present at that location. Rows are
    recorded for *all* habitual locations, reported ones included, so that "did
    this person visit their usual workplace on day D" is a lookup rather than a
    re-derivation from trips.

    These are presence facts, not conclusions: the table records how long
    somebody was somewhere and how the day related to it, never which location
    "was" the day's anchor. That judgement belongs to whichever consumer is
    making it, and different consumers may weigh these facts differently (by
    duration, by recurrence, by a purpose hierarchy).
    """

    habitual_location_id: int = schema_field(
        ge=1,
        fk_to="habitual_locations.habitual_location_id",
    )
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    day_id: int = schema_field(ge=1, fk_to="days.day_id")
    location_type: LocationType = schema_field(
        description="Kind of location, denormalized from the parent row for filtering."
    )
    dwell_minutes: float | None = schema_field(
        default=None,
        ge=0,
        description=(
            "Total measurable minutes spent at this location on this day. None "
            "when no visit that day had a measurable duration, which is not the "
            "same as zero: a stay that ran past the last trip of the day has no "
            "recorded end, and arrivals home carry no duration at all (see "
            "LinkedTripModel.d_activity_duration sentinels)."
        ),
    )
    n_visits: int = schema_field(
        ge=1,
        description=(
            "Number of separate arrivals at this location on this day, counted "
            "whether or not the stay had a measurable duration."
        ),
    )
    is_day_start: bool = schema_field(
        description="Whether the person's first trip of the day departed from here."
    )
    is_day_end: bool = schema_field(
        description=(
            "Whether the person's last trip of the day arrived here, i.e. they "
            "were still here when the diary day ended."
        )
    )
