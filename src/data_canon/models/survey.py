"""Pydantic models for the canonical survey tables.

Models represent individual records (rows) rather than entire DataFrames;
``CanonicalData.validate`` checks a table against its model row by row.

Any field without None as an allowed type is considered required core data.

# Completeness vs. model admissibility

Two kinds of flag answer two different questions and must not be conflated:

* ``survey_complete`` -- *did we collect this record?* Survey reporting completeness,
  cascaded from household down. Partial and overnight tours stay ``True``: they
  are valid survey data, useful for survey analysis. Declared here, because
  every run produces it.
* one column per **usability profile** -- *do the travel models take this
  record?* Reporting completeness AND an admissible tour structure.

The usability columns are deliberately **not** declared below. A project names
its profiles in config -- ``ctramp`` and ``analysis`` are this
project's names, not the schema's -- so which of them a run produces is a
project's choice. Declaring one would promise a column that a config need never
ask for. ``cascade_usability`` registers whichever it stamps as generated
columns, each described by what it admits, which says more than a fixed field
could: the description names the actual rules that profile relaxed.

The **weight** columns are absent for the same reason. A weighting run fits each
profile it is given and suffixes that profile's name onto every weight it writes
(``hh_weight_ctramp``), and the un-suffixed spelling is just a single
profile's weights with the label left off -- there is no unprofiled weight to
declare. The weighting registers what it wrote, carrying both what the number
counts and that it cannot be negative, so these columns are still described and
still checked. See [`processing.weighting.core.hierarchy`]
[processing.weighting.core.hierarchy].

See [`processing.usability`][processing.usability] for the per-level rules
and the admission vocabulary.
"""

from datetime import datetime

from pydantic import BaseModel, model_validator

from data_canon.codebook.days import BeginEndDay, TravelDow
from data_canon.codebook.generic import BooleanYesNo, LocationSource, LocationType
from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteFreq,
    Education,
    Employment,
    Ethnicity,
    Gender,
    Industry,
    JobType,
    Occupation,
    Race,
    SchoolType,
    Student,
    SurveyTech,
    WorkParking,
)
from data_canon.codebook.tours import (
    TourCategory,
    TourDataQuality,
    TourDirection,
    TourType,
)
from data_canon.codebook.trips import (
    AccessEgressMode,
    Driver,
    Mode,
    ModeType,
    Purpose,
    PurposeCategory,
    TNCType,
)
from data_canon.core.schema_field import schema_field

# Shared by the joint trip and joint tour tables.
_GROUP_HH_DAY_ID = (
    "The household-day this grouping happened on, shared by every member's "
    "day. Not any one member's person_day_id: a grouping belongs to the household's "
    "day, and a member's own day can be unusable while the others' are not."
)
_GROUP_TRAVEL_DATE = "The household-day's date, repeated here so it reads without a lookup."


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
    num_vehicles: int = schema_field(ge=0)
    exclusion_reason: str | None = schema_field(
        default=None,
        description=(
            "Why the project cleaner dropped this household, or null when kept. Any "
            "reason makes every one of the household's days unusable under every "
            "usability profile, and the household with them. survey_complete, the "
            "vendor's verdict, is untouched."
        ),
    )
    survey_complete: bool = schema_field()


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
    education: Education | None = schema_field(default=None)
    # Employer *provides* commute subsidy: Free (fully subsidized) parking at work
    commute_subsidy_provide_free_parking: BooleanYesNo | None = schema_field(default=None)
    # Employer *provides* commute subsidy: Discounted (partially subsidized) parking at work
    commute_subsidy_provide_discounted_parking: BooleanYesNo | None = schema_field(default=None)
    # Uses commute subsidy: Free (fully subsidized) parking at work
    commute_subsidy_use_free_parking: BooleanYesNo | None = schema_field(default=None)
    # Uses commute subsidy: Discounted (partially subsidized) parking at work
    commute_subsidy_use_discounted_parking: BooleanYesNo | None = schema_field(default=None)
    # NOTE: vague; a proxy_person_id on the proxied person would allow several reporters
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
    survey_tech: SurveyTech = schema_field(
        description=(
            "The technology that collected this person's travel: a call center, a "
            "browser, or GPS (a phone app). A person whose "
            "travel was reported by someone else carries the technology it was "
            "reported through; one whose travel was never collected is MISSING."
        ),
    )
    exclusion_reason: str | None = schema_field(
        default=None,
        description=(
            "Why the project cleaner dropped this person, or null when kept. Any "
            "reason makes every one of the person's days unusable under every "
            "usability profile. survey_complete, the vendor's verdict, is untouched."
        ),
    )
    num_days_complete: int = schema_field(ge=0, default=0)
    survey_complete: bool | None = schema_field(default=None)


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = schema_field(
        ge=1,
        fk_to="persons.person_id",
        required_child=True,
        # Unrelated members file no travel; requiring their day would mean fabricating one
        required_child_when="surveyable",
    )
    person_day_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    hh_day_id: int = schema_field(ge=1, fk_to="household_days.hh_day_id")
    travel_date: datetime = schema_field()
    travel_dow: TravelDow = schema_field()
    begin_day: BeginEndDay | None = schema_field(
        default=None,
        description=(
            "Where the respondent said the day began. Null where the survey did "
            "not ask. HOME or OTHER_HOME puts a home of theirs at the day's first "
            "trip origin; away from every reported home, that is another home."
        ),
    )
    end_day: BeginEndDay | None = schema_field(
        default=None,
        description=(
            "Where the respondent said the day ended. Null where the survey did "
            "not ask. HOME or OTHER_HOME puts a home of theirs at the day's last "
            "trip destination; away from every reported home, that is another home."
        ),
    )
    exclusion_reason: str | None = schema_field(
        default=None,
        description=(
            "Why the project cleaner dropped this day, or null when kept. Any reason "
            "makes the day unusable under every usability profile, and its "
            "household-date incomplete where a profile needs every member's day. "
            "survey_complete, the vendor's verdict, is untouched."
        ),
    )
    survey_complete: bool | None = schema_field(default=False)


class HouseholdDayModel(BaseModel):
    """One household's travel date: every member's person-day on it, together.

    The vendor sends no household-day file, so each project's cleaner builds
    this table from the person-days (``utils.create_ids.create_household_days``).
    Joint trips and joint tours belong to a household-day rather than to any one
    member's day.
    """

    hh_day_id: int = schema_field(
        ge=1,
        unique=True,
        description="hh_id * 100 + day_num: the household's day, as person_day_id is the person's.",
    )
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    travel_date: datetime = schema_field()
    survey_complete: bool | None = schema_field(
        default=None,
        description=(
            "Every surveyable member's day on this date is complete, as the vendor "
            "judged each one; False where no surveyable member has a day on it. "
            "Stamped by the cascade_usability step. Under a profile that asks "
            "for whole household-days, a person-day is only usable within one."
        ),
    )
    # usable_{profile} is named from config, so cascade_usability registers it
    # as a generated column instead of a field here.


class UnlinkedTripModel(BaseModel):
    """Trip data model for validation."""

    unlinked_trip_id: int = schema_field(ge=1, unique=True)
    person_day_id: int = schema_field(ge=1, fk_to="person_days.person_day_id")
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    linked_trip_id: int = schema_field(ge=1, fk_to="linked_trips.linked_trip_id")
    linked_trip_num: int | None = schema_field(
        ge=1,
        default=None,
        description=(
            "Sequence number of the linked trip this segment belongs to, within its person-day."
        ),
    )
    tour_id: int | None = schema_field(ge=1, fk_to="tours.tour_id")
    joint_tour_id: int | None = schema_field(
        ge=1,
        fk_to="joint_tours.joint_tour_id",
        default=None,
        description="The joint tour this segment's tour belongs to, if any.",
    )
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
    tnc_type: TNCType | None
    duration_minutes: float = schema_field(ge=0)
    distance_meters: float = schema_field(ge=0)
    depart_time: datetime | None = schema_field()
    arrive_time: datetime | None = schema_field()
    num_travelers: int = schema_field(ge=1)
    survey_complete: bool | None = schema_field(default=None)

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

    person_day_id: int = schema_field(ge=1, fk_to="person_days.person_day_id")
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
    d_activity_duration: int | None = schema_field(
        default=None,
        description=(
            "Whole minutes spent at the trip destination before the person's next "
            "departure that day. None for the last trip of a person-day, whose stay "
            "has no recorded end."
        ),
    )
    tour_direction: TourDirection = schema_field()
    # Tour membership, denormalised from the tours table so a trip carries its
    # own place in the hierarchy. The tour-level record remains authoritative.
    tour_num: int | None = schema_field(
        ge=1,
        default=None,
        description="The trip's tour, numbered within its person-day.",
    )
    subtour_num: int | None = schema_field(
        ge=0,
        default=None,
        description=(
            "Which at-work/at-school subtour of the parent tour this trip belongs "
            "to; 0 when the trip is on the parent tour itself."
        ),
    )
    parent_tour_id: int | None = schema_field(
        ge=1,
        fk_to="tours.tour_id",
        default=None,
        description=("The containing tour for a subtour trip. Primary tours self-reference."),
    )
    tour_purpose: PurposeCategory | None = schema_field(
        default=None,
        description="The primary purpose of the tour this trip belongs to.",
    )
    joint_tour_id: int | None = schema_field(
        ge=1,
        fk_to="joint_tours.joint_tour_id",
        default=None,
        description="The joint tour this trip's tour belongs to, if any.",
    )
    # Linking outputs: what the unlinked segments collapsed into.
    linked_trip_num: int | None = schema_field(
        ge=1,
        default=None,
        description="This linked trip's sequence number within its person-day.",
    )
    num_segments: int | None = schema_field(
        ge=1,
        default=None,
        description=(
            "Unlinked trip segments merged into this linked trip. Greater than "
            "one where access/egress or mode-change legs were joined."
        ),
    )
    travel_duration_minutes: int | None = schema_field(
        ge=0,
        default=None,
        description="Minutes in motion, summed across the merged segments.",
    )
    dwell_duration_minutes: int | None = schema_field(
        ge=0,
        default=None,
        description=(
            "Minutes waiting between the merged segments (e.g. a transfer), "
            "summed over each gap from one segment's arrival to the next "
            "segment's departure. Zero for a single-segment trip, which has no "
            "gap to wait in. Not exactly ``duration_minutes`` minus "
            "``travel_duration_minutes``: those two round separately, so the "
            "identity holds only to within a minute."
        ),
    )
    o_location_type: LocationType = schema_field(
        description="Classified location type of the trip origin (Home/Work/School/Other).",
    )
    d_location_type: LocationType = schema_field(
        description="Classified location type of the trip destination (Home/Work/School/Other).",
    )
    survey_complete: bool | None = schema_field(default=None)


class TourModel(BaseModel):
    """Tour-level records with clear, descriptive schema_field names."""

    tour_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    person_id: int = schema_field(ge=1, fk_to="persons.person_id")
    person_day_id: int = schema_field(ge=1, fk_to="person_days.person_day_id")
    tour_num: int = schema_field(ge=1)
    subtour_num: int = schema_field(ge=0)
    parent_tour_id: int = schema_field(ge=1, fk_to="tours.tour_id")
    joint_tour_id: int | None = schema_field(
        ge=1,
        default=None,
        fk_to="joint_tours.joint_tour_id",
    )

    tour_purpose: PurposeCategory | None = schema_field(
        default=None,
        description=(
            "The tour's primary activity: the highest-priority purpose among "
            "its destinations, ranked by the person's category (a worker's "
            "work trip outranks their shopping stop). Null where no stop "
            "qualifies -- every candidate was the return leg to the anchor or a "
            "mode change. On a closed tour that is `NO_DESTINATION`; on a "
            "partial one the missing half is the reason, so `tour_data_quality` "
            "names the open end instead."
        ),
    )
    tour_type: TourType = schema_field(
        description=(
            "What the tour is anchored on: home for a home-based tour, the "
            "workplace or campus for a subtour. Orthogonal to `tour_category`, "
            "which grades how completely the tour reaches that anchor."
        ),
    )
    tour_category: TourCategory = schema_field()
    tour_data_quality: TourDataQuality = schema_field(
        description=(
            "What is structurally wrong with this tour, or VALID. Derived from "
            "the tour's own trips only -- never from survey completeness or "
            "household coherence, which have their own columns. Codes 2-4 "
            "mirror `tour_category` value for value. Not itself a gate: which "
            "codes are admissible is a usability profile's decision."
        ),
    )
    trip_count: int = schema_field(
        ge=1,
        description=(
            "Linked trips in the tour. Stop counts are a formatter concern: they are "
            "a modelling convention, and a directional count has to come from the "
            "trips anyway."
        ),
    )

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
    survey_complete: bool | None = schema_field(default=None)

    @model_validator(mode="after")
    def validate_complete_tours(self) -> "TourModel":
        """Validate that a tour with a primary destination describes it fully.

        A null ``tour_purpose`` is a legitimate outcome, not an error: a tour
        whose every candidate stop was the return leg or a mode change has no
        activity to anchor on, and extraction records that as
        ``TourDataQuality.NO_DESTINATION``. What must hold is the converse --
        once a tour *has* a purpose, it reached a primary destination, so the
        arrival and the trip it was drawn from cannot be missing.

        Departure is not in that set. A tour only leaves its destination if the
        diary kept watching: one cut at the diary edge, or resuming the next
        day, arrived somewhere and stopped. ``tour_category`` already records
        which end is open, so a departure is required exactly of the tours that
        closed -- demanding it of the rest would reject a partial tour for being
        partial, which is what the quality codes exist to describe.
        """
        if self.tour_purpose is None:
            return self
        if self.dest_arrive_time is None:
            msg = f"Tour {self.tour_id}: a tour with a purpose must have dest_arrive_time"
            raise ValueError(msg)
        if self.dest_linked_trip_id is None:
            msg = f"Tour {self.tour_id}: a tour with a purpose must have dest_linked_trip_id"
            raise ValueError(msg)
        if self.tour_category == TourCategory.COMPLETE and self.dest_depart_time is None:
            msg = (
                f"Tour {self.tour_id}: a closed tour with a purpose must have "
                "dest_depart_time -- it left its destination to come back"
            )
            raise ValueError(msg)
        return self


class JointTripModel(BaseModel):
    """Joint trip group containing multiple linked trips from same household.

    Represents a detected shared trip where several household members travelled
    together. This is a *linking* table: it records which member trips form the
    group, and nothing about where or when they went. Those facts stay on the
    member trips, which hold them first-hand.

    Collapsing the members into one place or time means choosing a rule -- an
    average names a point none of them reported, a representative privileges one
    of them -- and which rule is right depends on what the answer is for. That
    choice belongs to whoever is asking, so it is made downstream rather than
    frozen here. Mirrors ``JointTourModel``.

    This table is an *overlay* on ``linked_trips``, not a partition of it: every
    member trip is still a row there in its own right. See ``joint_trip_weight``.
    """

    joint_trip_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    hh_day_id: int = schema_field(
        ge=1, fk_to="household_days.hh_day_id", description=_GROUP_HH_DAY_ID
    )
    travel_date: datetime = schema_field(description=_GROUP_TRAVEL_DATE)
    num_joint_travelers: int = schema_field(
        ge=2,
        description=(
            "Detected group size: how many member trips the clique detection put "
            "in this joint trip. Surveyed household members only - the reported "
            "travel party, which may include people outside the survey, is "
            "`num_travelers` on the member trips. Not a weighting quantity: it "
            "does not track which members the weighting later excluded."
        ),
    )
    survey_complete: bool | None = schema_field(default=None)


class JointTourModel(BaseModel):
    """Joint tour group containing the tours of several household members.

    A joint tour is a *meta-entity*: the participants' individual tours remain
    the real records and keep their own ``tour_weight``. This table exists so the
    group can be counted and weighted once, rather than inferred by grouping
    tours on ``joint_tour_id`` at every point of use.

    Its weight is the SUM of its member tours -- person-tours, matching
    ``joint_trips``. Membership requires at least two participants -- a group of
    one is not joint.
    """

    joint_tour_id: int = schema_field(ge=1, unique=True)
    hh_id: int = schema_field(ge=1, fk_to="households.hh_id")
    hh_day_id: int = schema_field(
        ge=1, fk_to="household_days.hh_day_id", description=_GROUP_HH_DAY_ID
    )
    travel_date: datetime = schema_field(description=_GROUP_TRAVEL_DATE)
    num_participants: int = schema_field(
        ge=2,
        description=(
            "Group size: household members on this joint tour, whether or not "
            "each carries weight. Not a weighting quantity - it does not track "
            "which members the weighting excluded."
        ),
    )
    survey_complete: bool | None = schema_field(default=None)


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

    ``location_num`` numbers locations of the same kind for a person: reported
    ones first, primary at ``1``, then the rest in the order the person was first
    seen there. An alternate worksite is therefore just a ``WORK`` row with
    ``location_num > 1`` and ``source = OBSERVED`` — there is no separate
    ``ALTERNATE_WORK`` kind. An "other home" (second home, other parent's house)
    is likewise a ``HOME`` row with ``location_num > 1``. Homes are never
    inferred from travel: every home is one the respondent reported or named as
    where a day began or ended.

    Survey cleaning delivers the table holding the reported locations only — the
    survey's home, work and school coordinates plus any further ones, such as a
    vendor's second home — numbered and identified. The
    ``detect_habitual_locations`` step appends the observed ones after them and
    never changes a delivered row. Each reported primary repeats its coordinate
    columns (``home_lat/lon`` on households, ``work_*``/``school_*`` on persons),
    and validation holds the two equal.

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
    person_day_id: int = schema_field(ge=1, fk_to="person_days.person_day_id")
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
            "recorded end. Always None at home, whose stays run past the day's "
            "ends, so a total would be only the part of the day that was seen."
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
