"""Rules for matching trip ends to habitual locations, and for finding them."""

from pydantic import BaseModel, ConfigDict, Field

from data_canon.codebook.trips import Purpose

DEFAULT_BUFFER_METERS = 300.0
DEFAULT_AT_ADDRESS_METERS = 100.0


class MatchConfig(BaseModel):
    """The rule for whether a point is at a habitual location.

    Each step that asks the question has its own copy: detecting the locations
    and matching tours to them are separate settings, which a project may give
    the same value through one config variable. The buffer has no default: it
    suits one survey's location noise, so the config states it.
    """

    model_config = ConfigDict(extra="forbid")

    at_address_meters: float = Field(
        default=DEFAULT_AT_ADDRESS_METERS,
        gt=0,
        description=(
            "Distance in metres within which a trip end is at the person's own "
            "primary home whatever its purpose. At their doorstep the purpose "
            "describes what they are doing -- exercising, working from home, "
            "dropping someone off -- not a different place, and refusing it "
            "welds two tours into one. Further out the purpose has to agree, "
            "because that is where the corner shop is. Homes only: a lunch "
            "place can be metres from the office door."
        ),
    )
    buffer_meters: float = Field(
        gt=0,
        description=(
            "Distance in metres within which two points are the same place. It "
            "groups stops into one location, decides a found place is a reported "
            "one, decides a trip end is at a location, and decides a tour came "
            "back to its primary destination. Reported locations are geocoded "
            "from what the respondent typed while trip ends are GPS, and people "
            "walk about a site, so "
            "it is a few hundred metres rather than a building's width. Set it "
            "to suit the survey's location noise."
        ),
    )


class HabitualLocationConfig(MatchConfig):
    """Rules for finding observed locations, on top of the match rule.

    Recurrence is deliberately not required (``min_distinct_days`` defaults to
    1): some survey platforms collect a single travel day, and most people go to
    work once a day anyway. Stay length is what separates a workplace from a
    brief stop.
    """

    include_observed_homes: bool = Field(
        description=(
            "Whether to add homes found from where days were said to begin or "
            "end, beside the reported ones. An analyst's choice with no default: "
            "a 'home' answer away from the reported home becomes another home, "
            "and tours touching it are graded OTHER_HOME."
        ),
    )
    min_dwell_minutes: float = Field(
        default=90.0,
        ge=0,
        description=(
            "Minimum length of one stop for it to count as evidence of a "
            "workplace or school, unless its purpose has its own entry in "
            "min_dwell_minutes_by_purpose."
        ),
    )
    min_dwell_minutes_by_purpose: dict[Purpose, float] = Field(
        default={
            Purpose.COLLEGE: 45.0,
            Purpose.WORK_ACTIVITY: 240.0,
            Purpose.VOLUNTEERING: 240.0,
            Purpose.OTHER_WORK: 240.0,
        },
        description=(
            "Per-purpose overrides of min_dwell_minutes. College is lower "
            "because a student may attend one class and leave. Work-related "
            "purposes are higher: their median stop is under an hour, a meeting "
            "or a delivery, and only a stay the length of a working day says "
            "the person works there."
        ),
    )
    min_distinct_days: int = Field(
        default=1,
        ge=1,
        description=(
            "Optional minimum number of distinct travel days an observed location "
            "was visited. Defaults to 1 (no filter)."
        ),
    )
