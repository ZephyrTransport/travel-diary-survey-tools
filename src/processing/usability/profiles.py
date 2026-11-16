"""What a usability profile is, and how config states one.

A profile answers five axes explicitly -- which home has to close a tour, what
the household-date has to show, which zone system has to hold every one of the
record's locations, what a joint tour has to be, and which days of the week it
takes. Nothing here touches a
table; it is the vocabulary the rest of the package is written against.
"""

from dataclasses import dataclass
from enum import StrEnum

from data_canon.codebook.days import TravelDow
from data_canon.codebook.tours import TourDataQuality

# --- Profile axes ------------------------------------------------------------
# A profile answers every one, explicitly. There is no default for any: a column
# whose meaning depends on a value nobody wrote is the thing profiles exist to
# stop. Each closed axis is an enum whose values are the words config writes.


class TourClosesAt(StrEnum):
    """Which home has to close a tour.

    The quality codes divide into other-home anchors, open ends and missing
    data, and this axis walks down the first two. No setting admits missing data.
    """

    PRIMARY_HOME = "primary_home"  # VALID only
    ANY_HOME = "any_home"  # + OTHER_HOME
    ANYWHERE = "anywhere"  # + PARTIAL_DAY_SPLIT, PARTIAL_DIARY_EDGE


class HouseholdDayNeeds(StrEnum):
    """What the household-date has to show."""

    ALL_MEMBERS = "all_members"  # every surveyable member's day complete
    NOTHING = "nothing"  # no household-date requirement


class JointTours(StrEnum):
    """What a joint tour has to be.

    Activity-based models draw a joint tour's trips once for everyone on it, so
    a person who leaves for a leg is not on the same tour; the survey's own
    groups need no such thing.
    """

    FULLY_JOINT = "fully_joint"  # everyone shares every leg; work/school/escort never joint
    PARTIAL = "partial"  # the groups the survey detected, as they are


# Which days of the week a profile takes, written in config as day names
# (``[tuesday, wednesday, thursday]``) and listed out rather than named as a set,
# so the config says which days without the reader knowing what a set means.
TRAVEL_DAYS_AXIS = "travel_days"
ALL_DAYS: tuple[TravelDow, ...] = tuple(TravelDow)
# The words config writes for each day: the codebook's names, in lower case.
_DAY_NAMES: dict[str, TravelDow] = {day.name.lower(): day for day in TravelDow}

# Which zone system has to be able to address the record. Unlike the other
# axes this vocabulary is open: the value names a zone geography the zone step
# produced, so the legal set is whatever that step was configured to build.
NO_ZONE_COVERAGE = "none"  # no geographic requirement

# A profile is a name -- `ctramp`, `analysis` -- and every column belonging to it
# is that name suffixed onto a family: `usable_ctramp` for the verdict (on
# household_days too, for the household-date reduction), `hh_weight_ctramp` for
# the weight fitted over it. One profile, one suffix, one rule per family.
USABLE_FAMILY = "usable"

# Survey reporting completeness, from the vendor. Deliberately not a profile: it
# answers "did we collect this record?", not "do the models take it?", and a
# consumer may name it to mean the whole valid survey. It has no family prefix
# because it belongs to no family.
SURVEY_COMPLETE = "survey_complete"


def usable_col_for(profile: str) -> str:
    """The column carrying *profile*'s verdict.

    Args:
        profile: A usability profile named in config, or
            :data:`SURVEY_COMPLETE` to mean the whole valid survey.

    Returns:
        ``usable_<profile>``, or :data:`SURVEY_COMPLETE` unchanged -- that column
        is vendor data rather than a verdict this pipeline reached, so it is
        available under its own name but not a member of the family.
    """
    if profile == SURVEY_COMPLETE:
        return SURVEY_COMPLETE
    return f"{USABLE_FAMILY}_{profile}"


def profile_of(usability_flag_col: str) -> str | None:
    """The profile a verdict column belongs to, or None for one outside the family.

    The inverse of :func:`usable_col_for`: ``usable_ctramp`` -> ``ctramp``.
    ``survey_complete``, or a hand-written flag, names no profile, so a reader
    looking for that profile's other columns (``joint_tour_id_<profile>``) gets
    None and falls back to the bare column.
    """
    prefix = f"{USABLE_FAMILY}_"
    if usability_flag_col.startswith(prefix) and len(usability_flag_col) > len(prefix):
        return usability_flag_col[len(prefix) :]
    return None


# Quality codes each closure setting admits, cumulatively. NO_DESTINATION and
# SPATIAL_GAP appear nowhere: they are not open ends but missing data -- no
# tolerance for a tour that stops somewhere unexpected makes a missing leg
# present, or an activity that never happened happen.
ADMITTED_QUALITY: dict[TourClosesAt, tuple[TourDataQuality, ...]] = {
    TourClosesAt.PRIMARY_HOME: (TourDataQuality.VALID,),
    TourClosesAt.ANY_HOME: (TourDataQuality.VALID, TourDataQuality.OTHER_HOME),
    TourClosesAt.ANYWHERE: (
        TourDataQuality.VALID,
        TourDataQuality.OTHER_HOME,
        TourDataQuality.PARTIAL_DAY_SPLIT,
        TourDataQuality.PARTIAL_DIARY_EDGE,
    ),
}


@dataclass(frozen=True)
class UsabilityProfile:
    """One usability standard, stated on every axis, and the columns it writes.

    A profile says which home has to close a tour, what the household-date has
    to show, which zone system has to be able to address the record, what a
    joint tour has to be, and which days of the week it takes. Config gives all
    five explicitly; none may be left implicit there.

    Coverage is an axis rather than a universal because the zone systems differ:
    a trip outside one model's area may sit comfortably inside another's, so
    "has a zone" is a question only a named consumer can answer.

    Two things hold at every setting and so are not axes. A profile is always a
    subset of ``survey_complete`` -- a usability column admitting unreported records
    would be a different flag wearing the name. And no profile admits a tour
    with *missing data* (a missing leg, an activity that never happened), as
    opposed to one that merely ends somewhere unexpected.

    A pass writes ``flag``, the verdict consumers read, on every table. On
    ``household_days`` it records whether *all* surveyable members' days passed
    that date. It is computed for every profile even where that profile does not
    gate on it, so the column means the same thing everywhere and can still
    answer "which dates cost this household its usability".
    """

    name: str
    tour_closes_at: TourClosesAt
    household_day_needs: HouseholdDayNeeds
    zone_coverage: str
    joint_tours: JointTours
    travel_days: tuple[TravelDow, ...]

    @property
    def flag(self) -> str:
        """Per-record verdict column."""
        return usable_col_for(self.name)

    @property
    def requires_zones(self) -> bool:
        """Whether this profile asks that a record's locations have a zone."""
        return self.zone_coverage != NO_ZONE_COVERAGE

    @property
    def joint_tour_id(self) -> str:
        """``joint_tour_id`` as this profile sees it: the id where the tour is on it, else null."""
        return f"joint_tour_id_{self.name}"

    @property
    def joint_trip_id(self) -> str:
        """``joint_trip_id`` as this profile sees it: the id where the trip is on it, else null."""
        return f"joint_trip_id_{self.name}"

    @property
    def needs_whole_household_day(self) -> bool:
        """Whether a date must show every surveyable member's day complete."""
        return self.household_day_needs == HouseholdDayNeeds.ALL_MEMBERS

    @property
    def requires_fully_joint(self) -> bool:
        """Whether everyone on a joint tour must share every leg of it."""
        return self.joint_tours == JointTours.FULLY_JOINT

    @property
    def limits_travel_days(self) -> bool:
        """Whether this profile leaves out any day of the week."""
        return set(self.travel_days) != set(ALL_DAYS)


# The closed axes, by the key config writes them under.
_AXES: dict[str, type[StrEnum]] = {
    "tour_closes_at": TourClosesAt,
    "household_day_needs": HouseholdDayNeeds,
    "joint_tours": JointTours,
}

# The axis whose values come from the zone step's configuration rather than a
# vocabulary fixed here, so it is required and typed but not membership-checked.
_ZONE_AXIS = "zone_coverage"


def parse_usability_profiles(spec: dict[str, dict[str, str]]) -> list[UsabilityProfile]:
    """Turn the configured profile block into profiles, or say why it cannot.

    Every profile answers every axis. A missing axis is an error rather than a
    default, because a default is exactly the implicit meaning profiles exist to
    remove: the config should say what a column means without the reader
    knowing a base rule.

    Args:
        spec: Mapping of profile name to ``{axis: value}``.

    Returns:
        One profile per entry, in declaration order.

    Raises:
        ValueError: If the block is empty, a name collides with a column the
            reporting cascade owns, or any axis is missing, unknown, or given a
            value outside its vocabulary.
    """
    if not spec:
        msg = (
            "usability_profiles is empty, so no usability column would be stamped and "
            "every downstream consumer would have nothing to read. Name at least one "
            "profile, giving every axis: "
            + ", ".join(f"{axis} ({'|'.join(values)})" for axis, values in _AXES.items())
            + f", {_ZONE_AXIS} (a zone_name add_zone_ids builds|{NO_ZONE_COVERAGE})"
            + f", {TRAVEL_DAYS_AXIS} (a list of day names: {', '.join(_DAY_NAMES)})."
        )
        raise ValueError(msg)

    profiles = [_one_profile(name, axes) for name, axes in spec.items()]
    return profiles


def _one_profile(name: str, axes: dict[str, str]) -> UsabilityProfile:
    """Build one profile from its configured axes, or say why it cannot.

    Raises:
        ValueError: If the name is reserved, or any axis is missing, unknown, or
            given a value outside its vocabulary.
    """
    # A profile named survey_complete would overwrite a survey fact with a
    # modelling judgement.
    if name == SURVEY_COMPLETE:
        msg = (
            f"usability_profile '{name}' collides with a column the completeness "
            f"cascade owns ({SURVEY_COMPLETE}). It records what the survey "
            "collected, not what a model will take; pick another name."
        )
        raise ValueError(msg)
    if not isinstance(axes, dict):
        msg = (
            f"usability_profile '{name}' must give each axis a value, as "
            f"'{{{', '.join(f'{a}: ...' for a in _AXES)}}}'. Got: {axes!r}."
        )
        raise ValueError(msg)  # noqa: TRY004 - malformed config, not a caller type error

    chosen: dict[str, StrEnum] = {}
    for axis, allowed in _AXES.items():
        if axis not in axes:
            msg = (
                f"usability_profile '{name}' does not say '{axis}'. Every profile "
                f"answers every axis, so a column's meaning can be read off the "
                f"config alone. Legal values: {', '.join(allowed)}."
            )
            raise ValueError(msg)
        try:
            chosen[axis] = allowed(axes[axis])
        except ValueError:
            msg = (
                f"usability_profile '{name}' sets {axis}: '{axes[axis]}', which is "
                f"not one of {', '.join(allowed)}."
            )
            raise ValueError(msg) from None

    # Coverage names a geography the zone step builds, so its legal values are
    # not knowable here. It is still required: silently defaulting to "no
    # geographic requirement" is exactly the implicit meaning profiles remove.
    if _ZONE_AXIS not in axes:
        msg = (
            f"usability_profile '{name}' does not say '{_ZONE_AXIS}'. Every profile "
            f"answers every axis. Give the zone_name of a geography add_zone_ids "
            f"builds, or '{NO_ZONE_COVERAGE}' to ask nothing of geography."
        )
        raise ValueError(msg)
    coverage = axes[_ZONE_AXIS]
    if not isinstance(coverage, str) or not coverage:
        msg = (
            f"usability_profile '{name}' sets {_ZONE_AXIS}: {coverage!r}. Give the "
            f"zone_name of a geography add_zone_ids builds, or "
            f"'{NO_ZONE_COVERAGE}'."
        )
        raise ValueError(msg)

    travel_days = _travel_days(name, axes)

    unknown = sorted(set(axes) - set(_AXES) - {_ZONE_AXIS, TRAVEL_DAYS_AXIS})
    if unknown:
        msg = (
            f"usability_profile '{name}' names unknown axis/axes: "
            f"{', '.join(unknown)}. Known axes: "
            f"{', '.join([*_AXES, _ZONE_AXIS, TRAVEL_DAYS_AXIS])}."
        )
        raise ValueError(msg)

    return UsabilityProfile(
        name=name,
        tour_closes_at=TourClosesAt(chosen["tour_closes_at"]),
        household_day_needs=HouseholdDayNeeds(chosen["household_day_needs"]),
        zone_coverage=coverage,
        joint_tours=JointTours(chosen["joint_tours"]),
        travel_days=travel_days,
    )


def _travel_days(name: str, axes: dict) -> tuple[TravelDow, ...]:
    """The days a profile's config lists, in week order, or say why they cannot be read.

    Raises:
        ValueError: If the list is missing, empty, not a list, names a day that
            does not exist, or names one twice.
    """
    legal = ", ".join(_DAY_NAMES)
    if TRAVEL_DAYS_AXIS not in axes:
        msg = (
            f"usability_profile '{name}' does not say '{TRAVEL_DAYS_AXIS}'. Every "
            f"profile answers every axis. List the days it takes, from: {legal}."
        )
        raise ValueError(msg)
    days = axes[TRAVEL_DAYS_AXIS]
    if not isinstance(days, list) or not days:
        msg = (
            f"usability_profile '{name}' sets {TRAVEL_DAYS_AXIS}: {days!r}. Give a "
            f"list of at least one day, from: {legal}."
        )
        raise ValueError(msg)
    unknown = [d for d in days if d not in _DAY_NAMES]
    if unknown:
        msg = (
            f"usability_profile '{name}' lists {unknown} in {TRAVEL_DAYS_AXIS}, "
            f"which are not days. Legal values: {legal}."
        )
        raise ValueError(msg)
    repeated = sorted({d for d in days if days.count(d) > 1})
    if repeated:
        msg = f"usability_profile '{name}' lists {repeated} twice in {TRAVEL_DAYS_AXIS}."
        raise ValueError(msg)
    return tuple(sorted((_DAY_NAMES[d] for d in days), key=lambda day: day.value))
