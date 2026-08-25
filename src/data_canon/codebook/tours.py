"""Tour building step for processing travel diary data."""

from data_canon.core.labeled_enum import LabeledEnum


class TourType(LabeledEnum):
    """What a tour is anchored on -- the ``tour_type`` column.

    A tour's anchor is the place it departs from and returns to. Home-based
    tours anchor at home; subtours anchor at the anchor of the tour that
    contains them (see ``detect_anchor_based_subtours``). This is orthogonal to
    :class:`TourCategory`, which says how *completely* a tour is observed
    against whichever anchor it has.
    """

    HOME_BASED = (1, "Home-based tour")
    WORK_BASED = (2, "Work-based tour (at-work subtour)")
    SCHOOL_BASED = (3, "School-based tour (at-school subtour)")


class PersonCategory:
    """Simplified person categories for tour purpose prioritization."""

    WORKER = "worker"
    STUDENT = "student"
    OTHER = "other"


class TourCategory(LabeledEnum):
    """How completely a tour is observed against its own anchor.

    The anchor is home for a home-based tour and the workplace (or campus) for
    a subtour -- see :class:`TourType`. A tour is COMPLETE when it both departs
    from and returns to that anchor, so one criterion admits a home-to-home
    tour and a work-to-work at-work subtour alike. Which anchor applies is
    ``tour_type``; this enum never encodes it.
    """

    COMPLETE = (1, "Start at anchor, end at anchor")
    PARTIAL_END = (2, "Start at anchor, end away from anchor")
    PARTIAL_START = (3, "Start away from anchor, end at anchor")
    PARTIAL_BOTH = (4, "Start away from anchor, end away from anchor")


class TourDirection(LabeledEnum):
    """Half-tour classification.

    Every tour has exactly two halves relative to its own anchor and primary
    destination, at-work subtours included. Subtour *membership* is a property
    of the tour (``parent_tour_id`` / ``subtour_num`` / ``tour_type``), not a
    direction, so it is deliberately not represented here: encoding it as a
    third direction discarded the real direction that DaySim (``half``) and
    CT-RAMP (``inbound``) both require.
    """

    OUTBOUND = (1, "Outbound half-tour")
    INBOUND = (2, "Inbound half-tour")


class TourDataQuality(LabeledEnum):
    """Why a tour is not a valid round trip. ``VALID`` means it is one.

    A tour is valid when it is a closed excursion from its anchor, observed end
    to end, with a real activity to anchor it on. Every other code names the
    single reason that failed. Where ``tour_category`` says *which* end is open,
    this says *why* it is open -- the two answer different questions and neither
    substitutes for the other.

    Read the ``PARTIAL_*`` codes as statements about observation, not about
    corruption: the trips are as the respondent reported them, and the tour does
    not close because the diary stopped watching. ``NO_DESTINATION`` and
    ``SPATIAL_GAP``, by contrast, mean the record itself cannot be trusted.

    This is a **leaf** fact, computed from the tour's own trips plus the
    surrounding trips of the same person, so the completeness cascade can read
    it without the derivation ever pointing back at ``model_usable``. Reporting
    completeness (``complete``), household-date coherence (``hh_day_complete``)
    and the model gate (``model_usable``) live in their own columns and never
    leak in here: a tour can be flawless and still be dropped because a
    housemate skipped that date, which is not a fact about this tour.
    """

    VALID = (0, "Valid tour")
    PARTIAL_OTHER_HOME = (1, "Open end is another home known for this person")
    PARTIAL_DAY_SPLIT = (2, "Chain resumes at the same place on the next diary day")
    PARTIAL_DIARY_EDGE = (3, "First or last trip in the diary; its open end is not a known home")
    NO_DESTINATION = (
        4,
        "No activity to anchor the tour on: returns to the anchor without "
        "stopping, or every stop was a mode change",
    )
    SPATIAL_GAP = (5, "Missing leg, inside the tour or at its boundary")
