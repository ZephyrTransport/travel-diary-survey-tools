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
    """Tour data quality classification for validation and filtering."""

    VALID = (0, "Valid tour")
    SINGLE_TRIP = (1, "Single-trip tour")
    LOOP_TRIP = (2, "Home-based loop trip")
    MISSING_ANCHOR = (3, "No anchor at either end of tour")
    INDETERMINATE = (4, "Invalid tour, cause unknown")
    CHANGE_MODE = (5, "Change mode as primary purpose (linking failure)")
    SPATIAL_GAP = (6, "Spatial gap between consecutive trips (missing leg)")
