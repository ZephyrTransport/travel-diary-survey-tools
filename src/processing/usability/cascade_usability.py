"""The pipeline step: reporting completeness once, then one pass per profile."""

import polars as pl

from pipeline.decoration import Tables, step

from .descriptions import log_gate_summary, profile_columns
from .profiles import parse_usability_profiles
from .survey_complete import cascade_complete
from .usable import stamp_usable


@step(
    requires={
        "households": {"hh_id"},
        "persons": {"person_id", "hh_id"},
        "household_days": {"hh_day_id", "hh_id"},
        "person_days": {
            "person_day_id",
            "person_id",
            "hh_id",
            "hh_day_id",
            "survey_complete",
            "travel_dow",
        },
        "tours": {
            "tour_id",
            "person_day_id",
            "parent_tour_id",
            "joint_tour_id",
            "survey_complete",
            "tour_data_quality",
            "tour_category",
        },
        "linked_trips": {"tour_id", "person_day_id", "joint_trip_id", "survey_complete"},
        "unlinked_trips": {"tour_id", "person_day_id", "survey_complete"},
        "joint_trips": {"joint_trip_id", "hh_id"},
        "joint_tours": {"joint_tour_id", "hh_id"},
    },
    produces={
        "households": {"survey_complete", "usable_<profile>"},
        "persons": {"survey_complete", "usable_<profile>"},
        "household_days": {"survey_complete", "usable_<profile>"},
        "person_days": {"usable_<profile>"},
        "unlinked_trips": {"survey_complete", "usable_<profile>", "joint_tour_id_<profile>"},
        "linked_trips": {
            "survey_complete",
            "usable_<profile>",
            "joint_trip_id_<profile>",
            "joint_tour_id_<profile>",
        },
        "tours": {"survey_complete", "usable_<profile>", "joint_tour_id_<profile>"},
        "joint_trips": {"usable_<profile>"},
        "joint_tours": {"usable_<profile>"},
    },
    generated_columns=profile_columns,
)
def cascade_usability(
    tables: Tables,
    usability_profiles: dict[str, dict[str, str]] | None = None,
) -> dict[str, pl.DataFrame]:
    """Cascade ``survey_complete`` through the hierarchy and stamp one column per profile.

    Walks both kinds of flag across every table so they are internally consistent
    with whatever was set upstream (the vendor's day-level completeness, any
    manual adjustments in the project cleaner, and the structural verdicts from
    tour extraction):

    * ``survey_complete`` -- survey reporting completeness, cascaded hh <-> person <->
      day <-> trip/tour, and over household-days. Never narrowed by model criteria, and never
      configurable: if it is wrong the fix belongs upstream in the cleaner.
    * one column per usability profile -- the subset of ``survey_complete`` that
      profile admits.

    A profile states its standard on five axes: which home has to close a tour,
    what the household-date has to show, which zone system must hold every
    location, what a joint tour has to be, and which days of the week it takes.
    Several can be stamped in one run so different consumers can hold different
    standards -- a joint-tour model needs whole households and fully joint
    tours, a trip-level estimation does not:

    ```yaml
    usability_profiles:
      ctramp:
        tour_closes_at: primary_home
        household_day_needs: all_members
        zone_coverage: taz
        joint_tours: fully_joint
        travel_days: [tuesday, wednesday, thursday]
      analysis:
        tour_closes_at: anywhere
        household_day_needs: nothing
        zone_coverage: none
        joint_tours: partial
        travel_days: [monday, tuesday, wednesday, thursday, friday, saturday, sunday]
    ```

    Nothing is implicit: every column stamped is named in config and every
    profile answers every axis. Consumers then choose which to honour by name
    (``weight_profiles`` in the weighting, ``usability_profile`` in each
    formatter), and none of
    them re-derive completeness or tour validity -- the formatters read the
    column and raise if it is absent. ``survey_complete`` is always available as the
    floor without being declared.

    See [`stamp_usable`][processing.usability.stamp_usable] for the per-level
    rules and [`parse_usability_profiles`]
    [processing.usability.parse_usability_profiles] for the vocabulary.

    Args:
        tables: The canonical tables in ``requires``; ``household_days`` is built
            by the project cleaner.
        usability_profiles: Profile name -> ``{tour_closes_at, household_day_needs,
            zone_coverage, joint_tours, travel_days}``.
            Required, and every profile answers every axis: there is no default
            at either level, so a verdict never appears unasked and a column's
            meaning is never implicit.

    Returns:
        The tables, each with ``survey_complete`` reconciled and one column per
        profile added.

    Raises:
        ValueError: If ``usability_profiles`` is missing or malformed.
    """
    if usability_profiles is None:
        msg = (
            "cascade_usability requires usability_profiles. There is no default "
            "because the choice decides what every downstream consumer can read. "
            "Declare at least one profile, e.g. 'ctramp: {tour_closes_at: "
            "primary_home, household_day_needs: all_members}'."
        )
        raise ValueError(msg)
    profiles = parse_usability_profiles(usability_profiles)

    stamped = dict(tables)
    cascade_complete(stamped)
    for profile in profiles:
        stamp_usable(stamped, profile)
    log_gate_summary(stamped, profiles)
    return stamped
