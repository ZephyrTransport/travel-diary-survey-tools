"""What each stamped column means, in words -- for the log and for the writer.

A profile's columns are named in config, so they cannot be model fields. They
are described here instead: once into the run log, so an analyst can see what a
gate admitted, and once onto the canonical data, so the delivered output
carries the same sentence.
"""

import logging

import polars as pl

from .profiles import TourClosesAt, UsabilityProfile, parse_usability_profiles

logger = logging.getLogger(__name__)

_CLOSURE_TEXT = {
    TourClosesAt.PRIMARY_HOME: "returning to the home it left",
    TourClosesAt.ANY_HOME: (
        "returning to any home this person is known to have, so a tour closing "
        "at a second residence counts"
    ),
    TourClosesAt.ANYWHERE: (
        "ending anywhere, so a tour cut by the diary edge or resuming the next "
        "day counts; its trips are whole even though it does not close"
    ),
}


def _describe(profile: UsabilityProfile) -> dict[str, str]:
    """Column descriptions for one profile, for the generated-column registry."""
    household = (
        "every surveyable member's day complete on the same date"
        if profile.needs_whole_household_day
        else (
            "no household-date requirement; a member whose day is missing costs the "
            "others nothing, and the household's weight redistributes onto those "
            "who did report -- which preserves the household count but leaves the "
            "reporters standing in for the whole household"
        )
    )
    coverage = (
        f", with every location holding a '{profile.zone_coverage}' zone (the "
        "household's home included, so a household the consumer cannot write "
        "takes its tours and days with it)"
        if profile.requires_zones
        else ", asking nothing of geography"
    )
    joint = (
        "; a joint tour only where everyone shares every leg and none is work, "
        "work-related, school or escort"
        if profile.requires_fully_joint
        else "; joint tours as the survey detected them"
    )
    days = (
        "; on " + ", ".join(day.label for day in profile.travel_days) + " only"
        if profile.limits_travel_days
        else "; any day of the week"
    )
    gate = (
        f"Usable under the '{profile.name}' profile: survey-complete, a tour "
        f"{_CLOSURE_TEXT[profile.tour_closes_at]}, and {household}{coverage}{joint}{days}. "
        "Never admits a tour with a missing leg or no activity to anchor on, "
        "or a household, person or day the cleaner excluded (exclusion_reason)."
    )
    joint_tour_id = (
        f"joint_tour_id as the '{profile.name}' profile sees it: the id where this "
        "record is on its joint tour, null where it is not. "
        + (
            "On it only where usable, where everyone on the tour made the same joint "
            "trips, and where the tour has two or more such people with one purpose "
            "that is not work, work-related, school or escort; a person who leaves for a leg "
            "travels alone, as an individual tour."
            if profile.requires_fully_joint
            else "On it where usable and the survey put it in a joint tour."
        )
    )
    joint_trip_id = (
        f"joint_trip_id as the '{profile.name}' profile sees it: the id where this "
        "trip is on its joint trip, null where it is not. "
        + (
            "On it only where usable and where its tour is on its joint tour."
            if profile.requires_fully_joint
            else "On it where usable and the survey put it in a joint trip."
        )
    )
    return {
        profile.flag: gate,
        profile.joint_tour_id: joint_tour_id,
        profile.joint_trip_id: joint_trip_id,
    }


def _describe_household_day(profile: UsabilityProfile) -> str:
    """What ``household_days.<flag>`` means under *profile*."""
    if profile.needs_whole_household_day:
        return (
            f"Every surveyable member's day is {profile.flag} on this travel_date. "
            f"A household needs at least one such date to be {profile.flag}."
        )
    return (
        f"Every surveyable member's day is {profile.flag} on this travel_date. "
        "Not a gate for this profile, which asks nothing of the household-date; "
        "recorded so a dropped household can still be traced to the dates that "
        "cost it."
    )


def log_gate_summary(
    tables: dict[str, pl.DataFrame],
    profiles: list[UsabilityProfile],
) -> None:
    """Log, per table, how many records each profile admits."""
    for profile in profiles:
        lines = [
            f'Usability gate applied: "{profile.flag}".',
            "  " + _describe(profile)[profile.flag],
            "",
            f"  {'table':<16}{'rows':>10}{'survey_complete':>16}"
            f"{profile.flag:>18}{'newly unusable':>16}",
        ]
        for name, df in tables.items():
            n_usable = df.filter(pl.col(profile.flag)).height
            # A joint table built from members without survey_complete has none.
            if "survey_complete" not in df.columns:
                lines.append(f"  {name:<16}{df.height:>10,}{'-':>12}{n_usable:>18,}{'-':>16}")
                continue
            n_complete = df.filter(pl.col("survey_complete").fill_null(value=False)).height
            # Survey data that reported fine and the model still cannot use,
            # which is the column worth reading: the loss this gate adds alone.
            newly_unusable = n_complete - n_usable
            lines.append(
                f"  {name:<16}{df.height:>10,}{n_complete:>12,}{n_usable:>18,}{newly_unusable:>16,}"
            )
        logger.info("\n".join(lines))


def profile_columns(usability_profiles: dict | None) -> dict[str, dict[str, str]]:
    """The per-profile columns `cascade_usability` writes, with what each admits.

    Every profile column is listed for every stamped table, a few more than any one
    table carries; the writer keeps only those a table has.
    """
    if usability_profiles is None:
        return {}  # the step itself raises, with the reason
    # household_days gets only each profile's flag, below
    stamped = (
        "households",
        "persons",
        "person_days",
        "unlinked_trips",
        "linked_trips",
        "joint_trips",
        "tours",
        "joint_tours",
    )
    out: dict[str, dict[str, str]] = {}
    for profile in parse_usability_profiles(usability_profiles):
        described = _describe(profile)
        for table in stamped:
            out.setdefault(table, {}).update(described)
        out.setdefault("household_days", {})[profile.flag] = _describe_household_day(profile)
    return out
