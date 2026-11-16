"""Habitual locations: find each person's anchors, and say which one a trip end is at.

A *habitual location* is an anchor: a place a person has a standing
relationship with — their home, their workplace, their school — as opposed to
somewhere they merely happened to stop. Two tables come out of here, because
identity and presence have different existence conditions. A habitual location
exists whether or not the person went there during the diary period — a
reported workplace belonging to someone who teleworked all week is still their
workplace — so it cannot live on a table keyed by day::

    habitual_locations      one row per (person, kind, number)
                            who / where / how we know

    habitual_location_days  one row per (location, day) the person was present
                            how long / how many visits / how the day related

For example::

    habitual_locations
      person  type  num  is_primary  source
      1       WORK  1    True        reported
      1       WORK  2    False       observed   <- a Tue/Thu office
      2       HOME  1    True        reported
      2       HOME  2    False       reported   <- a second home

The table is delivered by survey cleaning holding the reported locations —
the survey's home, work and school coordinates, plus any further ones such as
a vendor's second home — built by :func:`reported_habitual_locations` and
taken as given. The ``detect_habitual_locations`` step, after trip linking,
appends the observed ones and writes the day table; it never changes a delivered
row. Tour extraction and everything after only read it.

Observed locations:

- Observed homes, only when ``include_observed_homes`` is on (it has no
  default): days the respondent said began or ended at home or at their other
  home, placed where that day's travel began or ended. Away from every reported
  home, such a place is another home of theirs. Off, the only homes are the
  reported ones.
- Observed workplaces and schools: places the person went for that purpose and
  stayed long enough. Work-related stops count towards a workplace at their own
  longer cutoff: their median is under an hour, a meeting or a delivery, but a
  stay the length of a working day says the person works there.

Homes are never inferred from travel alone. A stay at "another residence" may be
a second home or a friend's flat, and only the respondent can say which.

A single buffer (``MatchConfig.buffer_meters``) is the whole spatial
vocabulary. Stops within it of one another are one place; a place within it of a
reported location is that location, so it is not added again; and a trip end
within it of a location is at that location when its purpose agrees with the
location's kind (:func:`matching.match_points`). Only an agreeing purpose
counts: an unknown one matches nothing. The exception is a person's own front
door: within ``at_address_meters`` of their primary home, any purpose is at that
home, because there the purpose describes what they were doing rather than a
different place. Tour extraction asks the same question through
:func:`match_trip_ends`, with the same settings.

Modules:

- ``detect_habitual_locations``: the pipeline step.
- ``habitual_location_configs``: the rules.
- ``reported``: the delivered table, built from the survey's coordinates.
- ``episodes``: trips as stays.
- ``observed``: clustering, and the observed locations.
- ``numbering``: primacy, numbering and identifiers.
- ``matching``: the one "at a location" test.
- ``location_days``: the per-day table.
"""

from .detect_habitual_locations import add_observed_locations, detect_habitual_locations
from .habitual_location_configs import HabitualLocationConfig, MatchConfig
from .matching import match_trip_ends
from .reported import reported_habitual_locations

__all__ = [
    "HabitualLocationConfig",
    "MatchConfig",
    "add_observed_locations",
    "detect_habitual_locations",
    "match_trip_ends",
    "reported_habitual_locations",
]
