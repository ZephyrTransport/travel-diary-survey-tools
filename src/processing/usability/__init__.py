"""Canonical completeness and model-usability flags: how each is derived.

Both kinds of flag start from two atomic facts: whether each **day** was
surveyed (``person_days.survey_complete``, which the project cleaner takes from
the survey vendor's own day verdict), and each tour's structural validity.

Each flag comes from one place in the tree -- its **direction** -- via a counting
**operation** (the ``op`` column below). Naming the direction makes it clear that
``household-day`` and the joint entities are the same kind of thing: cross-person
groupings, not one record inside another.

| direction | value comes from               | example                               |
|-----------|--------------------------------|---------------------------------------|
| self      | measured on the record         | a trip never filled out               |
| up        | count the records below it     | person with zero complete days        |
| down      | inherit its owner's verdict    | good trip on a dropped tour           |
| lateral   | aggregate a cross-person group | one member skips a day, whole date fails |


The flow, and the rule on each line:

```text
COMPLETE -- rolls up from complete days     op        rule
------------------------------------------------------------------------
person-day ...........................   direct    person_days.survey_complete, from the cleaner
                                                    (the vendor's day verdict, as given)
 ├ trip / tour .......................   inherit   its own value AND its day's (no own
                                                    value: the day's alone)
 ├ person ............................   >=1       has >=1 complete day
 └ household-day .....................   ALL       all surveyable members complete that date
    └ household ......................   >=1       has >=1 complete household-day

USABLE, UP (_cascade_up) -- counts from the tour fuse   op   rule
------------------------------------------------------------------------
household ...........................   >=1       has >=1 usable household-day
 ├ household-day ....................   ALL       all surveyable members' days usable that date
 └ person ...........................   >=1       has >=1 usable day
    └ day ...........................   >=1       on a day of the week it takes, its
                                                  person not excluded by the cleaner,
                                                  and >=1 usable tour (or a no-travel day)
       └ tour .......................   fuse      complete AND admitted-quality AND
                                                     hh-day complete AND has a zone
                                                     (those three are the axes)
                                                     AND every trip on it usable
          └ linked trip / unlinked trip   direct    its own survey_complete

USABLE, DOWN (_cascade_down) -- inherits from the household   op   rule
------------------------------------------------------------------------
household -> person -> day -> tour ..   inherit   unusable if the one it belongs to is
 └ linked trip / unlinked trip ......   inherit   unusable if its tour is

JOINT (flag_joint) -- judged beside the chain, after it     op   rule
------------------------------------------------------------------------
joint tour ..........................   >=2       >=2 tours on it, and its
                                                  household usable
joint trip ..........................   >=2       >=2 linked trips on it, likewise;
                                                  judged again after rejected joint
                                                  tours void their ids

JOINT IDS -- joint_tour_id_<profile> on tours and both trip tables, and
joint_trip_id_<profile> on linked trips: the bare id where the record is on it,
null where it is not. Decided first within flag_joint, read by the groupings
above, the weighting and the formatters; the bare id is the survey's own
answer and is never edited.
------------------------------------------------------------------------
partial .............................   direct    usable and in a survey-detected group
fully_joint .........................   class     usable, not work/school/escort, and
                                                  made the same joint trips as the
                                                  others (largest class of identical
                                                  legs); then the tour needs >=2 such
                                                  people and one purpose, or everyone
                                                  on it travels alone

no-travel day: a complete day with no trips. Whether it is complete is the vendor's call
  (a no-travel reason given, or, for a child, a proxy report), never re-judged here.
surveyable: persons whose travel the survey could collect at all. Unsurveyable persons
  (unrelated members, e.g. roommates) have no day rows in the vendor data; where a source
  carries any, they neither veto the household-day ALL reductions nor inherit their verdict.
VALID feeds the fuse: trips -> linked trips -> tour, home-to-home, no missing legs.
op: ALL / >=1 / >=2 = quantity gate (count members vs threshold);
    direct = measured; inherit = take a neighbour's verdict; fuse = AND of conditions
```

Because each level reads its neighbours, the derivation order is load-bearing;
getting it wrong fails loudly (an unflagged member table raises), never silently.

This package is the one place the logic lives. The ``cascade_usability``
pipeline step runs :func:`cascade_complete` once and then :func:`stamp_usable`
per profile; every downstream consumer only *reads* the resulting flags.

Modules:

- ``profiles``: what a profile is, and how config states one.
- ``reduce``: the one reduce-and-join every level is built from.
- ``survey_complete``: the completeness cascade, once per run, with the
  household-date reduction both kinds of flag use.
- ``usable``: one profile's verdict, from the tour fuse up and then down every
  table, with the zone checks.
- ``joint_groupings``: which records are on a joint tour or joint trip, per profile.
- ``descriptions``: what each stamped column means, in words.
- ``cascade_usability``: the pipeline step.
"""

from .cascade_usability import cascade_usability
from .joint_groupings import MIN_JOINT_PARTICIPANTS
from .profiles import (
    NO_ZONE_COVERAGE,
    HouseholdDayNeeds,
    JointTours,
    TourClosesAt,
    UsabilityProfile,
    parse_usability_profiles,
    profile_of,
    usable_col_for,
)
from .survey_complete import cascade_complete
from .usable import stamp_usable

__all__ = [
    "MIN_JOINT_PARTICIPANTS",
    "NO_ZONE_COVERAGE",
    "HouseholdDayNeeds",
    "JointTours",
    "TourClosesAt",
    "UsabilityProfile",
    "cascade_complete",
    "cascade_usability",
    "parse_usability_profiles",
    "profile_of",
    "stamp_usable",
    "usable_col_for",
]
