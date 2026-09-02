"""Canonical completeness and model-usability logic.

Two kinds of flag roll **up** from two atomic facts -- whether each **trip** was
surveyed, and each tour's structural validity:

* ``complete`` -- was it reported? Rolls up from surveyed trips. One answer per
  run, never narrowed by model criteria and never configurable: if it is wrong,
  the fix belongs upstream in the project cleaner.
* one column per **usability profile** -- can the model consume it? Fuses each
  tour's structure with its household-day coherence, then gates the rest.

A profile states a standard on three axes -- which home has to close a tour,
what the household-date has to show, and which zone system has to be able to
address the record -- and a run may stamp several, so different consumers can
hold different standards. A joint-tour model needs whole households; a
trip-level estimation does not, and needs no model geography at all.

Every profile is named in config and answers every axis, so a column's meaning
reads off the config without knowing a base rule, and no verdict appears that
nobody asked for. Consumers name the profile they read
(``usability_flag_col``), and ``complete`` is always available as the floor
beneath all of them.

Each flag comes from one place in the tree -- its **direction** -- via a counting
**operation** (the ``op`` column below). Naming the direction makes it clear that
``household-day`` and the joint entities are the same kind of thing: cross-person
groupings, not parent/child.

| direction | value comes from               | example                               |
|-----------|--------------------------------|---------------------------------------|
| self      | measured on the record         | a trip never filled out               |
| up        | aggregate your own children    | person with zero complete days        |
| down      | inherit your parent's verdict  | good trip on a dropped tour           |
| lateral   | aggregate a cross-person group | one member skips a day, whole date fails |


The flow, and the rule on each line:

```text
COMPLETE -- rolls up from surveyed trips     op        rule
------------------------------------------------------------------------
trip .................................   direct    trip_survey_complete (measured leaf)
 └ person-day ........................   ALL       all trips surveyed, else declared no-travel
    ├ person .........................   >=1       has >=1 complete day
    └ household-day ..................   ALL       all surveyable members complete that date
       └ household ...................   >=1       has >=1 complete household-day

USABLE -- rolls down from the tour fuse      op        rule
------------------------------------------------------------------------
household ...........................   >=1       has >=1 usable household-day
 ├ household-day ....................   ALL       all surveyable members' days usable that date
 └ person ...........................   >=1       has >=1 usable day
    └ day ...........................   >=1       >=1 usable tour (or a no-travel day)
       └ tour .......................   fuse      complete AND admitted-quality AND
                                                     hh-day complete AND addressable
                                                     (the last three are the axes)
          ├ linked trip .............   inherit   takes its tour's verdict
          |  └ unlinked trip ........   inherit   takes its linked trip's verdict
          ├ joint tour ..............   >=2       >=2 usable member tours
          └ joint trip ..............   >=2       >=2 usable member linked trips

declared no-travel: num_reasons_no_travel >= 1, OR proxy_complete (a proxy filled the day in --
  this is how children, who file no trips themselves, still get a complete day).
surveyable: persons whose travel the survey could collect at all. Unsurveyable persons
  (unrelated members, e.g. roommates) have no day rows in the vendor data; where a source
  carries any, they neither veto the household-day ALL reductions nor inherit their verdict.
VALID feeds the fuse: trips -> linked trips -> tour, home-to-home, no missing legs.
op: ALL / >=1 / >=2 = quantity gate (count members vs threshold);
    direct = measured; inherit = take a neighbour's verdict; fuse = AND of conditions
```

Because each level reads its neighbours, the derivation order is load-bearing;
getting it wrong fails loudly (an unflagged member table raises), never silently.

This module is the one place the logic lives. The ``cascade_completeness``
pipeline step runs :func:`cascade_complete` once and then :func:`stamp_usable`
per profile; every downstream consumer only *reads* the resulting flags.
"""

import logging
from dataclasses import dataclass

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step

logger = logging.getLogger(__name__)


# --- Profile axes ------------------------------------------------------------
# A profile answers every one, explicitly. There is no default for any: a column
# whose meaning depends on a value nobody wrote is the thing profiles exist to
# stop.

# Which home has to close a tour. The quality codes divide into open ends and
# missing data, and this axis walks down the open ends only.
PRIMARY_HOME = "primary_home"  # VALID only
ANY_HOME = "any_home"  # + PARTIAL_OTHER_HOME
ANYWHERE = "anywhere"  # + PARTIAL_DAY_SPLIT, PARTIAL_DIARY_EDGE
TOUR_CLOSES_AT = (PRIMARY_HOME, ANY_HOME, ANYWHERE)

# What the household-date has to show.
ALL_MEMBERS = "all_members"  # every surveyable member's day complete
NOTHING = "nothing"  # no household-date requirement
HOUSEHOLD_DAY_NEEDS = (ALL_MEMBERS, NOTHING)

# Which zone system has to be able to address the record. Unlike the other two
# axes this vocabulary is open: the value names a zone geography the zone step
# produced, so the legal set is whatever that step was configured to build.
NO_ZONE_COVERAGE = "none"  # no geographic requirement

# Zone columns the zone step writes, by table. A record is addressable when
# every one of its locations landed in the named geography.
_ZONE_PREFIXES: dict[str, tuple[str, ...]] = {
    "households": ("home",),
    "tours": ("o", "d"),
    "linked_trips": ("o", "d"),
    "unlinked_trips": ("o", "d"),
}

# The zone step leaves an unmatched location null; formatters have historically
# also written -1 as a missing sentinel, so neither is addressable.
_MISSING_ZONE = -1

# Quality codes each closure setting admits, cumulatively. NO_DESTINATION and
# SPATIAL_GAP appear nowhere: they are not open ends but missing data -- no
# tolerance for a tour that stops somewhere unexpected makes a missing leg
# present, or an activity that never happened happen.
_ADMITTED_QUALITY: dict[str, tuple[TourDataQuality, ...]] = {
    PRIMARY_HOME: (TourDataQuality.VALID,),
    ANY_HOME: (TourDataQuality.VALID, TourDataQuality.PARTIAL_OTHER_HOME),
    ANYWHERE: (
        TourDataQuality.VALID,
        TourDataQuality.PARTIAL_OTHER_HOME,
        TourDataQuality.PARTIAL_DAY_SPLIT,
        TourDataQuality.PARTIAL_DIARY_EDGE,
    ),
}


@dataclass(frozen=True)
class UsabilityProfile:
    """One usability standard, stated on both axes, and the columns it writes.

    A profile says which home has to close a tour, what the household-date has
    to show, and which zone system has to be able to address the record. Config
    gives all three explicitly; none may be left implicit there.

    Coverage is an axis rather than a universal because the zone systems differ:
    a trip outside one model's area may sit comfortably inside another's, so
    "addressable" is a question only a named consumer can answer.

    Two things hold at every setting and so are not axes. A profile is always a
    subset of ``complete`` -- a usability column admitting unreported records
    would be a different flag wearing the name. And no profile admits a tour
    with *missing data* (a missing leg, an activity that never happened), as
    opposed to one that merely ends somewhere unexpected.

    Two columns come out of a pass. ``flag`` is the per-record verdict consumers
    read. ``household_day`` records, per date, whether *all* surveyable members'
    days passed. It is computed for every profile even where that profile does
    not gate on it, so the column means the same thing everywhere and can still
    answer "which dates cost this household its usability".
    """

    name: str
    tour_closes_at: str
    household_day_needs: str
    # Config must state this like any other axis -- ``_one_profile`` rejects a
    # profile that omits it. The default serves in-process construction only,
    # where saying nothing about geography means asking nothing of it.
    zone_coverage: str = NO_ZONE_COVERAGE

    @property
    def flag(self) -> str:
        """Per-record verdict column."""
        return self.name

    @property
    def requires_zones(self) -> bool:
        """Whether this profile asks that a record be addressable in a geography."""
        return self.zone_coverage != NO_ZONE_COVERAGE

    @property
    def household_day(self) -> str:
        """All-surveyable-members-that-date column."""
        return f"hh_day_{self.name}"

    @property
    def admitted_quality(self) -> tuple[TourDataQuality, ...]:
        """Tour quality codes this profile's closure setting admits."""
        return _ADMITTED_QUALITY[self.tour_closes_at]

    @property
    def needs_whole_household_day(self) -> bool:
        """Whether a date must show every surveyable member's day complete."""
        return self.household_day_needs == ALL_MEMBERS


# Records that sit on a day: their ``complete`` is their own AND their day's
# (a trip or tour is no more complete than the day it belongs to).
_DAY_RECORDS = ("unlinked_trips", "linked_trips", "joint_trips", "tours", "joint_tours")

# Trip tables whose model-usability follows the tour they belong to (tour_id).
_TOUR_MEMBER_TABLES = ("unlinked_trips", "linked_trips")

# A joint entity is only joint while two of its members survive.
MIN_JOINT_PARTICIPANTS = 2

# Joint groupings and the member table they are formed from:
# (joint_table, member_table, shared key).
_JOINT_GROUPINGS = (
    ("joint_trips", "linked_trips", "joint_trip_id"),
    ("joint_tours", "tours", "joint_tour_id"),
)


def rollup_completeness(tables: dict[str, pl.DataFrame | None]) -> None:
    """Roll ``complete`` UP from days, then broadcast each day's value down, in place.

    Completeness is measured at the person-day (``day.complete`` is set upstream
    by the project cleaner from surveyed trips / a declared no-travel day). This
    derives the rest:

    * ``person.complete`` = it has at least one complete day (an ANY rollup).
    * every day-record (trip, tour, joint entity) = its own reporting AND its
      day complete -- broadcast down, since a trip is no more complete than the
      day it sits in.

    ``household.complete`` is a household-day rollup and is set separately by
    :func:`rollup_household_complete` (it needs ``hh_day_complete`` first). Tables
    lacking ``complete`` or the join key are left unchanged. Idempotent.
    """
    days = tables.get("days")
    if days is None or "complete" not in days.columns:
        return
    day_complete = pl.col("complete").fill_null(value=False)

    persons = tables.get("persons")
    if persons is not None and "person_id" in days.columns:
        per_flag = days.group_by("person_id").agg(day_complete.any().alias("_c"))
        tables["persons"] = (
            persons.drop("complete", strict=False)
            .join(per_flag, on="person_id", how="left")
            .with_columns(pl.col("_c").fill_null(value=False).alias("complete"))
            .drop("_c")
        )

    day_flag = days.select("day_id", day_complete.alias("_day_complete"))
    for name in _DAY_RECORDS:
        df = tables.get(name)
        if df is None or "complete" not in df.columns or "day_id" not in df.columns:
            continue
        tables[name] = (
            df.join(day_flag, on="day_id", how="left")
            .with_columns(
                (
                    pl.col("complete").fill_null(value=False)
                    & pl.col("_day_complete").fill_null(value=True)
                ).alias("complete")
            )
            .drop("_day_complete")
        )


def rollup_household_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Set ``household.complete`` = has at least one complete household-day, in place.

    A household is complete when at least one date was coherently observed (every
    member complete). Requires ``hh_day_complete`` on days (from
    :func:`flag_household_day_complete`). Left unchanged if days or the flag is
    absent.
    """
    households = tables.get("households")
    days = tables.get("days")
    if (
        households is None
        or days is None
        or "hh_day_complete" not in days.columns
        or "hh_id" not in days.columns
    ):
        return
    has_complete_day = (
        days.filter(pl.col("hh_day_complete").fill_null(value=False))
        .select("hh_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_h"))
    )
    tables["households"] = (
        households.drop("complete", strict=False)
        .join(has_complete_day, on="hh_id", how="left")
        .with_columns(pl.col("_h").fill_null(value=False).alias("complete"))
        .drop("_h")
    )


def _join_surveyable(days: pl.DataFrame, persons: pl.DataFrame | None) -> pl.DataFrame:
    """Return *days* with a ``_surveyable`` bool column joined from persons.

    A person is *surveyable* when the survey could collect their travel at all;
    unrelated household members (e.g. roommates) are not, file no trips, and
    must not veto the household-day reductions -- the vendor gives them no day
    rows whatsoever. When the persons table (or its ``surveyable`` column) is
    absent, every member-day counts, preserving the plain ALL reduction.
    """
    if persons is None or "surveyable" not in persons.columns or "person_id" not in days.columns:
        return days.with_columns(pl.lit(value=True).alias("_surveyable"))
    flag = persons.select(
        "person_id",
        pl.col("surveyable").cast(pl.Boolean).fill_null(value=True).alias("_surveyable"),
    )
    return days.join(flag, on="person_id", how="left").with_columns(
        pl.col("_surveyable").fill_null(value=True)
    )


def flag_household_day_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``hh_day_complete`` on the days table, in place (reverse cascade).

    A **household-day** -- the set of person-days sharing one ``hh_id`` and
    ``travel_date`` -- is complete only when every *surveyable* member's day is
    complete (an ALL reduction over surveyable member-days). The result is
    written back onto each day on that date, so ``hh_day_complete`` marks
    whether the day belongs to a coherently observed household-date.

    Unsurveyable members (see :func:`_join_surveyable`) neither veto the date
    nor borrow its verdict: their own day rows -- if a data source carries any
    -- keep their own ``complete``, which is normally False since they file no
    trips. A source that nonetheless marks one complete is believed rather than
    overruled: the contradiction is upstream, and inventing a verdict here would
    hide it.

    This runs after :func:`rollup_completeness`, so ``complete`` already
    reflects ancestry; the reduction then flows the other way, up from members to
    the shared date. Idempotent. Days without ``hh_id`` / ``travel_date`` (e.g.
    schema-only fixtures) fall back to each day's own ``complete``.
    """
    days = tables.get("days")
    if days is None or "complete" not in days.columns:
        return

    own = pl.col("complete").fill_null(value=False)
    if "hh_id" not in days.columns or "travel_date" not in days.columns:
        tables["days"] = days.with_columns(own.alias("hh_day_complete"))
        return

    days = _join_surveyable(days, tables.get("persons"))
    # all() over an empty set is True: a date observed only through unsurveyable
    # members has no surveyable observation to fail -- and no surveyable day to
    # gain usability from it either.
    household_day = days.group_by("hh_id", "travel_date").agg(
        own.filter(pl.col("_surveyable")).all().alias("_hh_day_complete")
    )
    tables["days"] = (
        days.join(household_day, on=["hh_id", "travel_date"], how="left")
        .with_columns(
            pl.when(pl.col("_surveyable"))
            .then(pl.col("_hh_day_complete").fill_null(value=False))
            .otherwise(own)
            .alias("hh_day_complete")
        )
        .drop("_hh_day_complete", "_surveyable")
    )


def flag_household_day_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the household-day usable flag: ALL surveyable member-days usable, in place.

    The usable-side mirror of :func:`flag_household_day_complete`: a household-day
    is *usable* only when every surveyable member's day is model-usable, not
    merely complete. ``household`` then needs at least one such date.
    Unsurveyable members neither veto the date nor inherit its verdict.
    Requires the per-record flag on days; days without ``hh_id`` /
    ``travel_date`` fall back to each day's own verdict.
    """
    days = tables.get("days")
    if days is None or cols.flag not in days.columns:
        return

    own = pl.col(cols.flag).fill_null(value=False)
    if "hh_id" not in days.columns or "travel_date" not in days.columns:
        tables["days"] = days.with_columns(own.alias(cols.household_day))
        return

    days = _join_surveyable(days, tables.get("persons"))
    household_day = days.group_by("hh_id", "travel_date").agg(
        own.filter(pl.col("_surveyable")).all().alias("_hh_day_usable")
    )
    tables["days"] = (
        days.join(household_day, on=["hh_id", "travel_date"], how="left")
        .with_columns(
            pl.when(pl.col("_surveyable"))
            .then(pl.col("_hh_day_usable").fill_null(value=False))
            .otherwise(own)
            .alias(cols.household_day)
        )
        .drop("_hh_day_usable", "_surveyable")
    )


def _flag_person_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Set a person's usable flag = has at least one usable day, in place.

    Requires the flag on days; a days table present but unflagged raises rather
    than silently passing every person.
    """
    persons = tables.get("persons")
    if persons is None or "complete" not in persons.columns:
        return
    days = tables.get("days")
    if days is not None and cols.flag not in days.columns:
        msg = (
            f"Cannot flag persons: days has no {cols.flag} column yet. Flag days first, "
            "otherwise every person silently passes on completeness alone."
        )
        raise ValueError(msg)
    if days is None or "person_id" not in days.columns:
        tables["persons"] = persons.with_columns(
            pl.col("complete").fill_null(value=False).alias(cols.flag)
        )
        return
    has_usable_day = (
        days.filter(pl.col(cols.flag).fill_null(value=False))
        .select("person_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_u"))
    )
    tables["persons"] = (
        persons.join(has_usable_day, on="person_id", how="left")
        .with_columns(pl.col("_u").fill_null(value=False).alias(cols.flag))
        .drop("_u")
    )


def _zone_valid(column: str) -> pl.Expr:
    """A location is addressable when the zone join actually placed it."""
    col = pl.col(column)
    return (col.is_not_null() & (col != _MISSING_ZONE)).fill_null(value=False)


def _zone_expr(frame: pl.DataFrame, table: str, cols: UsabilityProfile) -> pl.Expr:
    """Addressability of *table*'s own locations under this profile.

    Constant-true when the profile asks for no coverage, so callers combine it
    unconditionally rather than branching on the axis.

    Raises:
        ValueError: If the profile names a geography this frame does not carry,
            which means the zone step either did not run before the cascade or
            was not configured to build it.
    """
    if not cols.requires_zones:
        return pl.lit(value=True)

    wanted = [f"{prefix}_{cols.zone_coverage}" for prefix in _ZONE_PREFIXES.get(table, ())]
    missing = [column for column in wanted if column not in frame.columns]
    if missing:
        msg = (
            f"usability_profile '{cols.name}' sets zone_coverage: "
            f"'{cols.zone_coverage}', but {table} carries no {', '.join(missing)}. "
            f"Run add_zone_ids before cascade_completeness and declare a "
            f"zone_geography named '{cols.zone_coverage}', or set zone_coverage: "
            f"{NO_ZONE_COVERAGE} to ask nothing of geography."
        )
        raise ValueError(msg)

    expr = pl.lit(value=True)
    for column in wanted:
        expr = expr & _zone_valid(column)
    return expr


def _home_zone_ok(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> pl.DataFrame | None:
    """Per-household addressability of the home location, or None when unasked.

    The cascade reduces *upward*, so a household-level fact cannot reach its
    descendants by rolling up. It is joined into the tour and day verdicts
    instead, which leaves the cascade's direction alone: with every tour and
    every day of an unaddressable household failing, the person and household
    verdicts follow on their own. Days need the term as well as tours, because a
    genuine no-travel day passes the has-a-usable-tour test by design and would
    otherwise survive in a household that cannot be written at all.
    """
    if not cols.requires_zones:
        return None

    households = tables.get("households")
    if households is None or "hh_id" not in households.columns:
        return None

    return households.select(
        "hh_id", _zone_expr(households, "households", cols).alias("_home_zone_ok")
    )


def _flag_tours(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the usable flag on tours, in place.

    A tour is usable when its structure is admissible *and* it sits on a coherent
    household-date, so it agrees with its own day. Without the coherence term
    CT-RAMP (which reads the tour's flag) would keep a tour the weighting has
    zeroed. ``hh_day_complete`` is available because the reverse cascade runs
    before this.

    Subtours then take their parent's verdict on top of their own: an at-work
    subtour is travel *within* its parent tour, so keeping one whose parent was
    dropped would leave CT-RAMP an AT_WORK tour hanging off a tour that is not
    in the output, and would strand the parent's ``atWork_freq``.
    """
    tours = tables.get("tours")
    if tours is None or "complete" not in tours.columns:
        return

    usable = _tour_usable_expr(
        has_quality="tour_data_quality" in tours.columns,
        has_category="tour_category" in tours.columns,
        closes_at=cols.tour_closes_at,
    ) & _zone_expr(tours, "tours", cols)

    # The household's own home has to be addressable too, or the tour belongs to
    # a household the consumer cannot write. See _home_zone_ok on why this is
    # joined in rather than rolled down.
    home = _home_zone_ok(tables, cols)
    if home is not None and "hh_id" in tours.columns:
        tours = tours.join(home, on="hh_id", how="left")
        usable = usable & pl.col("_home_zone_ok").fill_null(value=False)
    else:
        home = None

    days = tables.get("days")
    coherence_required = cols.needs_whole_household_day
    if (
        not coherence_required
        or days is None
        or "hh_day_complete" not in days.columns
        or "day_id" not in tours.columns
    ):
        flagged = tours.with_columns(usable.alias(cols.flag))
    else:
        coherence = days.select(
            "day_id", pl.col("hh_day_complete").alias("_hh_day_complete")
        ).unique(subset="day_id")
        flagged = (
            tours.join(coherence, on="day_id", how="left")
            .with_columns(
                (usable & pl.col("_hh_day_complete").fill_null(value=False)).alias(cols.flag)
            )
            .drop("_hh_day_complete")
        )

    if home is not None:
        flagged = flagged.drop("_home_zone_ok")
    tables["tours"] = _flag_subtours_from_parent(flagged, cols)


def _flag_subtours_from_parent(
    tours: pl.DataFrame,
    cols: UsabilityProfile,
) -> pl.DataFrame:
    """Reduce each subtour's usable flag by its parent tour's verdict.

    Primary tours self-reference (``parent_tour_id == tour_id``) and so are
    unaffected. A subtour whose parent is missing entirely is left on its own
    verdict rather than silently dropped -- the parent's absence is a different
    defect, and dropping here would hide it.
    """
    if "parent_tour_id" not in tours.columns or "tour_id" not in tours.columns:
        return tours

    parent_usable = tours.select(
        pl.col("tour_id").alias("parent_tour_id"),
        pl.col(cols.flag).alias("_parent_usable"),
    )
    return (
        tours.join(parent_usable, on="parent_tour_id", how="left")
        .with_columns(
            (pl.col(cols.flag) & pl.col("_parent_usable").fill_null(value=True)).alias(cols.flag)
        )
        .drop("_parent_usable")
    )


def _tour_usable_expr(
    *,
    has_quality: bool,
    has_category: bool,
    closes_at: str,
) -> pl.Expr:
    """Tour-level usability for one closure setting.

    A tour qualifies when its (cascaded) reporting is complete and its quality
    code is one the setting admits:

    * ``primary_home`` -- VALID only: a whole round trip back to the home it
      left. The anchor is home for a home-based tour and the workplace for an
      at-work subtour, so one criterion admits both.
    * ``any_home`` -- also ``PARTIAL_OTHER_HOME``. That tour did reach a home of
      this person's, just not the one tours are built around; the trips are
      whole and only the anchor differs.
    * ``anywhere`` -- also the two other open ends, ``PARTIAL_DAY_SPLIT`` and
      ``PARTIAL_DIARY_EDGE``. The tour stops somewhere unexpected and you want
      the trips anyway.

    ``NO_DESTINATION`` and ``SPATIAL_GAP`` are admitted by no setting: they mark
    missing data rather than an open end.

    The ``tour_category`` term only applies at ``primary_home``. Past that the
    admitted codes are partial by construction, so a category term would
    contradict the quality term it sits beside -- the two columns state the same
    fact about where a tour ends.

    Args:
        has_quality: Whether the frame carries ``tour_data_quality``.
        has_category: Whether the frame carries ``tour_category``.
        closes_at: One of :data:`TOUR_CLOSES_AT`.
    """
    admitted = [q.value for q in _ADMITTED_QUALITY[closes_at]]

    # Each term is filled before it is combined. A null descriptor means the
    # structure was never established, which is not an admission; leaving the
    # null to propagate would put a three-valued verdict in a boolean column and
    # make every consumer decide separately what an unknown means.
    structural = pl.lit(value=True)
    if has_quality:
        structural = structural & pl.col("tour_data_quality").is_in(admitted).fill_null(value=False)
    if has_category and closes_at == PRIMARY_HOME:
        structural = structural & (
            (pl.col("tour_category") == TourCategory.COMPLETE.value).fill_null(value=False)
        )

    return pl.col("complete").fill_null(value=False) & structural


def _flag_joint_groupings(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the usable flag on the joint trip / joint tour tables, in place.

    A joint entity is a grouping, so it is usable only while it is still *joint*:
    at least :data:`MIN_JOINT_PARTICIPANTS` of its member records must themselves
    be model-usable. This is what stops a joint trip surviving after the tour it
    belonged to was dropped, and what stops a joint tour reduced to a single
    participant reaching CT-RAMP, where it would violate ``num_participants >= 2``.

    A member table that was never supplied is a legitimate partial call and the
    grouping falls back to its own ``complete``. A member table that *is* present
    but carries no verdict column is a caller ordering error and raises: the
    fallback would silently pass every grouping, which is exactly the behaviour
    this rule exists to replace.

    Raises:
        ValueError: If a member table is present but has not been flagged yet.
    """
    for joint_table, member_table, key in _JOINT_GROUPINGS:
        df = tables.get(joint_table)
        if df is None or "complete" not in df.columns:
            continue

        base = pl.col("complete").fill_null(value=False)
        members = tables.get(member_table)

        if members is not None and cols.flag not in members.columns:
            msg = (
                f"Cannot flag {joint_table}: {member_table} has no {cols.flag} column yet. "
                f"Flag the member table before the groupings that count it, otherwise "
                f"every {joint_table} record silently passes."
            )
            raise ValueError(msg)

        if members is None or key not in members.columns or key not in df.columns:
            tables[joint_table] = df.with_columns(base.alias(cols.flag))
            continue

        usable_members = (
            members.filter(pl.col(key).is_not_null() & pl.col(cols.flag))
            .group_by(key)
            .agg(pl.len().alias("_n_usable_members"))
        )
        tables[joint_table] = (
            df.join(usable_members, on=key, how="left")
            .with_columns(
                (base & (pl.col("_n_usable_members").fill_null(0) >= MIN_JOINT_PARTICIPANTS)).alias(
                    cols.flag
                )
            )
            .drop("_n_usable_members")
        )


def _flag_households(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the usable flag on households, in place.

    Strictly, a household is admissible only if it has at least one **usable
    household-day** -- a date on which every member's day is model-usable (the
    usable-side mirror of the complete-day rule). Without one there is no
    coherently usable household pattern to weight, so the household is dropped
    rather than left holding weight it cannot pass down.

    A profile whose ``household_day_needs`` is ``nothing`` counts usable *days*
    instead. Reading the household-day column here regardless would leave the
    household on the strict rule while its tours and days had already relaxed --
    the profile would look like it worked, and the household would still be
    dropped and its weight zeroed.

    Requires days to carry the profile's household-day column; a days table
    present but unflagged raises rather than silently passing every household.

    Raises:
        ValueError: If days is present but has not been flagged yet.
    """
    households = tables.get("households")
    if households is None or "complete" not in households.columns:
        return

    base = pl.col("complete").fill_null(value=False)
    # The date-level reduction is only the rule while coherence is required.
    counts = cols.household_day if cols.needs_whole_household_day else cols.flag
    days = tables.get("days")
    if days is not None and counts not in days.columns:
        msg = (
            f"Cannot flag households: days has no {counts} column yet. Flag days "
            "first, otherwise every household silently passes."
        )
        raise ValueError(msg)
    if days is None or "hh_id" not in days.columns:
        tables["households"] = households.with_columns(base.alias(cols.flag))
        return

    hh_has_usable_day = (
        days.filter(pl.col(counts).fill_null(value=False))
        .select("hh_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_hh_has_usable_day"))
    )
    tables["households"] = (
        households.join(hh_has_usable_day, on="hh_id", how="left")
        .with_columns((base & pl.col("_hh_has_usable_day").fill_null(value=False)).alias(cols.flag))
        .drop("_hh_has_usable_day")
    )


def cascade_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Derive every ``complete`` flag, in place.

    Survey reporting only: what the vendor collected, cascaded through the
    hierarchy. Nothing here consults a model criterion, and one run of the
    pipeline has exactly one answer, so this is independent of how many
    usability passes follow.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
    """
    # -- Completeness rolls UP from days (person = >=1 complete day) and each
    #    day-record inherits its day's complete.
    rollup_completeness(tables)

    # -- Household-day coherence (lateral: ALL member-days complete that date) -
    flag_household_day_complete(tables)

    # -- household.complete = >=1 complete household-day ----------------------
    rollup_household_complete(tables)


def stamp_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Derive one usability verdict for every table, in place.

    Reads the ``complete`` flags :func:`cascade_complete` left behind and writes
    the pair of columns *cols* names. It never writes ``complete``, so it can be
    run more than once over the same tables to stamp several verdicts side by
    side, each under its own name.

    See the module docstring for the derivation-order diagram and the per-level
    rule table.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        cols: Column names this pass writes.
    """
    # -- Tours ---------------------------------------------------------------
    _flag_tours(tables, cols)
    tours = tables.get("tours")

    # -- Days (needs a coherent household-date AND a usable tour) -------------
    # A day is usable only within a complete household-day: even a genuine
    # no-travel day is only a clean observation when the whole household was
    # observed that date. ``hh_day_complete`` already implies the day's own
    # ``complete`` (it is an ALL over the members, which includes this one), so
    # a profile admitting incomplete household-days falls back to that own
    # reporting rather than dropping the term entirely.
    days = tables.get("days")
    if days is not None and "complete" in days.columns:
        base = (
            pl.col("complete").fill_null(value=False)
            if not cols.needs_whole_household_day
            else pl.col("hh_day_complete").fill_null(value=False)
        )
        if tours is not None and cols.flag not in tours.columns:
            msg = (
                f"Cannot flag days: tours has no {cols.flag} column yet. Flag tours "
                "first, otherwise every day silently passes on completeness alone."
            )
            raise ValueError(msg)

        # A no-travel day has no tour to carry the household's addressability, so
        # it takes the term directly or it would outlive its own household.
        home = _home_zone_ok(tables, cols)
        if home is not None and "hh_id" in days.columns:
            days = days.join(home, on="hh_id", how="left")
            base = base & pl.col("_home_zone_ok").fill_null(value=False)
        else:
            home = None

        if tours is not None and "day_id" in tours.columns:
            day_has_usable = tours.group_by("day_id").agg(
                pl.col(cols.flag).any().alias("_day_has_usable_tour")
            )
            days = days.join(day_has_usable, on="day_id", how="left")
            # null == the day has no tours at all -> a legitimate no-travel day.
            flagged = days.with_columns(
                (base & pl.col("_day_has_usable_tour").fill_null(value=True)).alias(cols.flag)
            ).drop("_day_has_usable_tour")
        else:
            flagged = days.with_columns(base.alias(cols.flag))

        tables["days"] = flagged.drop("_home_zone_ok") if home is not None else flagged

    # -- Persons: usable = has >=1 usable day (mirror of complete) ------------
    # A person kept with no usable day contributes no travel but stays a real
    # person; their days simply fall out.
    _flag_person_usable(tables, cols)

    # -- Household-day usability (lateral: ALL member-days usable) then
    #    household = >=1 usable household-day.
    flag_household_day_usable(tables, cols)
    _flag_households(tables, cols)

    # -- Member trips follow their tour --------------------------------------
    tour_usable = None
    if tours is not None and cols.flag in tours.columns:
        tour_usable = tours.select("tour_id", pl.col(cols.flag).alias("_tour_usable"))
    for name in _TOUR_MEMBER_TABLES:
        df = tables.get(name)
        if df is None or "complete" not in df.columns:
            continue
        base = pl.col("complete").fill_null(value=False)
        if tour_usable is not None and "tour_id" in df.columns:
            df = df.join(tour_usable, on="tour_id", how="left")
            tables[name] = df.with_columns(
                (base & pl.col("_tour_usable").fill_null(value=False)).alias(cols.flag)
            ).drop("_tour_usable")
        else:
            tables[name] = df.with_columns(base.alias(cols.flag))

    # -- Joint groupings (need two surviving members to still be joint) -------
    # Last: they read the member tables flagged above.
    _flag_joint_groupings(tables, cols)


def compute_usability(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp ``complete`` and one profile's verdict on every table, in place.

    The two halves in order: reporting completeness once, then one usability
    pass over it. A run stamping several profiles calls :func:`cascade_complete`
    once and :func:`stamp_usable` per profile instead.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
        profile: The standard to apply, named on both axes.
    """
    cascade_complete(tables)
    stamp_usable(tables, profile)


_AXES: dict[str, tuple[str, ...]] = {
    "tour_closes_at": TOUR_CLOSES_AT,
    "household_day_needs": HOUSEHOLD_DAY_NEEDS,
}

# The axis whose values come from the zone step's configuration rather than a
# vocabulary fixed here, so it is required and typed but not membership-checked.
_ZONE_AXIS = "zone_coverage"

# Columns the reporting cascade owns. A profile taking one of these names would
# overwrite a survey fact with a modelling judgement.
_RESERVED_NAMES = ("complete", "hh_day_complete")


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
            + f", {_ZONE_AXIS} (a zone_name add_zone_ids builds|{NO_ZONE_COVERAGE})."
        )
        raise ValueError(msg)

    profiles = [_one_profile(name, axes) for name, axes in spec.items()]
    _reject_column_collisions(profiles)
    return profiles


def _one_profile(name: str, axes: dict[str, str]) -> UsabilityProfile:
    """Build one profile from its configured axes, or say why it cannot.

    Raises:
        ValueError: If the name is reserved, or any axis is missing, unknown, or
            given a value outside its vocabulary.
    """
    if name in _RESERVED_NAMES:
        msg = (
            f"usability_profile '{name}' collides with a column the reporting "
            f"cascade owns ({', '.join(_RESERVED_NAMES)}). Those record what the "
            "survey collected, not what a model will take; pick another name."
        )
        raise ValueError(msg)
    if not isinstance(axes, dict):
        msg = (
            f"usability_profile '{name}' must give each axis a value, as "
            f"'{{{', '.join(f'{a}: ...' for a in _AXES)}}}'. Got: {axes!r}."
        )
        raise ValueError(msg)  # noqa: TRY004 - malformed config, not a caller type error

    for axis, allowed in _AXES.items():
        if axis not in axes:
            msg = (
                f"usability_profile '{name}' does not say '{axis}'. Every profile "
                f"answers every axis, so a column's meaning can be read off the "
                f"config alone. Legal values: {', '.join(allowed)}."
            )
            raise ValueError(msg)
        if axes[axis] not in allowed:
            msg = (
                f"usability_profile '{name}' sets {axis}: '{axes[axis]}', which is "
                f"not one of {', '.join(allowed)}."
            )
            raise ValueError(msg)

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

    unknown = sorted(set(axes) - set(_AXES) - {_ZONE_AXIS})
    if unknown:
        msg = (
            f"usability_profile '{name}' names unknown axis/axes: "
            f"{', '.join(unknown)}. Known axes: {', '.join([*_AXES, _ZONE_AXIS])}."
        )
        raise ValueError(msg)

    return UsabilityProfile(
        name=name,
        tour_closes_at=axes["tour_closes_at"],
        household_day_needs=axes["household_day_needs"],
        zone_coverage=coverage,
    )


def _reject_column_collisions(profiles: list[UsabilityProfile]) -> None:
    """Two profiles must not write the same column.

    Each profile writes two columns and derives one of them from its name, so a
    profile called ``hh_day_ctramp_usable`` writes its verdict into the column
    ``ctramp_usable`` derives for its household-day reduction -- and whichever
    ran second would win, silently. Distinct names are not enough; the columns
    they generate have to be distinct too.

    Raises:
        ValueError: If any two profiles write the same column.
    """
    written: dict[str, str] = {}
    for profile in profiles:
        for column in (profile.flag, profile.household_day):
            if column in written:
                msg = (
                    f"usability_profiles '{written[column]}' and '{profile.name}' both "
                    f"write the column '{column}', so one would silently overwrite the "
                    "other. Each profile writes its own name and 'hh_day_' plus its "
                    "name; rename one of them."
                )
                raise ValueError(msg)
            written[column] = profile.name


def suggest_usability_columns(frame: pl.DataFrame) -> str:
    """A "did you mean" line naming the boolean columns *frame* actually has.

    For a consumer that was pointed at a usability column which is not there.
    The names come from config, so there is no pattern to match on: a rule like
    "ends in _usable" only holds until a project picks a name that does not fit
    it, and then the hint reports none rather than admitting it cannot tell.
    Every boolean column is offered instead. Some will be unrelated, which costs
    a reader one glance; a confident empty answer costs them a debugging session
    in the wrong step.
    """
    boolean = sorted(name for name, dtype in frame.schema.items() if dtype == pl.Boolean)
    if not boolean:
        return "It carries no boolean columns at all."
    return f"Did you mean one of these boolean columns? {', '.join(boolean)}."


_CLOSURE_TEXT = {
    PRIMARY_HOME: "returning to the home it left",
    ANY_HOME: (
        "returning to any home this person is known to have, so a tour closing "
        "at a second residence counts"
    ),
    ANYWHERE: (
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
        f", with every location addressable in '{profile.zone_coverage}' (the "
        "household's home included, so a household the consumer cannot write "
        "takes its tours and days with it)"
        if profile.requires_zones
        else ", asking nothing of geography"
    )
    gate = (
        f"Usable under the '{profile.name}' profile: survey-complete, a tour "
        f"{_CLOSURE_TEXT[profile.tour_closes_at]}, and {household}{coverage}. Never "
        "admits a tour with a missing leg or no activity to anchor on."
    )
    if profile.needs_whole_household_day:
        hh_day = (
            f"Every surveyable member's day is {profile.flag} on this travel_date. "
            f"A household needs at least one such date to be {profile.flag}."
        )
    else:
        hh_day = (
            f"Every surveyable member's day is {profile.flag} on this travel_date. "
            "Not a gate for this profile, which asks nothing of the household-date; "
            "recorded so a dropped household can still be traced to the dates that "
            "cost it."
        )
    return {profile.flag: gate, profile.household_day: hh_day}


def _log_gate_summary(
    tables: dict[str, pl.DataFrame | None],
    profiles: list[UsabilityProfile],
) -> None:
    """Log, per table, how many records each profile admits."""
    for profile in profiles:
        lines = [
            f'Usability gate applied: "{profile.flag}".',
            "  " + _describe(profile)[profile.flag],
            "",
            f"  {'table':<16}{'rows':>10}{'complete':>12}{profile.flag:>18}{'newly unusable':>16}",
        ]
        for name, df in tables.items():
            if df is None or profile.flag not in df.columns:
                continue
            n = df.height
            n_complete = df.filter(pl.col("complete").fill_null(value=False)).height
            n_usable = df.filter(pl.col(profile.flag)).height
            # Survey data that reported fine and the model still cannot use,
            # which is the column worth reading: the loss this gate adds alone.
            newly_unusable = n_complete - n_usable
            lines.append(
                f"  {name:<16}{n:>10,}{n_complete:>12,}{n_usable:>18,}{newly_unusable:>16,}"
            )
        logger.info("\n".join(lines))


@step(
    requires={
        "days": {"day_id", "hh_id", "travel_date", "complete"},
        "tours": {"tour_id", "day_id", "complete"},
    },
)
def cascade_completeness(
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
    usability_profiles: dict[str, dict[str, str]] | None = None,
    canonical_data: CanonicalData | None = None,
) -> dict[str, pl.DataFrame]:
    """Cascade ``complete`` through the hierarchy and stamp one column per profile.

    Walks both kinds of flag across every table so they are internally consistent
    with whatever was set upstream (the vendor's day-level completeness, any
    manual adjustments in the project cleaner, and the structural verdicts from
    tour extraction):

    * ``complete`` -- survey reporting completeness, cascaded hh <-> person <->
      day <-> trip/tour. Never narrowed by model criteria, and never
      configurable: if it is wrong the fix belongs upstream in the cleaner.
    * one column per usability profile -- the subset of ``complete`` that
      profile admits.

    A profile states its standard on two axes: which home has to close a tour,
    and what the household-date has to show. Several can be stamped in one run
    so different consumers can hold different standards -- a joint-tour model
    needs whole households, a trip-level estimation does not:

    ```yaml
    usability_profiles:
      ctramp_usable:
        tour_closes_at: primary_home
        household_day_needs: all_members
      analysis_usable:
        tour_closes_at: anywhere
        household_day_needs: nothing
    ```

    Nothing is implicit: every column stamped is named in config and every
    profile answers both axes. Consumers then choose which to honour by name
    (``usability_flag_col``, in the weighting and both formatters), and none of
    them re-derive completeness or tour validity -- the formatters read the
    column and raise if it is absent. ``complete`` is always available as the
    floor without being declared.

    See [`stamp_usable`][processing.completeness.stamp_usable] for the per-level
    rules and [`parse_usability_profiles`]
    [processing.completeness.parse_usability_profiles] for the vocabulary.

    Args:
        households: Canonical households.
        persons: Canonical persons.
        days: Canonical person-days.
        unlinked_trips: Canonical unlinked trips.
        linked_trips: Canonical linked trips.
        joint_trips: Aggregated joint trips.
        tours: Canonical tours (with ``tour_data_quality`` / ``tour_category``).
        joint_tours: Aggregated joint tours.
        usability_profiles: Profile name -> ``{tour_closes_at, household_day_needs}``.
            Required, and every profile answers both axes: there is no default
            at either level, so a verdict never appears unasked and a column's
            meaning is never implicit.
        canonical_data: Injected by the pipeline. Profile columns are named from
            config and so cannot be model fields; they are registered here with
            a description of what each admits, which keeps them in the delivered
            output and documented rather than silently dropped.

    Returns:
        The provided tables, each with ``complete`` reconciled and one column
        per profile added.

    Raises:
        ValueError: If ``usability_profiles`` is missing or malformed.
    """
    if usability_profiles is None:
        msg = (
            "cascade_completeness requires usability_profiles. There is no default "
            "because the choice decides what every downstream consumer can read. "
            "Declare at least one profile, e.g. 'ctramp_usable: {tour_closes_at: "
            "primary_home, household_day_needs: all_members}'."
        )
        raise ValueError(msg)
    profiles = parse_usability_profiles(usability_profiles)

    tables: dict[str, pl.DataFrame | None] = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
        "joint_tours": joint_tours,
    }

    cascade_complete(tables)
    for profile in profiles:
        stamp_usable(tables, profile)
    _log_gate_summary(tables, profiles)

    if canonical_data is not None:
        _register_profile_columns(tables, profiles, canonical_data)

    return {name: df for name, df in tables.items() if df is not None}


def _register_profile_columns(
    tables: dict[str, pl.DataFrame | None],
    profiles: list[UsabilityProfile],
    canonical_data: CanonicalData,
) -> None:
    """Declare each stamped column to the writer, with what it means."""
    for profile in profiles:
        described = _describe(profile)
        for name, df in tables.items():
            if df is None:
                continue
            present = {col: text for col, text in described.items() if col in df.columns}
            if present:
                canonical_data.register_generated_columns(name, present)
