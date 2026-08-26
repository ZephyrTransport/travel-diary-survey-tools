"""Canonical completeness and model-usability logic.

Two flags roll **up** from two atomic facts -- whether each **trip** was
surveyed, and each tour's structural validity:

* ``complete`` -- was it reported? Rolls up from surveyed trips.
* ``model_usable`` -- can the model consume it? Fuses each tour's structure with
  its household-day coherence, then gates the rest. This is the single gate the
  CT-RAMP/DaySim drop and the weighting read.

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
       └ tour .......................   fuse      complete AND VALID AND hh-day complete
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

This module is the one place the logic lives. :func:`compute_model_usable` is
applied once by the ``cascade_completeness`` pipeline step; every downstream
consumer only *reads* the resulting flags.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from pipeline.decoration import step

logger = logging.getLogger(__name__)

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
    -- keep their own ``complete`` (typically False), so they can never become
    usable travel days.

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


def flag_household_day_usable(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``hh_day_usable`` on days: ALL surveyable member-days usable that date, in place.

    The usable-side mirror of :func:`flag_household_day_complete`: a household-day
    is *usable* only when every surveyable member's day is model-usable, not
    merely complete. ``household.model_usable`` then needs at least one such
    date. Unsurveyable members neither veto the date nor inherit its verdict.
    Requires ``model_usable`` on days; days without ``hh_id`` / ``travel_date``
    fall back to each day's own ``model_usable``.
    """
    days = tables.get("days")
    if days is None or "model_usable" not in days.columns:
        return

    own = pl.col("model_usable").fill_null(value=False)
    if "hh_id" not in days.columns or "travel_date" not in days.columns:
        tables["days"] = days.with_columns(own.alias("hh_day_usable"))
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
            .alias("hh_day_usable")
        )
        .drop("_hh_day_usable", "_surveyable")
    )


def _flag_person_usable(tables: dict[str, pl.DataFrame | None]) -> None:
    """Set ``person.model_usable`` = has at least one usable day, in place.

    Requires ``model_usable`` on days; a days table present but unflagged raises
    rather than silently passing every person.
    """
    persons = tables.get("persons")
    if persons is None or "complete" not in persons.columns:
        return
    days = tables.get("days")
    if days is not None and "model_usable" not in days.columns:
        msg = (
            "Cannot flag persons: days has no model_usable column yet. Flag days first, "
            "otherwise every person silently passes on completeness alone."
        )
        raise ValueError(msg)
    if days is None or "person_id" not in days.columns:
        tables["persons"] = persons.with_columns(
            pl.col("complete").fill_null(value=False).alias("model_usable")
        )
        return
    has_usable_day = (
        days.filter(pl.col("model_usable").fill_null(value=False))
        .select("person_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_u"))
    )
    tables["persons"] = (
        persons.join(has_usable_day, on="person_id", how="left")
        .with_columns(pl.col("_u").fill_null(value=False).alias("model_usable"))
        .drop("_u")
    )


def _flag_tours(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``model_usable`` on tours, in place.

    A tour is usable when its structure is admissible *and* it sits on a coherent
    household-date, so it agrees with its own day. Without the coherence term
    CT-RAMP (which reads ``tour.model_usable``) would keep a tour the weighting
    has zeroed. ``hh_day_complete`` is available because the reverse cascade runs
    before this.

    Subtours then take their parent's verdict on top of their own: an at-work
    subtour is travel *within* its parent tour, so keeping one whose parent was
    dropped would leave CT-RAMP an AT_WORK tour hanging off a tour that is not
    in the output, and would strand the parent's ``atWork_freq``.
    """
    tours = tables.get("tours")
    if tours is None or "complete" not in tours.columns:
        return

    usable = _tour_model_usable_expr(
        has_quality="tour_data_quality" in tours.columns,
        has_category="tour_category" in tours.columns,
    )
    days = tables.get("days")
    if days is None or "hh_day_complete" not in days.columns or "day_id" not in tours.columns:
        tables["tours"] = _flag_subtours_from_parent(
            tours.with_columns(usable.alias("model_usable"))
        )
        return

    coherence = days.select("day_id", pl.col("hh_day_complete").alias("_hh_day_complete")).unique(
        subset="day_id"
    )
    tables["tours"] = _flag_subtours_from_parent(
        tours.join(coherence, on="day_id", how="left")
        .with_columns(
            (usable & pl.col("_hh_day_complete").fill_null(value=False)).alias("model_usable")
        )
        .drop("_hh_day_complete")
    )


def _flag_subtours_from_parent(tours: pl.DataFrame) -> pl.DataFrame:
    """Reduce each subtour's ``model_usable`` by its parent tour's verdict.

    Primary tours self-reference (``parent_tour_id == tour_id``) and so are
    unaffected. A subtour whose parent is missing entirely is left on its own
    verdict rather than silently dropped -- the parent's absence is a different
    defect, and dropping here would hide it.
    """
    if "parent_tour_id" not in tours.columns or "tour_id" not in tours.columns:
        return tours

    parent_usable = tours.select(
        pl.col("tour_id").alias("parent_tour_id"),
        pl.col("model_usable").alias("_parent_usable"),
    )
    return (
        tours.join(parent_usable, on="parent_tour_id", how="left")
        .with_columns(
            (pl.col("model_usable") & pl.col("_parent_usable").fill_null(value=True)).alias(
                "model_usable"
            )
        )
        .drop("_parent_usable")
    )


def _tour_model_usable_expr(*, has_quality: bool, has_category: bool) -> pl.Expr:
    """model_usable expression for the tours table.

    A tour is model-usable when its (cascaded) reporting is complete and its
    structure is admissible: VALID quality (not single-trip, loop, partial,
    change-mode or spatially gapped) AND COMPLETE category -- it departs from
    and returns to its own anchor. The anchor is home for a home-based tour and
    the workplace for an at-work subtour, so one criterion admits both. This
    matches the CT-RAMP / DaySim drop criterion exactly.

    VALID now implies COMPLETE, since the partial shapes are quality codes in
    their own right; the category term is kept as a guard for frames that carry
    a category but no quality column.
    """
    usable = pl.col("complete").fill_null(value=False)
    if has_quality:
        usable = usable & (pl.col("tour_data_quality") == TourDataQuality.VALID.value)
    if has_category:
        usable = usable & (pl.col("tour_category") == TourCategory.COMPLETE.value)
    return usable


def _flag_joint_groupings(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``model_usable`` on the joint trip / joint tour tables, in place.

    A joint entity is a grouping, so it is usable only while it is still *joint*:
    at least :data:`MIN_JOINT_PARTICIPANTS` of its member records must themselves
    be model-usable. This is what stops a joint trip surviving after the tour it
    belonged to was dropped, and what stops a joint tour reduced to a single
    participant reaching CT-RAMP, where it would violate ``num_participants >= 2``.

    A member table that was never supplied is a legitimate partial call and the
    grouping falls back to its own ``complete``. A member table that *is* present
    but carries no ``model_usable`` is a caller ordering error and raises: the
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

        if members is not None and "model_usable" not in members.columns:
            msg = (
                f"Cannot flag {joint_table}: {member_table} has no model_usable column yet. "
                f"Flag the member table before the groupings that count it, otherwise "
                f"every {joint_table} record silently passes."
            )
            raise ValueError(msg)

        if members is None or key not in members.columns or key not in df.columns:
            tables[joint_table] = df.with_columns(base.alias("model_usable"))
            continue

        usable_members = (
            members.filter(pl.col(key).is_not_null() & pl.col("model_usable"))
            .group_by(key)
            .agg(pl.len().alias("_n_usable_members"))
        )
        tables[joint_table] = (
            df.join(usable_members, on=key, how="left")
            .with_columns(
                (base & (pl.col("_n_usable_members").fill_null(0) >= MIN_JOINT_PARTICIPANTS)).alias(
                    "model_usable"
                )
            )
            .drop("_n_usable_members")
        )


def _flag_households(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``model_usable`` on households, in place.

    A household is admissible only if it has at least one **usable
    household-day** -- a date on which every member's day is model-usable (the
    usable-side mirror of the complete-day rule). Without one there is no
    coherently usable household pattern to weight, so the household is dropped
    rather than left holding weight it cannot pass down.

    Requires days to carry ``hh_day_usable`` (from
    :func:`flag_household_day_usable`); a days table present but unflagged raises
    rather than silently passing every household.

    Raises:
        ValueError: If days is present but has no ``hh_day_usable`` column.
    """
    households = tables.get("households")
    if households is None or "complete" not in households.columns:
        return

    base = pl.col("complete").fill_null(value=False)
    days = tables.get("days")
    if days is not None and "hh_day_usable" not in days.columns:
        msg = (
            "Cannot flag households: days has no hh_day_usable column yet. Run "
            "flag_household_day_usable first, otherwise every household silently passes."
        )
        raise ValueError(msg)
    if days is None or "hh_id" not in days.columns:
        tables["households"] = households.with_columns(base.alias("model_usable"))
        return

    hh_has_usable_day = (
        days.filter(pl.col("hh_day_usable").fill_null(value=False))
        .select("hh_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_hh_has_usable_day"))
    )
    tables["households"] = (
        households.join(hh_has_usable_day, on="hh_id", how="left")
        .with_columns(
            (base & pl.col("_hh_has_usable_day").fill_null(value=False)).alias("model_usable")
        )
        .drop("_hh_has_usable_day")
    )


def compute_model_usable(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``model_usable`` on every table, in place.

    Runs the two completeness cascades then derives ``model_usable`` per level.
    See the module docstring for the flag definitions, the derivation-order
    diagram, and the per-level rule table.

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

    # -- Tours ---------------------------------------------------------------
    _flag_tours(tables)
    tours = tables.get("tours")

    # -- Days (needs a coherent household-date AND a usable tour) -------------
    # A day is usable only within a complete household-day: even a genuine
    # no-travel day is only a clean observation when the whole household was
    # observed that date. ``hh_day_complete`` already implies the day's own
    # ``complete`` (it is an ALL over the members, which includes this one).
    days = tables.get("days")
    if days is not None and "complete" in days.columns:
        base = pl.col("hh_day_complete").fill_null(value=False)
        if tours is not None and "model_usable" not in tours.columns:
            msg = (
                "Cannot flag days: tours has no model_usable column yet. Flag tours "
                "first, otherwise every day silently passes on completeness alone."
            )
            raise ValueError(msg)
        if tours is not None and "day_id" in tours.columns:
            day_has_usable = tours.group_by("day_id").agg(
                pl.col("model_usable").any().alias("_day_has_usable_tour")
            )
            days = days.join(day_has_usable, on="day_id", how="left")
            # null == the day has no tours at all -> a legitimate no-travel day.
            tables["days"] = days.with_columns(
                (base & pl.col("_day_has_usable_tour").fill_null(value=True)).alias("model_usable")
            ).drop("_day_has_usable_tour")
        else:
            tables["days"] = days.with_columns(base.alias("model_usable"))

    # -- Persons: model_usable = has >=1 usable day (mirror of complete) ------
    # A person kept with no usable day contributes no travel but stays a real
    # person; their days simply fall out.
    _flag_person_usable(tables)

    # -- Household-day usability (lateral: ALL member-days usable) then
    #    household = >=1 usable household-day.
    flag_household_day_usable(tables)
    _flag_households(tables)

    # -- Member trips follow their tour --------------------------------------
    tour_usable = None
    if tours is not None and "model_usable" in tours.columns:
        tour_usable = tours.select("tour_id", pl.col("model_usable").alias("_tour_usable"))
    for name in _TOUR_MEMBER_TABLES:
        df = tables.get(name)
        if df is None or "complete" not in df.columns:
            continue
        base = pl.col("complete").fill_null(value=False)
        if tour_usable is not None and "tour_id" in df.columns:
            df = df.join(tour_usable, on="tour_id", how="left")
            tables[name] = df.with_columns(
                (base & pl.col("_tour_usable").fill_null(value=False)).alias("model_usable")
            ).drop("_tour_usable")
        else:
            tables[name] = df.with_columns(base.alias("model_usable"))

    # -- Joint groupings (need two surviving members to still be joint) -------
    # Last: they read the member tables flagged above.
    _flag_joint_groupings(tables)


def _log_gate_summary(tables: dict[str, pl.DataFrame | None]) -> None:
    """Log, per table, how many records are complete vs model-usable."""
    lines = [
        "Model-usability gate applied.",
        '  "complete" = survey reporting (partials/overnights included); '
        '"model_usable" = admissible to the tour-based model.',
        "",
        f"  {'table':<16}{'rows':>10}{'complete':>12}{'model_usable':>14}{'newly unusable':>16}",
    ]
    for name, df in tables.items():
        if df is None or "model_usable" not in df.columns:
            continue
        n = df.height
        n_complete = df.filter(pl.col("complete").fill_null(value=False)).height
        n_usable = df.filter(pl.col("model_usable")).height
        # Survey data that reported fine and the model still cannot use, which
        # is the column worth reading: the loss this gate adds on its own.
        newly_unusable = n_complete - n_usable
        lines.append(f"  {name:<16}{n:>10,}{n_complete:>12,}{n_usable:>14,}{newly_unusable:>16,}")
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
) -> dict[str, pl.DataFrame]:
    """Cascade ``complete`` through the hierarchy and stamp ``model_usable``.

    Walks both flags across every table so they are internally consistent with
    whatever was set upstream (the vendor's day-level completeness, any manual
    adjustments in the project cleaner, and the structural verdicts from tour
    extraction):

    * ``complete`` -- survey reporting completeness, cascaded hh <-> person <->
      day <-> trip/tour. Never narrowed by model criteria.
    * ``model_usable`` -- the subset of ``complete`` that is admissible to the
      tour-based models (VALID tour structure, COMPLETE home-to-home category,
      household-day coherence).

    The step is parameterless by design: it computes one canonical set of
    facts, and each downstream consumer chooses which flag to honour
    (weighting via ``usability_flag_col``, the CT-RAMP/DaySim formatters via
    ``drop_invalid_tours``). None of them re-derive completeness or tour
    validity. Because the step is cached, both flags are inspectable in the
    canonical output.

    See [`compute_model_usable`][processing.completeness.compute_model_usable]
    for the per-level rules.

    Args:
        households: Canonical households.
        persons: Canonical persons.
        days: Canonical person-days.
        unlinked_trips: Canonical unlinked trips.
        linked_trips: Canonical linked trips.
        joint_trips: Aggregated joint trips.
        tours: Canonical tours (with ``tour_data_quality`` / ``tour_category``).
        joint_tours: Aggregated joint tours.

    Returns:
        The provided tables, each with ``complete`` reconciled and a
        ``model_usable`` column added.
    """
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

    compute_model_usable(tables)
    _log_gate_summary(tables)

    return {name: df for name, df in tables.items() if df is not None}
