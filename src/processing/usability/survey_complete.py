"""The completeness cascade: what the survey collected, walked across the tables.

Survey reporting only. Nothing here consults a model criterion or reads a
profile, and one run of the pipeline has exactly one answer, so this is
independent of how many usability passes follow it.

A **household-day** is one household's travel date (``household_days``, built
by the project cleaner): the person-days sharing one ``hh_day_id``. Its verdict
is an ALL over its surveyable members' days. The same reduction serves the
usable walk, which imports it from here.
"""

import polars as pl

from .reduce import and_reduced

# Records that sit on a day: their ``survey_complete`` is their own AND their day's
# (a trip or tour is no more complete than the day it belongs to). Joint trips and
# joint tours are not here: they belong to a household date, not to one member's
# day, and their own value is already the AND of their members'.
_DAY_RECORDS = ("unlinked_trips", "linked_trips", "tours")


def cascade_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Derive every ``survey_complete`` flag, in place.

    Survey reporting only: what the vendor collected, cascaded through the
    hierarchy. Nothing here consults a model criterion, and one run of the
    pipeline has exactly one answer, so this is independent of how many
    usability passes follow.

    Args:
        tables: Mutable dict of table_name -> DataFrame; the joint tables may be None.
    """
    # -- Completeness rolls UP from days (person = >=1 complete day) and each
    #    day-record inherits its day's complete.
    _rollup_completeness(tables)

    # -- Household-day coherence (lateral: ALL member-days complete that date) -
    _flag_household_day_complete(tables)

    # -- households.survey_complete = >=1 complete household-day -------------
    _rollup_household_complete(tables)


def _rollup_completeness(tables: dict[str, pl.DataFrame | None]) -> None:
    """Roll ``survey_complete`` UP from days, then broadcast each day's value down, in place.

    Completeness is measured at the person-day (``person_days.survey_complete`` is set
    upstream by the project cleaner, from the vendor's day verdict). This derives
    the rest:

    * ``persons.survey_complete`` = it has at least one complete day (an ANY rollup).
    * every day-record (trip, tour) = its own reporting AND its day complete --
      broadcast down, since a trip is no more complete than the day it sits in.
      A record with no verdict of its own (null, e.g. a trip the vendor added
      without a trip survey) takes its day's. Joint trips and tours are not
      day-records; see ``_DAY_RECORDS``.

    ``households.survey_complete`` is a household-day rollup and is set separately by
    :func:`_rollup_household_complete` (it needs ``household_days`` judged first).
    Idempotent.
    """
    person_days = tables["person_days"]
    day_complete = pl.col("survey_complete").fill_null(value=False)

    tables["persons"] = and_reduced(
        tables["persons"].drop("survey_complete", strict=False),
        person_days,
        "person_id",
        day_complete.any(),
        fill=False,
        into="survey_complete",
    )

    for name in _DAY_RECORDS:
        # A null own verdict defers to the day's.
        tables[name] = and_reduced(
            tables[name].with_columns(pl.col("survey_complete").fill_null(value=True)),
            person_days,
            "person_day_id",
            day_complete.first(),
            fill=True,
            into="survey_complete",
        )


def _flag_household_day_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``household_days.survey_complete``, in place.

    A household-day is complete only when every *surveyable* member's day is
    complete: an ALL reduction over those person-days, which are the vendor's
    verdicts. It decides nothing of its own. Unsurveyable members (see
    :func:`_join_surveyable`) take no part.

    This runs after :func:`_rollup_completeness`, so ``survey_complete`` already
    reflects ancestry; the reduction then flows the other way, up from members to
    the shared date. Idempotent.
    """
    reduce_to_household_days(tables, pl.col("survey_complete"), "survey_complete")


def _rollup_household_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Set ``households.survey_complete`` = has at least one complete household-day, in place.

    A household is complete when at least one date was coherently observed (every
    member complete). Reads ``household_days.survey_complete``, from
    :func:`_flag_household_day_complete`.
    """
    tables["households"] = and_reduced(
        tables["households"].drop("survey_complete", strict=False),
        tables["household_days"],
        "hh_id",
        pl.col("survey_complete").fill_null(value=False).any(),
        fill=False,
        into="survey_complete",
    )


# -- The household-date reduction -----------------------------------------------


def _join_surveyable(person_days: pl.DataFrame, persons: pl.DataFrame) -> pl.DataFrame:
    """Return *person_days* with a ``_surveyable`` bool column joined from persons.

    A person is *surveyable* when the survey could collect their travel at all;
    unrelated household members (e.g. roommates) are not, file no trips, and
    must not veto the household-day reductions -- the vendor gives them no day
    rows whatsoever. A survey that marks no one unsurveyable need not write the
    column, and then every member-day counts.
    """
    if "surveyable" not in persons.columns:
        return person_days.with_columns(pl.lit(value=True).alias("_surveyable"))
    flag = persons.select(
        "person_id",
        pl.col("surveyable").cast(pl.Boolean).fill_null(value=True).alias("_surveyable"),
    )
    return person_days.join(flag, on="person_id", how="left").with_columns(
        pl.col("_surveyable").fill_null(value=True)
    )


def reduce_to_household_days(
    tables: dict[str, pl.DataFrame | None], own: pl.Expr, column: str
) -> None:
    """Write ALL(surveyable members' *own*) onto ``household_days.<column>``, in place.

    A household-day with no surveyable member's day on it still exists; nothing
    the survey meant to observe was observed on it, so it is False rather than
    vacuously True.
    """
    person_days = _join_surveyable(tables["person_days"], tables["persons"])
    tables["household_days"] = and_reduced(
        tables["household_days"].drop(column, strict=False),
        person_days.filter(pl.col("_surveyable")),
        "hh_day_id",
        own.fill_null(value=False).all(),
        fill=False,
        into=column,
    )


def with_household_day(
    person_days: pl.DataFrame,
    tables: dict[str, pl.DataFrame | None],
    column: str,
    alias: str,
) -> pl.DataFrame:
    """Return *person_days* with *alias*: its household-day's *column*.

    An unsurveyable member's day takes no part in the household-day, so it is
    not judged by it either: it keeps its own *column*.
    """
    verdict = tables["household_days"].select("hh_day_id", pl.col(column).alias("_hh_day_verdict"))
    person_days = _join_surveyable(person_days, tables["persons"]).join(
        verdict, on="hh_day_id", how="left"
    )
    return person_days.with_columns(
        pl.when(pl.col("_surveyable"))
        .then(pl.col("_hh_day_verdict").fill_null(value=False))
        .otherwise(pl.col(column).fill_null(value=False))
        .alias(alias)
    ).drop("_hh_day_verdict", "_surveyable")


def with_household_day_excluded(
    person_days: pl.DataFrame,
    persons: pl.DataFrame,
    alias: str,
) -> pl.DataFrame:
    """Return *person_days* with *alias*: any surveyable member's day on its date was excluded.

    An excluded day is one the project cleaner gave an ``exclusion_reason``. An
    unsurveyable member's day takes no part in the household-day, so it answers
    for its own exclusion only.
    """
    own = pl.col("exclusion_reason").is_not_null()
    person_days = _join_surveyable(person_days, persons)
    any_excluded = (
        person_days.filter(pl.col("_surveyable"))
        .group_by("hh_day_id")
        .agg(own.any().alias("_hh_day_excluded"))
    )
    return (
        person_days.join(any_excluded, on="hh_day_id", how="left")
        .with_columns(
            pl.when(pl.col("_surveyable"))
            .then(pl.col("_hh_day_excluded").fill_null(value=False))
            .otherwise(own)
            .alias(alias)
        )
        .drop("_hh_day_excluded", "_surveyable")
    )
