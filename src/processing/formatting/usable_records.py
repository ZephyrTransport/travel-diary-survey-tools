"""Select the records a formatter may see, by reading a verdict rather than making one.

A formatter does not decide which records exist. `cascade_completeness` decides
that once, for every consumer, and stamps it as a usability profile; this reads
the profile the config names and keeps what it admits.

That is the whole of it, and the reason is arithmetic rather than tidiness. The
weighting sums over the records a profile admits, so anything the formatter
removed *afterwards* left a weight standing for members that are not in the
output -- a joint group's `num_participants` counted from survivors while its
weight was summed over more of them. Filtering on the same verdict the weighting
used makes the two agree by construction.

Filtering each table independently is safe because the cascade is already
consistent: member trips inherit their tour's verdict, the upward reductions are
`>= 1` so a usable tour implies a usable day, person and household, and
`_flag_joint_groupings` guarantees a surviving joint group still has two usable
members. Nothing can be orphaned by removing what the flag rejects.

Zero-weight records are *not* removed. A record can be usable to this consumer
and still carry no weight, because the weighting names one profile and the
formatters may name others -- until weights are computed per profile, dropping
those would be one consumer silently deleting another's data. They are reported
instead, and the count is the measure of that mismatch.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourDataQuality
from processing.completeness import MIN_JOINT_PARTICIPANTS, suggest_usability_columns

logger = logging.getLogger(__name__)

# Every table the gate can apply to; a formatter passes the ones it holds.
# All are stamped by the cascade, so a
# missing verdict is a broken frame rather than a table that opted out.
_GATED_TABLES = (
    "households",
    "persons",
    "days",
    "tours",
    "linked_trips",
    "unlinked_trips",
    "joint_trips",
    "joint_tours",
)

# Group memberships, as (member table, id column, group table). Rejecting one
# member can leave the rest below the quorum that makes a group joint at all.
_GROUP_MEMBERSHIPS = (
    ("linked_trips", "joint_trip_id", "joint_trips"),
    ("tours", "joint_tour_id", "joint_tours"),
)

# What each table's weight is called, for the zero-weight report.
_WEIGHT_COLUMNS = {
    "households": "hh_weight",
    "persons": "person_weight",
    "days": "day_weight",
    "tours": "tour_weight",
    "linked_trips": "linked_trip_weight",
    "unlinked_trips": "unlinked_trip_weight",
    "joint_trips": "joint_trip_weight",
    "joint_tours": "joint_tour_weight",
}


def keep_usable(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> dict[str, pl.DataFrame]:
    """Keep the records *usability_flag_col* admits, across every table.

    Args:
        tables: Canonical tables, keyed by name.
        usability_flag_col: The usability profile this consumer reads. Naming a
            different profile from the one the weighting used is legitimate, and
            reported by the zero-weight check below.

    Returns:
        The same mapping, filtered. Tables absent from the input stay absent.

    Raises:
        ValueError: If a gated table carries no verdict, which means
            `cascade_completeness` did not run or stamped a different profile.
    """
    missing = [
        name
        for name in _GATED_TABLES
        if name in tables and usability_flag_col not in tables[name].columns
    ]
    if missing:
        sample = tables[missing[0]]
        msg = (
            f"{', '.join(missing)} carry no '{usability_flag_col}' column, so there "
            f"is nothing to gate on. Declare it in cascade_completeness's "
            f"usability_profiles. {suggest_usability_columns(sample)}"
        )
        raise ValueError(msg)

    logger.info("CT-RAMP record universe gated on %s", usability_flag_col)

    # A null means the cascade never reached this row -- a broken frame, not
    # licence to guess a criterion of our own.
    keep = pl.col(usability_flag_col).fill_null(value=False)

    kept: dict[str, pl.DataFrame] = dict(tables)
    for name in _GATED_TABLES:
        if name not in kept:
            continue
        before = kept[name]
        kept[name] = before.filter(keep)
        logger.info("  %-14s %d -> %d", name, before.height, kept[name].height)

    _demote_undersized_groups(kept)
    _log_exclusion_summary(tables.get("tours"), tables.get("linked_trips"), usability_flag_col)
    _warn_zero_weight(kept, usability_flag_col)
    return kept


def _demote_undersized_groups(tables: dict[str, pl.DataFrame]) -> None:
    """Un-joint any grouping the gate left with fewer than two members, in place.

    The cascade already refuses a grouping whose members are not usable, so this
    is not a second opinion on that. It is the consequence: rejecting a member
    leaves the *others* still carrying the group's id, and a group of one is not
    a party -- CT-RAMP's schema says so with ``num_participants >= 2``.

    Counting surviving members is the criterion, rather than whether the group's
    own row survived. Joint tour output is built by grouping ``tours`` on
    ``joint_tour_id``; the ``joint_tours`` table only carries the weight. So a
    rule keyed on that table would both miss real undersized groups and demote
    perfectly good ones wherever it was not supplied.

    Demotion, not deletion: the survivor's travel happened, it is simply no
    longer joint.
    """
    for member_table, id_col, group_table in _GROUP_MEMBERSHIPS:
        members = tables.get(member_table)
        if members is None or id_col not in members.columns:
            continue

        undersized = (
            members.filter(pl.col(id_col).is_not_null())
            .group_by(id_col)
            .agg(pl.len().alias("_n"))
            .filter(pl.col("_n") < MIN_JOINT_PARTICIPANTS)[id_col]
        )
        if undersized.is_empty():
            continue

        logger.info(
            "Demoted %d %s to individual travel: fewer than %d members survived the gate",
            undersized.len(),
            group_table,
            MIN_JOINT_PARTICIPANTS,
        )
        tables[member_table] = members.with_columns(
            pl.when(pl.col(id_col).is_in(undersized.implode()))
            .then(None)
            .otherwise(pl.col(id_col))
            .alias(id_col)
        )
        groups = tables.get(group_table)
        if groups is not None and id_col in groups.columns:
            tables[group_table] = groups.filter(~pl.col(id_col).is_in(undersized.implode()))


def _warn_zero_weight(tables: dict[str, pl.DataFrame], usability_flag_col: str) -> None:
    """Report records this consumer can use that carry no weight.

    Zero weight means the balancer left the record out of its seed, since
    `min_weight` puts a floor of 1 under anything it weighted. So a usable record
    with no weight is one the *weighting's* profile excluded and this consumer's
    profile admits -- the one-size-fits-none case, and the reason per-profile
    weighting exists as an open question.

    Not an error, and not a drop. The formatter has no way to know which profile
    the weighting used, so it can report that the two disagree but not what about.
    """
    for name, weight_col in _WEIGHT_COLUMNS.items():
        df = tables.get(name)
        if df is None or weight_col not in df.columns:
            continue
        zero = df.filter(pl.col(weight_col).fill_null(0) <= 0)
        if zero.is_empty():
            continue
        logger.warning(
            "%d %s admitted by %s carry no %s. The weighting named a different "
            "profile, so it excluded records this consumer keeps; they expand to "
            "nothing rather than being dropped, which would delete another "
            "consumer's data.",
            zero.height,
            name,
            usability_flag_col,
            weight_col,
        )


def _completeness_split(df: pl.DataFrame) -> tuple[int, int]:
    """Return (already_incomplete, newly_unusable) counts from the ``complete`` flag.

    ``already_incomplete`` are records flagged incomplete (household / person /
    day cascade) — they were already excluded from the weighted model.
    ``newly_unusable`` are otherwise-complete records the profile still rejects.
    When the ``complete`` column is absent (e.g. tests), everything is treated as
    newly unusable.
    """
    total = df.height
    if "complete" not in df.columns:
        return 0, total
    incomplete = df.filter(~pl.col("complete").fill_null(value=False)).height
    return incomplete, total - incomplete


def _exclusion_grid(title: str, df: pl.DataFrame, denom: int) -> list[str]:
    """Build the lines of one grid (tours or member trips).

    ``new %`` is the newly-unusable share of all records — the genuinely new loss.
    """
    header = f"  {title:<14}{'excluded':>9}{'incomplete':>13}{'newly unusable':>16}{'new %':>8}"

    def row(name: str, incomplete: int, newly_unusable: int) -> str:
        total = incomplete + newly_unusable
        pct = newly_unusable * 100 / denom if denom > 0 else 0
        return f"  {name:<14}{total:>9,}{incomplete:>13,}{newly_unusable:>16,}{pct:>7.1f}%"

    lines = [header]
    for label in ("invalid", "partial/open"):
        incomplete, newly_unusable = _completeness_split(df.filter(pl.col("_reason") == label))
        if incomplete + newly_unusable == 0:
            continue
        lines.append(row(label, incomplete, newly_unusable))
    incomplete, newly_unusable = _completeness_split(df)
    lines.append(row("total", incomplete, newly_unusable))
    return lines


def _log_exclusion_summary(
    tours: pl.DataFrame | None,
    linked_trips: pl.DataFrame | None,
    usability_flag_col: str,
) -> None:
    """Log what the profile excluded, split by reason and by how new the loss is.

    Reporting only: the reason labels describe a verdict already reached, they do
    not contribute to it. Splitting already-incomplete from otherwise-complete
    records is what makes the number readable — a large exclusion of records the
    survey never collected is not the same finding as a small one of complete
    records the model cannot use.
    """
    if tours is None or usability_flag_col not in tours.columns:
        return

    excluded = tours.filter(~pl.col(usability_flag_col).fill_null(value=False))
    if excluded.is_empty():
        return

    if "tour_data_quality" in excluded.columns:
        is_valid = pl.col("tour_data_quality") == TourDataQuality.VALID.value
        reason = pl.when(~is_valid).then(pl.lit("invalid")).otherwise(pl.lit("partial/open"))
    else:
        reason = pl.lit("partial/open")
    excluded = excluded.with_columns(reason.alias("_reason"))

    lines = [
        f"Tours and member trips excluded by {usability_flag_col}.",
        '  "incomplete" = flagged incomplete via the household>person>day>trip cascade; '
        '"newly unusable" = otherwise-complete records.',
        "",
        *_exclusion_grid("TOURS", excluded, tours.height),
    ]
    if linked_trips is not None and "tour_id" in linked_trips.columns:
        excluded_trips = linked_trips.join(
            excluded.select("tour_id", "_reason"), on="tour_id", how="inner"
        )
        lines += ["", *_exclusion_grid("MEMBER TRIPS", excluded_trips, linked_trips.height)]

    logger.info("\n".join(lines))
