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

Weights are read for the profile this consumer names, so its universe and its
weights are the same universe. Where a record still carries no weight it is
reported, not removed: a zero means its scope kept no usable record to share the
parent's claim among, a null means no weight was ever estimated for it, and both
are recoverable in a way that deleting the record is not.
"""

import logging
from dataclasses import dataclass, field

import polars as pl

from data_canon.codebook.tours import TourDataQuality
from data_canon.core.dataclass import CANONICAL_MODELS
from data_canon.validation.relational import foreign_key_edges
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

# The declared references between canonical tables, derived rather than listed.
# There are three dozen, denormalised across the trip tables, and a
# hand-maintained subset omits whichever one matters next -- which is how a
# person outlived their household, and a trip outlived its joint tour.
_EDGES = foreign_key_edges(CANONICAL_MODELS)

# The joint groupings, as (id column, table whose surviving rows are its members,
# the group's own table).
_GROUPINGS = (
    ("joint_tour_id", "tours", "joint_tours"),
    ("joint_trip_id", "linked_trips", "joint_trips"),
)

# Reconciliation is iterated because each rule can trigger another: dropping a
# household drops its persons, which can leave a joint group short, which voids a
# reference. The bound guards against a cycle rather than counting expected work;
# drops only shrink and voids only remove references, so it settles quickly.
_MAX_RECONCILE_PASSES = 10

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


@dataclass
class _Ledger:
    """What each rule removed, per table, accumulated across passes.

    Kept because the per-rule log read alarmingly out of context: a stream of
    "dropped 953 persons" lines invites the reader to think the formatter is
    making judgements of its own, when every one of those persons left because
    their household did not pass the profile. Naming the reason next to the count
    is the difference between a ledger and a list of casualties.

    Voided references are tracked separately from removals because they are not
    losses at all -- the record stays in the output, only its grouping is gone.
    """

    excluded: dict[str, int] = field(default_factory=dict)
    orphaned: dict[str, int] = field(default_factory=dict)
    undersized: dict[str, int] = field(default_factory=dict)
    unjointed: dict[str, int] = field(default_factory=dict)

    def add(self, bucket: dict[str, int], key: str, n: int) -> None:
        bucket[key] = bucket.get(key, 0) + n


def _log_ledger(
    ledger: _Ledger,
    kept: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> None:
    """Log one row per table, with a column per reason and the survivors.

    The profile's own column sits beside the consequential ones on purpose. A
    reader wondering whether too much was removed should be looking at the
    profile, since the other columns record bookkeeping that follows from it and
    decide nothing.
    """
    rows = [name for name in _GATED_TABLES if name in kept]
    if not rows:
        return

    lines = [
        f"Records kept for the model, gated on {usability_flag_col}:",
        "",
        f"  {'table':<16}{'not ' + usability_flag_col:>22}"
        f"{'parent dropped':>18}{'below quorum':>15}{'kept':>12}",
    ]
    for name in rows:
        excluded = ledger.excluded.get(name, 0)
        orphaned = ledger.orphaned.get(name, 0)
        undersized = ledger.undersized.get(name, 0)
        lines.append(
            f"  {name:<16}{-excluded:>22,}{-orphaned:>18,}"
            f"{-undersized if undersized else 0:>15,}{kept[name].height:>12,}"
        )

    if ledger.unjointed:
        lines += ["", "  no longer joint (records kept, only the grouping removed):"]
        for reference, n in sorted(ledger.unjointed.items()):
            lines.append(f"    {reference:<34}{n:>10,}")

    logger.info("\n".join(lines))


def select_profile_weights(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> dict[str, pl.DataFrame]:
    """Resolve this consumer's weight columns to their base names.

    When the weighting fits several profiles it writes one column set per
    profile, suffixed with the profile's name. A consumer already names the
    profile it reads, and the suffix *is* that name, so one setting answers both
    questions and there is no second one to disagree with it.

    Renaming here rather than at each use is what keeps every downstream read --
    ``sampleRate``, ``hhexpfac``, ``pdexpfac`` and the rest -- correct without
    knowing profiles exist at all.

    Args:
        tables: Canonical tables, keyed by name.
        usability_flag_col: The profile this consumer reads.

    Returns:
        The same mapping with this profile's weights under the base names. A
        table with no suffixed column at all is returned untouched, which is the
        single-weight-set case.

    Raises:
        ValueError: If the weighting wrote suffixed columns but none for this
            profile. The bare column would then be read instead -- whatever it
            happens to hold -- and a formatter would publish expansion factors
            for a universe it is not describing.
    """
    resolved: dict[str, pl.DataFrame] = dict(tables)
    read_from: dict[str, str] = {}

    for name, df in tables.items():
        base = _WEIGHT_COLUMNS.get(name)
        if base is None:
            continue
        mine = f"{base}_{usability_flag_col}"

        # Another profile's copy is not ours to deliver, and leaving it beside
        # the base name invites reading the wrong one.
        others = [col for col in df.columns if col.startswith(f"{base}_") and col != mine]

        if mine not in df.columns:
            if others:
                weighted = sorted(col.removeprefix(f"{base}_") for col in others)
                msg = (
                    f"{name} carries weights for {weighted} but not for "
                    f"{usability_flag_col!r}, the profile this consumer reads. Add it to "
                    f"the weighting step's weight_profiles; falling back to {base!r} would "
                    f"publish another universe's expansion factors, or none."
                )
                raise ValueError(msg)
            continue

        if base in df.columns:
            logger.warning(
                "%s carries both %s and %s; reading %s, since that is the profile "
                "this consumer gates on.",
                name,
                base,
                mine,
                mine,
            )
        resolved[name] = df.drop(base, *others, strict=False).rename({mine: base})
        read_from[name] = mine

    if read_from:
        logger.info(
            "Weights read for %s: %s",
            usability_flag_col,
            ", ".join(f"{name} <- {col}" for name, col in sorted(read_from.items())),
        )
    return resolved


def keep_usable(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> dict[str, pl.DataFrame]:
    """Keep the records *usability_flag_col* admits, across every table.

    Args:
        tables: Canonical tables, keyed by name.
        usability_flag_col: The usability profile this consumer reads. It names
            both the verdict to gate on and the weight columns to read, since a
            profile's weight columns carry its name.

    Returns:
        The same mapping, filtered, with this profile's weights under the base
        column names. Tables absent from the input stay absent.

    Raises:
        ValueError: If a gated table carries no verdict, which means
            `cascade_completeness` did not run or stamped a different profile.
    """
    tables = select_profile_weights(tables, usability_flag_col)

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

    # A null means the cascade never reached this row -- a broken frame, not
    # licence to guess a criterion of our own.
    keep = pl.col(usability_flag_col).fill_null(value=False)

    ledger = _Ledger()
    kept: dict[str, pl.DataFrame] = dict(tables)
    for name in _GATED_TABLES:
        if name not in kept:
            continue
        before = kept[name]
        kept[name] = before.filter(keep)
        ledger.add(ledger.excluded, name, before.height - kept[name].height)

    _reconcile(kept, ledger)
    _log_ledger(ledger, kept, usability_flag_col)
    _log_exclusion_summary(tables.get("tours"), tables.get("linked_trips"), usability_flag_col)
    _warn_zero_weight(kept, usability_flag_col)
    return kept


def _reconcile(tables: dict[str, pl.DataFrame], ledger: _Ledger) -> None:
    """Restore the relationships filtering broke, in place.

    Three rules, each stated once and applied until nothing changes. Iterating
    matters because they feed each other: dropping a household drops its
    persons, which can leave a joint group below quorum, which voids a reference
    on a trip.

    Raises:
        RuntimeError: If the rules have not settled within
            :data:`_MAX_RECONCILE_PASSES`, which would mean two of them disagree
            rather than that the data is large.
    """
    for _ in range(_MAX_RECONCILE_PASSES):
        changed = _cascade_references(tables, ledger)
        changed = _demote_undersized_groups(tables, ledger) or changed
        changed = _unjoint_tours_with_individual_trips(tables, ledger) or changed
        if not changed:
            return

    msg = (
        f"Record reconciliation did not settle in {_MAX_RECONCILE_PASSES} passes. "
        "Two rules are undoing each other rather than converging."
    )
    raise RuntimeError(msg)


def _cascade_references(tables: dict[str, pl.DataFrame], ledger: _Ledger) -> bool:
    """Follow every declared reference whose target is gone, in place.

    The schema already says what to do. A *required* reference means the child
    cannot exist without its parent, so the child goes: a person with no
    household has no home to report and reaches CT-RAMP as a null home TAZ. An
    *optional* reference means the child stands on its own and only the reference
    is void, so it is nulled: a trip that stops being joint is still a trip.

    Returns:
        Whether anything changed, so the caller can iterate to a fixed point.
    """
    changed = False
    for child, column, parent, parent_column, optional in _EDGES:
        c, p = tables.get(child), tables.get(parent)
        if c is None or p is None or column not in c.columns or parent_column not in p.columns:
            continue

        dangling = pl.col(column).is_not_null() & ~pl.col(column).is_in(p[parent_column].implode())
        n_dangling = c.filter(dangling).height
        if not n_dangling:
            continue

        changed = True
        if optional:
            tables[child] = c.with_columns(
                pl.when(dangling).then(None).otherwise(pl.col(column)).alias(column)
            )
            ledger.add(ledger.unjointed, f"{child}.{column}", n_dangling)
            logger.debug("  voided %d %s.%s: no matching %s", n_dangling, child, column, parent)
        else:
            tables[child] = c.filter(~dangling)
            ledger.add(ledger.orphaned, child, n_dangling)
            logger.debug(
                "  dropped %d %s: %s.%s is required and had no matching %s",
                n_dangling,
                child,
                child,
                column,
                parent,
            )
    return changed


def _demote_undersized_groups(tables: dict[str, pl.DataFrame], ledger: _Ledger) -> bool:
    """Un-joint any grouping left with fewer than two members, in place.

    The cascade already refuses a grouping whose members are not usable, so this
    is not a second opinion on that. It is the consequence: rejecting a member
    leaves the others carrying the group's id, and a group of one is not a party
    -- CT-RAMP says so with ``num_participants >= 2``.

    Only the group's own row is removed here. Voiding the members' references to
    it is left to :func:`_cascade_references`, which does that for every table
    carrying a copy rather than the ones remembered at this call site.

    Returns:
        Whether any group was removed.
    """
    changed = False
    for id_col, member_table, group_table in _GROUPINGS:
        members, groups = tables.get(member_table), tables.get(group_table)
        if members is None or groups is None:
            continue
        if id_col not in members.columns or id_col not in groups.columns:
            continue

        surviving = (
            members.filter(pl.col(id_col).is_not_null())
            .group_by(id_col)
            .agg(pl.len().alias("_n"))
            .filter(pl.col("_n") >= MIN_JOINT_PARTICIPANTS)[id_col]
        )
        undersized = groups.filter(~pl.col(id_col).is_in(surviving.implode()))
        if undersized.is_empty():
            continue

        changed = True
        ledger.add(ledger.undersized, group_table, undersized.height)
        logger.debug(
            "  demoted %d %s: fewer than %d members survived",
            undersized.height,
            group_table,
            MIN_JOINT_PARTICIPANTS,
        )
        tables[group_table] = groups.filter(pl.col(id_col).is_in(surviving.implode()))
    return changed


def _unjoint_tours_with_individual_trips(tables: dict[str, pl.DataFrame], ledger: _Ledger) -> bool:
    """Clear ``joint_tour_id`` where any of a tour's trips is not joint, in place.

    CT-RAMP's fully-joint rule, and the one it checks: a joint
    tour whose every member shares every trip is the only kind it can represent,
    so a single non-joint leg disqualifies the whole tour. ``format_joint_tours``
    refuses such a tour outright, because that leg would reach neither output
    file -- the individual trip file excludes joint tours, and the joint trip
    file needs a ``joint_trip_id``.

    A leg loses its group when the group falls below quorum, which can happen
    while the tour itself still has two members and survives. The tour then
    becomes individual travel: the trips are all still real, they are simply no
    longer recorded as shared.

    Returns:
        Whether any tour was un-jointed.
    """
    tours, trips = tables.get("tours"), tables.get("linked_trips")
    if tours is None or trips is None:
        return False
    if "joint_tour_id" not in trips.columns or "joint_trip_id" not in trips.columns:
        return False

    # A frame recording no trip-level sharing at all is not claiming these tours
    # are partly shared -- it simply never had joint trips. The same judgement
    # _validate_joint_tours_are_wholly_joint makes before refusing one.
    if trips["joint_trip_id"].null_count() == trips.height:
        return False

    partly = (
        trips.filter(pl.col("joint_tour_id").is_not_null() & pl.col("joint_trip_id").is_null())[
            "joint_tour_id"
        ]
        .unique()
        .implode()
    )
    is_partly = pl.col("joint_tour_id").is_in(partly)
    if trips.filter(is_partly).is_empty():
        return False

    n_tours = 0
    for name in ("tours", "linked_trips", "unlinked_trips"):
        df = tables.get(name)
        if df is None or "joint_tour_id" not in df.columns:
            continue
        if name == "tours":
            n_tours = df.filter(is_partly)["joint_tour_id"].n_unique()
        ledger.add(ledger.unjointed, f"{name}.joint_tour_id", df.filter(is_partly).height)
        tables[name] = df.with_columns(
            pl.when(is_partly).then(None).otherwise(pl.col("joint_tour_id")).alias("joint_tour_id")
        )

    logger.debug(
        "  un-jointed %d tours: a leg of each is no longer shared, and CT-RAMP "
        "has no partly-joint tour",
        n_tours,
    )
    return True


def _warn_zero_weight(tables: dict[str, pl.DataFrame], usability_flag_col: str) -> None:
    """Report records this consumer can use that carry no weight.

    Zero and null are different findings and have different fixes, so they are
    reported apart:

    * **zero** -- a *stranded scope*. `min_weight` puts a floor of 1 under
      anything the balancer weighted, so a fitted record cannot be zero. It means
      the parent's claim had no usable child to share it among, and the weighting
      deliberately left that claim unrepresented rather than pool it across other
      parents. `_check_hierarchy` logs the matching shortfall.
    * **null** -- no weight was ever estimated. Either the control geography could
      not place the household, so it belonged to no balancing zone, or no
      weighting step ran at all.

    Neither is an error and neither is a drop: expanding to nothing is recoverable,
    deleting the record is not.
    """
    for name, weight_col in _WEIGHT_COLUMNS.items():
        df = tables.get(name)
        if df is None or weight_col not in df.columns:
            continue

        n_null = df.filter(pl.col(weight_col).is_null()).height
        n_zero = df.filter(pl.col(weight_col) <= 0).height

        if n_zero:
            logger.warning(
                "%d %s admitted by %s carry a zero %s: their scope kept no usable "
                "record to share the parent's weight among, so it stays unrepresented "
                "rather than being pooled onto another parent's children.",
                n_zero,
                name,
                usability_flag_col,
                weight_col,
            )
        if n_null:
            logger.warning(
                "%d %s admitted by %s have no %s at all: either the control geography "
                "could not place the household, so it was in no balancing zone, or no "
                "weighting step ran.",
                n_null,
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
