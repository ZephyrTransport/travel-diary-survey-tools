"""Which records a usability profile admits, read off every table at once.

Nothing here removes a record from the canonical data. ``keep_records``
returns *filtered copies* of the tables it is given -- the rows whose verdict
column is true -- and leaves the inputs as they were. Only a formatter ever
writes fewer records than the survey holds, and it writes exactly these.

Filtering each table on its own verdict is enough because the cascade already
made the verdicts agree: it carries the household's verdict down to every
record in it, voids the joint ids of any grouping it rejects, and dissolves
any grouping left with one record. What this module adds is the proof. After
filtering it follows every reference the schema declares and raises on the
first one that no longer resolves, so a cascade that stops agreeing with
itself fails here, loudly, rather than being quietly patched.

The formatters gate on this, and the weighting runs it as a check: weight on
a record the filter excludes is weight the output would lose.
"""

import logging

import polars as pl

from data_canon.core.dataclass import CANONICAL_MODELS
from data_canon.validation.relational import foreign_key_edges
from processing.usability import MIN_JOINT_PARTICIPANTS

logger = logging.getLogger(__name__)

# Every table the gate can apply to; a formatter passes the ones it holds. All are
# stamped by the cascade, so a missing verdict is a broken frame, not an opt-out.
GATED_TABLES = (
    "households",
    "persons",
    "person_days",
    "tours",
    "linked_trips",
    "unlinked_trips",
    "joint_trips",
    "joint_tours",
)

# The declared references between canonical tables, derived from the schema so
# that none is left out.
_EDGES = foreign_key_edges(CANONICAL_MODELS)

# The joint groupings, as (id column, table whose surviving rows are its members,
# the group's own table).
_GROUPINGS = (
    ("joint_tour_id", "tours", "joint_tours"),
    ("joint_trip_id", "linked_trips", "joint_trips"),
)

# Each gated table's weight column.
WEIGHT_COLUMNS = {
    "households": "hh_weight",
    "persons": "person_weight",
    "person_days": "person_day_weight",
    "tours": "tour_weight",
    "linked_trips": "linked_trip_weight",
    "unlinked_trips": "unlinked_trip_weight",
    "joint_trips": "joint_trip_weight",
    "joint_tours": "joint_tour_weight",
}

# Every column a profile writes its own copy of, per table: the weight, and
# the joint ids saying which joint tour or joint trip a record is on.
_JOINT_ID_COLUMNS = {
    "tours": ("joint_tour_id",),
    "linked_trips": ("joint_tour_id", "joint_trip_id"),
    "unlinked_trips": ("joint_tour_id",),
}
_PROFILE_COLUMNS: dict[str, tuple[str, ...]] = {
    name: (weight, *_JOINT_ID_COLUMNS.get(name, ())) for name, weight in WEIGHT_COLUMNS.items()
}

# Rows of a failing check to name before summarising the rest.
_MAX_REPORTED = 5


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


def keep_records(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> tuple[dict[str, pl.DataFrame], dict[str, int]]:
    """Keep the records *usability_flag_col* admits, and prove nothing dangles.

    Args:
        tables: Canonical tables, keyed by name, carrying the profile's joint
            ids under the bare names (the formatter resolves them first).
        usability_flag_col: The stamped verdict to gate on.

    Returns:
        The kept tables, and how many rows each lost. Tables absent from the
        input stay absent.

    Raises:
        ValueError: If a gated table carries no verdict, which means
            `cascade_usability` did not run or stamped a different profile;
            or if the kept records do not agree with one another, which means
            the cascade did.
    """
    missing = [
        name
        for name in GATED_TABLES
        if name in tables and usability_flag_col not in tables[name].columns
    ]
    if missing:
        sample = tables[missing[0]]
        msg = (
            f"{', '.join(missing)} carry no '{usability_flag_col}' column, so there "
            f"is nothing to gate on. Declare it in cascade_usability's "
            f"usability_profiles. {suggest_usability_columns(sample)}"
        )
        raise ValueError(msg)

    # A null means the cascade never reached this row -- a broken frame, not
    # licence to guess a criterion of our own.
    keep = pl.col(usability_flag_col).fill_null(value=False)

    excluded: dict[str, int] = {}
    kept: dict[str, pl.DataFrame] = dict(tables)
    for name in GATED_TABLES:
        if name not in kept:
            continue
        before = kept[name]
        kept[name] = before.filter(keep)
        excluded[name] = before.height - kept[name].height

    check_kept_records_agree(kept, usability_flag_col)
    return kept, excluded


def select_profile_columns(
    tables: dict[str, pl.DataFrame],
    usability_profile: str,
    *,
    only: tuple[str, ...] | None = None,
) -> dict[str, pl.DataFrame]:
    """Resolve this consumer's per-profile columns to their base names.

    The weighting writes one weight column per profile, and the cascade one
    copy of each joint id per profile, each suffixed with the profile's name. A
    consumer already names the profile it reads, and the suffix *is* that name,
    so one setting answers every question and there is no second one to
    disagree with it.

    Renaming here rather than at each use is what keeps every downstream read --
    ``sampleRate``, ``hhexpfac``, which tours are joint -- correct without
    knowing profiles exist at all.

    Args:
        tables: Canonical tables, keyed by name.
        usability_profile: The profile this consumer reads.
        only: Base columns to resolve, for a caller that wants the joint ids
            but not the weights; every column a profile writes, by default.

    Returns:
        The same mapping with this profile's columns under the base names. A
        table with no suffixed column at all is returned untouched, which is the
        single-weight-set case.

    Raises:
        ValueError: If a table carries suffixed columns but none for this
            profile. The bare column would then be read instead -- whatever it
            happens to hold -- and a formatter would publish another universe's
            expansion factors or joint tours.
    """
    resolved: dict[str, pl.DataFrame] = dict(tables)
    read_from: list[str] = []

    for name, table in tables.items():
        df = table
        for base in _PROFILE_COLUMNS.get(name, ()):
            if only is not None and base not in only:
                continue
            mine = f"{base}_{usability_profile}"
            # Another profile's copy is not ours to deliver, and leaving it
            # beside the base name invites reading the wrong one.
            others = [c for c in df.columns if c.startswith(f"{base}_") and c != mine]
            if mine not in df.columns:
                if others:
                    found = sorted(c.removeprefix(f"{base}_") for c in others)
                    what = "weights" if base.endswith("_weight") else base
                    msg = (
                        f"{name} carries {what} for {found} but not for "
                        f"{usability_profile!r}, the profile this consumer reads. Add it to "
                        f"the weighting step's weight_profiles and cascade_usability's "
                        f"usability_profiles; falling back to {base!r} would publish another "
                        "universe's expansion factors or joint tours, or none."
                    )
                    raise ValueError(msg)
                continue
            if base in df.columns and base.endswith("_weight"):
                logger.warning(
                    "%s carries both %s and %s; reading %s, since that is the profile "
                    "this consumer gates on.",
                    name,
                    base,
                    mine,
                    mine,
                )
            df = df.drop(base, *others, strict=False).rename({mine: base})
            read_from.append(f"{name}.{base} <- {mine}")
        resolved[name] = df

    if read_from:
        logger.info("Columns read for %s: %s", usability_profile, ", ".join(sorted(read_from)))
    return resolved


def check_kept_records_agree(tables: dict[str, pl.DataFrame], usability_flag_col: str) -> None:
    """Raise if the kept records do not form a consistent set.

    Two things, each once a repair this module made silently:

    1. Every reference the schema declares resolves. A required one that does
       not is a record whose household, person or day the profile rejected;
       an optional one is a joint id the profile should have voided.
    2. No joint id is carried by fewer than two records.

    Each is something the cascade guarantees under every profile, so a failure
    names a cascade bug. A rule that holds under some profiles only -- such as
    every leg of a joint tour being a joint trip, which ``partial`` does not
    promise -- belongs to the consumer that needs it, since this function
    receives a column name and cannot know the profile's settings.

    Args:
        tables: The kept tables.
        usability_flag_col: The verdict they were gated on, for the message.

    Raises:
        ValueError: On the first check that fails.
    """
    for child, column, parent, parent_column, optional in _EDGES:
        c, p = tables.get(child), tables.get(parent)
        if c is None or p is None or column not in c.columns or parent_column not in p.columns:
            continue
        dangling = c.filter(
            pl.col(column).is_not_null() & ~pl.col(column).is_in(p[parent_column].implode())
        )
        if dangling.is_empty():
            continue
        why = (
            "the cascade voids a grouping's ids when it rejects the grouping"
            if optional
            else "the cascade carries a verdict down to everything inside it"
        )
        msg = (
            f"{dangling.height} {child} record(s) admitted by {usability_flag_col} reference "
            f"a {parent} record it does not admit ({child}.{column} -> {parent}.{parent_column}), "
            f"though {why}. Sample {column}: {dangling[column].head(_MAX_REPORTED).to_list()}."
        )
        logger.error(msg)
        raise ValueError(msg)

    for id_col, member_table, group_table in _GROUPINGS:
        members = tables.get(member_table)
        if members is None or id_col not in members.columns:
            continue
        alone = (
            members.filter(pl.col(id_col).is_not_null())
            .group_by(id_col)
            .len(name="_n")
            .filter(pl.col("_n") < MIN_JOINT_PARTICIPANTS)
        )
        if alone.is_empty():
            continue
        msg = (
            f"{alone.height} {group_table} id(s) are carried by a single {member_table} record "
            f"under {usability_flag_col}, though the cascade dissolves a grouping left with "
            f"one. Sample {id_col}: {alone[id_col].head(_MAX_REPORTED).to_list()}."
        )
        logger.error(msg)
        raise ValueError(msg)
