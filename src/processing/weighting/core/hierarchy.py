"""The canonical weight hierarchy, declared once.

Every table that carries a weight is declared here exactly once -- its weight
column, where the weight comes from, and the group within which that weight is
conserved. Everything else is a projection of that list, so the shape of the
hierarchy cannot drift between the propagation ([`processing.weighting.core.propagation`]
[processing.weighting.core.propagation]), the supplied-weight path and the checksum.

Weights disaggregate *down* from households and aggregate back *up* into
groupings; both directions are edges of one tree.

|Table      |Weight                  |Derivation
|-----------|------------------------|-----------------------------------------
|households |``hh_weight``           |Anchor -- from the balancer, or supplied
|persons    |``person_weight``       |Down from ``hh_weight`` via ``hh_id``
|days       |``day_weight``          |Split: ``person_weight / n_usable_days``
|unlinked   |``unlinked_trip_weight``|Down from ``day_weight`` via ``day_id``
|linked     |``linked_trip_weight``  |Up: mean of member ``unlinked_trip_weight``
|tours      |``tour_weight``         |Up: mean of member ``linked_trip_weight``
|joint trips|``joint_trip_weight``   |Up: **sum** of member ``linked_trip_weight``
|joint tours|``joint_tour_weight``   |Up: **sum** of member ``tour_weight``

# Two units

Most weights count *one row of their own table*.  The joint levels instead sum
their members, so they count **person-trips** -- the travel of the whole party,
not the joint occasion -- and are therefore an *overlay* on the member table,
never a partition of it: ``sum(linked) + sum(joint)`` double counts.

To count occasions instead, divide by the number of members that carried weight
-- counted from the member table, not read off the grouping::

    events = joint_trip_weight / n_member_trips_with_weight

A party-size column is not that divisor: it counts travellers, including any the
weighting excluded.

# One column set per profile

A run may weight several usability profiles, each fitted to its own universe. The
columns above are then suffixed with the profile's name, verbatim and unabridged
(``hh_weight_ctramp_usable``), so a column names the universe it expands and no
rule is needed to read one back. Only [`weight_col_for`][processing.weighting.core
.hierarchy.weight_col_for] and its siblings spell that suffix; ``Level.weight_col``
stays the base name, which is what keeps ``id_col`` derivable.
"""

from dataclasses import dataclass
from enum import Enum, auto

from data_canon.validation.column import GeneratedColumn

_SEED_COLUMNS: frozenset[str] = frozenset({"base_weight"})
"""Pre-balancing columns that are also per-profile.

``base_weight`` is derived from the seed's own composition, so a different seed
gives a different base weight -- it belongs to a fit, not to the survey.
"""


def _suffixed(name: str, profile: str | None) -> str:
    """*name* under *profile*: the profile name appended verbatim, or *name* itself."""
    return name if profile is None else f"{name}_{profile}"


class Flow(Enum):
    """Direction a weight moves along a hierarchy edge."""

    DOWN = auto()
    """Disaggregate: the parent's weight is shared out among its children."""

    UP = auto()
    """Aggregate: a grouping combines the weights of its usable members."""


class Agg(Enum):
    """How an ``UP`` edge combines its members' weights."""

    MEAN = auto()
    """The grouping counts *once*: one linked trip, one tour. Its weight is the
    mean of its usable members, so the weight stays in the unit of its own row."""

    SUM = auto()
    """The grouping carries the combined weight of its members -- person-trips,
    not occasions. This puts it in the member table's unit while overlapping it,
    so summing both double counts."""


@dataclass(frozen=True, kw_only=True)
class Level:
    """One table in the weight hierarchy.

    Only what cannot be derived is declared: ``id_col`` and ``key`` follow
    mechanically from ``weight_col`` and the edge, so they are properties --
    there is no way to spell them inconsistently.

    Attributes:
        table: Canonical table name.
        weight_col: Weight column this level owns. Names the level -- ``id_col``
            is read off it, and it is the key naming this level in the
            supplied-weights config.
        means: What one unit of this weight counts, for the delivered codebook.
            Carried here rather than on a model field because the column's name
            comes from config once a run weights profiles separately, and a
            reader of the joint levels in particular needs telling: they are an
            overlay on their member table, not a partition of it.
        parent: Table the weight is derived from; None marks the anchor.
        flow: Which way the weight moves along the edge.
        scope: Group within which this weight is conserved.
        split: If True, the parent's weight is divided equally among its usable
            children (``parent_weight / n_usable``) instead of copied to each.
            Days use this -- see [`processing.weighting.core.propagation`]
            [processing.weighting.core.propagation].
        agg: How an ``UP`` edge combines its members. ``DOWN`` edges must leave
            it at the default.
    """

    table: str
    weight_col: str
    means: str
    parent: str | None = None
    flow: Flow | None = None
    scope: str | None = None
    split: bool = False
    agg: Agg = Agg.MEAN

    @property
    def id_col(self) -> str:
        """Primary key of the table, e.g. ``tour_weight`` -> ``tour_id``."""
        return f"{self.weight_col.removesuffix('_weight')}_id"

    def weight_col_for(self, profile: str | None) -> str:
        """This level's weight column under *profile*.

        The suffix is applied here and nowhere else, so ``weight_col`` -- and
        therefore ``id_col`` and ``key``, which are read off it -- keeps its base
        name however many profiles a run weights.
        """
        return _suffixed(self.weight_col, profile)

    @property
    def key(self) -> str | None:
        """The id column carried on this side of the edge.

        ``DOWN`` edges carry the parent's id, identifying the record the weight
        comes from; ``UP`` edges carry their own, identifying the grouping a
        member belongs to.
        """
        if self.flow is None or self.parent is None:
            return None
        return LEVELS[self.parent].id_col if self.flow is Flow.DOWN else self.id_col

    def __post_init__(self) -> None:
        """Reject a half-declared edge at import time rather than mid-pipeline."""
        edge = (self.parent, self.flow, self.scope)
        if self.parent is None:
            if any(part is not None for part in edge[1:]):
                msg = f"{self.table}: anchor level must not declare flow/scope"
                raise ValueError(msg)
        elif any(part is None for part in edge):
            msg = f"{self.table}: derived level needs parent, flow and scope"
            raise ValueError(msg)

        if self.flow is not Flow.UP and self.agg is not Agg.MEAN:
            msg = f"{self.table}: agg only applies to an UP edge"
            raise ValueError(msg)


HIERARCHY: tuple[Level, ...] = (
    Level(
        table="households",
        weight_col="hh_weight",
        means="Households represented by this record.",
    ),
    Level(
        table="persons",
        weight_col="person_weight",
        means="Persons represented by this record.",
        parent="households",
        flow=Flow.DOWN,
        scope="hh_id",
    ),
    Level(
        table="days",
        weight_col="day_weight",
        means=(
            "Person-days represented by this record. A person's usable days sum to "
            "their person weight, the average-day convention, so a day is a share of "
            "that person rather than a count of its own."
        ),
        parent="persons",
        flow=Flow.DOWN,
        # Conserved within the person: a person's usable days sum to their
        # person weight (the vendor's average-day convention, split=True).
        scope="person_id",
        split=True,
    ),
    Level(
        table="unlinked_trips",
        weight_col="unlinked_trip_weight",
        means="Unlinked trips (single vehicle or mode segments) represented.",
        parent="days",
        flow=Flow.DOWN,
        scope="day_id",
    ),
    Level(
        table="linked_trips",
        weight_col="linked_trip_weight",
        means="Linked trips (whole origin-to-destination journeys) represented.",
        parent="unlinked_trips",
        flow=Flow.UP,
        scope="day_id",
    ),
    Level(
        table="joint_trips",
        weight_col="joint_trip_weight",
        means=(
            "Person-trips represented: the SUM of the member linked trip weights, so "
            "the record stands for its whole party. Same unit as linked_trips, which "
            "this table OVERLAYS rather than partitions, so summing both double counts."
        ),
        parent="linked_trips",
        flow=Flow.UP,
        scope="day_id",
        agg=Agg.SUM,
    ),
    Level(
        table="tours",
        weight_col="tour_weight",
        means="Tours represented by this record.",
        parent="linked_trips",
        flow=Flow.UP,
        scope="day_id",
    ),
    Level(
        table="joint_tours",
        weight_col="joint_tour_weight",
        means=(
            "Person-tours represented: the SUM of the member tour weights. Same unit "
            "as tours, which this table OVERLAYS rather than partitions, so summing "
            "both double counts."
        ),
        parent="tours",
        flow=Flow.UP,
        scope="day_id",
        agg=Agg.SUM,
    ),
)


def _validate_hierarchy() -> None:
    """Fail at import if the declared hierarchy is not a well-formed tree."""
    by_table = {level.table: level for level in HIERARCHY}
    if len(by_table) != len(HIERARCHY):
        msg = "HIERARCHY declares the same table twice"
        raise ValueError(msg)

    anchors = [level.table for level in HIERARCHY if level.parent is None]
    if len(anchors) != 1:
        msg = f"HIERARCHY needs exactly one anchor, found {anchors}"
        raise ValueError(msg)

    seen: set[str] = set()
    for level in HIERARCHY:
        if level.parent is not None:
            if level.parent not in by_table:
                msg = f"{level.table}: unknown parent {level.parent!r}"
                raise ValueError(msg)
            if level.parent not in seen:
                msg = f"{level.table}: parent {level.parent!r} is declared after it"
                raise ValueError(msg)
        seen.add(level.table)


_validate_hierarchy()

# ---------------------------------------------------------------------------
# Projections of HIERARCHY. These are views, never independent facts.
# ---------------------------------------------------------------------------

LEVELS: dict[str, Level] = {level.table: level for level in HIERARCHY}
TABLE_NAMES: list[str] = [level.table for level in HIERARCHY]

_WEIGHT_BASES: frozenset[str] = frozenset(level.weight_col for level in HIERARCHY)


def weight_col_for(base: str, profile: str | None) -> str:
    """The hierarchy weight column *base* as written under *profile*.

    Args:
        base: A declared weight column, e.g. ``"hh_weight"``.
        profile: Usability profile the fit was gated on, or None for a single
            un-suffixed set.

    Raises:
        ValueError: If *base* is not a weight column the hierarchy declares.
            Checked so a caller cannot coin a column the hierarchy has no level
            for -- the suffix rule is only trustworthy if the stem is.
    """
    if base not in _WEIGHT_BASES:
        msg = f"{base!r} is not a hierarchy weight column; expected one of {sorted(_WEIGHT_BASES)}"
        raise ValueError(msg)
    return _suffixed(base, profile)


def seed_col_for(base: str, profile: str | None) -> str:
    """The pre-balancing column *base* as written under *profile*.

    Separate from [`weight_col_for`][processing.weighting.core.hierarchy.weight_col_for]
    because these columns belong to no level: they describe the seed a fit was
    solved from, not a weight the hierarchy propagates.

    Raises:
        ValueError: If *base* is not a declared seed column.
    """
    if base not in _SEED_COLUMNS:
        msg = f"{base!r} is not a seed column; expected one of {sorted(_SEED_COLUMNS)}"
        raise ValueError(msg)
    return _suffixed(base, profile)


def weight_columns_for(profile: str | None) -> dict[str, str]:
    """Map each table to its weight column under *profile*."""
    return {level.table: level.weight_col_for(profile) for level in HIERARCHY}


WEIGHT_COLUMNS: dict[str, str] = weight_columns_for(None)
WEIGHT_CONFIG_MAPPING: dict[str, tuple[str, str, str]] = {
    level.weight_col: (level.table, level.id_col, level.weight_col) for level in HIERARCHY
}
"""Supplied-weight config key -> (table, id column, weight column).

The config key *is* the weight column: one name for one thing, so a config
cannot name a level the hierarchy does not have.
"""


SEED_TABLE = "household_weights"
"""Table the weighting's working columns move to before the data is handed back."""


def describe_weight_columns(
    profiles: tuple[str | None, ...],
) -> dict[str, dict[str, GeneratedColumn]]:
    """Describe and constrain every weight column a set of fits writes.

    These columns are not model fields. A run that weights profiles separately
    names them from config, and even the un-suffixed set is a profile's weights
    with the label left off -- there is no un-profiled weight. So the promise a
    field would have carried is made here instead: what the number counts, and
    that it cannot be negative.

    Args:
        profiles: The profiles weighted. ``None`` means the un-suffixed set.

    Returns:
        One entry per canonical table that gained columns.
    """
    described: dict[str, dict[str, GeneratedColumn]] = {}
    for profile in profiles:
        universe = (
            f"Fitted over the {profile} universe: zero where {profile} excludes the record, "
            "null where no weight could be estimated for it."
            if profile is not None
            else "Zero where the weighting excluded the record, null where none was estimated."
        )
        for level in HIERARCHY:
            described.setdefault(level.table, {})[level.weight_col_for(profile)] = GeneratedColumn(
                f"{level.means} {universe}", ge=0
            )
        described.setdefault(SEED_TABLE, {}).update(
            {
                seed_col_for("base_weight", profile): GeneratedColumn(
                    "Pre-balancing seed weight, before the balancer fit it to the controls: "
                    "the zone or sample segment's population divided by its responses, and "
                    f"the denominator of the expansion factor. {universe}",
                    ge=0,
                ),
                weight_col_for("hh_weight", profile): GeneratedColumn(
                    "The household weight, repeated from households so the expansion factor "
                    f"can be read off this table alone. {universe}",
                    ge=0,
                ),
            }
        )
    return described


def levels_with_flow(flow: Flow) -> list[Level]:
    """Return the levels whose weight moves in *flow*, in hierarchy order."""
    return [level for level in HIERARCHY if level.flow is flow]
