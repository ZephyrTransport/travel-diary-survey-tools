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
|joint trips|``joint_trip_weight``   |Up: mean of member ``linked_trip_weight``
|joint tours|``joint_tour_weight``   |Up: mean of member ``tour_weight``

"""

from dataclasses import dataclass
from enum import Enum, auto


class Flow(Enum):
    """Direction a weight moves along a hierarchy edge."""

    DOWN = auto()
    """Disaggregate: the parent's weight is shared out among its children."""

    UP = auto()
    """Aggregate: a grouping combines the weights of its usable members."""


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
        parent: Table the weight is derived from; None marks the anchor.
        flow: Which way the weight moves along the edge.
        scope: Group within which this weight is conserved.
        split: If True, the parent's weight is divided equally among its usable
            children (``parent_weight / n_usable``) instead of copied to each.
            Days use this -- see [`processing.weighting.core.propagation`]
            [processing.weighting.core.propagation].
    """

    table: str
    weight_col: str
    parent: str | None = None
    flow: Flow | None = None
    scope: str | None = None
    split: bool = False

    @property
    def id_col(self) -> str:
        """Primary key of the table, e.g. ``tour_weight`` -> ``tour_id``."""
        return f"{self.weight_col.removesuffix('_weight')}_id"

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


HIERARCHY: tuple[Level, ...] = (
    Level(table="households", weight_col="hh_weight"),
    Level(
        table="persons",
        weight_col="person_weight",
        parent="households",
        flow=Flow.DOWN,
        scope="hh_id",
    ),
    Level(
        table="days",
        weight_col="day_weight",
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
        parent="days",
        flow=Flow.DOWN,
        scope="day_id",
    ),
    Level(
        table="linked_trips",
        weight_col="linked_trip_weight",
        parent="unlinked_trips",
        flow=Flow.UP,
        scope="day_id",
    ),
    Level(
        table="joint_trips",
        weight_col="joint_trip_weight",
        parent="linked_trips",
        flow=Flow.UP,
        scope="day_id",
    ),
    Level(
        table="tours",
        weight_col="tour_weight",
        parent="linked_trips",
        flow=Flow.UP,
        scope="day_id",
    ),
    Level(
        table="joint_tours",
        weight_col="joint_tour_weight",
        parent="tours",
        flow=Flow.UP,
        scope="day_id",
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
WEIGHT_COLUMNS: dict[str, str] = {level.table: level.weight_col for level in HIERARCHY}
WEIGHT_CONFIG_MAPPING: dict[str, tuple[str, str, str]] = {
    level.weight_col: (level.table, level.id_col, level.weight_col) for level in HIERARCHY
}
"""Supplied-weight config key -> (table, id column, weight column).

The config key *is* the weight column: one name for one thing, so a config
cannot name a level the hierarchy does not have.
"""


def levels_with_flow(flow: Flow) -> list[Level]:
    """Return the levels whose weight moves in *flow*, in hierarchy order."""
    return [level for level in HIERARCHY if level.flow is flow]
