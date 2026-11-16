"""Central registry for step workflow contracts.

Steps declare which fields they require as input and produce as output
via the @step() decorator. This registry stores those contracts and
provides lookup functions used by validation and documentation.

Models define what a field is (type, constraints, relationships).
Steps define when a field must exist (workflow contracts).
This registry bridges the two.
"""

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# A contract may name a column the config names, e.g. ``usable_<profile>``.
_CONFIG_NAMED = re.compile(r"<[^<>]+>")


def is_config_named(field_name: str) -> bool:
    """Whether a contract entry is a pattern like ``usable_<profile>``, not a column.

    The pattern documents the column; the real name comes from the step's config,
    so the step itself checks for it, not the input validation.
    """
    return bool(_CONFIG_NAMED.search(field_name))


@dataclass(frozen=True)
class TableContract:
    """Workflow contract for a single table within a step.

    ``takes`` says whether the step receives the table; ``optional`` whether it
    may be absent. A table the step only produces is not taken.
    """

    requires: frozenset[str] = field(default_factory=frozenset)
    produces: frozenset[str] = field(default_factory=frozenset)
    takes: bool = False
    optional: bool = False


@dataclass(frozen=True)
class StepContract:
    """Workflow contract for a pipeline step."""

    tables: dict[str, TableContract] = field(default_factory=dict)

    @property
    def taken(self) -> dict[str, TableContract]:
        """The tables the step receives, by name."""
        return {name: tc for name, tc in self.tables.items() if tc.takes}


# Module-level registry, populated at import time by @step() decorators
STEP_REGISTRY: dict[str, StepContract] = {}


def register_step(
    step_name: str,
    requires: dict[str, set[str]] | None = None,
    produces: dict[str, set[str]] | None = None,
    optional: dict[str, set[str]] | None = None,
) -> None:
    """Register a step's workflow contract in the global registry.

    Called automatically by the @step() decorator at import time.

    Args:
        step_name: Name of the step (typically the function name).
        requires: Tables the step takes, each with the columns it reads.
        produces: Mapping of table name to set of produced output field names.
        optional: Tables the step takes that may be absent, each with the
            columns it reads when present.

    Raises:
        TypeError: If a table is both required and optional.
    """
    requires = requires or {}
    produces = produces or {}
    optional = optional or {}
    both = sorted(set(requires) & set(optional))
    if both:
        msg = f"{step_name}: {both} are in both requires and optional."
        raise TypeError(msg)

    taken = {**requires, **optional}
    STEP_REGISTRY[step_name] = StepContract(
        tables={
            t: TableContract(
                requires=frozenset(taken.get(t, set())),
                produces=frozenset(produces.get(t, set())),
                takes=t in taken,
                optional=t in optional,
            )
            for t in dict.fromkeys([*taken, *produces])  # declared order, not a set's
        }
    )


def get_required_fields(table_name: str, step_name: str) -> set[str]:
    """Get fields required by a step for a given table.

    Args:
        table_name: Name of the canonical table.
        step_name: Name of the pipeline step.

    Returns:
        Set of field names required as input. Empty if step or table
        has no registered contract.
    """
    contract = STEP_REGISTRY.get(step_name)
    if not contract:
        return set()
    tc = contract.tables.get(table_name)
    return set(tc.requires) if tc else set()


def get_all_required_fields(table_name: str) -> set[str]:
    """Get the union of all required fields for a table across every step.

    Useful for checking that raw input data contains every field that
    *any* downstream step will need.

    Args:
        table_name: Name of the canonical table.

    Returns:
        Set of field names required by at least one registered step.
    """
    fields: set[str] = set()
    for contract in STEP_REGISTRY.values():
        tc = contract.tables.get(table_name)
        if tc:
            fields.update(tc.requires)
    return fields
