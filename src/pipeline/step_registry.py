"""Central registry for step workflow contracts.

Steps declare which fields they require as input and produce as output
via the @step() decorator. This registry stores those contracts and
provides lookup functions used by validation and documentation.

Models define what a field is (type, constraints, relationships).
Steps define when a field must exist (workflow contracts).
This registry bridges the two.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableContract:
    """Workflow contract for a single table within a step."""

    requires: frozenset[str] = field(default_factory=frozenset)
    produces: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class StepContract:
    """Workflow contract for a pipeline step."""

    tables: dict[str, TableContract] = field(default_factory=dict)


# Module-level registry, populated at import time by @step() decorators
STEP_REGISTRY: dict[str, StepContract] = {}


def register_step(
    step_name: str,
    requires: dict[str, set[str]] | None = None,
    produces: dict[str, set[str]] | None = None,
) -> None:
    """Register a step's workflow contract in the global registry.

    Called automatically by the @step() decorator at import time.

    Args:
        step_name: Name of the step (typically the function name).
        requires: Mapping of table name to set of required input field names.
        produces: Mapping of table name to set of produced output field names.
    """
    requires = requires or {}
    produces = produces or {}

    table_names = set(requires) | set(produces)
    STEP_REGISTRY[step_name] = StepContract(
        tables={
            t: TableContract(
                requires=frozenset(requires.get(t, set())),
                produces=frozenset(produces.get(t, set())),
            )
            for t in table_names
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


def get_produced_fields(table_name: str, step_name: str) -> set[str]:
    """Get fields produced by a step for a given table.

    Args:
        table_name: Name of the canonical table.
        step_name: Name of the pipeline step.

    Returns:
        Set of field names produced as output. Empty if step or table
        has no registered contract.
    """
    contract = STEP_REGISTRY.get(step_name)
    if not contract:
        return set()
    tc = contract.tables.get(table_name)
    return set(tc.produces) if tc else set()


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


def get_step_validation_summary() -> dict[str, dict[str, list[str]]]:
    """Get a summary of required fields across all steps and tables.

    Returns:
        Nested dict: step_name -> table_name -> sorted list of required fields.
    """
    summary: dict[str, dict[str, list[str]]] = {}
    for step_name, step_contract in STEP_REGISTRY.items():
        tables: dict[str, list[str]] = {}
        for table_name, table_contract in step_contract.tables.items():
            if table_contract.requires:
                tables[table_name] = sorted(table_contract.requires)
        if tables:
            summary[step_name] = tables
    return summary
