"""Helper functions for resolving enum labels to values.

This module provides utilities for working with codebook enumerations,
particularly for resolving enum member names to their numeric values.
"""

import inspect
import logging
from typing import get_args

from data_canon.core.dataclass import CANONICAL_MODELS
from data_canon.core.labeled_enum import LabeledEnum

logger = logging.getLogger(__name__)


def _extract_enum_from_annotation(annotation: object) -> type[LabeledEnum] | None:
    """Extract the enum class from a field's type annotation.

    Handles both direct enum types and Union types (e.g., IncomeBroad | None).

    Args:
        annotation: The field's type annotation

    Returns:
        The enum class if found and is a LabeledEnum, None otherwise

    Example:
        >>> _extract_enum_from_annotation(IncomeBroad | None)
        <enum 'IncomeBroad'>
        >>> _extract_enum_from_annotation(str)
        None
    """
    # A Union (IncomeBroad | None) offers its members; a plain type offers itself
    candidates = get_args(annotation) or (annotation,)
    return next(
        (a for a in candidates if inspect.isclass(a) and issubclass(a, LabeledEnum)),
        None,
    )


def get_enum_class_for_field(table_name: str, field_name: str) -> type[LabeledEnum] | None:
    """Find the enum class for a given field in a table.

    Uses the Pydantic model's type annotations to determine the enum class
    for a field. For example, if HouseholdModel has `income_bin: IncomeBroad`,
    this function will return the IncomeBroad enum class.

    Args:
        table_name: Name of the table (e.g., 'households', 'persons', 'unlinked_trips')
        field_name: Name of the field/column to find enum for

    Returns:
        The enum class if found, None otherwise

    Example:
        >>> enum_class = get_enum_class_for_field('households', 'income_bin')
        >>> enum_class
        <enum 'IncomeBroad'>
    """
    # Get the model class for this table
    model_class = CANONICAL_MODELS.get(table_name)
    if not model_class:
        msg = "Unknown table name '%s' - cannot find model class"
        raise ValueError(msg % table_name)

    # Get the field info from the model
    if field_name not in model_class.model_fields:
        msg = "Field '%s' not found in model for table '%s'"
        raise ValueError(msg % (field_name, table_name))

    field_info = model_class.model_fields[field_name]
    enum_class = _extract_enum_from_annotation(field_info.annotation)

    if not enum_class:
        msg = "No enum class found for field '%s' in table '%s'"
        raise ValueError(msg % (field_name, table_name))

    return enum_class


def resolve_enum_labels(
    table_name: str, field_name: str, enum_labels: list[str | int]
) -> list[int | str]:
    """Resolve enum labels to their values for a given field.

    Args:
        table_name: Name of the table containing the field
        field_name: Name of the field/column
        enum_labels: List of enum member names (e.g., ['MISSING', 'PNTA'])

    Returns:
        List of resolved values (integers or strings)

    Example:
        >>> resolve_enum_labels('households', 'income_bin', ['MISSING', 'PNTA'])
        [995, 999]
    """
    # Integer codes pass through unchecked, with a warning
    if all(
        isinstance(label, int) or (isinstance(label, str) and label.isdigit())
        for label in enum_labels
    ):
        logger.warning(
            "%s.%s: given integer codes %s rather than labels; using them unchecked",
            table_name,
            field_name,
            enum_labels,
        )
        return [int(label) for label in enum_labels]

    enum_class = get_enum_class_for_field(table_name, field_name)

    resolved_values = []
    for label in enum_labels:
        try:
            resolved_values.append(enum_class[str(label)].value)
        except KeyError as err:
            msg = (
                f"Label '{label}' not found in enum '{enum_class.__name__}' "
                f"for field '{field_name}'"
            )
            raise ValueError(msg) from err

    return resolved_values
