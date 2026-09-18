"""Module for creating Pydantic fields with relational schema metadata."""

from typing import Any

from pydantic import Field


def schema_field(
    *,
    unique: bool = False,
    fk_to: str | None = None,
    required_child: bool = False,
    required_child_when: str | None = None,
    **field_kwargs: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Create a Pydantic Field with relational schema metadata.

    This is a convenience wrapper that attaches relational metadata
    (uniqueness, foreign keys, required children) to Pydantic fields
    via json_schema_extra. Workflow metadata (which steps require
    which fields) is managed separately by the step registry.

    Args:
        unique: Whether this field should be unique across records.
                Used to validate uniqueness constraints at the table level.
        fk_to: Foreign key reference in format "parent_table.parent_column".
               The referenced parent column must be marked as unique.
        required_child: If True, marks this FK as requiring the parent
                       record to have at least one child record (bidirectional
                       FK constraint). Only valid when fk_to is specified.
        required_child_when: Name of a boolean column on the *parent* table.
                       Only parent rows where it is true need a child; rows
                       where it is false are legitimately childless. A missing
                       column or a null value still needs a child, so the
                       constraint never weakens silently. Only valid with
                       required_child=True.
        **field_kwargs: All other Field parameters (ge, le, default, etc.)

    Returns:
        Field instance with schema metadata attached

    Example:
        >>> # Unique primary key
        >>> person_id: int = schema_field(ge=1, unique=True)

        >>> # Foreign key with required child constraint
        >>> hh_id: int = schema_field(
        ...     ge=1,
        ...     fk_to="households.hh_id",
        ...     required_child=True,
        ... )

        >>> # Self-referential foreign key
        >>> parent_tour_id: int | None = schema_field(
        ...     ge=1,
        ...     fk_to="tours.tour_id",
        ...     default=None,
        ... )
    """
    if "json_schema_extra" not in field_kwargs:
        field_kwargs["json_schema_extra"] = {}

    if unique:
        field_kwargs["json_schema_extra"]["unique"] = True

    if fk_to:
        field_kwargs["json_schema_extra"]["fk_to"] = fk_to

    if required_child:
        if not fk_to:
            msg = "required_child=True requires fk_to to be specified"
            raise ValueError(msg)
        field_kwargs["json_schema_extra"]["required_child"] = True

    if required_child_when:
        if not required_child:
            msg = "required_child_when requires required_child=True"
            raise ValueError(msg)
        field_kwargs["json_schema_extra"]["required_child_when"] = required_child_when

    return Field(**field_kwargs)
