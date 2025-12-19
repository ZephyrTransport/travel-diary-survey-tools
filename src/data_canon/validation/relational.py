"""Relational validation functions for canonical survey data.

This module provides validation for foreign key constraints and
bidirectional relationships (required children).
"""

import logging

import polars as pl
from pydantic import BaseModel

from data_canon.core.exceptions import DataValidationError
from data_canon.validation.column import get_unique_fields

logger = logging.getLogger(__name__)


def get_foreign_key_fields(
    model: type[BaseModel],
) -> dict[str, tuple[str, str]]:
    """Extract foreign key relationships from model metadata.

    Args:
        model: Pydantic model class

    Returns:
        Dict mapping child field name to (parent_table, parent_column)
        Example: {"hh_id": ("households", "hh_id")}
    """
    fk_fields = {}

    for field_name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra or {}
        fk_to = extra.get("fk_to")

        if fk_to:
            # Parse "parent_table.parent_column" format
            if "." not in fk_to:
                msg = (
                    f"Invalid fk_to format: '{fk_to}'. Expected 'table.column'"
                )
                raise ValueError(msg)

            parent_table, parent_column = fk_to.split(".", 1)
            fk_fields[field_name] = (parent_table, parent_column)

    return fk_fields


def get_required_children_fields(
    model: type[BaseModel],
) -> dict[str, tuple[str, str]]:
    """Extract required_child FK relationships from model metadata.

    Args:
        model: Pydantic model class

    Returns:
        Dict mapping child field name to (parent_table, parent_column)
        for fields that require bidirectional FK constraint
    """
    required_children = {}

    for field_name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra or {}

        if extra.get("required_child", False):
            fk_to = extra.get("fk_to")
            if not fk_to:
                msg = (
                    f"Field '{field_name}' has required_child=True "
                    f"but no fk_to specified"
                )
                raise ValueError(msg)

            parent_table, parent_column = fk_to.split(".", 1)
            required_children[field_name] = (parent_table, parent_column)

    return required_children


def validate_fk_references(
    models: dict[str, type[BaseModel]],
) -> None:
    """Validate that all FK references point to unique fields.

    Args:
        models: Dict mapping table names to Pydantic model classes

    Raises:
        ValueError: If FK references non-unique field or missing table/field
    """
    # Build map of unique fields by table
    unique_fields_by_table = {}
    for table_name, model in models.items():
        unique_fields_by_table[table_name] = set(get_unique_fields(model))

    # Validate all FK references
    for table_name, model in models.items():
        fk_fields = get_foreign_key_fields(model)

        for field_name, (parent_table, parent_column) in fk_fields.items():
            # Check parent table exists - if not, skip validation
            # (table may be created by a later pipeline step)
            if parent_table not in models:
                logger.debug(
                    "Skipping FK validation for %s.%s -> %s.%s "
                    "(parent table not in models yet)",
                    table_name,
                    field_name,
                    parent_table,
                    parent_column,
                )
                continue

            # Check parent column exists in parent model
            parent_model = models[parent_table]
            if parent_column not in parent_model.model_fields:
                msg = (
                    f"FK {table_name}.{field_name} references "
                    f"'{parent_table}.{parent_column}' but column "
                    f"'{parent_column}' does not exist in {parent_table}"
                )
                raise ValueError(msg)

            # Check parent column is unique
            if parent_column not in unique_fields_by_table[parent_table]:
                msg = (
                    f"FK {table_name}.{field_name} references "
                    f"'{parent_table}.{parent_column}' but column "
                    f"'{parent_column}' is not marked as unique. "
                    f"Add unique=True to {parent_table}.{parent_column}"
                )
                raise ValueError(msg)


def check_foreign_keys(
    table_name: str,
    df: pl.DataFrame,
    fk_fields: dict[str, tuple[str, str]],
    get_table_func: callable,
) -> None:
    """Check foreign key constraints using FK metadata from models.

    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate (child table)
        fk_fields: Dict mapping child FK field to (parent_table, parent_col)
                   from get_foreign_key_fields()
        get_table_func: Function to retrieve other tables by name

    Raises:
        DataValidationError: If foreign key constraint is violated
    """
    for child_col, (parent_table, parent_col) in fk_fields.items():
        # Skip if child column doesn't exist yet (will be added later)
        if child_col not in df.columns:
            continue

        # Get parent table
        parent_df = get_table_func(parent_table)
        if parent_df is None:
            # Skip FK validation when parent table doesn't exist
            # This can happen when:
            # 1. Tables validated in isolation during step input validation
            # 2. Parent table created by a later step in the pipeline
            # Actual FK constraint validated when tables in same
            # CanonicalData
            logger.debug(
                "Skipping FK validation for %s.%s -> %s.%s: "
                "parent table not found",
                table_name,
                child_col,
                parent_table,
                parent_col,
            )
            continue

        # Check parent column exists
        if parent_col not in parent_df.columns:
            raise DataValidationError(
                table=table_name,
                rule="foreign_key",
                column=child_col,
                message=(
                    f"Referenced column '{parent_col}' not found "
                    f"in table '{parent_table}'"
                ),
            )

        # Get non-null child values
        child_values = df.filter(pl.col(child_col).is_not_null())
        if len(child_values) == 0:
            continue

        # Get parent values as set
        parent_values = set(parent_df[parent_col].to_list())

        # Find orphaned child values
        child_set = set(child_values[child_col].to_list())
        orphaned = child_set - parent_values

        if orphaned:
            orphaned_list = sorted(orphaned)
            max_display = 10
            raise DataValidationError(
                table=table_name,
                rule="foreign_key",
                column=child_col,
                message=(
                    f"FK violation: {len(orphaned)} values in '{child_col}' "
                    f"not found in '{parent_table}.{parent_col}': "
                    f"{orphaned_list[:max_display]}"
                    f"{' ...' if len(orphaned) > max_display else ''}"
                ),
            )


__all__ = [
    "check_foreign_keys",
    "get_foreign_key_fields",
    "get_required_children_fields",
    "validate_fk_references",
]
