"""Column-level validation functions for canonical survey data.

This module provides validation for column constraints such as uniqueness.
"""

import polars as pl
from pydantic import BaseModel

from data_canon.core.exceptions import DataValidationError


def get_unique_fields(model: type[BaseModel]) -> list[str]:
    """Get list of fields marked as unique in the model.

    Args:
        model: Pydantic model class

    Returns:
        List of field names marked as unique
    """
    unique_fields = []

    for field_name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra or {}
        if extra.get("unique", False):
            unique_fields.append(field_name)

    return unique_fields


def check_unique_constraints(
    table_name: str,
    df: pl.DataFrame,
    unique_columns: list[str],
) -> None:
    """Check uniqueness constraints on specified columns.

    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate
        unique_columns: List of column names that must be unique

    Raises:
        DataValidationError: If uniqueness constraint is violated
    """
    for col in unique_columns:
        if col not in df.columns:
            raise DataValidationError(
                table=table_name,
                rule="unique_constraint",
                column=col,
                message=f"Column '{col}' not found in table",
            )

        # Get non-null values
        non_null = df.filter(pl.col(col).is_not_null())
        if len(non_null) == 0:
            continue

        # Check for duplicates using Polars
        duplicates = (
            non_null.group_by(col)
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
        )

        if len(duplicates) > 0:
            dup_values = duplicates[col].to_list()
            raise DataValidationError(
                table=table_name,
                rule="unique_constraint",
                column=col,
                message=(
                    f"Duplicate values found: {dup_values[:10]}"
                    f"{' ...' if len(dup_values) > 10 else ''}"  # noqa: PLR2004
                ),
            )


__all__ = [
    "check_unique_constraints",
    "get_unique_fields",
]
