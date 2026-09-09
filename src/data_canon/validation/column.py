"""Column-level validation functions for canonical survey data.

Two kinds of column are checked here. A **declared** field carries its
constraints in the model, and pydantic enforces them row by row. A **generated**
column cannot: its name comes from configuration, so there is no field to hang
them on. Without something here a generated column is delivered unchecked -- the
per-profile weight columns would accept a negative weight in silence.

So a generated column may declare the same constraints a field would, and they
are checked over the column rather than per row.
"""

from dataclasses import dataclass

import polars as pl
from pydantic import BaseModel

from data_canon.core.exceptions import DataValidationError


@dataclass(frozen=True)
class GeneratedColumn:
    """What a step promises about a column it named from configuration.

    Attributes:
        description: What the column means, for the delivered codebook. A
            generated column has nowhere else to say it.
        ge: Lower bound, inclusive, if the values are numeric and bounded. The
            equivalent of ``schema_field(ge=...)`` on a declared field.
    """

    description: str = ""
    ge: float | None = None


def check_generated_constraints(
    table_name: str,
    df: pl.DataFrame,
    specs: dict[str, GeneratedColumn],
) -> None:
    """Check the constraints generated columns declared, or raise.

    Only columns actually present are checked: a step registers what it *can*
    produce, and a run that produced fewer is not thereby invalid.

    Args:
        table_name: Name of the table being validated.
        df: DataFrame to validate.
        specs: Column name -> what the generating step promised.

    Raises:
        DataValidationError: If a column violates its declared bound.
    """
    for column, spec in specs.items():
        if spec.ge is None or column not in df.columns:
            continue

        offending = df.filter(pl.col(column) < spec.ge)
        if offending.is_empty():
            continue

        raise DataValidationError(
            table=table_name,
            rule="generated_column_constraint",
            column=column,
            message=(
                f"{offending.height} of {df.height} values are below the declared "
                f"minimum of {spec.ge} (lowest {offending[column].min()})"
            ),
        )


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
        duplicates = non_null.group_by(col).agg(pl.len().alias("count")).filter(pl.col("count") > 1)

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
