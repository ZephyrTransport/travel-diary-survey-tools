"""Row-level validation framework for canonical survey data.

This module provides step-aware row validation using Pydantic models.
Step-aware validation allows fields to be required only in specific pipeline
steps, enabling progressive data refinement throughout the pipeline.

Note: ValidationError class is defined in data_canon.core.dataclass to avoid
circular imports.
"""

import logging
import time
from typing import Any, cast

import polars as pl
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from data_canon.core.exceptions import DataValidationError
from data_canon.core.validators import (
    get_required_fields_for_step,
)

logger = logging.getLogger(__name__)


# DataFrame-Level Validation -----------------------------------------------
# Note: Row-level validation functions imported from data_canon.core.validators


def validate_row_for_step(
    row_dict: dict[str, Any],
    model: type[BaseModel],
    step_name: str | None = None,
) -> None:
    """Validate a single row for a specific pipeline step.

    This function validates that all fields required for the given step
    are present and valid. Fields not required for this step are still
    type-checked if present, but are not required.

    Args:
        row_dict: Dictionary representing a single row
        model: Pydantic model class to validate against
        step_name: Name of the pipeline step. If None, validates all fields.

    Raises:
        PydanticValidationError: If validation fails
        ValueError: If required fields are missing
    """
    if step_name is None:
        # No step specified - validate all fields strictly
        model(**row_dict)
        return

    # Get fields required for this step
    required_fields = get_required_fields_for_step(model, step_name)

    # Check for missing required fields
    missing_fields = [field_name for field_name in required_fields if field_name not in row_dict]

    if missing_fields:
        msg = f"Missing required fields for step '{step_name}': {', '.join(missing_fields)}"
        raise ValueError(msg)

    # Build dict with only non-None values to avoid Pydantic's
    # required field enforcement for step-conditional fields
    # Include all required fields (even if None) + all non-None fields
    filtered_row = {k: v for k, v in row_dict.items() if v is not None or k in required_fields}

    # Validate all present fields in a single pass using model_validate
    # This is much faster than validating each field individually
    try:
        model.model_validate(filtered_row, strict=False)
    except PydanticValidationError as e:
        # Only re-raise errors for fields that are actually present
        # or required for this step
        relevant_errors = [
            err
            for err in e.errors()
            if (
                err.get("loc", [None])[0] in filtered_row
                or err.get("loc", [None])[0] in required_fields
            )
        ]
        if relevant_errors:
            raise PydanticValidationError.from_exception_data(
                model.__name__,
                cast("list[Any]", relevant_errors),
            ) from e


def validate_dataframe_rows(
    table_name: str,
    df: pl.DataFrame,
    model: type[BaseModel],
    step: str | None = None,
) -> None:
    """Validate all rows in a DataFrame using step-aware validation.

    Args:
        table_name: Name of the table being validated (for error messages)
        df: DataFrame to validate
        model: Pydantic model class for row validation
        step: Pipeline step name for step-aware validation.
             If None, validates all fields strictly.

    Raises:
        DataValidationError: If any row fails validation
    """
    if len(df) == 0:
        return

    total_rows = len(df)
    start_time = time.time()
    update_interval = 2  # seconds

    # Convert entire DataFrame to list of dicts once (faster than iter_rows)
    rows = df.to_dicts()

    # Batch validate with progress reporting
    batch_size = 100_000
    error_groups: dict[str, list[int]] = {}
    max_unique_errors = 10

    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch = rows[batch_start:batch_end]
        current_time = time.time()

        # Validate each row in batch
        for i, row in enumerate(batch):
            row_idx = batch_start + i
            try:
                validate_row_for_step(row, model, step)
            except (PydanticValidationError, ValueError) as e:
                # Group errors by message
                msg = str(e)
                if msg not in error_groups:
                    error_groups[msg] = []
                error_groups[msg].append(row_idx)

                # Stop if we've collected enough unique error types
                if len(error_groups) >= max_unique_errors:
                    break

        # Progress updates for large datasets (time-based)
        if (total_rows > batch_size) and (current_time - start_time >= update_interval):
            percent_done = (batch_end / total_rows) * 100
            logger.info(
                "Row validation progress for '%s': %.1f%% (%s/%s rows)",
                table_name,
                percent_done,
                batch_end,
                total_rows,
            )

        # Stop early if we've collected enough unique errors
        if len(error_groups) >= max_unique_errors:
            break

        # Raise with error details
        _report_errors(error_groups, table_name)


def _report_errors(error_groups: dict[str, list[int]], table_name: str) -> None:
    """Report validation errors for a DataFrame."""
    if not error_groups:
        return

    total_error_count = sum(len(rows) for rows in error_groups.values())

    # Format error summary
    error_lines = []
    max_rows_to_show = 3

    for msg, row_indices in error_groups.items():
        num_affected = len(row_indices)

        if num_affected <= max_rows_to_show:
            rows_str = f"Row(s) {', '.join(map(str, row_indices))}"
        else:
            shown = row_indices[:max_rows_to_show]
            rows_str = f"{num_affected} rows (e.g., {', '.join(map(str, shown))})"

        error_lines.append(f"  {rows_str}: {msg}")

    error_summary = "\n".join(error_lines)
    num_unique = len(error_groups)

    summary_msg = (
        f"Found {num_unique} unique error type"
        f"{'s' if num_unique > 1 else ''} "
        f"affecting {total_error_count} row"
        f"{'s' if total_error_count > 1 else ''}:\n"
        f"{error_summary}"
    )

    raise DataValidationError(
        table=table_name,
        rule="row_validation",
        message=summary_msg,
    )
