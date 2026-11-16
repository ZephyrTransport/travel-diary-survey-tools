"""Row-level validation framework for canonical survey data.

This module provides step-aware row validation using Pydantic models.
Step-aware validation allows fields to be required only in specific pipeline
steps, enabling progressive data refinement throughout the pipeline.
"""

import logging
import time
from typing import Any, cast

import polars as pl
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from data_canon.core.exceptions import DataValidationError
from pipeline.step_registry import get_required_fields as get_required_fields_for_step
from pipeline.step_registry import is_config_named

logger = logging.getLogger(__name__)


def validate_row_for_step(
    row_dict: dict[str, Any],
    model: type[BaseModel],
    table_name: str,
    step_name: str | None = None,
) -> None:
    """Validate a single row for a specific pipeline step.

    This function validates that all fields required for the given step
    are present and valid. Fields not required for this step are still
    type-checked if present, but are not required: a null in one is dropped
    before validation, so the model does not demand it.

    A model-level validator reports an empty ``loc``; it is about the row as a
    whole, so its errors are always kept.

    Args:
        row_dict: Dictionary representing a single row
        model: Pydantic model class to validate against
        table_name: Canonical table name (e.g. "unlinked_trips")
        step_name: Name of the pipeline step. If None, validates all fields.

    Raises:
        PydanticValidationError: If validation fails
        ValueError: If required fields are missing
    """
    if step_name is None:
        # No step specified - validate all fields strictly
        model(**row_dict)
        return

    # Get fields required for this step; config-named patterns are the step's to check
    required_fields = {
        f for f in get_required_fields_for_step(table_name, step_name) if not is_config_named(f)
    }

    # Check for missing required fields
    missing_fields = [field_name for field_name in required_fields if field_name not in row_dict]

    if missing_fields:
        msg = f"Missing required fields for step '{step_name}': {', '.join(missing_fields)}"
        raise ValueError(msg)

    # Required fields stay even when null; other nulls are dropped
    filtered_row = {k: v for k, v in row_dict.items() if v is not None or k in required_fields}

    # Validate all present fields in a single pass using model_validate
    try:
        model.model_validate(filtered_row, strict=False)
    except PydanticValidationError as e:
        # Keep errors on present or required fields, and row-level ones
        def _is_relevant(err: dict) -> bool:
            loc = err.get("loc") or ()
            if not loc:
                return True
            return loc[0] in filtered_row or loc[0] in required_fields

        relevant_errors = [err for err in e.errors() if _is_relevant(err)]
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
    progress_every = 100_000  # rows
    max_unique_errors = 10
    error_groups: dict[str, list[int]] = {}

    # One to_dicts() is faster than iter_rows
    for row_idx, row in enumerate(df.to_dicts()):
        try:
            validate_row_for_step(row, model, table_name, step)
        except (PydanticValidationError, ValueError) as e:
            error_groups.setdefault(str(e), []).append(row_idx)
            if len(error_groups) >= max_unique_errors:
                break

        done = row_idx + 1
        if done % progress_every == 0 and time.time() - start_time >= update_interval:
            logger.info(
                "Row validation progress for '%s': %.1f%% (%s/%s rows)",
                table_name,
                done / total_rows * 100,
                done,
                total_rows,
            )

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
