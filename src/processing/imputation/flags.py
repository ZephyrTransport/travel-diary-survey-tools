"""Diagnostic flag columns for tracking imputed values.

When ``create_flags=True`` (the default), the imputation step creates a
boolean column for every imputed field::

    {column}_imputed   - True if the value was filled in, False otherwise

Examples: ``mode_imputed``, ``distance_imputed``, ``age_imputed``.

Use cases:

* **Quality control**: identify records that contain imputed values.
* **Sensitivity analysis**: compare results with vs. without imputed records.
* **Downstream modelling**: include imputation status as a feature.
"""

import polars as pl


def create_flag_column(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    column: str,
) -> pl.DataFrame:
    """Create a boolean flag column indicating which values were imputed.

    Args:
        df: DataFrame with imputed values
        original_df: Original DataFrame before imputation
        column: Name of the column that was imputed

    Returns:
        DataFrame with added flag column named '{column}_imputed'
    """
    flag_name = f"{column}_imputed"

    # Check if column was null in original and is not null in imputed
    was_null = original_df[column].is_null()
    is_not_null = df[column].is_not_null()
    imputed_flag = was_null & is_not_null

    return df.with_columns(imputed_flag.alias(flag_name))


def create_flag_columns(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """Create boolean flag columns for multiple imputed columns.

    Args:
        df: DataFrame with imputed values
        original_df: Original DataFrame before imputation
        columns: List of column names that were imputed

    Returns:
        DataFrame with added flag columns named '{column}_imputed'
    """
    result_df = df
    for col in columns:
        if col in df.columns and col in original_df.columns:
            result_df = create_flag_column(result_df, original_df, col)
    return result_df
