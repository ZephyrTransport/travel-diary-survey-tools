"""Stash pre-imputation values for auditing and downstream analysis.

When ``stash_preimputed=True`` (the default), the imputation step copies the
original column value *before* any transforms (missing-value replacement,
imputation) into a companion column::

    {column}_preimputed   - original value (null, PNTA 999, MISSING 995, etc.)

Examples: ``mode_preimputed``, ``income_broad_preimputed``, ``gender_preimputed``.

This preserves the full original value so downstream consumers can distinguish
between genuinely missing data (null), "Prefer Not To Answer" (e.g. 999),
"Missing Response" (e.g. 995), or any other substantive value that was
overwritten during imputation.

To recover the old boolean "was this imputed?" flag::

    was_imputed = df[col + "_preimputed"].is_null() & df[col].is_not_null()
"""

import polars as pl


def stash_preimputed_column(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    column: str,
) -> pl.DataFrame:
    """Copy the pre-imputation value of *column* from *original_df* onto *df*.

    The stashed column is named ``{column}_preimputed`` and has the same
    dtype as the original.  It captures the value **before**
    ``prepare_column_for_imputation`` converted sentinel codes to null,
    so PNTA / MISSING codes are preserved.

    Args:
        df: DataFrame with imputed values.
        original_df: Original DataFrame before imputation.
        column: Name of the column that was imputed.

    Returns:
        DataFrame with added ``{column}_preimputed`` column.
    """
    stash_name = f"{column}_preimputed"
    return df.with_columns(original_df[column].alias(stash_name))


def stash_preimputed_columns(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    """Stash pre-imputation values for multiple columns.

    Args:
        df: DataFrame with imputed values.
        original_df: Original DataFrame before imputation.
        columns: List of column names that were imputed.

    Returns:
        DataFrame with added ``{column}_preimputed`` columns.
    """
    result_df = df
    for col in columns:
        if col in df.columns and col in original_df.columns:
            result_df = stash_preimputed_column(result_df, original_df, col)
    return result_df
