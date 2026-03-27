"""MICE-based imputation for missing values."""

import logging
from typing import Any

import polars as pl
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer

from .impute_utils import (
    build_feature_matrix,
    decode_dense_to_integer,
    decode_integer_to_string,
    encode_integer_categoricals,
    encode_string_columns,
    validate_features_exist,
)

logger = logging.getLogger(__name__)


def impute_mice(
    df: pl.DataFrame,
    columns: list[str],
    max_iter: int = 10,
    random_state: int | None = None,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
    verbose: bool = True,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in multiple correlated columns using MICE.

    **Best for:** multiple correlated columns with missing values (e.g.
    depart_hour / arrive_hour / duration, or race / ethnicity).

    MICE (Multiple Imputation by Chained Equations) imputes several variables
    together, preserving their joint distribution.

    How it works:

    1. Initialise missing values with simple imputation (mean/mode).
    2. For each column with missing values:

       a. Treat it as the target variable.
       b. Use the other columns as predictors in a regression model.
       c. Predict and update missing values.

    3. Repeat iteratively until convergence (``max_iter`` rounds).

    Categorical integer columns (e.g. enum codes 1-6) are automatically
    encoded to dense 0..N codes before imputation and decoded back
    afterwards.  String columns are auto-encoded to integers for the
    MICE model and decoded to original labels after imputation.

    Assumes **Missing At Random (MAR)**: missingness may depend on observed
    values but not on the missing value itself.  If data is Missing Not At
    Random (MNAR), results may be biased.

    Example use cases:

    * Time fields (depart_hour, arrive_hour, duration) — highly correlated.
    * Spatial coordinates (origin_lat, origin_lon) — spatially correlated.
    * Socio-demographic variables (income, education, employment) — often
      correlated.

    Performance: iterative, can be slow for many columns or large datasets.

    Args:
        df: DataFrame containing the columns to impute.
        columns: Column names to impute together.
        max_iter: Maximum number of imputation rounds (default: 10).
        random_state: Random seed for reproducibility.
        numeric_features: Numeric/continuous feature columns.
        categorical_features: Categorical feature columns (one-hot encoded).
        verbose: Whether to log progress during imputation.

    Returns:
        Tuple of (imputed DataFrame, stats dict).  The stats dict is keyed
        by column name, each containing ``n_missing``, ``n_imputed``, and
        ``pct_imputed``.
    """
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        msg = f"Columns not found in DataFrame: {missing_cols}"
        raise ValueError(msg)

    validate_features_exist(df, numeric_features, categorical_features)

    stats = {
        col: {"n_missing": df[col].null_count(), "n_imputed": 0, "pct_imputed": 0.0}
        for col in columns
    }
    total_missing = sum(s["n_missing"] for s in stats.values())

    if total_missing == 0:
        logger.info("Columns %s: No missing values, skipping imputation", columns)
        return df, stats

    # Encode string target columns to integers for MICE
    df_work, encodings = encode_string_columns(df, columns, verbose=verbose)

    # Encode non-contiguous integer categoricals to dense 0..N codes
    df_work, int_encodings = encode_integer_categoricals(df_work, columns, verbose=verbose)

    # Build feature matrix (shared helper)
    feature_matrix, column_indices = build_feature_matrix(
        df_work, columns, numeric_features or [], categorical_features or []
    )

    if not column_indices:
        logger.warning(
            "Columns %s: No target columns in feature matrix. "
            "Ensure target columns are numeric (or string columns will be auto-encoded).",
            columns,
        )
        return df, stats

    imputer = IterativeImputer(
        max_iter=max_iter, random_state=random_state, verbose=2 if verbose else 0
    )
    imputed_matrix = imputer.fit_transform(feature_matrix)

    # Update DataFrame with imputed values
    df_imputed = df
    for col, col_idx in column_indices.items():
        if verbose:
            logger.info("Imputing column '%s' with MICE (max_iter=%d)", col, max_iter)
        imputed_values = imputed_matrix[:, col_idx]

        if col in encodings:
            decoded = decode_integer_to_string(imputed_values, encodings[col])
            imputed_series = pl.Series(col, decoded)
            original_series = df[col]
            imputed_series = (
                pl.when(original_series.is_null())
                .then(imputed_series)
                .otherwise(original_series)
                .alias(col)
            )
        elif col in int_encodings:
            decoded = decode_dense_to_integer(imputed_values, int_encodings[col])
            imputed_series = pl.Series(col, decoded).cast(df[col].dtype)
            original_series = df[col]
            imputed_series = (
                pl.when(original_series.is_null())
                .then(imputed_series)
                .otherwise(original_series)
                .alias(col)
            )
        else:
            original_dtype = df[col].dtype
            imputed_series = pl.Series(col, imputed_values)
            if original_dtype.is_integer():
                imputed_series = imputed_series.round().cast(original_dtype)

        df_imputed = df_imputed.with_columns(imputed_series)

        n_missing = stats[col]["n_missing"]
        stats[col]["n_imputed"] = n_missing
        stats[col]["pct_imputed"] = (n_missing / len(df)) * 100

    return df_imputed, stats
