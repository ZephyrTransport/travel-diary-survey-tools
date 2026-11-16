"""KNN-based imputation for missing values."""

from typing import Any, Literal

import polars as pl
from sklearn.impute import KNNImputer

from .impute_utils import (
    build_feature_matrix,
    count_missing,
    decode_target,
    encode_integer_categoricals,
)


def impute_knn(
    df: pl.DataFrame,
    column: str,
    n_neighbors: int = 5,
    neighbor_weights: Literal["uniform", "distance"] = "distance",
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in a single column using K-Nearest Neighbors.

    **Best for:** single columns with isolated missing values where similar
    records exist in the dataset.

    How it works:

    1. Build a feature matrix from ``numeric_features`` (used as-is) and
       ``categorical_features`` (one-hot encoded for distance calculation).
    2. Non-contiguous integer codes (e.g. enum values 1, 2, 3, 995, 999) are
       automatically encoded to dense 0..N codes so they don't distort
       distance calculations, then decoded back after imputation.
    3. For each row with a missing value, find the *K* most similar
       records based on Euclidean distance across all features.
    4. Impute the missing value using the weighted average (or mode for
       categoricals) of the *K* neighbours.

    ``neighbor_weights='distance'`` weights closer neighbours more heavily;
    ``neighbor_weights='uniform'`` treats all *K* neighbours equally.

    Example use cases:

    * Missing trip mode when other trip attributes are known.
    * Missing person age when household/demographic info is available.
    * Missing trip distance when other spatial/temporal features exist.

    Performance: O(n log n) complexity; scales well to medium-large datasets.

    Args:
        df: DataFrame containing the column to impute.
        column: Name of the column to impute.
        n_neighbors: Number of similar records to use (default: 5).
        neighbor_weights: ``'distance'`` or ``'uniform'``
            (default: ``'distance'``).
        numeric_features: Numeric/continuous feature columns.  Used as-is.
        categorical_features: Categorical feature columns.  One-hot encoded
            into binary columns for distance calculation.

    Returns:
        Tuple of (imputed DataFrame, stats dict).  The stats dict contains
        ``n_missing``, ``n_imputed``, and ``pct_imputed``.
    """
    n_missing, pct_imputed, skip_stats = count_missing(
        df, column, numeric_features, categorical_features
    )
    if skip_stats is not None:
        return df, skip_stats

    # Encode non-contiguous integer codes to dense 0..N for distance calc
    df_work, int_encodings = encode_integer_categoricals(df, [column])

    # Build feature matrix (shared helper)
    feature_matrix, column_indices, _feature_names = build_feature_matrix(
        df_work, [column], numeric_features or [], categorical_features or []
    )
    target_idx = column_indices[column]

    # Run KNN imputation
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=neighbor_weights)
    imputed_values = imputer.fit_transform(feature_matrix)[:, target_idx]

    imputed_series = decode_target(imputed_values, column, df[column].dtype, int_encodings)
    return df.with_columns(imputed_series), {
        "n_missing": n_missing,
        "n_imputed": n_missing,
        "pct_imputed": pct_imputed,
    }
