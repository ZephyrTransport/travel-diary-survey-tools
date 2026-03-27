"""Random Forest imputation for missing values."""

import logging
from typing import Any

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

from .impute_utils import (
    build_feature_matrix,
    decode_dense_to_integer,
    encode_integer_categoricals,
    is_categorical,
    validate_features_exist,
)

logger = logging.getLogger(__name__)


def impute_random_forest(
    df: pl.DataFrame,
    column: str,
    n_estimators: int = 100,
    max_depth: int | None = None,
    random_state: int | None = None,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Impute missing values in a single column using Random Forest.

    **Best for:** single columns with complex non-linear relationships or
    mixed feature types where KNN may struggle with decision boundaries.

    How it works:

    1. Split rows into *known* (have a value) and *missing* (need imputation).
    2. Train a Random Forest model on the known rows using all features.
    3. Automatically select ``RandomForestClassifier`` for categorical targets
       (integer / string dtypes) or ``RandomForestRegressor`` for continuous
       targets (float dtypes).
    4. Predict missing values using the trained model.
    5. NaN values in features are filled with column medians before training.

    Non-contiguous integer codes (e.g. enum values 1, 2, 3, 995, 999) are
    automatically encoded to dense 0..N codes so they don't distort the
    model, then decoded back after prediction.

    Example use cases:

    * Missing education level when employment, occupation, and age are
      available.
    * Missing income category with many mixed-type predictors.
    * Cases where KNN struggles with non-linear decision boundaries.

    Performance: trains on known values only; handles mixed types well but
    can be memory-intensive with many trees.

    Args:
        df: DataFrame containing the column to impute.
        column: Name of the column to impute.
        n_estimators: Number of trees in the forest (default: 100).
        max_depth: Maximum tree depth (default: None = unlimited).
        random_state: Random seed for reproducibility.
        numeric_features: Numeric/continuous feature columns.
        categorical_features: Categorical feature columns (one-hot encoded).

    Returns:
        Tuple of (imputed DataFrame, stats dict).  The stats dict contains
        ``n_missing``, ``n_imputed``, and ``pct_imputed``.
    """
    if column not in df.columns:
        msg = f"Column '{column}' not found in DataFrame"
        raise ValueError(msg)

    validate_features_exist(df, numeric_features, categorical_features)

    n_missing = df[column].null_count()
    n_total = len(df)
    pct_imputed = (n_missing / n_total) * 100

    if n_missing == 0:
        logger.info("Column '%s': No missing values, skipping imputation", column)
        return df, {"n_missing": 0, "n_imputed": 0, "pct_imputed": 0.0}

    if n_missing == n_total:
        logger.warning("Column '%s': All values are missing, cannot impute", column)
        return df, {"n_missing": n_missing, "n_imputed": 0, "pct_imputed": 100.0}

    original_dtype = df[column].dtype

    # Encode non-contiguous integer codes to dense 0..N
    df_work, int_encodings = encode_integer_categoricals(df, [column])

    # Build feature matrix (shared helper)
    feature_matrix, column_indices = build_feature_matrix(
        df_work, [column], numeric_features or [], categorical_features or []
    )
    target_idx = column_indices[column]

    # Split into known / missing masks
    target_values = feature_matrix[:, target_idx]
    known_mask = ~np.isnan(target_values)
    missing_mask = ~known_mask

    # Build X (all columns except target) and y (target) for training
    feature_cols = [i for i in range(feature_matrix.shape[1]) if i != target_idx]
    x_all = feature_matrix[:, feature_cols]

    # Replace NaN in features with column medians (RF can't handle NaN)
    col_medians = np.nanmedian(x_all, axis=0)
    nan_locs = np.isnan(x_all)
    x_all[nan_locs] = np.take(col_medians, np.where(nan_locs)[1])

    x_train = x_all[known_mask]
    y_train = target_values[known_mask]
    x_predict = x_all[missing_mask]

    # Choose classifier vs regressor
    use_classifier = is_categorical(df, column)
    if use_classifier:
        y_train = y_train.astype(int)
        model = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=random_state,
            n_jobs=-1,
        )
    else:
        model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=random_state,
            n_jobs=-1,
        )

    model.fit(x_train, y_train)
    predicted = model.predict(x_predict)

    # Reconstruct full column: keep originals, fill predictions
    full_values = target_values.copy()
    full_values[missing_mask] = predicted

    # Decode back to original codes
    if column in int_encodings:
        decoded = decode_dense_to_integer(full_values, int_encodings[column])
        imputed_series = pl.Series(column, decoded).cast(original_dtype)
    elif original_dtype.is_integer():
        imputed_series = pl.Series(column, full_values).round().cast(original_dtype)
    else:
        imputed_series = pl.Series(column, full_values)

    return df.with_columns(imputed_series), {
        "n_missing": n_missing,
        "n_imputed": n_missing,
        "pct_imputed": pct_imputed,
    }
