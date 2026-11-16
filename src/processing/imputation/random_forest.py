"""Random Forest imputation for missing values."""

import logging
from typing import Any

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

from .impute_utils import (
    build_feature_matrix,
    count_missing,
    decode_target,
    encode_integer_categoricals,
    is_categorical,
)

logger = logging.getLogger(__name__)


def _feature_importance(feature_names: list[str], importances: np.ndarray) -> dict[str, float]:
    """Mean decrease in impurity per input feature, one-hot columns summed, descending.

    A one-hot column ``{feature}={value}`` counts towards ``feature``.
    """
    summed: dict[str, float] = {}
    for name, importance in dict(zip(feature_names, importances, strict=False)).items():
        feature = name.split("=", 1)[0]
        summed[feature] = summed.get(feature, 0.0) + importance
    return dict(sorted(summed.items(), key=lambda x: x[1], reverse=True))


def impute_random_forest(
    df: pl.DataFrame,
    column: str,
    n_estimators: int = 100,
    max_depth: int | None = None,
    random_state: int | None = None,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
    verbose: bool = True,
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
        verbose: If True, logs top features by importance.

    Returns:
        Tuple of (imputed DataFrame, stats dict).  The stats dict contains
        ``n_missing``, ``n_imputed``, ``pct_imputed``, and
        ``feature_importance`` (a ``dict[str, float]`` mapping each input
        feature to its Mean Decrease in Impurity score, sorted descending).
        See the *Feature Importance* section in ``docs/pipeline_steps/imputation.md``
        for interpretation guidance.
    """
    n_missing, pct_imputed, skip_stats = count_missing(
        df, column, numeric_features, categorical_features
    )
    if skip_stats is not None:
        return df, skip_stats

    # Encode non-contiguous integer codes to dense 0..N
    df_work, int_encodings = encode_integer_categoricals(df, [column])

    # Build feature matrix (shared helper)
    feature_matrix, column_indices, feature_names = build_feature_matrix(
        df_work, [column], numeric_features or [], categorical_features or []
    )
    target_idx = column_indices[column]

    # Split into known / missing masks
    target_values = feature_matrix[:, target_idx]
    known_mask = ~np.isnan(target_values)
    missing_mask = ~known_mask

    # Build X (all columns except target) and y (target) for training
    feature_cols = [i for i in range(feature_matrix.shape[1]) if i != target_idx]
    x_feature_names = [feature_names[i] for i in feature_cols]
    x_all = feature_matrix[:, feature_cols]

    # Replace NaN in features with column medians (RF can't handle NaN)
    col_medians = np.nanmedian(x_all, axis=0)
    nan_locs = np.isnan(x_all)
    x_all[nan_locs] = np.take(col_medians, np.where(nan_locs)[1])

    x_train = x_all[known_mask]
    y_train = target_values[known_mask]
    x_predict = x_all[missing_mask]
    # The forest samples training rows by position; sorting by value keeps it the
    # same whatever order the rows arrive in. Identical rows are interchangeable.
    order = np.lexsort(np.column_stack([x_train, y_train]).T[::-1])
    x_train, y_train = x_train[order], y_train[order]

    # Classifier for a categorical target, regressor for a continuous one
    use_classifier = is_categorical(df, column)
    if use_classifier:
        y_train = y_train.astype(int)
    forest = RandomForestClassifier if use_classifier else RandomForestRegressor
    model = forest(
        n_estimators=n_estimators, max_depth=max_depth, random_state=random_state, n_jobs=-1
    )
    model.fit(x_train, y_train)
    # Threads sum the trees' votes in finishing order, which flips near-ties
    # between runs; one thread predicts the same every time.
    model.set_params(n_jobs=1)
    predicted = model.predict(x_predict)

    feature_importance = _feature_importance(x_feature_names, model.feature_importances_)
    if feature_importance and verbose:
        top = list(feature_importance.items())[:5]
        lines = [f"  {i}. {name:<20s} {imp:.3f}" for i, (name, imp) in enumerate(top, 1)]
        logger.info(
            "Column '%s' (RF): Top features by importance\n%s",
            column,
            "\n".join(lines),
        )

    # Reconstruct full column: keep originals, fill predictions
    full_values = target_values.copy()
    full_values[missing_mask] = predicted

    imputed_series = decode_target(full_values, column, df[column].dtype, int_encodings)
    return df.with_columns(imputed_series), {
        "n_missing": n_missing,
        "n_imputed": n_missing,
        "pct_imputed": pct_imputed,
        "feature_importance": feature_importance,
    }
