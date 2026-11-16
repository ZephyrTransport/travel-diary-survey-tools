"""Internal helpers for the imputation pipeline step.

The public entry point lives in :mod:`processing.imputation.impute`.
This module provides the shared scaffolding used by all three methods
(KNN, Random Forest, MICE): config grouping, per-config dispatch, and
k-fold validation.
"""

import logging
from typing import Any, Literal

import polars as pl

from processing.imputation.knn import impute_knn
from processing.imputation.mice import impute_mice
from processing.imputation.random_forest import impute_random_forest
from processing.imputation.validation import log_validation_results, validate_method

from .impute_utils import (
    enrich_dataframe,
    log_imputation_stats,
    method_params,
    metric_row,
    missing_labels,
    prepare_column_for_imputation,
)

logger = logging.getLogger(__name__)

# Fixed execution order: KNN --> RF --> MICE, so later phases can use earlier results
_METHOD_ORDER: list[Literal["knn", "rf", "mice"]] = ["knn", "rf", "mice"]


def _prepare_missing(
    df: pl.DataFrame,
    table_name: str,
    target_columns: list[str],
    missing_values_config: dict | list,
) -> pl.DataFrame:
    """Replace each target column's missing-value labels with null."""
    for column in target_columns:
        labels = missing_labels(missing_values_config, column)
        if labels:
            df, _ = prepare_column_for_imputation(df, table_name, column, labels)
    return df


def _impute(
    df: pl.DataFrame,
    target_columns: list[str],
    method: Literal["knn", "mice", "rf"],
    params: dict[str, Any],
    random_state: int | None,
    numeric_features: list[str] | None,
    categorical_features: list[str] | None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Run one method on *df* with its hyperparameters *params*."""
    if method == "knn":
        return impute_knn(
            df,
            target_columns[0],
            params["n_neighbors"],
            params["neighbor_weights"],
            numeric_features,
            categorical_features,
        )
    if method == "rf":
        return impute_random_forest(
            df,
            target_columns[0],
            params["n_estimators"],
            params["max_depth"],
            random_state,
            numeric_features,
            categorical_features,
        )
    return impute_mice(
        df,
        target_columns,
        params["max_iter"],
        random_state,
        numeric_features,
        categorical_features,
    )


def _impute_config(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    table_name: str,
    config: dict[str, Any],
    method: Literal["knn", "mice", "rf"],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
    tables: dict[str, pl.DataFrame] | None,
) -> tuple[pl.DataFrame, list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    """Impute one config block: prepare missing, enrich, impute, strip, validate.

    Missing labels are replaced before the enrichment, as validation does, so
    household counts skip missing answers. Per-method validation is skipped
    when ``compare_methods`` is on, because the comparison benchmarks every
    method.

    Returns:
        Tuple of (imputed_df, imputed column names, validation metric rows,
        feature importance rows)
    """
    # KNN/RF name one 'column', MICE names 'columns'
    target_columns = config["columns"] if method == "mice" else [config["column"]]
    numeric_features = config.get("numeric_features")
    categorical_features = config.get("categorical_features")
    missing_values_config = config.get("missing_values", {} if method == "mice" else [])

    if not numeric_features and not categorical_features:
        label = f"Columns {target_columns}" if method == "mice" else f"Column '{target_columns[0]}'"
        msg = f"{label}: At least one of numeric_features or categorical_features required"
        raise ValueError(msg)

    df = _prepare_missing(df, table_name, target_columns, missing_values_config)
    df, added_columns, numeric_features = enrich_dataframe(
        df,
        table_name,
        tables,
        config.get("join_tables", []),
        target_columns,
        config.get("aggregate_from"),
        numeric_features,
    )

    df, stats = _impute(
        df,
        target_columns,
        method,
        method_params(config, method),
        random_state,
        numeric_features,
        categorical_features,
    )
    log_imputation_stats(method.upper(), target_columns, stats, len(df))

    importance_rows: list[dict[str, Any]] = []
    fi = stats.get("feature_importance") if isinstance(stats, dict) else None
    if fi and method == "rf":
        importance_rows = [
            {
                "table": table_name,
                "column": target_columns[0],
                "feature": feature,
                "importance": importance,
                "rank": rank,
            }
            for rank, (feature, importance) in enumerate(fi.items(), 1)
        ]

    # Strip the columns joined or aggregated on as features
    df = df.drop(added_columns, strict=False)

    validation_rows: list[dict[str, Any]] = []
    if (
        validate_imputation
        and validate_imputation.get("enabled", False)
        and not validate_imputation.get("compare_methods", False)
    ):
        validation_rows = _validate_config(
            original_df,
            table_name,
            target_columns,
            config,
            method,
            missing_values_config,
            validate_imputation,
            random_state,
            tables,
        )

    return df, target_columns, validation_rows, importance_rows


def _process_imputation(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    table_name: str,
    configs: list[dict[str, Any]],
    method: Literal["knn", "mice", "rf"],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
    tables: dict[str, pl.DataFrame] | None = None,
) -> tuple[pl.DataFrame, list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    """Process imputation for a table using the specified method.

    Shared scaffold, per config block: prepare missing, enrich, impute, strip,
    validate. The *method* parameter selects the imputation strategy (KNN, RF,
    or MICE).

    Args:
        df: Current DataFrame to impute
        original_df: Original DataFrame (for validation)
        table_name: Name of the table
        configs: List of column/group configurations
        method: Imputation method - ``"knn"``, ``"rf"``, or ``"mice"``
        validate_imputation: Optional validation config
        random_state: Random seed for reproducibility
        tables: Dict of all canonical tables (for cross-table joins)

    Returns:
        Tuple of (imputed_df, list of imputed column names, list of validation
        metric dicts, list of feature importance dicts)
    """
    imputed_columns: list[str] = []
    validation_results: list[dict[str, Any]] = []
    importance_rows: list[dict[str, Any]] = []

    for config in configs:
        df, columns, val_rows, imp_rows = _impute_config(
            df, original_df, table_name, config, method, validate_imputation, random_state, tables
        )
        imputed_columns.extend(columns)
        validation_results.extend(val_rows)
        importance_rows.extend(imp_rows)

    return df, imputed_columns, validation_results, importance_rows


def _validate_config(
    original_df: pl.DataFrame,
    table_name: str,
    target_columns: list[str],
    config: dict[str, Any],
    method: Literal["knn", "mice", "rf"],
    missing_values_config: dict | list,
    validate_imputation: dict[str, Any],
    random_state: int | None,
    tables: dict[str, pl.DataFrame] | None,
) -> list[dict[str, Any]]:
    """Run k-fold cross-validation for a single imputation config block.

    The original table is prepared and enriched as for imputation, starting
    from the configured features.

    Returns:
        List of per-column metric dicts, each containing ``table``, ``variable``,
        ``method``, and the computed metrics (accuracy/precision/recall/f1 for
        categoricals, rmse/mae/r2 for continuous).
    """
    prepared = _prepare_missing(
        original_df.clone(), table_name, target_columns, missing_values_config
    )
    prepared, _, val_num_features = enrich_dataframe(
        prepared,
        table_name,
        tables,
        config.get("join_tables", []),
        target_columns,
        config.get("aggregate_from"),
        config.get("numeric_features"),
    )

    shown = target_columns if method == "mice" else target_columns[0]
    logger.info("Validating %s imputation for %s.%s", method.upper(), table_name, shown)
    metrics = validate_method(
        method,
        prepared,
        target_columns,
        validate_imputation.get("n_folds", 5),
        validate_imputation.get("sample_pct", 5.0),
        method_params(config, method),
        random_state,
        val_num_features,
        config.get("categorical_features"),
    )

    log_validation_results(metrics)

    # Normalise single-column metrics to per-column dict
    per_col = {str(metrics["column"]): metrics} if "column" in metrics else metrics
    return [
        metric_row(table_name, col, method, m) for col, m in per_col.items() if "error" not in m
    ]


def _group_configs_by_method(
    impute_columns: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Group per-table configs by imputation method.

    Returns a ``{method: {table: [configs]}}`` structure ordered by
    ``_METHOD_ORDER``.
    """
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = {m: {} for m in _METHOD_ORDER}

    for table_name, configs in impute_columns.items():
        for config in configs:
            method = config.get("method")
            if method not in _METHOD_ORDER:
                msg = (
                    f"Invalid imputation method '{method}' for table '{table_name}'. "
                    f"Must be one of {_METHOD_ORDER}."
                )
                raise ValueError(msg)
            grouped[method].setdefault(table_name, []).append(config)

    return grouped
