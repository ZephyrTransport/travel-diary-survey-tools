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
from processing.imputation.validation import (
    log_validation_results,
    validate_knn_imputation,
    validate_mice_imputation,
    validate_rf_imputation,
)

from .impute_utils import (
    enrich_dataframe,
    log_imputation_stats,
    prepare_column_for_imputation,
    strip_joined_columns,
)

logger = logging.getLogger(__name__)


def _process_imputation(  # noqa: C901
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

    Shared scaffold: enrich → prepare missing → impute → strip → validate.
    The *method* parameter selects the imputation strategy (KNN, RF, or MICE).

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
        # Normalise target columns: KNN/RF use 'column', MICE uses 'columns'
        target_columns = config["columns"] if method == "mice" else [config["column"]]

        # Common config extraction
        numeric_features = config.get("numeric_features")
        categorical_features = config.get("categorical_features")
        join_tables_list = config.get("join_tables", [])
        aggregate_from_config = config.get("aggregate_from")
        missing_values_config = config.get("missing_values", {} if method == "mice" else [])

        if not numeric_features and not categorical_features:
            label = (
                f"Columns {target_columns}" if method == "mice" else f"Column '{target_columns[0]}'"
            )
            msg = f"{label}: At least one of numeric_features or categorical_features required"
            raise ValueError(msg)

        # 1. Enrich (parent joins + child aggregations)
        df, added_columns, categorical_features = enrich_dataframe(
            df,
            table_name,
            tables,
            join_tables_list,
            target_columns,
            categorical_features,
            aggregate_from_config,
        )

        # 2. Prepare missing values (replace enum labels with null)
        for column in target_columns:
            labels = (
                missing_values_config.get(column, [])
                if isinstance(missing_values_config, dict)
                else missing_values_config
            )
            if labels:
                df, _ = prepare_column_for_imputation(df, table_name, column, labels)

        # 3. Impute (strategy dispatch)
        # NOTE: This is kind of hacky to hard-code, but its hard to conform the interfaces
        # Plus we may not need and infinite number of impute methods.
        if method == "knn":
            df, stats = impute_knn(
                df,
                target_columns[0],
                config.get("n_neighbors", 5),
                config.get("neighbor_weights", "distance"),
                numeric_features,
                categorical_features,
            )
        elif method == "rf":
            df, stats = impute_random_forest(
                df,
                target_columns[0],
                config.get("n_estimators", 100),
                config.get("max_depth"),
                random_state,
                numeric_features,
                categorical_features,
            )
        else:
            df, stats = impute_mice(
                df,
                target_columns,
                config.get("max_iter", 10),
                random_state,
                numeric_features,
                categorical_features,
            )

        log_imputation_stats(method.upper(), target_columns, stats, len(df))
        imputed_columns.extend(target_columns)

        # Collect feature importance (RF only)
        fi = stats.get("feature_importance") if isinstance(stats, dict) else None
        if fi and method == "rf":
            for rank, (feature, importance) in enumerate(fi.items(), 1):
                importance_rows.append(
                    {
                        "table": table_name,
                        "column": target_columns[0],
                        "feature": feature,
                        "importance": importance,
                        "rank": rank,
                    }
                )

        # 4. Strip temporary joined columns
        if added_columns:
            df = strip_joined_columns(df, added_columns)

        # 5. Optional per-method validation (skipped when compare_methods
        #    is active because the comparison already benchmarks every method)
        if (
            validate_imputation
            and validate_imputation.get("enabled", False)
            and not validate_imputation.get("compare_methods", False)
        ):
            val_metrics = _validate_config(
                original_df,
                table_name,
                target_columns,
                config,
                method,
                missing_values_config,
                validate_imputation,
                random_state,
                tables,
                categorical_features,
            )
            validation_results.extend(val_metrics)

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
    categorical_features: list[str] | None,
) -> list[dict[str, Any]]:
    """Run k-fold cross-validation for a single imputation config block.

    Returns:
        List of per-column metric dicts, each containing ``table``, ``variable``,
        ``method``, and the computed metrics (accuracy/precision/recall/f1 for
        categoricals, rmse/mae/r2 for continuous).
    """
    n_folds = validate_imputation.get("n_folds", 5)
    sample_pct = validate_imputation.get("sample_pct", 5.0)
    join_tables_list = config.get("join_tables", [])
    aggregate_from_config = config.get("aggregate_from")
    numeric_features = config.get("numeric_features")

    # Prepare the original for validation (same enrichment as imputation)
    prepared = original_df.clone()
    for column in target_columns:
        labels = (
            missing_values_config.get(column, [])
            if isinstance(missing_values_config, dict)
            else missing_values_config
        )
        if labels:
            prepared, _ = prepare_column_for_imputation(prepared, table_name, column, labels)

    prepared, _, val_cat_features = enrich_dataframe(
        prepared,
        table_name,
        tables,
        join_tables_list,
        target_columns,
        categorical_features,
        aggregate_from_config,
    )

    if method == "knn":
        logger.info("Validating KNN imputation for %s.%s", table_name, target_columns[0])
        metrics = validate_knn_imputation(
            prepared,
            target_columns[0],
            n_folds,
            sample_pct,
            config.get("n_neighbors", 5),
            config.get("neighbor_weights", "distance"),
            random_state,
            numeric_features,
            val_cat_features,
        )
    elif method == "rf":
        logger.info("Validating RF imputation for %s.%s", table_name, target_columns[0])
        metrics = validate_rf_imputation(
            prepared,
            target_columns[0],
            n_folds,
            sample_pct,
            config.get("n_estimators", 100),
            config.get("max_depth"),
            random_state,
            numeric_features,
            val_cat_features,
        )
    else:
        logger.info("Validating MICE imputation for %s.%s", table_name, target_columns)
        metrics = validate_mice_imputation(
            prepared,
            target_columns,
            n_folds,
            sample_pct,
            config.get("max_iter", 10),
            random_state,
            numeric_features,
            val_cat_features,
        )

    log_validation_results(metrics)

    # Normalise single-column metrics to per-column dict
    per_col = {str(metrics["column"]): metrics} if "column" in metrics else metrics

    results: list[dict[str, Any]] = []
    for col, m in per_col.items():
        if "error" in m:
            continue
        row: dict[str, Any] = {
            "table": table_name,
            "variable": col,
            "method": method,
            "type": m.get("type", "unknown"),
            "n_samples": m.get("n_samples", 0),
            "n_folds": m.get("n_folds", 0),
        }
        # Categorical metrics
        for key in ("accuracy", "precision", "recall", "f1"):
            row[key] = m.get(key)
        # Continuous metrics
        for key in ("rmse", "mae", "r2"):
            row[key] = m.get(key)
        results.append(row)

    return results


# Fixed execution order: KNN --> RF --> MICE, so later phases can use earlier results
_METHOD_ORDER: list[Literal["knn", "rf", "mice"]] = ["knn", "rf", "mice"]


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
