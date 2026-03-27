"""Head-to-head comparison of imputation methods via k-fold cross-validation.

For each imputed column, every supported method (KNN, RF, MICE) is evaluated
using the same k-fold splits and the same feature set.  The result is a
summary DataFrame that makes it easy to pick the best method for each field.
"""

import logging
from typing import Any

import polars as pl

from .impute_utils import enrich_dataframe, prepare_column_for_imputation
from .validation import (
    log_validation_results,
    validate_knn_imputation,
    validate_mice_imputation,
    validate_rf_imputation,
)

logger = logging.getLogger(__name__)

# Methods to benchmark and their default hyper-parameters
_COMPARISON_METHODS: dict[str, dict[str, Any]] = {
    "knn": {"n_neighbors": 5, "neighbor_weights": "distance"},
    "rf": {"n_estimators": 100, "max_depth": None},
    "mice": {"max_iter": 10},
}


def _flatten_configs(
    impute_columns: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Expand every config block into one entry per (table, column).

    MICE ``columns: [a, b]`` blocks are split into two entries so that each
    method can be compared on exactly one column at a time.  Features,
    join_tables, aggregate_from, and missing_values are preserved.
    """
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for table_name, configs in impute_columns.items():
        for config in configs:
            method = config.get("method", "knn")

            # Determine the target column(s)
            columns = config.get("columns", []) if method == "mice" else [config["column"]]

            for col in columns:
                key = (table_name, col)
                if key in seen:
                    continue
                seen.add(key)

                # Build missing_values for this single column
                raw_mv = config.get("missing_values", [])
                col_mv = raw_mv.get(col, []) if isinstance(raw_mv, dict) else raw_mv

                entries.append(
                    {
                        "table": table_name,
                        "column": col,
                        "missing_values": col_mv,
                        "numeric_features": config.get("numeric_features"),
                        "categorical_features": config.get("categorical_features"),
                        "join_tables": config.get("join_tables", []),
                        "aggregate_from": config.get("aggregate_from"),
                    }
                )

    return entries


def _validate_one_method(
    prepared: pl.DataFrame,
    column: str,
    method: str,
    n_folds: int,
    sample_pct: float,
    random_state: int | None,
    numeric_features: list[str] | None,
    categorical_features: list[str] | None,
) -> dict[str, Any]:
    """Run k-fold validation for a single (column, method) pair."""
    defaults = _COMPARISON_METHODS[method]

    if method == "knn":
        return validate_knn_imputation(
            prepared,
            column,
            n_folds,
            sample_pct,
            defaults["n_neighbors"],
            defaults["neighbor_weights"],
            random_state,
            numeric_features,
            categorical_features,
        )
    if method == "rf":
        return validate_rf_imputation(
            prepared,
            column,
            n_folds,
            sample_pct,
            defaults["n_estimators"],
            defaults["max_depth"],
            random_state,
            numeric_features,
            categorical_features,
        )
    # mice — single-column validation
    return validate_mice_imputation(
        prepared,
        [column],
        n_folds,
        sample_pct,
        defaults["max_iter"],
        random_state,
        numeric_features,
        categorical_features,
    )


def compare_imputation_methods(
    impute_columns: dict[str, list[dict[str, Any]]],
    tables: dict[str, pl.DataFrame],
    n_folds: int = 5,
    sample_pct: float = 5.0,
    random_state: int | None = None,
    output_path: str | None = None,
) -> pl.DataFrame:
    """Run k-fold validation for every method on every imputed column.

    For each unique (table, column) found in *impute_columns*, KNN, RF, and
    MICE are each evaluated using the same enrichment and feature set.  This
    produces a comparison table that helps choose the best method per field.

    Args:
        impute_columns: The same config dict passed to ``imputation()``.
        tables: Dict of canonical DataFrames (already cleaned).
        n_folds: Number of cross-validation folds.
        sample_pct: Percentage of non-missing values to test (0-100).
        random_state: Random seed for reproducibility.
        output_path: Optional path to save the comparison CSV.

    Returns:
        Polars DataFrame with columns: table, variable, method, type,
        n_samples, n_folds, accuracy, precision, recall, f1, rmse, mae, r2.
    """
    entries = _flatten_configs(impute_columns)
    rows: list[dict[str, Any]] = []

    for entry in entries:
        table_name = entry["table"]
        column = entry["column"]

        if table_name not in tables:
            logger.warning("Comparison: table '%s' not in provided tables, skipping", table_name)
            continue

        df = tables[table_name].clone()

        # Prepare missing values (replace enum labels with null)
        mv = entry["missing_values"]
        if mv:
            df, _ = prepare_column_for_imputation(df, table_name, column, mv)

        # Enrich with joins / aggregations (same as imputation pipeline)
        categorical_features = entry["categorical_features"]
        df, _, categorical_features = enrich_dataframe(
            df,
            table_name,
            tables,
            entry["join_tables"],
            [column],
            categorical_features,
            entry["aggregate_from"],
        )

        numeric_features = entry["numeric_features"]

        for method in _COMPARISON_METHODS:
            logger.info(
                "Comparing %s for %s.%s",
                method.upper(),
                table_name,
                column,
            )

            metrics = _validate_one_method(
                df,
                column,
                method,
                n_folds,
                sample_pct,
                random_state,
                numeric_features,
                categorical_features,
            )

            # MICE returns {col: {...}} — normalise to flat dict
            if column in metrics and isinstance(metrics[column], dict):
                metrics = metrics[column]

            if "error" in metrics:
                logger.warning(
                    "  %s/%s.%s: %s",
                    method.upper(),
                    table_name,
                    column,
                    metrics["error"],
                )
                continue

            row: dict[str, Any] = {
                "table": table_name,
                "variable": column,
                "method": method,
                "type": metrics.get("type", "unknown"),
                "n_samples": metrics.get("n_samples", 0),
                "n_folds": metrics.get("n_folds", 0),
            }
            for key in ("accuracy", "precision", "recall", "f1", "rmse", "mae", "r2"):
                row[key] = metrics.get(key)
            rows.append(row)

            log_validation_results(metrics)

    if not rows:
        logger.warning("No comparison results produced")
        return pl.DataFrame()

    summary = pl.DataFrame(rows)
    col_order = [
        c
        for c in [
            "table",
            "variable",
            "method",
            "type",
            "n_samples",
            "n_folds",
            "accuracy",
            "precision",
            "recall",
            "f1",
            "rmse",
            "mae",
            "r2",
        ]
        if c in summary.columns
    ]
    summary = summary.select(col_order)

    if output_path:
        summary.write_csv(output_path)
        logger.info("Method comparison saved to %s", output_path)
    else:
        logger.info("Method comparison results:\n%s", summary)

    return summary
