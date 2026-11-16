"""Head-to-head comparison of imputation methods via k-fold cross-validation.

For each imputed column, every supported method (KNN, RF, MICE) is evaluated
using the same k-fold splits and the same feature set.  The result is a
summary DataFrame that makes it easy to pick the best method for each field.
"""

import logging
from typing import Any

import polars as pl

from .impute_utils import (
    METHOD_DEFAULTS,
    enrich_dataframe,
    metric_row,
    missing_labels,
    prepare_column_for_imputation,
)
from .validation import log_validation_results, validate_method

logger = logging.getLogger(__name__)


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
            columns = config.get("columns", []) if method == "mice" else [config["column"]]

            for col in columns:
                key = (table_name, col)
                if key in seen:
                    continue
                seen.add(key)

                entries.append(
                    {
                        "table": table_name,
                        "column": col,
                        "missing_values": missing_labels(config.get("missing_values", []), col),
                        "numeric_features": config.get("numeric_features"),
                        "categorical_features": config.get("categorical_features"),
                        "join_tables": config.get("join_tables", []),
                        "aggregate_from": config.get("aggregate_from"),
                    }
                )

    return entries


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
    MICE are each evaluated with their default hyperparameters, using the same
    enrichment and feature set.  This produces a comparison table that helps
    choose the best method per field.

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
    rows: list[dict[str, Any]] = []

    for entry in _flatten_configs(impute_columns):
        table_name = entry["table"]
        column = entry["column"]

        if table_name not in tables:
            logger.warning("Comparison: table '%s' not in provided tables, skipping", table_name)
            continue

        df = tables[table_name].clone()
        if entry["missing_values"]:
            df, _ = prepare_column_for_imputation(df, table_name, column, entry["missing_values"])

        # Same enrichment as the imputation itself
        df, _, numeric_features = enrich_dataframe(
            df,
            table_name,
            tables,
            entry["join_tables"],
            [column],
            entry["aggregate_from"],
            entry["numeric_features"],
        )

        for method, params in METHOD_DEFAULTS.items():
            logger.info("Comparing %s for %s.%s", method.upper(), table_name, column)

            metrics = validate_method(
                method,
                df,
                [column],
                n_folds,
                sample_pct,
                params,
                random_state,
                numeric_features,
                entry["categorical_features"],
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

            rows.append(metric_row(table_name, column, method, metrics))
            log_validation_results(metrics)

    if not rows:
        logger.warning("No comparison results produced")
        return pl.DataFrame()

    summary = pl.DataFrame(rows)

    if output_path:
        summary.write_csv(output_path)
        logger.info("Method comparison saved to %s", output_path)
    else:
        logger.info("Method comparison results:\n%s", summary)

    return summary
