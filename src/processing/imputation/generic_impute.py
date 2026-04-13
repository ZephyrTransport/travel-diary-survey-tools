"""Generic imputation step using KNN, Random Forest, and MICE methods."""

import logging
from typing import Any, Literal

import polars as pl

from pipeline.decoration import step
from processing.imputation.comparison import compare_imputation_methods
from processing.imputation.flags import create_flag_columns
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


def _process_imputation(
    df: pl.DataFrame,
    original_df: pl.DataFrame,
    table_name: str,
    configs: list[dict[str, Any]],
    method: Literal["knn", "mice", "rf"],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
    tables: dict[str, pl.DataFrame] | None = None,
) -> tuple[pl.DataFrame, list[str], list[dict[str, Any]]]:
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
        metric dicts — one per column validated)
    """
    imputed_columns: list[str] = []
    validation_results: list[dict[str, Any]] = []

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

    return df, imputed_columns, validation_results


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


@step()
def imputation(  # noqa: C901
    # Optional canonical tables
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    # Config parameters
    impute_columns: dict[str, list[dict[str, Any]]] | None = None,
    create_flags: bool = True,
    random_state: int | None = None,
    validate_imputation: dict[str, Any] | None = None,
) -> dict[str, pl.DataFrame]:
    """Impute missing values using KNN, Random Forest, and/or MICE methods.

    Each config block specifies its ``method`` (``knn``, ``rf``, or ``mice``)
    along with the method-specific parameters.  Configs are grouped by method
    and executed in a fixed order (KNN → RF → MICE) across all tables so that
    later phases can benefit from values filled in earlier phases.

    Handling Missing Values with Enum Labels:
        Survey data often uses special codes for missing values (e.g. 995 for
        "Missing Response", 999 for "Prefer not to answer").  Use **enum
        member names** (labels) rather than raw numeric values in the config:

            missing_values: [MISSING, PNTA]   # enum labels, not 995/999

        The module automatically:

        1. Maps the table name to the appropriate codebook module
           (e.g. ``households`` → ``data_canon.codebook.households``).
        2. Finds the enum class whose ``canonical_field_name`` matches the
           target column (e.g. ``income_broad`` → ``IncomeBroad``).
        3. Resolves enum member names to their values
           (e.g. ``MISSING`` → 995).
        4. Replaces those values with null before imputation.

        For MICE with multiple columns, ``missing_values`` can be a dict
        mapping each column to its own labels, or a single list applied to
        all columns:

            # Per-column
            missing_values:
              race: [MISSING]
              ethnicity: [MISSING, PNTA]

            # Shared
            missing_values: [MISSING, PNTA]   # applied to all columns

    Cross-Table Features:
        By default only features from the same table are used.  Adding
        ``join_tables`` to a config block pulls columns from parent tables
        via left-join on known foreign keys, which can significantly improve
        quality.

        Behaviour:

        1. Columns from the specified parent table(s) are joined onto the
           child table (e.g. ``persons`` ← ``households`` via ``hh_id``).
        2. For each target column a ``hh_mode_{column}`` feature is
           auto-generated — the mode of that column among *other* household
           members (exclude-self).  This captures within-household
           correlation (e.g. siblings sharing race/ethnicity).
        3. Auto-generated ``hh_mode_*`` columns are appended to
           ``categorical_features`` automatically.
        4. After imputation all joined/aggregated columns are stripped;
           the output schema is unchanged.

    Example:
            impute_columns:
              persons:
                - method: knn
                  column: gender
                  n_neighbors: 5
                  join_tables: [households]
                  categorical_features: [age, employment, income_bin, residence_type]
                  #                                       ^^^^^^^^^^  ^^^^^^^^^^^^^^
                  #                              columns from the households table

    Child-to-Parent Aggregation:
        Using the config, ``aggregate_from``, this is
        the reverse of ``join_tables``: aggregate child rows up to a parent
        table.  Useful when imputing parent-level fields that depend on
        household composition (e.g. predicting household income from the
        employment/education mix of its members).

        For each child table and each field listed under ``pivot_count``, the
        module groups child rows by the parent's FK and creates one column per
        unique value, counting occurrences.  Generated columns are named
        ``{child_table}_count_{field}_{value}`` and are automatically added to
        ``numeric_features``.  After imputation, all generated columns are
        stripped.

    Example:
            impute_columns:
              households:
                - method: mice
                  columns: [income_bin]
                  aggregate_from:
                    persons:
                      pivot_count: [employment, education, student]
                  categorical_features: [residence_type, residence_rent_own]
                  max_iter: 10

    Args:
        households: Households table (optional).
        persons: Persons table (optional).
        days: Days table (optional).
        unlinked_trips: Unlinked trips table (optional).
        linked_trips: Linked trips table (optional).
        tours: Tours table (optional).
        impute_columns: Dict mapping table names to list of imputation configs.
            Every config dict **must** include a ``method`` key (``knn``, ``rf``,
            or ``mice``).  The remaining keys are method-specific:

            **KNN** (``method: knn``):

            - column: Column name to impute.
            - missing_values: Enum labels to treat as missing.
            - n_neighbors: Number of neighbors (default: 5).
            - neighbor_weights: ``'uniform'`` or ``'distance'``
              (default: ``'distance'``).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Parent tables to left-join for extra features.
            - aggregate_from: Child-to-parent pivot-count config.

            **Random Forest** (``method: rf``):

            - column: Column name to impute.
            - missing_values: Enum labels to treat as missing.
            - n_estimators: Number of trees (default: 100).
            - max_depth: Maximum tree depth (default: None, unlimited).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Parent tables to left-join for extra features.
            - aggregate_from: Child-to-parent pivot-count config.

            **MICE** (``method: mice``):

            - columns: Column names to impute together.
            - missing_values: Dict mapping column → enum labels, or a
              single list applied to all columns.
            - max_iter: Maximum iterations (default: 10).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Parent tables to left-join for extra features.
            - aggregate_from: Child-to-parent pivot-count config.

            At least one of ``numeric_features`` or ``categorical_features``
            is required in every config block.

        create_flags: Whether to create ``{column}_imputed`` boolean flag
            columns (default: True).
        random_state: Random seed for reproducibility across all imputation.
        validate_imputation: Optional validation config with keys:

            - enabled: Whether to run validation (default: False).
            - n_folds: Number of k-folds (default: 5).
            - sample_pct: Percentage of non-missing values to test
              (default: 5.0).
            - output_path: Path to save validation or comparison CSV.
            - compare_methods: When True, run all three methods (KNN, RF,
              MICE) against every column instead of validating only the
              configured method (default: False).

    Returns:
        Dictionary of imputed tables.  When validation is enabled, an extra
        key ``_validation_summary`` contains a Polars DataFrame with columns:
        table, variable, method, type, n_samples, n_folds, accuracy,
        precision, recall, f1, rmse, mae, r2.

        When ``compare_methods`` is True, an extra key
        ``_method_comparison`` contains a Polars DataFrame comparing
        KNN, RF, and MICE for every imputed column.

    Example config:
        .. code-block:: yaml

            impute_columns:
              households:
                - method: knn
                  column: income_broad
                  missing_values: [MISSING, PNTA]
                  n_neighbors: 5
                  neighbor_weights: distance
                  numeric_features: [num_persons, num_vehicles, num_workers]
              persons:
                - method: knn
                  column: gender
                  missing_values: [MISSING]
                  n_neighbors: 5
                  join_tables: [households]
                  numeric_features: [age]
                  categorical_features: [relationship, employment, income_bin]
                - method: rf
                  column: education
                  missing_values: [MISSING]
                  n_estimators: 200
                  max_depth: 15
                  numeric_features: [age]
                  categorical_features: [employment, occupation]
                - method: mice
                  columns: [race, ethnicity]
                  missing_values:
                    race: [MISSING]
                    ethnicity: [MISSING, PNTA]
                  join_tables: [households]
                  max_iter: 10
                  numeric_features: [age]
            random_state: 42
            create_flags: true
            validate_imputation:
              enabled: true
              n_folds: 5
              sample_pct: 5.0
    """
    # Collect input tables
    tables = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "tours": tours,
    }

    # Remove None tables
    tables = {name: df for name, df in tables.items() if df is not None}

    if not tables:
        logger.warning("No tables provided for imputation")
        return {}

    logger.info("Starting imputation for tables: %s", list(tables.keys()))

    if not impute_columns:
        logger.warning("No impute_columns configs provided")
        return dict(tables.items())

    # Clone all tables and track originals for validation/flags
    originals = {name: df.clone() for name, df in tables.items()}
    current_dfs = dict(tables.items())
    all_imputed_columns: dict[str, list[str]] = {name: [] for name in tables}
    all_validation_results: list[dict[str, Any]] = []

    # Group configs by method (KNN → RF → MICE)
    method_configs = _group_configs_by_method(impute_columns)

    # Process each method phase across all tables
    for phase_num, method in enumerate(_METHOD_ORDER, 1):
        configs_by_table = method_configs[method]
        if not configs_by_table:
            continue

        logger.info("Phase %d: %s imputation", phase_num, method.upper())
        for table_name, configs in configs_by_table.items():
            if table_name not in current_dfs:
                logger.warning(
                    "Table '%s' referenced in impute_columns but not provided", table_name
                )
                continue
            logger.info("  %s imputation for %s", method.upper(), table_name)
            current_dfs[table_name], cols, val_rows = _process_imputation(
                current_dfs[table_name],
                originals[table_name],
                table_name,
                configs,
                method,
                validate_imputation,
                random_state,
                tables=current_dfs,
            )
            all_imputed_columns[table_name].extend(cols)
            all_validation_results.extend(val_rows)

    # Create flag columns for all tables
    result_tables = {}
    for table_name in tables:
        current_df = current_dfs[table_name]
        imputed_columns = all_imputed_columns[table_name]

        if create_flags and imputed_columns:
            logger.info(
                "Creating imputation flags for %s (%d columns)",
                table_name,
                len(imputed_columns),
            )
            current_df = create_flag_columns(current_df, originals[table_name], imputed_columns)

        result_tables[table_name] = current_df

    # Run either per-method validation summary or full method comparison
    if validate_imputation and validate_imputation.get("compare_methods", False):
        logger.info("Running head-to-head method comparison for all imputed columns")
        comparison_df = compare_imputation_methods(
            impute_columns,
            current_dfs,
            n_folds=validate_imputation.get("n_folds", 5),
            sample_pct=validate_imputation.get("sample_pct", 5.0),
            random_state=random_state,
            output_path=validate_imputation.get("output_path"),
        )
        result_tables["_method_comparison"] = comparison_df
    elif all_validation_results:
        summary_df = pl.DataFrame(all_validation_results)
        output_path = validate_imputation.get("output_path") if validate_imputation else None
        if output_path:
            summary_df.write_csv(output_path)
            logger.info("Validation summary saved to %s", output_path)
        result_tables["_validation_summary"] = summary_df

    logger.info("Imputation complete for %d tables", len(result_tables))
    return result_tables
